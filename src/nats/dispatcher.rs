use std::{
    any::Any,
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use async_nats::{Client, ConnectOptions, Event, Message, Subscriber};
use eyre::bail;
use tokio::{
    select,
    sync::mpsc,
    task::{JoinError, JoinSet},
};

use crate::{
    handlers::{HandlerRequest, Response},
    registry::{HandlerArc, HandlerRegistry},
};

use futures::{stream::StreamExt, FutureExt};

use super::{NatsMetadata, NatsPayload, NatsResponse};

type NatsHandlerArc<S> = HandlerArc<NatsPayload, NatsMetadata, S, NatsResponse>;

/// How the dispatcher executes handler invocations for a single message type.
#[derive(Clone, Copy, Debug)]
pub enum ExecutionMode {
    /// Spawn a task per message, with at most `max_in_flight` running
    /// concurrently per handler type. Highest throughput for slow handlers,
    /// but completion order is not guaranteed to match arrival order.
    /// Panics in handlers are isolated by the task boundary.
    Concurrent { max_in_flight: usize },
    /// Process messages strictly in arrival order on a single long-lived
    /// worker task per handler type. Panics are caught per message via
    /// `catch_unwind`, so a panicking handler aborts only the message that
    /// caused it. Requires the default `panic = "unwind"` strategy.
    Ordered,
}

#[derive(Clone, Copy, Debug)]
pub struct DispatcherConfig {
    pub mode: ExecutionMode,
    /// Capacity of the in-process queue between the subscription reader and
    /// the executor, per handler type.
    ///
    /// `None` (the default) means unbounded: the reader always drains the
    /// NATS client's subscription buffer immediately, so dawnflow never
    /// causes client-side message drops. Under sustained overload (publish
    /// rate > processing rate) the backlog grows without limit and memory is
    /// the only bound — growth is reported via backlog warnings.
    ///
    /// `Some(n)` bounds memory instead: when the queue is full the reader
    /// stops pulling, the NATS client buffer (`subscription_capacity`) fills,
    /// and the client will DROP further messages (reported through the
    /// connection event log).
    pub queue_capacity: Option<usize>,
    /// Per-subscription buffer of the underlying NATS client (async-nats
    /// `subscription_capacity`, default 65536). Only relevant headroom when
    /// `queue_capacity` is bounded or the reader is CPU-starved.
    pub subscription_capacity: usize,
    /// Warn when the in-process backlog of a handler type exceeds this many
    /// messages (repeated on every doubling, recovery is logged once the
    /// backlog halves below the threshold).
    pub backlog_warn_threshold: usize,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            mode: ExecutionMode::Concurrent { max_in_flight: 10 },
            queue_capacity: None,
            subscription_capacity: 1024 * 64,
            backlog_warn_threshold: 10_000,
        }
    }
}

/// Tracks the queue depth between reader and executor so overload is visible
/// in the logs instead of silent. Counter updates are exact; the log-edge
/// detection is racy in the benign sense (worst case a duplicate log line).
struct Backlog {
    subject: String,
    depth: AtomicUsize,
    warn_threshold: usize,
    next_warn: AtomicUsize,
}

impl Backlog {
    fn new(subject: String, warn_threshold: usize) -> Arc<Self> {
        let warn_threshold = warn_threshold.max(1);
        Arc::new(Self {
            subject,
            depth: AtomicUsize::new(0),
            warn_threshold,
            next_warn: AtomicUsize::new(warn_threshold),
        })
    }

    fn on_enqueue(&self) {
        let depth = self.depth.fetch_add(1, Ordering::Relaxed) + 1;
        let next_warn = self.next_warn.load(Ordering::Relaxed);
        if depth >= next_warn {
            self.next_warn.store(next_warn * 2, Ordering::Relaxed);
            tracing::warn!(
                "Backlog for {} reached {} messages — handlers are not keeping up with the publish rate",
                self.subject,
                depth
            );
        }
    }

    fn on_dequeue(&self) {
        let depth = self.depth.fetch_sub(1, Ordering::Relaxed) - 1;
        if depth < self.warn_threshold / 2
            && self.next_warn.load(Ordering::Relaxed) > self.warn_threshold
        {
            self.next_warn.store(self.warn_threshold, Ordering::Relaxed);
            tracing::info!(
                "Backlog for {} recovered to {} messages",
                self.subject,
                depth
            );
        }
    }
}

enum QueueTx {
    Bounded(mpsc::Sender<Message>),
    Unbounded(mpsc::UnboundedSender<Message>),
}

enum QueueRx {
    Bounded(mpsc::Receiver<Message>),
    Unbounded(mpsc::UnboundedReceiver<Message>),
}

impl QueueTx {
    async fn send(&self, msg: Message) -> Result<(), ()> {
        match self {
            QueueTx::Bounded(tx) => tx.send(msg).await.map_err(|_| ()),
            QueueTx::Unbounded(tx) => tx.send(msg).map_err(|_| ()),
        }
    }
}

impl QueueRx {
    async fn recv(&mut self) -> Option<Message> {
        match self {
            QueueRx::Bounded(rx) => rx.recv().await,
            QueueRx::Unbounded(rx) => rx.recv().await,
        }
    }
}

fn queue(capacity: Option<usize>) -> (QueueTx, QueueRx) {
    match capacity {
        Some(n) => {
            let (tx, rx) = mpsc::channel(n.max(1));
            (QueueTx::Bounded(tx), QueueRx::Bounded(rx))
        }
        None => {
            let (tx, rx) = mpsc::unbounded_channel();
            (QueueTx::Unbounded(tx), QueueRx::Unbounded(rx))
        }
    }
}

/// Everything a worker needs to execute handlers for one subscription.
struct EndpointContext<S> {
    handlers: Vec<NatsHandlerArc<S>>,
    state: S,
    client: Client,
    subject: String,
    with_reply: bool,
    backlog: Arc<Backlog>,
}

pub struct NatsDispatcher {}

#[deprecated(note = "misspelled; use `NatsDispatcher`")]
pub type NatsDipatcher = NatsDispatcher;

impl NatsDispatcher {
    fn log_response(res: &Response<NatsResponse>) {
        if res.success {
            tracing::debug!("Request handled successfully");
        } else {
            let handler_name = res.handler_name.unwrap_or("no-handler-name");
            match &res.report {
                Some(report) => {
                    tracing::error!("Error in handler: {handler_name}\nMessage:\n    {report:?}")
                }
                None => tracing::error!("Error in handler: {handler_name} (no report)"),
            }
        }
    }

    fn log_panic(subject: &str, panic: Box<dyn Any + Send>) {
        let message = panic
            .downcast_ref::<&'static str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("<non-string panic payload>");
        tracing::error!("Panic in handler for subject {subject}: {message}");
    }

    fn log_join_result(subject: &str, res: Result<(), JoinError>) {
        match res {
            Ok(()) => {}
            Err(err) if err.is_cancelled() => {
                tracing::error!("Handler task for subject {subject} was cancelled")
            }
            Err(err) if err.is_panic() => Self::log_panic(subject, err.into_panic()),
            Err(err) => {
                tracing::error!("Unexpected error joining handler task for {subject}: {err:?}")
            }
        }
    }

    fn split_message(msg: Message, with_reply: bool) -> (NatsMetadata, NatsPayload) {
        let metadata = NatsMetadata {
            subject: msg.subject,
            reply: if with_reply { msg.reply } else { None },
        };
        let payload = NatsPayload {
            payload: msg.payload,
        };
        (metadata, payload)
    }

    /// Runs a single handler call and, if the response carries a reply
    /// subject, publishes the response from within the same task so the
    /// ingestion loop never blocks on publishing.
    async fn invoke<S: Clone + Sync + Send + 'static>(
        handler: NatsHandlerArc<S>,
        metadata: NatsMetadata,
        payload: NatsPayload,
        state: S,
        client: Client,
    ) {
        let response = handler
            .call(HandlerRequest { metadata, payload }, state)
            .await;
        Self::log_response(&response);
        if let Response {
            payload:
                Some(NatsResponse {
                    response,
                    reply: Some(reply),
                    ..
                }),
            ..
        } = response
        {
            if let Err(err) = client.publish(reply, response.into()).await {
                tracing::error!("while publishing nats response: {err:?}");
            }
        }
    }

    /// Reads messages from the subscription into the in-process queue as fast
    /// as possible. This is the only task touching the NATS client buffer, so
    /// with an unbounded queue that buffer cannot overflow (which is where
    /// the client would silently drop messages).
    async fn pump_reader(mut sub: Subscriber, tx: QueueTx, backlog: Arc<Backlog>) {
        let guard = elegant_departure::get_shutdown_guard();
        loop {
            let next = select! {
                r = sub.next() => match r { Some(m) => m, None => break },
                _ = guard.wait() => break,
            };
            backlog.on_enqueue();
            if tx.send(next).await.is_err() {
                break;
            }
        }
        // dropping tx lets the executor drain the queue and exit
    }

    /// Task-per-message execution with bounded concurrency. Backpressure is
    /// applied by awaiting a completion when the set is full — no polling, no
    /// sleeps: execution resumes the moment a slot frees up.
    async fn pump_concurrent_worker<S: Clone + Sync + Send + 'static>(
        mut rx: QueueRx,
        ctx: EndpointContext<S>,
        max_in_flight: usize,
    ) {
        let mut join_set = JoinSet::new();
        let max_in_flight = max_in_flight.max(1);
        loop {
            let next = if join_set.is_empty() {
                rx.recv().await
            } else {
                select! {
                    m = rx.recv() => m,
                    Some(res) = join_set.join_next() => {
                        Self::log_join_result(&ctx.subject, res);
                        continue;
                    },
                }
            };
            let Some(next) = next else { break };
            ctx.backlog.on_dequeue();

            while join_set.len() >= max_in_flight {
                if let Some(res) = join_set.join_next().await {
                    Self::log_join_result(&ctx.subject, res);
                }
            }

            let (metadata, payload) = Self::split_message(next, ctx.with_reply);
            for handler in &ctx.handlers {
                join_set.spawn(Self::invoke(
                    handler.clone(),
                    metadata.clone(),
                    payload.clone(),
                    ctx.state.clone(),
                    ctx.client.clone(),
                ));
            }
        }
        // finish in-flight work before shutting down
        while let Some(res) = join_set.join_next().await {
            Self::log_join_result(&ctx.subject, res);
        }
    }

    /// Processes queued messages strictly in order. Each handler call is
    /// wrapped in `catch_unwind`, so a panic aborts only the current message
    /// and the worker keeps running.
    async fn pump_ordered_worker<S: Clone + Sync + Send + 'static>(
        mut rx: QueueRx,
        ctx: EndpointContext<S>,
    ) {
        while let Some(msg) = rx.recv().await {
            ctx.backlog.on_dequeue();
            let (metadata, payload) = Self::split_message(msg, ctx.with_reply);
            for handler in &ctx.handlers {
                let call = Self::invoke(
                    handler.clone(),
                    metadata.clone(),
                    payload.clone(),
                    ctx.state.clone(),
                    ctx.client.clone(),
                );
                if let Err(panic) = AssertUnwindSafe(call).catch_unwind().await {
                    Self::log_panic(&ctx.subject, panic);
                }
            }
        }
    }

    /// Runs the NATS client's connection handler and all subscription
    /// readers on a dedicated runtime with its own OS threads. This keeps
    /// ingestion draining the client's subscription buffers even when
    /// handler tasks saturate the main runtime's workers (e.g. CPU-heavy
    /// handlers); a starved reader would otherwise let the client buffer
    /// overflow, which silently drops messages.
    ///
    /// The owning thread parks until service shutdown and then drops the
    /// runtime there (a runtime must not be dropped from async context).
    fn spawn_ingestion_runtime() -> eyre::Result<tokio::runtime::Handle> {
        let (handle_tx, handle_rx) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("dawnflow-ingestion".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_name("dawnflow-ingestion-worker")
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(err) => {
                        let _ = handle_tx.send(Err(err));
                        return;
                    }
                };
                let _ = handle_tx.send(Ok(rt.handle().clone()));
                rt.block_on(async {
                    elegant_departure::get_shutdown_guard().wait().await;
                });
            })?;
        Ok(handle_rx.recv()??)
    }

    pub async fn start_dispatcher<S: Clone + Sync + Send + 'static>(
        nats_url: &str,
        state: S,
        reg: HandlerRegistry<NatsPayload, NatsMetadata, S, NatsResponse>,
        join_set: JoinSet<()>,
    ) -> eyre::Result<JoinSet<()>> {
        Self::start_dispatcher_with_config(nats_url, state, reg, join_set, Default::default())
            .await
    }

    pub async fn start_dispatcher_with_config<S: Clone + Sync + Send + 'static>(
        nats_url: &str,
        state: S,
        reg: HandlerRegistry<NatsPayload, NatsMetadata, S, NatsResponse>,
        mut join_set: JoinSet<()>,
        config: DispatcherConfig,
    ) -> eyre::Result<JoinSet<()>> {
        let options = ConnectOptions::new()
            .subscription_capacity(config.subscription_capacity.max(1))
            .event_callback(|event| async move {
                match event {
                    Event::SlowConsumer(sid) => tracing::error!(
                        "NATS client DROPPED messages on subscription {sid} (buffer full) — \
                         increase subscription_capacity / use an unbounded queue_capacity, \
                         or reduce the publish rate"
                    ),
                    Event::Disconnected => {
                        tracing::warn!("NATS connection lost — messages published meanwhile may be missed")
                    }
                    Event::Connected => tracing::info!("NATS connection (re)established"),
                    Event::ServerError(err) => tracing::error!("NATS server error: {err}"),
                    Event::ClientError(err) => tracing::error!("NATS client error: {err}"),
                    other => tracing::info!("NATS connection event: {other}"),
                }
            });
        // connect on the ingestion runtime so the async-nats connection
        // handler task lives on its threads, immune to main-runtime
        // saturation by handler workloads
        let ingest = Self::spawn_ingestion_runtime()?;
        let nats_url = nats_url.to_string();
        let client = match ingest
            .spawn(async move { options.connect(nats_url).await })
            .await?
        {
            Ok(x) => x,
            Err(err) => bail!("{}", err),
        };

        // (subject, handlers, with_reply); only request handlers get a reply
        // subject to answer to
        let mut endpoints: Vec<(String, Vec<NatsHandlerArc<S>>, bool)> = Vec::new();
        for (name, handler) in reg.consumers {
            endpoints.push((format!("consumer.{}", name), vec![handler], false));
        }
        for (name, handlers) in reg.subscribers {
            endpoints.push((format!("subscriber.{}", name), handlers, false));
        }
        for (name, handler) in reg.handlers {
            endpoints.push((format!("request.{}", name), vec![handler], true));
        }

        for (subject, handlers, with_reply) in endpoints {
            let sub = client.subscribe(subject.clone()).await?;
            let backlog = Backlog::new(subject.clone(), config.backlog_warn_threshold);
            let (tx, rx) = queue(config.queue_capacity);
            ingest.spawn(Self::pump_reader(sub, tx, backlog.clone()));
            let ctx = EndpointContext {
                handlers,
                state: state.clone(),
                client: client.clone(),
                subject,
                with_reply,
                backlog,
            };
            match config.mode {
                ExecutionMode::Concurrent { max_in_flight } => {
                    join_set.spawn(Self::pump_concurrent_worker(rx, ctx, max_in_flight));
                }
                ExecutionMode::Ordered => {
                    join_set.spawn(Self::pump_ordered_worker(rx, ctx));
                }
            }
        }
        Ok(join_set)
    }
}
