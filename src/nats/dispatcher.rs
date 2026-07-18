use std::{any::Any, panic::AssertUnwindSafe};

use async_nats::{Client, Message, Subscriber};
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
    /// worker task per handler type. A bounded queue decouples the NATS
    /// subscription from the worker so ingestion keeps draining the socket
    /// during slow handler calls. Panics are caught per message via
    /// `catch_unwind`, so a panicking handler aborts only the message that
    /// caused it. Requires the default `panic = "unwind"` strategy.
    Ordered,
}

#[derive(Clone, Copy, Debug)]
pub struct DispatcherConfig {
    pub mode: ExecutionMode,
    /// Capacity of the queue between the NATS subscription and the worker in
    /// `Ordered` mode. Larger values absorb bigger bursts before ingestion
    /// applies backpressure onto the NATS client's subscription buffer.
    /// Ignored in `Concurrent` mode.
    pub ordered_queue_capacity: usize,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            mode: ExecutionMode::Concurrent { max_in_flight: 10 },
            ordered_queue_capacity: 1024,
        }
    }
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

    /// Task-per-message execution with bounded concurrency. Backpressure is
    /// applied by awaiting a completion when the set is full — no polling, no
    /// sleeps: ingestion resumes the moment a slot frees up.
    async fn pump_concurrent<S: Clone + Sync + Send + 'static>(
        mut sub: Subscriber,
        handlers: Vec<NatsHandlerArc<S>>,
        state: S,
        client: Client,
        subject: String,
        max_in_flight: usize,
        with_reply: bool,
    ) {
        let guard = elegant_departure::get_shutdown_guard();
        let mut join_set = JoinSet::new();
        let max_in_flight = max_in_flight.max(1);
        loop {
            while join_set.len() >= max_in_flight {
                if let Some(res) = join_set.join_next().await {
                    Self::log_join_result(&subject, res);
                }
            }

            let next = if join_set.is_empty() {
                select! {
                    r = sub.next() => match r { Some(m) => m, None => break },
                    _ = guard.wait() => break,
                }
            } else {
                select! {
                    r = sub.next() => match r { Some(m) => m, None => break },
                    Some(res) = join_set.join_next() => {
                        Self::log_join_result(&subject, res);
                        continue;
                    },
                    _ = guard.wait() => break,
                }
            };

            let (metadata, payload) = Self::split_message(next, with_reply);
            for handler in &handlers {
                join_set.spawn(Self::invoke(
                    handler.clone(),
                    metadata.clone(),
                    payload.clone(),
                    state.clone(),
                    client.clone(),
                ));
            }
        }
        // finish in-flight work before shutting down
        while let Some(res) = join_set.join_next().await {
            Self::log_join_result(&subject, res);
        }
    }

    /// Reads messages from the subscription into a bounded queue. Kept
    /// separate from the worker so the socket is drained quickly even while
    /// a slow handler is running.
    async fn pump_ordered_reader(mut sub: Subscriber, tx: mpsc::Sender<Message>) {
        let guard = elegant_departure::get_shutdown_guard();
        loop {
            let next = select! {
                r = sub.next() => match r { Some(m) => m, None => break },
                _ = guard.wait() => break,
            };
            if tx.send(next).await.is_err() {
                break;
            }
        }
        // dropping tx lets the worker drain the queue and exit
    }

    /// Processes queued messages strictly in order. Each handler call is
    /// wrapped in `catch_unwind`, so a panic aborts only the current message
    /// and the worker keeps running.
    async fn pump_ordered_worker<S: Clone + Sync + Send + 'static>(
        mut rx: mpsc::Receiver<Message>,
        handlers: Vec<NatsHandlerArc<S>>,
        state: S,
        client: Client,
        subject: String,
        with_reply: bool,
    ) {
        while let Some(msg) = rx.recv().await {
            let (metadata, payload) = Self::split_message(msg, with_reply);
            for handler in &handlers {
                let call = Self::invoke(
                    handler.clone(),
                    metadata.clone(),
                    payload.clone(),
                    state.clone(),
                    client.clone(),
                );
                if let Err(panic) = AssertUnwindSafe(call).catch_unwind().await {
                    Self::log_panic(&subject, panic);
                }
            }
        }
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
        let client = match async_nats::connect(nats_url).await {
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
            match config.mode {
                ExecutionMode::Concurrent { max_in_flight } => {
                    join_set.spawn(Self::pump_concurrent(
                        sub,
                        handlers,
                        state.clone(),
                        client.clone(),
                        subject,
                        max_in_flight,
                        with_reply,
                    ));
                }
                ExecutionMode::Ordered => {
                    let (tx, rx) = mpsc::channel(config.ordered_queue_capacity.max(1));
                    join_set.spawn(Self::pump_ordered_reader(sub, tx));
                    join_set.spawn(Self::pump_ordered_worker(
                        rx,
                        handlers,
                        state.clone(),
                        client.clone(),
                        subject,
                        with_reply,
                    ));
                }
            }
        }
        Ok(join_set)
    }
}
