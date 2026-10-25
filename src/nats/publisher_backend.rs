use std::{sync::Arc, time::Duration};

use async_nats::{subject::ToSubject, Client, ConnectOptions, Event, Request};

use crate::publisher::{BytePublisherBackend, PubConsOptions, PubReqOptions, PubSubOptions, PublisherError};

use super::{connection, ERROR_HEADER};

/// Requests without a timeout of their own wait this long (the async-nats
/// default, which a `None` would otherwise switch off).
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

pub struct NatsPublisherBackend {
    client: Arc<Client>,
}

impl NatsPublisherBackend {
    /// `connection_string` may carry credentials and TLS settings, see
    /// [`crate::nats::connection`].
    pub async fn new(connection_string: &str) -> Result<Self, async_nats::Error> {
        let options = ConnectOptions::new().event_callback(|event| async move {
            match event {
                Event::Disconnected => tracing::warn!(
                    "NATS publisher connection lost — publishes are buffered until it is back"
                ),
                Event::Connected => tracing::info!("NATS publisher connection (re)established"),
                Event::ServerError(err) => tracing::error!("NATS server error: {err}"),
                Event::ClientError(err) => tracing::error!("NATS client error: {err}"),
                other => tracing::info!("NATS publisher connection event: {other}"),
            }
        });
        let (server, options) = connection::configure(connection_string, options).await?;
        Ok(Self {
            client: Arc::new(options.connect(server).await?),
        })
    }

    pub async fn new_with_client(client: async_nats::Client) -> Result<Self, async_nats::Error> {
        Ok(Self {
            client: Arc::new(client),
        })
    }
}

#[async_trait::async_trait]
impl BytePublisherBackend for NatsPublisherBackend {
    async fn pub_sub(&self, name: &str, msg: Vec<u8>) -> Result<(), PublisherError> {
        self.client
            .publish(format!("subscriber.{}", name), msg.into())
            .await?;
        Ok(())
    }

    async fn pub_cons(&self, name: &str, msg: Vec<u8>) -> Result<(), PublisherError> {
        self.client
            .publish(format!("consumer.{}", name), msg.into())
            .await?;
        Ok(())
    }

    async fn pub_req(&self, name: &str, msg: Vec<u8>) -> Result<Vec<u8>, PublisherError> {
        self.pub_req_with_opts(
            name,
            msg,
            &PubReqOptions {
                timeout: Some(DEFAULT_REQUEST_TIMEOUT),
                ..Default::default()
            },
        )
        .await
    }

    async fn flush(&self) -> Result<(), PublisherError> {
        self.client.flush().await?;
        Ok(())
    }

    async fn pub_sub_with_opts(
        &self,
        name: &str,
        msg: Vec<u8>,
        _opts: &PubSubOptions,
    ) -> Result<(), PublisherError> {
        self.pub_sub(name, msg).await
    }

    async fn pub_cons_with_opts(
        &self,
        name: &str,
        msg: Vec<u8>,
        _opts: &PubConsOptions,
    ) -> Result<(), PublisherError> {
        self.pub_cons(name, msg).await
    }

    async fn pub_req_with_opts(
        &self,
        name: &str,
        msg: Vec<u8>,
        opts: &PubReqOptions,
    ) -> Result<Vec<u8>, PublisherError> {
        let timeout = opts.timeout.unwrap_or(DEFAULT_REQUEST_TIMEOUT);
        let request = Request::new().timeout(Some(timeout)).payload(msg.into());
        // the client's own timeout starts only once the request is queued,
        // which blocks while NATS is unreachable and the client buffer is full
        let message = tokio::time::timeout(
            timeout,
            self.client
                .send_request(format!("request.{}", name).to_subject(), request),
        )
        .await
        .map_err(|_| PublisherError::Timeout)??;
        if let Some(error) = message
            .headers
            .as_ref()
            .and_then(|headers| headers.get(ERROR_HEADER))
        {
            return Err(PublisherError::Remote(error.as_str().to_string()));
        }
        Ok(message.payload.into())
    }
}
