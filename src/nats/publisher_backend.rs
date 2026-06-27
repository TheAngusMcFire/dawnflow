use std::{sync::Arc, time::Duration};

use async_nats::{subject::ToSubject, Client, Request};

use crate::publisher::{BytePublisherBackend, PubConsOptions, PubReqOptions, PubSubOptions, PublisherError};

pub struct NatsPublisherBackend {
    client: Arc<Client>,
}

impl NatsPublisherBackend {
    pub async fn new(nats_url: &str) -> Result<Self, async_nats::Error> {
        Ok(Self {
            client: Arc::new(async_nats::connect(nats_url).await?),
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
                timeout: Some(Duration::from_secs(10)),
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
        let request = Request::new()
            .timeout(opts.timeout)
            .payload(msg.into());
        Ok(self
            .client
            .send_request(format!("request.{}", name).to_subject(), request)
            .await?
            .payload
            .into())
    }
}
