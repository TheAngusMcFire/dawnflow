use std::{sync::Arc, time::Duration};

use async_nats::{subject::ToSubject, Client, Request};

use crate::publisher::{BytePublisherBackend, PublisherError};

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
        let request = Request::new()
            .timeout(Some(Duration::from_secs(10)))
            .payload(msg.into());
        Ok(self
            .client
            .send_request(format!("request.{}", name).to_subject(), request)
            .await?
            .payload
            .into())
    }
}
