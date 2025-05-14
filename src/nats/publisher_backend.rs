use std::sync::Arc;

use async_nats::Client;

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
        Ok(self
            .client
            .request(format!("request.{}", name), msg.into())
            .await?
            .payload
            .into())
    }
}
