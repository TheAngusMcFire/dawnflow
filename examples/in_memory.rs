use std::{any::Any, sync::Arc};

use dawnflow::{
    handlers::{FromRequestBody, FromRequestMetadata, HandlerRequest},
    in_memory::{InMemoryMetadata, InMemoryPayload, InMemoryResponse},
    publisher::Publisher,
    registry::HandlerRegistry,
};
use eyre::bail;

#[derive(Debug)]
pub struct Consummable {
    name: String,
    id: usize,
}

pub struct Subscribable {
    name: String,
    id: usize,
}

pub struct Request1 {
    name: String,
}

pub struct Response1 {
    id: usize,
}

pub struct Request2 {
    name: String,
}

pub struct Response2 {
    id: usize,
}

pub async fn subscriber(state: MyState, Req(_sub): Req<Subscribable>) -> eyre::Result<()> {
    state
        .publisher
        .pub_cons(Consummable {
            name: "some name".into(),
            id: 24,
        })
        .await?;

    Ok(())
}

pub async fn consumer(Req(sub): Req<Consummable>) -> eyre::Result<()> {
    println!("{:?}", sub);
    Ok(())
}

pub async fn request1(Req(_sub): Req<Request1>) -> eyre::Result<Response1> {
    Ok(Response1 { id: 24 })
}

pub async fn request2(Req(_sub): Req<Request2>) -> eyre::Result<Response2> {
    Ok(Response2 { id: 124 })
}

#[derive(Clone)]
pub struct MyState {
    publisher: Arc<Publisher>,
}

#[async_trait::async_trait]
impl FromRequestMetadata<MyState, InMemoryMetadata, InMemoryResponse> for MyState {
    type Rejection = Result<InMemoryResponse, eyre::Report>;
    async fn from_request_metadata(
        _req: &mut InMemoryMetadata,
        state: &MyState,
    ) -> Result<Self, Self::Rejection> {
        Ok(state.clone())
    }
}

pub struct Req<T>(T);
#[async_trait::async_trait]
impl<T: Any> FromRequestBody<MyState, InMemoryPayload, InMemoryMetadata, InMemoryResponse>
    for Req<T>
{
    type Rejection = Result<InMemoryResponse, eyre::Report>;
    async fn from_request(
        req: InMemoryPayload,
        _meta: &mut InMemoryMetadata,
        _state: &MyState,
    ) -> Result<Self, Self::Rejection> {
        match req.payload.downcast::<T>() {
            Ok(x) => Ok(Req(*x)),
            Err(_) => Err(Err(eyre::eyre!(
                "Unable to downcast payload to the target type"
            ))),
        }
    }
}

#[tokio::main]
async fn main() {
    let handlers =
        HandlerRegistry::<InMemoryPayload, InMemoryMetadata, MyState, InMemoryResponse>::default();
    handlers
        .register_subscriber(subscriber)
        .register_consumer(consumer)
        .register_handler(request1)
        .register_handler(request2);
    println!("this is the in memory example")
}
