use std::any::Any;

use dawnflow::{
    handlers::{FromRequestBody, HandlerRequest},
    in_memory::{InMemoryMetadata, InMemoryPayload, InMemoryResponse},
    registry::HandlerRegistry,
};

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

pub async fn subscriber(Req(_sub): Req<Subscribable>) -> eyre::Result<()> {
    Ok(())
}

pub async fn consumer(Req(_sub): Req<Consummable>) -> eyre::Result<()> {
    Ok(())
}

pub async fn request1(Req(_sub): Req<Request1>) -> eyre::Result<Response1> {
    Ok(Response1 { id: 24 })
}

pub async fn request2(Req(_sub): Req<Request2>) -> eyre::Result<Response2> {
    Ok(Response2 { id: 124 })
}

pub struct MyState {}

pub struct Req<T>(T);
#[async_trait::async_trait]
impl<T: Any> FromRequestBody<MyState, InMemoryPayload, InMemoryMetadata, InMemoryResponse>
    for Req<T>
{
    type Rejection = Result<InMemoryResponse, eyre::Report>;
    async fn from_request(
        req: HandlerRequest<InMemoryPayload, InMemoryMetadata>,
        _state: &MyState,
    ) -> Result<Self, Self::Rejection> {
        Ok(Req(*req.payload.payload.downcast::<T>().unwrap()))
    }
}

fn main() {
    let handlers =
        HandlerRegistry::<InMemoryPayload, InMemoryMetadata, MyState, InMemoryResponse>::default();
    handlers
        .register(subscriber)
        .register(subscriber)
        .register(request1)
        .register(request2);
    println!("this is the in memory example")
}
