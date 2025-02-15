use std::{
    any::Any,
    sync::Arc,
    time::{Duration, SystemTime},
};

use dawnflow::{
    handlers::{FromRequestBody, FromRequestMetadata, HandlerRequest},
    in_memory::{InMemoryMetadata, InMemoryPayload, InMemoryResponse},
    in_memory_publisher_backend::DefaultInMemoryPublisherBackend,
    publisher::Publisher,
    registry::HandlerRegistry,
};
use eyre::bail;

#[derive(Debug)]
pub struct Consummable {
    name: String,
    id: usize,
}

#[derive(Clone)]
pub struct Subscribable {
    name: String,
    id: usize,
}

#[derive(Debug)]
pub struct Request1 {
    name: String,
}

#[derive(Debug)]
pub struct Response1 {
    id: usize,
}

#[derive(Debug)]
pub struct Request2 {
    name: String,
}

#[derive(Debug)]
pub struct Response2 {
    id: usize,
}

pub async fn subscriber(state: MyState, Req(_sub): Req<Subscribable>) -> eyre::Result<()> {
    // println!("subscriber function");
    state
        .publisher
        .pub_cons(Consummable {
            name: "some name".into(),
            id: 24,
        })
        .await?;

    Ok(())
}

pub async fn consumer(state: MyState, Req(sub): Req<Consummable>) -> eyre::Result<()> {
    // println!("consumer function");
    // println!("{:?}", sub);
    let resp: Response1 = state
        .publisher
        .pub_req(Request1 {
            name: "msg1".into(),
        })
        .await?;
    // println!("{:?}", resp);
    Ok(())
}

pub async fn request1(state: MyState, Req(sub): Req<Request1>) -> eyre::Result<Response1> {
    // println!("request1 function");
    // println!("{:?}", sub);
    let resp: Response2 = state
        .publisher
        .pub_req(Request2 {
            name: "msg1".into(),
        })
        .await?;
    // println!("{:?}", resp);
    Ok(Response1 { id: 24 })
}

pub async fn request2(state: MyState, Req(sub): Req<Request2>) -> eyre::Result<Response2> {
    // println!("request2 function");
    // println!("{:?}", sub);
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

    let handlers = handlers
        .register_subscriber::<Subscribable, _, _>(subscriber)
        .register_consumer::<Consummable, _, _>(consumer)
        .register_handler::<Request1, _, _>(request1)
        .register_handler::<Request2, _, _>(request2);

    let i = Publisher::new_in_memory().await;
    let state = MyState {
        publisher: Arc::new(i),
    };

    let backend = DefaultInMemoryPublisherBackend::new(state.clone(), handlers).await;
    state.publisher.register_in_memory_backend(backend).await;

    let now = SystemTime::now();
    for x in 0..100000000 {
        state
            .publisher
            .pub_sub(Subscribable {
                name: "test".into(),
                id: x,
            })
            .await
            .unwrap();
    }
    let after = SystemTime::now();
    let res = after - now.duration_since(SystemTime::UNIX_EPOCH).unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    println!("{res:?}");

    tokio::time::sleep(Duration::from_secs(100)).await;

    println!("this is the in memory example")
}
