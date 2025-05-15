use std::sync::Arc;

use dawnflow::{
    handlers::FromRequestMetadata,
    nats::{dispatcher::NatsDipatcher, NatsMetadata, NatsPayload, NatsResponse},
    publisher::Publisher,
    registry::HandlerRegistry,
    Req,
};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

#[derive(Debug, Serialize, Deserialize)]
pub struct Consumable {
    name: String,
    id: usize,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Subscribable {
    name: String,
    id: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Request1 {
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response1 {
    id: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Request2 {
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response2 {
    id: usize,
}

pub async fn subscriber(state: MyState, Req(_sub): Req<Subscribable>) -> eyre::Result<()> {
    state
        .publisher
        .pub_cons(Consumable {
            name: "some name".into(),
            id: 24,
        })
        .await?;
    Ok(())
}

pub async fn consumer(state: MyState, Req(_sub): Req<Consumable>) -> eyre::Result<()> {
    let _resp: Response1 = state
        .publisher
        .pub_req(Request1 {
            name: "msg1".into(),
        })
        .await?;
    Ok(())
}

pub async fn request1(state: MyState, Req(_sub): Req<Request1>) -> eyre::Result<Response1> {
    let _resp: Response2 = state
        .publisher
        .pub_req(Request2 {
            name: "msg1".into(),
        })
        .await?;
    Ok(Response1 { id: 24 })
}

pub async fn request2(_state: MyState, Req(_sub): Req<Request2>) -> eyre::Result<Response2> {
    Ok(Response2 { id: 124 })
}

#[derive(Clone)]
pub struct MyState {
    publisher: Arc<Publisher>,
}

#[async_trait::async_trait]
impl FromRequestMetadata<MyState, NatsMetadata, NatsResponse> for MyState {
    type Rejection = eyre::Report;
    async fn from_request_metadata(
        _req: &mut NatsMetadata,
        state: &MyState,
    ) -> Result<Self, Self::Rejection> {
        Ok(state.clone())
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let tr_sub = tracing_subscriber::fmt()
        .compact()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(tr_sub).unwrap();

    let handlers = HandlerRegistry::<NatsPayload, NatsMetadata, MyState, NatsResponse>::default();

    let handlers = handlers
        .register_subscriber::<Subscribable, _, _>(subscriber)
        .register_consumer::<Consumable, _, _>(consumer)
        .register_handler::<Request1, _, _>(request1)
        .register_handler::<Request2, _, _>(request2);

    let connection_string = std::env::var("NATS_CONNECTION_STRING").unwrap();

    let state = MyState {
        publisher: Arc::new(
            Publisher::new_nats_publisher(&connection_string)
                .await
                .unwrap(),
        ),
    };

    let join_set = NatsDipatcher::start_dispatcher(
        &connection_string,
        state.clone(),
        handlers,
        JoinSet::new(),
    )
    .await?;

    for x in 0..1 {
        state
            .publisher
            .pub_sub(Subscribable {
                name: "test".into(),
                id: x,
            })
            .await
            .unwrap();
    }

    elegant_departure::tokio::depart()
        .on_completion(async {
            join_set.join_all().await;
        })
        .on_termination()
        .await;

    tracing::warn!("shutting down service");
    Ok(())
    // join_set.join_all().await;
}
