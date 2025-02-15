pub mod handlers;
pub mod in_memory;
pub mod publisher;
pub mod registry;

// use std::{
//     any::Any,
//     collections::{HashMap, HashSet},
//     sync::Arc,
// };

// pub enum PublisherError {}

// #[derive(Default)]
// pub struct Publisher {}

// impl Publisher {
//     pub async fn publish_sub<T: 'static>(sub: T) -> Result<(), PublisherError> {
//         Ok(())
//     }
// }

// pub struct DawnContext<S> {
//     state: Arc<S>,
// }

// pub struct MyState {}

// pub struct MyConsumable {}

// // pub enum HandlerError {

// // }
// // https://github.com/twittner/minicbor
// pub struct MessageContainer {
//     data: Box<dyn Any + 'static + Send + Sync>,
// }

// #[async_trait::async_trait]
// pub trait BaseTrait: Send + Sync + 'static {
//     async fn handler_call(&self, cont: MessageContainer) -> eyre::Result<()>;
// }

// pub struct MyCallStruct<S> {
//     state: S,
// }

// #[async_trait::async_trait]
// impl BaseTrait for MyCallStruct<MyState> {
//     async fn handler_call(&self, cont: MessageContainer) -> eyre::Result<()> {
//         let req = *cont.data.downcast::<MyConsumable>().unwrap();
//         self.handler(req).await
//     }
// }

// #[async_trait::async_trait]
// impl MyCall for MyCallStruct<MyState> {
//     async fn handler(&self, req: MyConsumable) -> eyre::Result<()> {
//         todo!()
//     }
// }

// #[async_trait::async_trait]
// pub trait MyCall {
//     async fn handler(&self, req: MyConsumable) -> eyre::Result<()>;
// }

// // impl MyCall for DawnContext<MyState> {
// //     async fn handler(&self, req: MyConsumable) -> eyre::Result<()> {
// //         todo!()
// //     }
// // }

// struct HandlerReg {
//     hndlrs: HashMap<String, Arc<dyn BaseTrait + 'static + Send + Sync>>,
// }

// impl HandlerReg {
//     pub fn insert<T: BaseTrait>(&mut self, name: String, bt: T) {
//         self.hndlrs.insert(name, Arc::new(bt));
//     }
// }
