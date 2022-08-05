use tonic;
use tokio;

mod ring {
  include!("ring.rs");
}

#[derive(Default)]
pub struct RingServerImpl {}

#[tonic::async_trait]
impl ring::ring_server::Ring for RingServerImpl {
  async fn get_topology(
    &self,
    _request: tonic::Request<ring::GetTopologyRequest>
  ) -> Result<tonic::Response<ring::GetTopologyResponse>, tonic::Status> {
    let response = ring::GetTopologyResponse{
      test: "asdf".to_owned()
    };
    Ok(tonic::Response::new(response))
  }
}

pub async fn start_server() -> Result<(), std::sync::Arc<dyn std::error::Error>> {
  println!("listening on 50051");
  let addr = "127.0.0.1:50051".parse().unwrap();
  let server = RingServerImpl::default();

  println!("listening on 50051 2");

  let x = tonic::transport::Server::builder()
    .add_service(ring::ring_server::RingServer::new(server))
    .serve(addr)
    .await;
  println!("listening on 50051 half done");

  x.unwrap();
  println!("listening on 50051 done");
  Ok(())
}