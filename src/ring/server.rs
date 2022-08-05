use tonic;
use crate::config;

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

pub async fn start_server(cfg: config::Config) -> Result<(), std::sync::Arc<dyn std::error::Error>> {
  println!("listening on 50051");
  let addr = format!("127.0.0.1:{}", cfg.ring_svc_listen_port).parse().unwrap();
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

pub async fn start_join(cfg: config::Config) {
  let uri = cfg.ring_svc_seed_nodes[0].clone().to_owned();
  let endpoint  = tonic::transport::Endpoint::from_shared(uri).unwrap();
  let mut client = ring::ring_client::RingClient::connect(endpoint).await.unwrap();
  let req = ring::GetTopologyRequest{
    test: "asf".to_owned()
  };

  println!("making the request");
  let result = client.get_topology(req).await.unwrap();
  println!("response = {:?}", result)
}