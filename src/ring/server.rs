use crate::config;
use crate::ring::Ring;
use log;
use std::sync::{Arc, RwLock};
use tonic::{transport, Request, Response, Status};

use super::{Node, NodeStatus};

mod ring {
    include!("ring.rs");
}

#[derive(Default)]
pub struct RingServerImpl {
    ring_arc: Option<Arc<RwLock<Ring>>>,
}

#[tonic::async_trait]
impl ring::ring_server::Ring for RingServerImpl {
    async fn get_topology(
        &self,
        _request: Request<ring::GetTopologyRequest>,
    ) -> Result<Response<ring::GetTopologyResponse>, Status> {
        let ring = self.ring_arc.as_ref().unwrap();
        let rlock = ring.write().unwrap();

        let mut nodes = Vec::<ring::Node>::new();
        for n in rlock.nodes.iter() {
            let node = ring::Node {
                hostname: n.hostname.clone(),
                node_id: n.node_id.clone(),
                port: n.port,
            };
            nodes.push(node)
        }

        let response = ring::GetTopologyResponse { nodes: nodes };

        Ok(tonic::Response::new(response))
    }

    async fn join_ring(
        &self,
        grpc_req: Request<ring::JoinRequest>,
    ) -> Result<Response<ring::JoinResponse>, Status> {
        let join_req = grpc_req.get_ref();
        let new_node = join_req.node.as_ref().unwrap();

        let ring_arc = self.ring_arc.as_ref().unwrap().clone();
        let mut rw = ring_arc.write();
        let ring = rw.as_mut().unwrap();
        let new_node = Node {
            hostname: new_node.hostname.clone(),
            node_id: new_node.node_id.clone(),
            port: new_node.port,
        };
        ring.nodes.push(new_node);

        println!("{:?}", ring);

        let response = ring::JoinResponse {};
        Ok(Response::new(response))
    }
}

pub async fn start_server(
    cfg: config::Config,
    ring_arc: Arc<RwLock<Ring>>,
) -> Result<(), std::sync::Arc<dyn std::error::Error>> {
    let addr = format!("127.0.0.1:{}", cfg.ring_svc_listen_port)
        .parse()
        .unwrap();
    let mut server = RingServerImpl::default();
    server.ring_arc = Some(ring_arc);

    log::info!("starting ring server at {}", addr);

    tonic::transport::Server::builder()
        .add_service(ring::ring_server::RingServer::new(server))
        .serve(addr)
        .await
        .unwrap();
    Ok(())
}

pub async fn start_join(cfg: config::Config, ring_arc: Arc<RwLock<Ring>>) {
    log::info!("joining cluster");
    change_status(ring_arc.clone(), NodeStatus::SeedPending);

    // Get the ring topology from the seed nodes
    let uri = cfg.ring_svc_seed_nodes[0].clone().to_owned();
    let endpoint = transport::Endpoint::from_shared(uri).unwrap();
    let mut client = ring::ring_client::RingClient::connect(endpoint)
        .await
        .unwrap();
    let req = ring::GetTopologyRequest {};
    let get_topo_res = client.get_topology(req).await.unwrap();
    let ring_topo = get_topo_res.into_inner();
    // TODO if the call above fails, then we're in SeedFailed state
    // TODO check if another ndoe is already joining and then wait

    log::info!(
        "received cluster topology #nodes: {}",
        ring_topo.nodes.len()
    );

    change_status(ring_arc.clone(), NodeStatus::Joining);

    // send join request to all other nodes in the ring
    for node in &ring_topo.nodes {
        // TODO somehow reuse these clients
        // TODO don't assume it's http
        let node_uri = format!("http://{}:{}", node.hostname, node.port);
        log::info!(
            "sending join notification to node {} at {}",
            node.node_id,
            node_uri
        );
        let node_endpoint = transport::Endpoint::from_shared(node_uri).unwrap();
        let mut node_client = ring::ring_client::RingClient::connect(node_endpoint)
            .await
            .unwrap();

        let join_req = ring::JoinRequest {
            node: Some(ring::Node {
                node_id: cfg.node_id.clone(),
                hostname: cfg.ring_svc_broadcast_host.clone(),
                port: cfg.ring_svc_listen_port,
            }),
        };
        let join_res = node_client.join_ring(join_req).await.unwrap();
        println!("join response {:?}", join_res)
    }
}

fn change_status(ring_arc: Arc<RwLock<Ring>>, next_status: NodeStatus) {
    let mut rw = ring_arc.write();
    let ring = rw.as_mut().unwrap();
    ring.status = next_status;
}
