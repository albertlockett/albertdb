use crate::config;

pub mod server;

#[derive(Clone, Debug)]
pub enum NodeStatus {
    NotJoined,
    SeedPending,
    // SeedFailed,  // TODO this is when seed node fails to respond
    // JoinWait,    // TODO this is when can't join cause another join in progress
    Joining,
    // Ready        // TODO this is when the node is ready
}

#[derive(Clone, Debug)]
pub struct Node {
    node_id: String,
    hostname: String,
    port: u32,
}

#[derive(Clone, Debug)]
pub struct Ring {
    pub status: NodeStatus,
    pub nodes: Vec<Node>,
}

pub fn init(cfg: &config::Config) -> Ring {
    let node = Node {
        node_id: cfg.node_id.clone(),
        hostname: cfg.ring_svc_broadcast_host.clone(),
        port: cfg.ring_svc_listen_port,
    };
    Ring {
        status: NodeStatus::NotJoined,
        nodes: vec![node],
    }
}
