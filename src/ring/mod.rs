use crate::config;

pub mod server;

#[derive(Clone, Debug)]
pub struct Node {
    node_id: String,
    hostname: String,
    port: u32,
}

#[derive(Clone, Debug)]
pub struct Ring {
    nodes: Vec<Node>,
}

pub fn init(cfg: &config::Config) -> Ring {
    let node = Node {
        node_id: cfg.node_id.clone(),
        hostname: cfg.ring_svc_broadcast_host.clone(),
        port: cfg.ring_svc_listen_port,
    };
    let guy = Ring { nodes: vec![node] };
    return guy;
}
