use actix_web::{web, App, HttpResponse, HttpServer};
use std::io::Result;

use log;
use serde::Deserialize;
use std::str;
use std::sync::{Arc, RwLock};

use crate::config::Config;
use crate::engine::Engine;
use crate::ring;

pub async fn start(config: Config) -> Result<()> {
    env_logger::init();
    log::info!("starting albertdb with web frontend");

    // TODO this sets up the ring server multiple times
    // TODO FIXME FIXME FIXME FIXME

    let web_cfg = config.clone();
    let server = HttpServer::new(move || {
        App::new()
            .configure(|cfg| {
              configure(web_cfg.clone(), cfg)
            })
    });

    server
        // TODO add this port to config
        .bind(format!("127.0.0.1:{}", config.http_listen_port))
        .expect("error binding server")
        .run()
        .await
}

pub fn configure(config: Config,  cfg: &mut web::ServiceConfig) {
  let ring = ring::init(&config);
  let ring_arc = Arc::new(RwLock::new(ring));

  // wait the server go
  let threaded_rt = tokio::runtime::Runtime::new().unwrap();
  let ring_svc_config = config.clone();
  let ring_svc_ptr = ring_arc.clone();
  let _handle = threaded_rt.spawn(async move {
      ring::server::start_server(ring_svc_config, ring_svc_ptr.clone())
          .await
          .unwrap();
  });

  let memtable_mgr = Engine::new(config.clone());
  let mmt_arc = Arc::new(RwLock::new(memtable_mgr));


  let web_cfg = config.clone();
  cfg
    .data(mmt_arc.clone())
    .data(web_cfg.clone())
    .data(ring_arc.clone())
    .route("/force_flush", web::post().to(force_flush))
    .route("/force_compact", web::post().to(force_compact))
    .route("/ring-join", web::post().to(ring_join))
    .route("/node-status", web::post().to(node_status))
    .route("/write", web::post().to(handle_write))
    .route("/read", web::post().to(handle_read))
    .route("/delete", web::post().to(handle_delete));
}

fn force_flush(mmt_arc: web::Data<Arc<RwLock<Engine>>>) -> HttpResponse {
    mmt_arc.write().unwrap().force_flush();
    HttpResponse::Ok().body("nice")
}

fn force_compact(mtt_arc: web::Data<Arc<RwLock<Engine>>>) -> HttpResponse {
    mtt_arc.read().unwrap().force_compact();
    HttpResponse::Ok().body("nice")
}

fn ring_join(cfg: web::Data<Config>, ring_arc: web::Data<Arc<RwLock<ring::Ring>>>) -> HttpResponse {
    let threaded_rt = tokio::runtime::Runtime::new().unwrap();
    let caller_cfg = cfg.as_ref().clone();
    let caller_ring_arc = ring_arc.as_ref().clone();
    threaded_rt.block_on(async move {
        ring::server::start_join(caller_cfg, caller_ring_arc).await;
    });
    HttpResponse::Ok().body("nice")
}

fn node_status(ring: web::Data<Arc<RwLock<ring::Ring>>>) -> HttpResponse {
    let rw = ring.read();
    let status = format!("{:?}", rw.as_ref().unwrap().status);
    HttpResponse::Ok().body(status)
}

#[derive(Clone, Debug, Deserialize)]
pub struct WritePayload {
    key: String,
    value: String,
}

fn handle_write(
    mmt_arc: web::Data<Arc<RwLock<Engine>>>,
    req: web::Json<WritePayload>,
) -> HttpResponse {
    mmt_arc
        .write()
        .unwrap()
        .write(req.key.as_bytes(), req.value.as_bytes());
    HttpResponse::Ok().body("nice")
}

#[derive(Clone, Debug, Deserialize)]
pub struct ReadPayload {
    key: String,
}

fn handle_read(
    mmt_arc: web::Data<Arc<RwLock<Engine>>>,
    req: web::Json<ReadPayload>,
) -> HttpResponse {
    let found = mmt_arc.read().unwrap().find(req.key.as_bytes());
    if !matches!(found, None) {
        let value = String::from_utf8(found.unwrap()).unwrap();
        HttpResponse::Ok().body(value)
    } else {
        HttpResponse::Ok().body("biffed it")
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct DeletePayload {
    key: String,
}

fn handle_delete(
    mmt_arc: web::Data<Arc<RwLock<Engine>>>,
    req: web::Json<DeletePayload>,
) -> HttpResponse {
    mmt_arc.write().unwrap().delete(req.key.as_bytes());
    HttpResponse::Ok().body("OK")
}
