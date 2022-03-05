use actix_web::{web, App, HttpResponse, HttpServer};
use albertdb::engine::Engine;
use log;
use serde::Deserialize;
use std::str;
use std::sync::{Arc, RwLock};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    log::info!("starting albertdb with web frontend");

    let memtable_mgr = Engine::new();
    let mmt_arc = Arc::new(RwLock::new(memtable_mgr));

    let server = HttpServer::new(move || {
        App::new()
            .data(mmt_arc.clone())
            .route("/write2", web::post().to(handle_write))
            .route("/read", web::post().to(handle_read))
            .route("/delete", web::post().to(handle_delete))
    });

    server
        .bind("127.0.0.1:4000")
        .expect("error binding server")
        .run()
        .await
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
