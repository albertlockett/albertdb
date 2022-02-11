use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use albertdb::engine;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("hello, world");

    let mut memtable1 = albertdb::memtable::Memtable::new();
    let mut memtable2 = albertdb::memtable::memtable2::Memtable::new();

    let arcm1 = std::sync::Arc::new(memtable1);
    let arcm1_1 = arcm1.clone();
    let arcm1_2 = arcm1.clone();

    let (sender2, reciever2) =
        std::sync::mpsc::channel::<std::sync::Arc<albertdb::memtable::Memtable>>();
    let s2 = sender2.clone();

    // let (sender2, reciever2) = std::sync::mpsc::channel::<engine::write::WritePayload>();
    let handle3 = std::thread::spawn(move || {
        println!("{:?}", arcm1_1.size());
        // memtable2.searc();
    });
    let handle2 = std::thread::spawn(move || {
        println!("{:?}", arcm1_2.size());
        memtable2.insert();
    });

    let write_engine = engine::write::WriteEngine::new();
    let (sender, reciever) = std::sync::mpsc::channel::<engine::write::WritePayload>();

    let handle = std::thread::spawn(move || {
        let mut write_engine = engine::write::WriteEngine {
            reciever,
            memtable: albertdb::memtable::Memtable::new(),
        };
        write_engine.start();
    });

    let memtable_mgr = albertdb::engine::memtable::MemtableManager::new();
    let mmtArc = std::sync::Arc::new(std::sync::RwLock::new(memtable_mgr));

    let server = HttpServer::new(move || {
        App::new()
            .data(sender.clone())
            .data(mmtArc.clone())
            .route(
                "/write",
                web::post().to(
                    |sender: web::Data<std::sync::mpsc::Sender<engine::write::WritePayload>>,
                     req: web::Json<engine::write::WritePayload>| {
                        let result = sender.send(req.clone());
                        println!("{:?}", result);
                        HttpResponse::Ok().body("nice")
                    },
                ),
            )
            .route(
                "/write2",
                web::post().to(
                    |mmt_arc: web::Data<
                        std::sync::Arc<
                            std::sync::RwLock<albertdb::engine::memtable::MemtableManager>,
                        >,
                    >,
                     req: web::Json<engine::write::WritePayload>| {
                        mmt_arc.write().unwrap().write();
                        HttpResponse::Ok().body("nice")
                    },
                ),
            )
            .route(
                "/read",
                web::post().to(
                    |mmt_arc: web::Data<
                        std::sync::Arc<
                            std::sync::RwLock<albertdb::engine::memtable::MemtableManager>,
                        >,
                    >,
                     req: web::Json<engine::write::WritePayload>| {
                        if mmt_arc.read().unwrap().find() {
                            HttpResponse::Ok().body("nice")
                        } else {
                            HttpResponse::Ok().body("biffed it")
                        }
                    },
                ),
            )
    });

    server
        .bind("127.0.0.1:4000")
        .expect("error binding server")
        .run()
        .await
}
