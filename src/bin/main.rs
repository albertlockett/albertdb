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

    let (sender2, reciever2) = std::sync::mpsc::channel::<std::sync::Arc<albertdb::memtable::Memtable>>();
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

    let server = HttpServer::new(move || {
        App::new().data(sender.clone()).route(
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
    });

    server
        .bind("127.0.0.1:4000")
        .expect("error binding server")
        .run()
        .await
}

struct WriteHandler {
    write_engine: engine::write::WriteEngine,
}
