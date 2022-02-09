use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use albertdb::engine;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("hello, world");

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
