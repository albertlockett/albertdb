use albertdb::frontend;


extern crate albertdb;

use albertdb::{
  config::Config,
  frontend::http,
};


#[cfg(test)]
mod tests {
  use actix_web::{ test };

  use super::*;
  #[actix_rt::test]
  async fn smoke_test() {
    let config = Config::from_file("/home/albertlockett/Development/albertdb/config-replica1.yaml");
    http::start(config).await;
  }
}