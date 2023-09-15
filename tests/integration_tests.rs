extern crate albertdb;

use albertdb::{
  config::Config,
  frontend::http,
};

// https://cloudmaker.dev/actix-integration-tests//

#[cfg(test)]
mod tests {
  use actix_web::{ test::{ self, TestRequest } };
  use serde_json::json;

  use super::*;
  #[actix_rt::test]
  async fn read_write_smoketest() {
    let config = Config::from_file("/home/albertlockett/Development/albertdb/config-replica1.yaml");

    let mut app = actix_web::test::init_service(
      actix_web::App::new().configure(|cfg| {
        http::configure(config, cfg)
      })
    ).await;

    let write_request_body = json!({
      "key": "key1",
      "value": "val1"
    });
    let write_resp = TestRequest::post()
      .uri("/write")
      .set_json(&write_request_body)
      .send_request(&mut app)
      .await;
    assert!(write_resp.status().is_success(), "failed to write");

    let read_request_body = json!({
      "key": "key1",
    });
    let read_resp = TestRequest::post()
      .uri("/read")
      .set_json(&read_request_body)
      .send_request(&mut app)
      .await;
    assert!(read_resp.status().is_success(), "failed to read");

    // TODO assert on the response
  }
}