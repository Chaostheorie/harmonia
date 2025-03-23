use crate::{cache_control_max_age_1d, cache_control_max_age_1y, config::Config, nixhash};
use actix_web::{http, web, HttpResponse};
use awc::{
    error::HeaderValue,
    http::{uri::PathAndQuery, Uri},
    Client,
};
use std::{error::Error, time::Duration};

pub(crate) async fn get(
    hash: web::Path<String>,
    settings: web::Data<Config>,
    // TODO: make error better
) -> Result<HttpResponse, Box<dyn Error>> {
    eprintln!("Got request for {}", hash);

    let narlist = match nixhash(&hash) {
        Some(store_path) => {
            println!("Found in store");
            libnixstore::get_nar_list(&store_path)?
        }
        None => {
            // Fetch from upstream
            let header_narlist = HeaderValue::from_str("application/json")
                .expect("narinfo conversion failed for header");

            for upstream in &settings.upstreams {
                println!("Upstream {upstream:?}");
                let mut uri = upstream.url.clone().into_parts();
                uri.path_and_query = Some(uri.path_and_query.map_or(
                    format!("/{hash}.ls").parse::<PathAndQuery>().unwrap(),
                    |url| {
                        format!("{}{}.ls", url.path(), hash)
                            .parse::<PathAndQuery>()
                            .unwrap()
                    },
                ));

                match Client::new()
                    .request(http::Method::GET, Uri::try_from(uri).unwrap())
                    .timeout(Duration::from_secs(5))
                    .no_decompress()
                    .send()
                    .await
                {
                    Ok(response) => match response.status() == http::StatusCode::OK {
                        true => {
                            let headers = response.headers();

                            eprintln!("Headers: {headers:?}");

                            let ct = match headers.get(http::header::CONTENT_TYPE) {
                                Some(ct) => match ct == header_narlist {
                                    true => ct,
                                    false => {
                                        eprintln!("Received invalid content-type from upstream");
                                        continue;
                                    }
                                },
                                None => {
                                    eprintln!("Received invalid content-type from upstream");
                                    continue;
                                }
                            };

                            // TODO: handle content length? Date?
                            match headers.get(http::header::CONTENT_ENCODING) {
                                Some(ce) => {
                                    return Ok(HttpResponse::Ok()
                                        .insert_header(cache_control_max_age_1d())
                                        .insert_header((http::header::CONTENT_TYPE, ct))
                                        .insert_header((http::header::CONTENT_ENCODING, ce))
                                        .streaming(response));
                                }
                                None => {
                                    return Ok(HttpResponse::Ok()
                                        .insert_header(cache_control_max_age_1d())
                                        .insert_header((http::header::CONTENT_TYPE, ct))
                                        .streaming(response));
                                }
                            }
                        }
                        false => {
                            eprintln!("Failed to make cache reqest: {response:?}");
                        }
                    },
                    Err(err) => {
                        eprintln!("Failed to construct cache reqest: {err:?}")
                    }
                }
            }

            return Ok(HttpResponse::NotFound()
                .insert_header(crate::cache_control_no_store())
                .body("missed hash for narlist"));
        }
    };

    Ok(HttpResponse::Ok()
        .insert_header(cache_control_max_age_1y())
        .insert_header(http::header::ContentType(mime::APPLICATION_JSON))
        .body(narlist))
}
