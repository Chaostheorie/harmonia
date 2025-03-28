use crate::config::Config;
use crate::{cache_control_max_age_1d, nixhash};
use actix_web::http::uri::PathAndQuery;
use actix_web::http::Uri;
use actix_web::{http, web, HttpResponse};
use awc::error::HeaderValue;
use awc::Client;
use libnixstore::Radix;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use std::{error::Error, path::Path};
use tokio_stream::StreamExt;

use crate::config::Config;
use crate::{cache_control_max_age_1d, nixhash, some_or_404};

#[derive(Debug, Deserialize)]
pub struct Param {
    json: Option<String>,
}

#[derive(Debug, Serialize)]
struct NarInfo {
    store_path: String,
    url: String,
    compression: String,
    nar_hash: String,
    nar_size: u64,
    references: Vec<String>,
    deriver: Option<String>,
    system: Option<String>,
    sigs: Vec<String>,
    ca: Option<String>,
}

fn fingerprint_path(
    store_path: &str,
    nar_hash: &str,
    nar_size: u64,
    refs: &[String],
) -> Result<Option<String>, Box<dyn Error>> {
    let root_store_dir = libnixstore::get_store_dir();
    if store_path[0..root_store_dir.len()] != root_store_dir || &nar_hash[0..7] != "sha256:" {
        return Ok(None);
    }

    let mut nar_hash = nar_hash.to_owned();
    if nar_hash.len() == 71 {
        nar_hash = format!(
            "sha256:{}",
            libnixstore::convert_hash("sha256", &nar_hash[7..], Radix::default())?
        );
    }

    if nar_hash.len() != 59 {
        return Ok(None);
    }

    for r in refs {
        if r[0..root_store_dir.len()] != root_store_dir {
            return Ok(None);
        }
    }

    Ok(Some(format!(
        "1;{};{};{};{}",
        store_path,
        nar_hash,
        nar_size,
        refs.join(",")
    )))
}

fn extract_filename(path: &str) -> Option<String> {
    Path::new(path)
        .file_name()
        .and_then(|v| v.to_str().map(ToOwned::to_owned))
}

fn query_narinfo(
    store_path: &str,
    hash: &str,
    sign_keys: &Vec<String>,
) -> Result<NarInfo, Box<dyn Error>> {
    let path_info = libnixstore::query_path_info(store_path, Radix::default())?;
    let mut res = NarInfo {
        store_path: store_path.into(),
        url: format!(
            "nar/{}.nar?hash={}",
            path_info.narhash.split_once(':').map_or(hash, |x| x.1),
            hash
        ),
        compression: "none".into(),
        nar_hash: path_info.narhash,
        nar_size: path_info.size,
        references: vec![],
        deriver: None,
        system: None,
        sigs: vec![],
        ca: path_info.ca,
    };

    let refs = path_info.refs.clone();
    if !path_info.refs.is_empty() {
        res.references = path_info
            .refs
            .into_iter()
            .filter_map(|r| extract_filename(&r))
            .collect::<Vec<String>>();
    }

    if let Some(drv) = path_info.drv {
        res.deriver = extract_filename(&drv);

        if libnixstore::is_valid_path(&drv) {
            res.system = Some(libnixstore::derivation_from_path(&drv)?.platform);
        }
    }

    for sk in sign_keys {
        let fingerprint = fingerprint_path(store_path, &res.nar_hash, res.nar_size, &refs)?;
        if let Some(fp) = fingerprint {
            res.sigs.push(libnixstore::sign_string(sk, &fp)?);
        }
    }

    if res.sigs.is_empty() {
        res.sigs.clone_from(&path_info.sigs);
    }

    Ok(res)
}

fn format_narinfo_txt(narinfo: &NarInfo) -> String {
    let mut res = vec![
        format!("StorePath: {}", narinfo.store_path),
        format!("URL: {}", narinfo.url),
        format!("Compression: {}", narinfo.compression),
        format!("FileHash: {}", narinfo.nar_hash),
        format!("FileSize: {}", narinfo.nar_size),
        format!("NarHash: {}", narinfo.nar_hash),
        format!("NarSize: {}", narinfo.nar_size),
    ];

    if !narinfo.references.is_empty() {
        res.push(format!("References: {}", &narinfo.references.join(" ")));
    }

    if let Some(drv) = &narinfo.deriver {
        res.push(format!("Deriver: {}", drv));
    }

    if let Some(sys) = &narinfo.system {
        res.push(format!("System: {}", sys));
    }

    for sig in &narinfo.sigs {
        res.push(format!("Sig: {}", sig));
    }

    if let Some(ca) = &narinfo.ca {
        res.push(format!("CA: {}", ca));
    }

    res.push("".into());
    res.join("\n")
}

pub(crate) async fn get(
    hash: web::Path<String>,
    param: web::Query<Param>,
    settings: web::Data<Config>,
) -> Result<HttpResponse, Box<dyn Error>> {
    let hash = hash.into_inner();

    if let Some(store_path) = nixhash(&hash) {
        let narinfo = query_narinfo(&store_path, &hash, &settings.secret_keys)?;
        if param.json.is_some() {
            return Ok(HttpResponse::Ok()
                .insert_header(cache_control_max_age_1d())
                .json(narinfo));
        } else {
            let res = format_narinfo_txt(&narinfo);
            return Ok(HttpResponse::Ok()
                .insert_header((http::header::CONTENT_TYPE, "text/x-nix-narinfo"))
                .insert_header(("Nix-Link", narinfo.url))
                .insert_header(cache_control_max_age_1d())
                .body(res));
        }
    }

    let header_narinfo =
        HeaderValue::from_str("text/x-nix-narinfo").expect("narinfo conversion failed for header");

    for upstream in &settings.upstreams {
        println!("Upstream {upstream:?}");

        let mut uri_parts = upstream.url.clone().into_parts();
        uri_parts.path_and_query = Some(format!("/{hash}.narinfo").parse::<PathAndQuery>()?);
        let uri = Uri::try_from(uri_parts)?;

        // TODO: connection pooling
        match Client::new()
            .request(http::Method::GET, uri)
            .timeout(Duration::from_secs(5))
            .no_decompress()
            .send()
            .await
        {
            Ok(response) => {
                if response.status() == http::StatusCode::OK {
                    // validate ct
                    let ct = match response.headers().get(http::header::CONTENT_TYPE) {
                        Some(ct) => match ct == header_narinfo {
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

                    // TODO: deserilize to json?
                    match param.json.is_some() {
                        true => {
                            eprint!("Not implemented rewriting this in json");
                            continue;
                        }
                        false => {
                            // NOTE(cobalt): We cannot verify if we stream this...; if the client has all the upstream caches we use set up as well, this should not be a problem, ergo we dont need to resign
                            return Ok(HttpResponse::Ok()
                                .insert_header(cache_control_max_age_1d())
                                .insert_header((http::header::CONTENT_TYPE, ct))
                                // .insert_header(("Nix-Link", nix_link))
                                .streaming(response));
                        }
                    }
                } else {
                    eprintln!("Failed to make cache reqest: {response:?}");
                    continue;
                }
            }
            Err(err) => {
                eprintln!("Failed to construct cache reqest: {err:?}");
                continue;
            }
        }
    }

    Ok(HttpResponse::NotFound()
        .insert_header(crate::cache_control_no_store())
        .body("missed hash"))
}
