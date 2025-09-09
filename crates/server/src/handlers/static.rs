use std::io::Cursor;
use std::net::IpAddr;
use std::str::FromStr;
use std::time::SystemTime;

use anyhow::{Context, Result};
use base64::{engine::general_purpose, Engine as _};
use camino::Utf8PathBuf;
use chrono;
use data_url::mime::Mime;
use data_url::DataUrl;
use hyper::{Body, Request, Response, StatusCode};
use image::{DynamicImage, ImageFormat};
use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Pool, Sqlite};
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use torii_processors::fetch::{fetch_content_from_http, fetch_content_from_ipfs};
use torii_sqlite::constants::TOKENS_TABLE;
use tracing::{debug, error, trace};

use super::Handler;

pub(crate) const LOG_TARGET: &str = "torii::server::handlers::static";

fn parse_image_query(query_str: &str) -> ImageQuery {
    let mut height = None;
    let mut width = None;

    for pair in query_str.split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            match key {
                "h" | "height" => {
                    if let Ok(h) = value.parse::<u32>() {
                        height = Some(h);
                    }
                }
                "w" | "width" => {
                    if let Ok(w) = value.parse::<u32>() {
                        width = Some(w);
                    }
                }
                _ => {}
            }
        }
    }

    ImageQuery { height, width }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ImageQuery {
    #[serde(alias = "h")]
    height: Option<u32>,
    #[serde(alias = "w")]
    width: Option<u32>,
}

#[derive(Debug)]
pub struct StaticHandler {
    artifacts_dir: Utf8PathBuf,
    pool: Pool<Sqlite>,
}

impl StaticHandler {
    pub fn new(artifacts_dir: Utf8PathBuf, pool: Pool<Sqlite>) -> Self {
        Self {
            artifacts_dir,
            pool,
        }
    }
}

#[async_trait::async_trait]
impl Handler for StaticHandler {
    fn should_handle(&self, req: &Request<Body>) -> bool {
        req.uri().path().starts_with("/static")
    }

    async fn handle(&self, req: Request<Body>, _client_addr: IpAddr) -> Response<Body> {
        let path = req.uri().path();

        // Remove "/static/" prefix to get the actual path
        let path = path.strip_prefix("/static/").unwrap_or("");

        // Parse query parameters
        let query = req.uri().query().unwrap_or("");
        let query = parse_image_query(query);

        match self.serve_static_file(path, query, &req).await {
            Ok(response) => response,
            Err(e) => {
                error!(target: LOG_TARGET, error = ?e, "Failed to serve static file");
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap()
            }
        }
    }
}
impl StaticHandler {
    async fn serve_static_file(
        &self,
        path: &str,
        query: ImageQuery,
        req: &Request<Body>,
    ) -> Result<Response<Body>> {
        // Split the path and validate format
        let parts: Vec<&str> = path.split('/').collect();

        if parts.len() != 3 || parts[2] != "image" {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .unwrap());
        }

        // Validate contract_address format
        if !parts[0].starts_with("0x") {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .unwrap());
        }

        // Validate token_id format
        if !parts[1].starts_with("0x") {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .unwrap());
        }

        let token_image_dir = self.artifacts_dir.join(parts[0]).join(parts[1]);
        let token_id = format!("{}:{}", parts[0], parts[1]);

        // We'll generate ETag from content hash after reading the file

        // We'll get Last-Modified from actual file metadata (matches content-based ETag approach)

        // Store conditional request headers for later comparison
        let client_etag = req
            .headers()
            .get("if-none-match")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());

        let client_modified_since = req
            .headers()
            .get("if-modified-since")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| httpdate::parse_http_date(s).ok());

        // Check if image needs to be refetched based on timestamps
        let should_fetch = if token_image_dir.exists() {
            match self
                .check_if_image_outdated(&token_image_dir, &token_id)
                .await
            {
                Ok(needs_update) => needs_update,
                Err(e) => {
                    error!(target: LOG_TARGET, error = ?e, "Failed to check image timestamps, will attempt to fetch");
                    true
                }
            }
        } else {
            true
        };

        let db_timestamp = match self.get_token_updated_at(&token_id).await {
            Ok(timestamp) => Some(timestamp),
            Err(e) => {
                debug!(target: LOG_TARGET, error = ?e, "Failed to get database timestamp");
                None
            }
        };

        if should_fetch {
            match self
                .fetch_and_process_image(&token_id, db_timestamp.as_deref())
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    error!(target: LOG_TARGET, error = ?e, "Failed to fetch and process image for token_id: {}", token_id);
                    return Ok(Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Body::empty())
                        .unwrap());
                }
            };
        }

        let file_name = match self.file_name_from_dir_and_query(token_image_dir, &query) {
            Ok(file_name) => file_name,
            Err(e) => {
                error!(target: LOG_TARGET, error = ?e, "Failed to get file name from directory and query");
                return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap());
            }
        };

        match File::open(&file_name).await {
            Ok(mut file) => {
                let mut contents = vec![];
                if file.read_to_end(&mut contents).await.is_ok() {
                    let mime = mime_guess::from_path(&file_name)
                        .first_or_octet_stream()
                        .to_string();

                    // Generate ETag from content hash
                    let mut hasher = Sha256::new();
                    hasher.update(&contents);
                    let hash_bytes = hasher.finalize();
                    let etag = format!(
                        "\"{}\"",
                        hash_bytes[..8]
                            .iter()
                            .map(|b| format!("{:02x}", b))
                            .collect::<String>()
                    );

                    // Check conditional requests now that we have the content ETag
                    if let Some(ref client_etag_str) = client_etag {
                        if client_etag_str == &etag {
                            return Ok(Response::builder()
                                .status(StatusCode::NOT_MODIFIED)
                                .header("etag", etag)
                                .header(
                                    "cache-control",
                                    "public, max-age=3600, stale-while-revalidate=86400",
                                )
                                .body(Body::empty())
                                .unwrap());
                        }
                    }

                    // Get file modification time for Last-Modified header from the file path
                    let file_last_modified = if let Ok(metadata) = std::fs::metadata(&file_name) {
                        metadata.modified().ok()
                    } else {
                        None
                    };

                    // Check If-Modified-Since against file modification time
                    if let (Some(client_time), Some(file_mod_time)) =
                        (client_modified_since, file_last_modified)
                    {
                        let server_time_secs = file_mod_time
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        let client_time_secs = client_time
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();

                        if server_time_secs <= client_time_secs {
                            return Ok(Response::builder()
                                .status(StatusCode::NOT_MODIFIED)
                                .header("etag", etag)
                                .header("last-modified", httpdate::fmt_http_date(file_mod_time))
                                .header(
                                    "cache-control",
                                    "public, max-age=3600, stale-while-revalidate=86400",
                                )
                                .body(Body::empty())
                                .unwrap());
                        }
                    }

                    // Build response with content-based ETag and file-based Last-Modified
                    let mut response_builder = Response::builder()
                        .header("content-type", mime)
                        .header("etag", etag)
                        .header(
                            "cache-control",
                            "public, max-age=3600, stale-while-revalidate=86400",
                        );

                    // Add Last-Modified header from file modification time
                    if let Some(file_mod_time) = file_last_modified {
                        response_builder = response_builder
                            .header("last-modified", httpdate::fmt_http_date(file_mod_time));
                    }

                    Ok(response_builder.body(Body::from(contents)).unwrap())
                } else {
                    Ok(Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Body::empty())
                        .unwrap())
                }
            }
            Err(_) => Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .unwrap()),
        }
    }

    fn file_name_from_dir_and_query(
        &self,
        token_image_dir: Utf8PathBuf,
        query: &ImageQuery,
    ) -> Result<Utf8PathBuf> {
        let mut entries = std::fs::read_dir(&token_image_dir)
            .ok()
            .into_iter()
            .flatten()
            .flatten();

        // Find the base image (without @medium or @small)
        let base_image = entries
            .find(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .map(|name| name.starts_with("image") && !name.contains('@'))
                    .unwrap_or(false)
            })
            .with_context(|| "Failed to find base image")?;

        let base_filename = base_image.file_name();
        let base_filename = base_filename.to_str().unwrap();
        let base_ext = base_filename.split('.').next_back().unwrap();

        let suffix = match (query.width, query.height) {
            // If either dimension is <= 100px, use small version
            (Some(w), _) if w <= 100 => "@small",
            (_, Some(h)) if h <= 100 => "@small",
            // If either dimension is <= 250px, use medium version
            (Some(w), _) if w <= 250 => "@medium",
            (_, Some(h)) if h <= 250 => "@medium",
            // If no dimensions specified or larger than 250px, use original
            _ => "",
        };

        let target_filename = format!("image{}.{}", suffix, base_ext);
        Ok(token_image_dir.join(target_filename))
    }

    async fn get_token_updated_at(&self, token_id: &str) -> Result<String> {
        let query = sqlx::query_as::<_, (String,)>(&format!(
            "SELECT updated_at FROM {TOKENS_TABLE} WHERE id = ?"
        ))
        .bind(token_id)
        .fetch_one(&self.pool)
        .await
        .context("Failed to fetch updated_at from database")?;

        Ok(query.0)
    }

    async fn check_if_image_outdated(
        &self,
        token_image_dir: &Utf8PathBuf,
        token_id: &str,
    ) -> Result<bool> {
        // Find the base image file in the directory
        let mut entries = match std::fs::read_dir(token_image_dir) {
            Ok(entries) => entries,
            Err(_) => return Ok(true), // Directory doesn't exist, need to fetch
        };

        let base_image_file = entries.find_map(|entry| {
            let entry = entry.ok()?;
            let file_name = entry.file_name();
            let file_name_str = file_name.to_str()?;
            if file_name_str.starts_with("image") && !file_name_str.contains('@') {
                Some(entry.path())
            } else {
                None
            }
        });

        let existing_image_path = match base_image_file {
            Some(path) => path,
            None => return Ok(true), // No existing image, need to fetch
        };

        // Get file modification time
        let file_modified_time = match std::fs::metadata(&existing_image_path) {
            Ok(metadata) => match metadata.modified() {
                Ok(time) => time,
                Err(_) => return Ok(true), // Can't get file time, refetch
            },
            Err(_) => return Ok(true), // Can't read file metadata, refetch
        };

        // Get token updated_at timestamp from database
        let db_timestamp = self.get_token_updated_at(token_id).await?;

        // Parse the database timestamp format: "2025-09-09 11:46:17"
        let db_updated_time = match chrono::NaiveDateTime::parse_from_str(
            &db_timestamp,
            "%Y-%m-%d %H:%M:%S",
        ) {
            Ok(naive_dt) => {
                let timestamp_utc = naive_dt.and_utc();
                SystemTime::from(timestamp_utc)
            }
            Err(_) => {
                // If we can't parse the timestamp, assume we need to refetch
                debug!(target: LOG_TARGET, "Failed to parse updated_at timestamp: {}", db_timestamp);
                return Ok(true);
            }
        };

        // Compare timestamps - refetch if database was updated after file
        let needs_refetch = db_updated_time > file_modified_time;
        Ok(needs_refetch)
    }

    async fn patch_svg_images_regex(&self, svg_data: &[u8]) -> anyhow::Result<Vec<u8>> {
        let svg_str = std::str::from_utf8(svg_data)?;
        // Regex for href and xlink:href in <image ...> tags
        let re = Regex::new(r#"(href|xlink:href)\s*=\s*["']([^"']+)["']"#).unwrap();

        let mut patched_svg = String::with_capacity(svg_str.len());
        let mut last_end = 0;

        for cap in re.captures_iter(svg_str) {
            let m = cap.get(0).unwrap();
            let attr_name = &cap[1];
            let href = &cap[2];

            patched_svg.push_str(&svg_str[last_end..m.start()]);

            // Only patch if not already a data URI
            if href.starts_with("data:") {
                patched_svg.push_str(m.as_str());
            } else {
                // Fetch the image bytes using your fetchers
                let image_bytes = if href.starts_with("http://") || href.starts_with("https://") {
                    fetch_content_from_http(href).await?
                } else if href.starts_with("ipfs://") {
                    let cid = href.strip_prefix("ipfs://").unwrap();
                    fetch_content_from_ipfs(cid).await?
                } else {
                    // fallback: leave as is
                    patched_svg.push_str(m.as_str());
                    last_end = m.end();
                    continue;
                };
                let mime = mime_guess::from_path(href).first_or_octet_stream();
                let b64 = general_purpose::STANDARD.encode(&image_bytes);
                let data_uri = format!("{}=\"data:{};base64,{}\"", attr_name, mime, b64);
                patched_svg.push_str(&data_uri);
            }
            last_end = m.end();
        }
        patched_svg.push_str(&svg_str[last_end..]);
        Ok(patched_svg.into_bytes())
    }

    fn set_file_timestamp(
        &self,
        file_path: &std::path::Path,
        timestamp_str: &str,
    ) -> anyhow::Result<()> {
        use filetime::{set_file_times, FileTime};

        // Parse database timestamp format: "2025-09-09 11:46:17"
        let timestamp = chrono::NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S")
            .context("Failed to parse timestamp")?;

        // Assume UTC timezone for database timestamps
        let timestamp_utc = timestamp.and_utc();
        let system_time = SystemTime::from(timestamp_utc);
        let file_time = FileTime::from_system_time(system_time);

        // Set both access and modification times to the database timestamp
        set_file_times(file_path, file_time, file_time).context("Failed to set file times")?;

        Ok(())
    }

    async fn fetch_and_process_image(
        &self,
        token_id: &str,
        db_timestamp: Option<&str>,
    ) -> anyhow::Result<String> {
        let query = sqlx::query_as::<_, (String,)>(&format!(
            "SELECT metadata FROM {TOKENS_TABLE} WHERE id = ?"
        ))
        .bind(token_id)
        .fetch_one(&self.pool)
        .await
        .context("Failed to fetch metadata from database")?;

        let metadata: serde_json::Value =
            serde_json::from_str(&query.0).context("Failed to parse metadata")?;
        let image_uri = metadata
            .get("image")
            .context("Image URL not found in metadata")?
            .as_str()
            .context("Image field not a string")?
            .to_string();

        let image_type = match &image_uri {
            uri if uri.starts_with("http") || uri.starts_with("https") => {
                debug!(image_uri = %uri, "Fetching image from http/https URL");
                // Fetch image from HTTP/HTTPS URL
                let response = fetch_content_from_http(uri)
                    .await
                    .context("Failed to fetch image from URL")?;

                // svg files typically start with <svg or <?xml
                if response.starts_with(b"<svg") || response.starts_with(b"<?xml") {
                    ErcImageType::Svg(response.to_vec())
                } else {
                    let format = image::guess_format(&response).with_context(|| {
                        format!(
                            "Unknown file format for token_id: {}, data: {:?}",
                            token_id, &response
                        )
                    })?;
                    ErcImageType::DynamicImage((
                        image::load_from_memory_with_format(&response, format)
                            .context("Failed to load image from bytes")?,
                        format,
                    ))
                }
            }
            uri if uri.starts_with("ipfs") => {
                debug!(image_uri = %uri, "Fetching image from IPFS");
                let cid = uri.strip_prefix("ipfs://").unwrap();
                let response = fetch_content_from_ipfs(cid)
                    .await
                    .context("Failed to read image bytes from IPFS response")?;

                if response.starts_with(b"<svg") || response.starts_with(b"<?xml") {
                    ErcImageType::Svg(response.to_vec())
                } else {
                    let format = image::guess_format(&response).with_context(|| {
                        format!(
                            "Unknown file format for token_id: {}, cid: {}, data: {:?}",
                            token_id, cid, &response
                        )
                    })?;
                    ErcImageType::DynamicImage((
                        image::load_from_memory_with_format(&response, format)
                            .context("Failed to load image from bytes")?,
                        format,
                    ))
                }
            }
            uri if uri.starts_with("data") => {
                debug!("Parsing image from data URI");
                trace!(data_uri = %uri);
                // Parse and decode data URI
                let data_url = DataUrl::process(uri).context("Failed to parse data URI")?;

                // Check if it's an SVG
                if data_url.mime_type() == &Mime::from_str("image/svg+xml").unwrap() {
                    let decoded = data_url
                        .decode_to_vec()
                        .context("Failed to decode data URI")?;
                    ErcImageType::Svg(decoded.0)
                } else {
                    let decoded = data_url
                        .decode_to_vec()
                        .context("Failed to decode data URI")?;
                    let format = image::guess_format(&decoded.0).with_context(|| {
                        format!("Unknown file format for token_id: {}", token_id)
                    })?;
                    ErcImageType::DynamicImage((
                        image::load_from_memory_with_format(&decoded.0, format)
                            .context("Failed to load image from bytes")?,
                        format,
                    ))
                }
            }
            uri => {
                return Err(anyhow::anyhow!("Unsupported URI scheme: {}", uri));
            }
        };

        // Extract contract_address and token_id from token_id
        let parts: Vec<&str> = token_id.split(':').collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!(
                "token_id must be in format contract_address:token_id"
            ));
        }
        let contract_address = parts[0];
        let token_id_part = parts[1];

        let dir_path = self
            .artifacts_dir
            .join(contract_address)
            .join(token_id_part);

        // Create directories if they don't exist
        fs::create_dir_all(&dir_path)
            .await
            .context("Failed to create directories for image storage")?;

        // Define base image name
        let base_image_name = "image";

        let relative_path = Utf8PathBuf::new()
            .join(contract_address)
            .join(token_id_part);

        match image_type {
            ErcImageType::DynamicImage((img, format)) => {
                let format_ext = format.extensions_str()[0];

                let target_sizes = [("medium", 250, 250), ("small", 100, 100)];

                // Save original image
                let original_file_name = format!("{}.{}", base_image_name, format_ext);
                let original_file_path = dir_path.join(&original_file_name);
                let mut file = fs::File::create(&original_file_path)
                    .await
                    .with_context(|| format!("Failed to create file: {:?}", original_file_path))?;
                let encoded_image = self
                    .encode_image_to_vec(&img, format)
                    .with_context(|| format!("Failed to encode image: {:?}", original_file_path))?;
                file.write_all(&encoded_image).await.with_context(|| {
                    format!("Failed to write image to file: {:?}", original_file_path)
                })?;

                // Set file timestamp to match database timestamp for outdated check
                if let Some(timestamp) = db_timestamp {
                    if let Err(e) =
                        self.set_file_timestamp(original_file_path.as_std_path(), timestamp)
                    {
                        debug!(target: LOG_TARGET, error = ?e, "Failed to set file timestamp");
                    }
                }

                // Save resized images
                for (label, max_width, max_height) in &target_sizes {
                    let resized_image = self.resize_image_to_fit(&img, *max_width, *max_height);
                    let file_name = format!("@{}.{}", label, format_ext);
                    let file_path = dir_path.join(format!("{}{}", base_image_name, file_name));
                    let mut file = fs::File::create(&file_path)
                        .await
                        .with_context(|| format!("Failed to create file: {:?}", file_path))?;
                    let encoded_image = self
                        .encode_image_to_vec(&resized_image, format)
                        .context("Failed to encode image")?;
                    file.write_all(&encoded_image).await.with_context(|| {
                        format!("Failed to write image to file: {:?}", file_path)
                    })?;

                    // Set file timestamp to match database timestamp for outdated check
                    if let Some(timestamp) = db_timestamp {
                        if let Err(e) = self.set_file_timestamp(file_path.as_std_path(), timestamp)
                        {
                            debug!(target: LOG_TARGET, error = ?e, "Failed to set file timestamp for resized image");
                        }
                    }
                }

                // No need to store hash files anymore - we use timestamp comparison

                Ok(format!("{}/{}", relative_path, base_image_name))
            }
            ErcImageType::Svg(svg_data) => {
                // Patch SVG to embed images
                let patched_svg = self.patch_svg_images_regex(&svg_data).await?;
                let file_name = format!("{}.svg", base_image_name);
                let file_path = dir_path.join(&file_name);
                // Save the patched SVG file
                let mut file = File::create(&file_path)
                    .await
                    .with_context(|| format!("Failed to create file: {:?}", file_path))?;
                file.write_all(&patched_svg)
                    .await
                    .with_context(|| format!("Failed to write SVG to file: {:?}", file_path))?;

                // Set file timestamp to match database timestamp for outdated check
                if let Some(timestamp) = db_timestamp {
                    if let Err(e) = self.set_file_timestamp(file_path.as_std_path(), timestamp) {
                        debug!(target: LOG_TARGET, error = ?e, "Failed to set file timestamp for SVG");
                    }
                }
                Ok(format!("{}/{}", relative_path, file_name))
            }
        }
    }

    fn resize_image_to_fit(
        &self,
        image: &DynamicImage,
        max_width: u32,
        max_height: u32,
    ) -> DynamicImage {
        image.resize_to_fill(max_width, max_height, image::imageops::FilterType::Lanczos3)
    }

    fn encode_image_to_vec(&self, image: &DynamicImage, format: ImageFormat) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        image
            .write_to(&mut Cursor::new(&mut buf), format)
            .with_context(|| "Failed to encode image")?;
        Ok(buf)
    }
}

#[derive(Debug)]
pub enum ErcImageType {
    DynamicImage((DynamicImage, ImageFormat)),
    Svg(Vec<u8>),
}
