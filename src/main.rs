use futures_util::StreamExt;
use actix_web::{web, App, HttpResponse, HttpServer};
use azure_storage_blobs::prelude::*;
use azure_storage_blobs::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio;
use std::env;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use anyhow::Result;
use dotenv::dotenv;
use std::io::Write;
use azure_identity;
use azure_storage::StorageCredentials;
use url::Url;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Content {
    title: String,
    year: Option<String>,
    rating: Option<f32>,
    genre: Vec<String>,
    description: String,
    where_to_watch: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct UserPreferences {
    favorite_genres: Vec<String>,
    minimum_rating: f32,
}

// In-memory cache to reduce storage operations
struct ContentCache {
    data: HashMap<String, Vec<Content>>,
    last_updated: chrono::DateTime<chrono::Utc>,
}

impl ContentCache {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
            last_updated: chrono::Utc::now(),
        }
    }

    fn needs_update(&self) -> bool {
        let now = chrono::Utc::now();
        now.signed_duration_since(self.last_updated).num_hours() > 12
    }
}

struct ContentService {
    blob_client: ContainerClient,
    cache: Arc<RwLock<ContentCache>>,
    tmdb_api_key: String,
}

impl ContentService {
    async fn new() -> Result<Self> {
        // Get TMDB API key
        let tmdb_api_key = env::var("TMDB_API_KEY")?;

        // Get Azure Storage connection string
        let connection_string = env::var("AZURE_STORAGE_CONNECTION_STRING")
            .expect("AZURE_STORAGE_CONNECTION_STRING must be set");

        println!("Parsing connection string...");

        // Parse connection string components
        let mut parts = std::collections::HashMap::new();
        for part in connection_string.split(';') {
            if let Some((key, value)) = part.split_once('=') {
                println!("Found connection string part: {} = {}", key,
                         if key == "AccountKey" { "***" } else { value });
                parts.insert(key, value);
            }
        }

        // Extract required values
        let account = parts.get("AccountName")
            .ok_or_else(|| anyhow::anyhow!("AccountName not found"))?;
        let key = parts.get("AccountKey")
            .ok_or_else(|| anyhow::anyhow!("AccountKey not found"))?;

        println!("Account: {}", account);
        println!("Key length: {}", key.len());

        // Create credentials first
        let credentials = StorageCredentials::access_key(account.to_string(), key.to_string());
        println!("Created credentials");

        // Create the service client with just the account name
        let blob_service_client = BlobServiceClient::new(account.to_string(), credentials);
        println!("Created blob service client");

        let container_name = "content-data";
        let container_client = blob_service_client.container_client(container_name);
        println!("Created container client for: {}", container_name);

        // Try to list containers first
        println!("\nListing all containers to test connectivity...");
        let mut containers = blob_service_client.list_containers()
            .into_stream();

        let mut found = false;
        while let Some(container_result) = containers.next().await {
            match container_result {
                Ok(response) => {
                    for container in response.containers {
                        println!("Found container: {}", container.name);
                        println!("  Last modified: {}", container.last_modified);
                        println!("  Public access: {:?}", container.public_access);
                        println!("  Lease status: {:?}", container.lease_status);

                        if container.name == container_name {
                            found = true;
                            println!("Target container already exists");
                        }
                    }

                    if let Some(marker) = response.next_marker {
                        println!("More containers available, next marker: {}", marker);
                    }
                },
                Err(e) => {
                    println!("Error listing containers: {}", e);
                    println!("Full error details: {:?}", e);
                }
            }
        }

        if !found {
            println!("\nTarget container not found, attempting to create it...");
            match container_client.create()
                .public_access(PublicAccess::None)
                .await
            {
                Ok(_) => println!("Container created successfully"),
                Err(e) => {
                    println!("Error creating container: {}", e);
                    println!("Full error details: {:?}", e);
                    return Err(anyhow::anyhow!("Failed to create container: {}", e));
                }
            }
        }

        Ok(Self {
            blob_client: container_client,
            cache: Arc::new(RwLock::new(ContentCache::new())),
            tmdb_api_key,
        })
    }

    async fn scrape_content(&self) -> Result<Vec<Content>> {
        let client = reqwest::Client::new();
        let mut all_content = Vec::new();

        // Create authorization header
        let auth_header = format!("Bearer {}", self.tmdb_api_key);

        // Get trending movies from TMDB
        let movies_url = "https://api.themoviedb.org/3/trending/movie/week?language=en-US";

        println!("Fetching trending movies...");
        let response = client.get(movies_url)
            .header("Authorization", &auth_header)
            .header("accept", "application/json")
            .send()
            .await?;

        println!("Movies API response status: {}", response.status());

        if response.status().is_success() {
            let data: Value = response.json().await?;
            if let Some(results) = data["results"].as_array() {
                println!("Found {} movies in response", results.len());
                for movie in results {
                    let movie_id = movie["id"].as_i64().unwrap_or_default();
                    println!("Processing movie ID: {}", movie_id);

                    let genres = self.get_movie_genres(&client, movie_id, &auth_header).await.unwrap_or_default();
                    let providers = self.get_watch_providers(&client, "movie", movie_id, &auth_header).await.unwrap_or_default();

                    let content = Content {
                        title: movie["title"].as_str().unwrap_or_default().to_string(),
                        year: movie["release_date"]
                            .as_str()
                            .and_then(|d| d.split('-').next())
                            .map(String::from),
                        rating: movie["vote_average"].as_f64().map(|r| r as f32),
                        genre: genres,
                        description: movie["overview"].as_str().unwrap_or_default().to_string(),
                        where_to_watch: providers,
                    };
                    println!("Added movie: {}", content.title);
                    all_content.push(content);
                }
            }
        } else {
            println!("Failed to get movies. Status: {}", response.status());
            let error_text = response.text().await?;
            println!("Error response: {}", error_text);
        }

        // Get trending TV shows from TMDB
        let tv_url = "https://api.themoviedb.org/3/trending/tv/week?language=en-US";

        println!("Fetching trending TV shows...");
        let response = client.get(tv_url)
            .header("Authorization", &auth_header)
            .header("accept", "application/json")
            .send()
            .await?;

        println!("TV API response status: {}", response.status());

        if response.status().is_success() {
            let data: Value = response.json().await?;
            if let Some(results) = data["results"].as_array() {
                println!("Found {} TV shows in response", results.len());
                for show in results {
                    let show_id = show["id"].as_i64().unwrap_or_default();
                    println!("Processing TV show ID: {}", show_id);

                    let genres = self.get_tv_genres(&client, show_id, &auth_header).await.unwrap_or_default();
                    let providers = self.get_watch_providers(&client, "tv", show_id, &auth_header).await.unwrap_or_default();

                    let content = Content {
                        title: show["name"].as_str().unwrap_or_default().to_string(),
                        year: show["first_air_date"]
                            .as_str()
                            .and_then(|d| d.split('-').next())
                            .map(String::from),
                        rating: show["vote_average"].as_f64().map(|r| r as f32),
                        genre: genres,
                        description: show["overview"].as_str().unwrap_or_default().to_string(),
                        where_to_watch: providers,
                    };
                    println!("Added TV show: {}", content.title);
                    all_content.push(content);
                }
            }
        } else {
            println!("Failed to get TV shows. Status: {}", response.status());
            let error_text = response.text().await?;
            println!("Error response: {}", error_text);
        }

        println!("Scraped {} items total", all_content.len());
        Ok(all_content)
    }

    async fn get_movie_genres(&self, client: &reqwest::Client, movie_id: i64, auth_header: &str) -> Result<Vec<String>> {
        let url = format!(
            "https://api.themoviedb.org/3/movie/{}?language=en-US",
            movie_id
        );

        let response = client.get(&url)
            .header("Authorization", auth_header)
            .header("accept", "application/json")
            .send()
            .await?;

        let mut genres = Vec::new();

        if response.status().is_success() {
            let data: Value = response.json().await?;
            if let Some(genre_array) = data["genres"].as_array() {
                for genre in genre_array {
                    if let Some(name) = genre["name"].as_str() {
                        genres.push(name.to_string());
                    }
                }
            }
        }

        Ok(genres)
    }

    async fn get_tv_genres(&self, client: &reqwest::Client, tv_id: i64, auth_header: &str) -> Result<Vec<String>> {
        let url = format!(
            "https://api.themoviedb.org/3/tv/{}?language=en-US",
            tv_id
        );

        let response = client.get(&url)
            .header("Authorization", auth_header)
            .header("accept", "application/json")
            .send()
            .await?;

        let mut genres = Vec::new();

        if response.status().is_success() {
            let data: Value = response.json().await?;
            if let Some(genre_array) = data["genres"].as_array() {
                for genre in genre_array {
                    if let Some(name) = genre["name"].as_str() {
                        genres.push(name.to_string());
                    }
                }
            }
        }

        Ok(genres)
    }

    async fn get_watch_providers(&self, client: &reqwest::Client, media_type: &str, id: i64, auth_header: &str) -> Result<Vec<String>> {
        let url = format!(
            "https://api.themoviedb.org/3/{}/{}/watch/providers",
            media_type, id
        );

        let response = client.get(&url)
            .header("Authorization", auth_header)
            .header("accept", "application/json")
            .send()
            .await?;

        let mut providers = Vec::new();

        if response.status().is_success() {
            let data: Value = response.json().await?;
            if let Some(us_data) = data.get("results").and_then(|r| r.get("US")) {
                for provider_type in ["flatrate", "free"].iter() {
                    if let Some(provider_list) = us_data.get(provider_type).and_then(|p| p.as_array()) {
                        for provider in provider_list {
                            if let Some(name) = provider.get("provider_name").and_then(|n| n.as_str()) {
                                providers.push(name.to_string());
                            }
                        }
                    }
                }
            }
        }

        Ok(providers)
    }

    async fn update_content(&self) -> Result<()> {
        {
            let cache = self.cache.read();
            if !cache.needs_update() {
                return Ok(());
            }
        }

        println!("Starting content scraping...");
        let content = self.scrape_content().await?;
        println!("Scraped {} items", content.len());

        let json = serde_json::to_string(&content)?;
        println!("JSON serialized, size: {} bytes", json.len());

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(json.as_bytes())?;
        let compressed = encoder.finish()?;
        println!("Compressed size: {} bytes", compressed.len());

        let blob_client = self.blob_client.blob_client("latest.json.gz");
        blob_client.put_block_blob(compressed).into_future().await?;
        println!("Successfully uploaded to blob storage");

        let mut cache = self.cache.write();
        cache.data.insert("latest".to_string(), content);
        cache.last_updated = chrono::Utc::now();
        println!("Cache updated");

        Ok(())
    }

    // Modify the get_recommendations method in ContentService to add retry logic and fallback
    async fn get_recommendations(&self, prefs: &UserPreferences) -> Result<Vec<Content>> {
        // First try cache
        {
            let cache = self.cache.read();
            if let Some(content) = cache.data.get("latest") {
                return Ok(content.iter()
                    .filter(|c| {
                        c.rating.unwrap_or(0.0) >= prefs.minimum_rating &&
                            c.genre.iter().any(|g| prefs.favorite_genres.contains(g))
                    })
                    .cloned()
                    .collect());
            }
        }

        // If not in cache, try to load from blob with retries
        let blob_client = self.blob_client.blob_client("latest.json.gz");
        let mut retry_count = 0;
        let max_retries = 3;

        while retry_count < max_retries {
            match blob_client.get_properties().await {
                Ok(_) => {
                    let mut stream = blob_client.get().into_stream();
                    let mut data = Vec::new();

                    use futures_util::StreamExt;
                    while let Some(chunk) = stream.next().await {
                        match chunk {
                            Ok(chunk) => {
                                let bytes = chunk.data.collect().await?;
                                data.extend(bytes);
                            },
                            Err(e) => {
                                eprintln!("Error reading chunk: {}", e);
                                break;
                            }
                        }
                    }

                    if !data.is_empty() {
                        match self.process_blob_data(&data) {
                            Ok(content) => return Ok(content.into_iter()
                                .filter(|c| {
                                    c.rating.unwrap_or(0.0) >= prefs.minimum_rating &&
                                        c.genre.iter().any(|g| prefs.favorite_genres.contains(g))
                                })
                                .collect()),
                            Err(e) => eprintln!("Error processing blob data: {}", e)
                        }
                    }
                },
                Err(e) => {
                    eprintln!("Failed to load from blob storage (attempt {}): {}", retry_count + 1, e);
                }
            }

            retry_count += 1;
            if retry_count < max_retries {
                // Exponential backoff
                tokio::time::sleep(tokio::time::Duration::from_secs(2u64.pow(retry_count as u32))).await;
            }
        }

        // If we couldn't load from blob storage, try to fetch fresh content
        println!("Blob storage access failed, fetching fresh content...");
        match self.scrape_content().await {
            Ok(content) => {
                // Update cache with fresh content
                let mut cache = self.cache.write();
                cache.data.insert("latest".to_string(), content.clone());
                cache.last_updated = chrono::Utc::now();

                Ok(content.into_iter()
                    .filter(|c| {
                        c.rating.unwrap_or(0.0) >= prefs.minimum_rating &&
                            c.genre.iter().any(|g| prefs.favorite_genres.contains(g))
                    })
                    .collect())
            },
            Err(e) => {
                eprintln!("Failed to fetch fresh content: {}", e);
                Ok(vec![]) // Return empty vector as last resort
            }
        }
    }

    // Add this helper method to ContentService
    fn process_blob_data(&self, data: &[u8]) -> Result<Vec<Content>> {
        let mut decoder = flate2::read::GzDecoder::new(data);
        let mut decompressed = String::new();
        std::io::Read::read_to_string(&mut decoder, &mut decompressed)?;

        let content: Vec<Content> = serde_json::from_str(&decompressed)?;

        // Update cache
        let mut cache = self.cache.write();
        cache.data.insert("latest".to_string(), content.clone());
        cache.last_updated = chrono::Utc::now();

        Ok(content)
    }
}

async fn get_recommendations(
    prefs: web::Json<UserPreferences>,
    service: web::Data<ContentService>,
) -> HttpResponse {
    match service.get_recommendations(&prefs).await {
        Ok(content) => HttpResponse::Ok().json(content),
        Err(e) => {
            eprintln!("Error getting recommendations: {}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

#[actix_web::main]
async fn main() -> Result<()> {
    dotenv().ok();

    // Ensure TMDB_API_KEY is set
    if env::var("TMDB_API_KEY").is_err() {
        eprintln!("TMDB_API_KEY must be set in environment");
        std::process::exit(1);
    }

    let service = ContentService::new().await?;

    // Perform initial content update
    println!("Performing initial content update...");
    service.update_content().await?;
    println!("Initial content update completed");

    let service = web::Data::new(service);
    let service_clone = service.clone();

    // Update content periodically
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(12 * 3600)).await;
            println!("Starting periodic content update...");
            if let Err(e) = service_clone.update_content().await {
                eprintln!("Error updating content: {}", e);
            }
        }
    });

    println!("Starting HTTP server on 0.0.0.0:8080");
    HttpServer::new(move || {
        App::new()
            .app_data(service.clone())
            .route("/recommendations", web::post().to(get_recommendations))
    })
        .bind("0.0.0.0:8080")?
        .run()
        .await?;

    Ok(())
}