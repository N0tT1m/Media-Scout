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

        // Parse the connection string parts into owned strings
        let account = connection_string
            .split(';')
            .find(|s| s.starts_with("AccountName="))
            .and_then(|s| s.split('=').nth(1))
            .ok_or_else(|| anyhow::anyhow!("AccountName not found in connection string"))?
            .to_string();

        let key = connection_string
            .split(';')
            .find(|s| s.starts_with("AccountKey="))
            .and_then(|s| s.split('=').nth(1))
            .ok_or_else(|| anyhow::anyhow!("AccountKey not found in connection string"))?
            .to_string();

        // Create the storage URL
        let url = format!("https://{}.blob.core.windows.net", account);

        // Create credentials using owned values
        let credentials = StorageCredentials::access_key(account.clone(), key);

        // Create the blob client
        let blob_client = BlobServiceClient::new(&url, credentials)
            .container_client("content-data");

        // Create container if it doesn't exist
        match blob_client.create().await {
            Ok(_) => println!("Container created or already exists"),
            Err(e) => eprintln!("Warning: Could not create container: {}", e),
        }

        Ok(Self {
            blob_client,
            cache: Arc::new(RwLock::new(ContentCache::new())),
            tmdb_api_key,
        })
    }

    async fn scrape_content(&self) -> Result<Vec<Content>> {
        let client = reqwest::Client::new();
        let mut all_content = Vec::new();

        // Get trending movies from TMDB
        let movies_url = format!(
            "https://api.themoviedb.org/3/trending/movie/week?api_key={}&language=en-US",
            self.tmdb_api_key
        );

        let response = client.get(&movies_url).send().await?;
        if response.status().is_success() {
            let data: Value = response.json().await?;
            if let Some(results) = data["results"].as_array() {
                for movie in results {
                    let genres = self.get_movie_genres(&client, movie["id"].as_i64().unwrap_or_default()).await?;
                    let providers = self.get_watch_providers(&client, "movie", movie["id"].as_i64().unwrap_or_default()).await?;

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
                    all_content.push(content);
                }
            }
        }

        // Get trending TV shows from TMDB
        let tv_url = format!(
            "https://api.themoviedb.org/3/trending/tv/week?api_key={}&language=en-US",
            self.tmdb_api_key
        );

        let response = client.get(&tv_url).send().await?;
        if response.status().is_success() {
            let data: Value = response.json().await?;
            if let Some(results) = data["results"].as_array() {
                for show in results {
                    let genres = self.get_tv_genres(&client, show["id"].as_i64().unwrap_or_default()).await?;
                    let providers = self.get_watch_providers(&client, "tv", show["id"].as_i64().unwrap_or_default()).await?;

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
                    all_content.push(content);
                }
            }
        }

        println!("Scraped {} items total", all_content.len());
        Ok(all_content)
    }

    async fn get_movie_genres(&self, client: &reqwest::Client, movie_id: i64) -> Result<Vec<String>> {
        let url = format!(
            "https://api.themoviedb.org/3/movie/{}?api_key={}&language=en-US",
            movie_id, self.tmdb_api_key
        );

        let response = client.get(&url).send().await?;
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

    async fn get_tv_genres(&self, client: &reqwest::Client, tv_id: i64) -> Result<Vec<String>> {
        let url = format!(
            "https://api.themoviedb.org/3/tv/{}?api_key={}&language=en-US",
            tv_id, self.tmdb_api_key
        );

        let response = client.get(&url).send().await?;
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

    async fn get_watch_providers(&self, client: &reqwest::Client, media_type: &str, id: i64) -> Result<Vec<String>> {
        let url = format!(
            "https://api.themoviedb.org/3/{}/{}/watch/providers?api_key={}",
            media_type, id, self.tmdb_api_key
        );

        let response = client.get(&url).send().await?;
        let mut providers = Vec::new();

        if response.status().is_success() {
            let data: Value = response.json().await?;
            // Look for US providers first
            if let Some(results) = data["results"]["US"]["flatrate"].as_array() {
                for provider in results {
                    if let Some(name) = provider["provider_name"].as_str() {
                        providers.push(name.to_string());
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

        // If not in cache, load from blob
        let blob_client = self.blob_client.blob_client("latest.json.gz");
        match blob_client.get_properties().await {
            Ok(_) => {
                let mut stream = blob_client.get().into_stream();
                let mut data = Vec::new();

                use futures_util::StreamExt;
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk?;
                    let bytes = chunk.data.collect().await?;
                    data.extend(bytes);
                }

                let mut decoder = flate2::read::GzDecoder::new(&data[..]);
                let mut decompressed = String::new();
                std::io::Read::read_to_string(&mut decoder, &mut decompressed)?;

                let content: Vec<Content> = serde_json::from_str(&decompressed)?;

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
                eprintln!("Failed to load from blob storage: {}", e);
                Ok(vec![])
            }
        }
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