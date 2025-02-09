use actix_web::{web, App, HttpResponse, HttpServer};
use azure_storage_blob::prelude::*;
use serde::{Deserialize, Serialize};
use tokio;
use std::env;
use std::collections::HashMap;
use anyhow::Result;
use dotenv::dotenv;

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
    blob_client: BlobClient,
    cache: ContentCache,
}

impl ContentService {
    async fn new() -> Result<Self> {
        // Use Azure Storage (has a good free tier)
        let blob_client = BlobClient::from_connection_string(
            &env::var("AZURE_STORAGE_CONNECTION_STRING")?,
            "content-data",
            BlobClientOptions::default(),
        )?;

        Ok(Self {
            blob_client,
            cache: ContentCache::new(),
        })
    }

    async fn scrape_content(&self) -> Result<Vec<Content>> {
        let client = reqwest::Client::builder()
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            .build()?;

        // Scrape from free APIs and public data
        let mut content = Vec::new();

        // Example: Scrape from JustWatch new releases page
        let response = client
            .get("https://www.justwatch.com/us/new")
            .send()
            .await?;

        let text = response.text().await?;
        // Parse HTML and extract content...

        // Store only essential data to save space
        let filtered_content: Vec<Content> = content.into_iter()
            .map(|mut c| {
                // Trim description to save space
                c.description = c.description.chars().take(200).collect();
                c
            })
            .collect();

        Ok(filtered_content)
    }

    async fn update_content(&mut self) -> Result<()> {
        if !self.cache.needs_update() {
            return Ok(());
        }

        // Scrape new content
        let content = self.scrape_content().await?;

        // Store in blob storage, using compression
        let json = serde_json::to_string(&content)?;
        let compressed = deflate::deflate_bytes(json.as_bytes());

        self.blob_client
            .container("content")
            .blob("latest.json.gz")
            .put_blob(compressed)
            .await?;

        // Update cache
        self.cache.data.insert("latest".to_string(), content);
        self.cache.last_updated = chrono::Utc::now();

        Ok(())
    }

    async fn get_recommendations(&self, prefs: &UserPreferences) -> Vec<Content> {
        // First try cache
        if let Some(content) = self.cache.data.get("latest") {
            return content.iter()
                .filter(|c| {
                    c.rating.unwrap_or(0.0) >= prefs.minimum_rating &&
                        c.genre.iter().any(|g| prefs.favorite_genres.contains(g))
                })
                .cloned()
                .collect();
        }

        // If not in cache, load from blob
        vec![] // Return empty if failed to load
    }
}

// API endpoints
async fn get_recommendations(
    prefs: web::Json<UserPreferences>,
    service: web::Data<ContentService>,
) -> HttpResponse {
    match service.get_recommendations(&prefs).await {
        content => HttpResponse::Ok().json(content),
    }
}

#[actix_web::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let mut service = ContentService::new().await?;
    let service = web::Data::new(service);
    let service_clone = service.clone();

    // Update content periodically
    tokio::spawn(async move {
        loop {
            if let Err(e) = service_clone.update_content().await {
                eprintln!("Error updating content: {}", e);
            }
            // Only update every 12 hours to stay within API limits
            tokio::time::sleep(tokio::time::Duration::from_secs(12 * 3600)).await;
        }
    });

    // Start HTTP server
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