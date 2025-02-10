use futures_util::StreamExt;
use actix_web::{web, App, HttpResponse, HttpServer};
use actix_cors::Cors;
use azure_storage_blobs::prelude::*;
use azure_storage_blobs::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio;
use std::env;
use std::collections::{HashMap, HashSet};
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

// Add this new struct for tracking already seen content
#[derive(Debug)]
struct ContentTracker {
    seen_ids: std::collections::HashSet<i64>,
}

impl ContentTracker {
    fn new() -> Self {
        Self {
            seen_ids: std::collections::HashSet::new(),
        }
    }

    fn is_new(&mut self, id: i64) -> bool {
        self.seen_ids.insert(id)
    }
}

// First, modify the ContentCache struct to track used recommendations
struct ContentCache {
    data: HashMap<String, Vec<Content>>,
    used_recommendations: HashMap<String, HashSet<String>>, // Track used content by user
    last_updated: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CacheData {
    content: Vec<Content>,
    used_recommendations: HashMap<String, HashSet<String>>,
    last_updated: chrono::DateTime<chrono::Utc>,
}


impl ContentCache {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
            used_recommendations: HashMap::new(),
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

    async fn fetch_movies(&self, client: &reqwest::Client, auth_header: &str,
                          tracker: &mut ContentTracker, url: String) -> Result<Vec<Content>> {
        let mut movies = Vec::new();

        println!("Fetching movies from: {}", url);
        let response = client.get(&url)
            .header("Authorization", auth_header)
            .header("accept", "application/json")
            .send()
            .await?;

        if response.status().is_success() {
            let data: Value = response.json().await?;
            if let Some(results) = data["results"].as_array() {
                for movie in results {
                    let movie_id = movie["id"].as_i64().unwrap_or_default();

                    // Skip if we've already seen this movie
                    if !tracker.is_new(movie_id) {
                        continue;
                    }

                    let genres = self.get_movie_genres(client, movie_id, auth_header).await
                        .unwrap_or_default();
                    let providers = self.get_watch_providers(client, "movie", movie_id, auth_header)
                        .await.unwrap_or_default();

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
                    movies.push(content);
                }
            }
        }

        Ok(movies)
    }

    async fn fetch_tv_shows(&self, client: &reqwest::Client, auth_header: &str,
                            tracker: &mut ContentTracker, url: String) -> Result<Vec<Content>> {
        let mut shows = Vec::new();

        println!("Fetching TV shows from: {}", url);
        let response = client.get(&url)
            .header("Authorization", auth_header)
            .header("accept", "application/json")
            .send()
            .await?;

        if response.status().is_success() {
            let data: Value = response.json().await?;
            if let Some(results) = data["results"].as_array() {
                for show in results {
                    let show_id = show["id"].as_i64().unwrap_or_default();

                    // Skip if we've already seen this show
                    if !tracker.is_new(show_id) {
                        continue;
                    }

                    let genres = self.get_tv_genres(client, show_id, auth_header).await
                        .unwrap_or_default();
                    let providers = self.get_watch_providers(client, "tv", show_id, auth_header)
                        .await.unwrap_or_default();

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
                    shows.push(content);
                }
            }
        }

        Ok(shows)
    }

    // Helper function to generate a unique key for each user's preference combination
    fn generate_user_key(&self, prefs: &UserPreferences) -> String {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();

        // Sort genres for consistent hashing
        let mut genres = prefs.favorite_genres.clone();
        genres.sort();

        // Hash the preferences
        genres.hash(&mut hasher);
        prefs.minimum_rating.to_bits().hash(&mut hasher);

        format!("user_{:x}", hasher.finish())
    }

    // Update the scrape_content method to get even more content
    async fn scrape_content(&self) -> Result<Vec<Content>> {
        let client = reqwest::Client::new();
        let mut all_content = Vec::new();
        let mut tracker = ContentTracker::new();
        let auth_header = format!("Bearer {}", self.tmdb_api_key);

        // Increase pages to get more content
        for page in 1..=5 {  // Increased from 3 to 5 pages
            // Trending Movies (Week)
            all_content.extend(
                self.fetch_movies(&client, &auth_header, &mut tracker,
                                  format!("https://api.themoviedb.org/3/trending/movie/week?language=en-US&page={}", page)
                ).await?
            );

            // Trending Movies (Day)
            all_content.extend(
                self.fetch_movies(&client, &auth_header, &mut tracker,
                                  format!("https://api.themoviedb.org/3/trending/movie/day?language=en-US&page={}", page)
                ).await?
            );

            // Popular Movies
            all_content.extend(
                self.fetch_movies(&client, &auth_header, &mut tracker,
                                  format!("https://api.themoviedb.org/3/movie/popular?language=en-US&page={}", page)
                ).await?
            );

            // Top Rated Movies
            all_content.extend(
                self.fetch_movies(&client, &auth_header, &mut tracker,
                                  format!("https://api.themoviedb.org/3/movie/top_rated?language=en-US&page={}", page)
                ).await?
            );

            // Now Playing Movies
            all_content.extend(
                self.fetch_movies(&client, &auth_header, &mut tracker,
                                  format!("https://api.themoviedb.org/3/movie/now_playing?language=en-US&page={}", page)
                ).await?
            );

            // Trending TV Shows (Week)
            all_content.extend(
                self.fetch_tv_shows(&client, &auth_header, &mut tracker,
                                    format!("https://api.themoviedb.org/3/trending/tv/week?language=en-US&page={}", page)
                ).await?
            );

            // Trending TV Shows (Day)
            all_content.extend(
                self.fetch_tv_shows(&client, &auth_header, &mut tracker,
                                    format!("https://api.themoviedb.org/3/trending/tv/day?language=en-US&page={}", page)
                ).await?
            );

            // Popular TV Shows
            all_content.extend(
                self.fetch_tv_shows(&client, &auth_header, &mut tracker,
                                    format!("https://api.themoviedb.org/3/tv/popular?language=en-US&page={}", page)
                ).await?
            );

            // Top Rated TV Shows
            all_content.extend(
                self.fetch_tv_shows(&client, &auth_header, &mut tracker,
                                    format!("https://api.themoviedb.org/3/tv/top_rated?language=en-US&page={}", page)
                ).await?
            );

            // Currently Airing TV Shows
            all_content.extend(
                self.fetch_tv_shows(&client, &auth_header, &mut tracker,
                                    format!("https://api.themoviedb.org/3/tv/on_the_air?language=en-US&page={}", page)
                ).await?
            );
        }

        // Shuffle the content for variety
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();
        all_content.shuffle(&mut rng);

        println!("Scraped {} unique items total", all_content.len());
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

    // async fn update_content(&self) -> Result<()> {
    //     {
    //         let cache = self.cache.read();
    //         if !cache.needs_update() {
    //             return Ok(());
    //         }
    //     }
    //
    //     println!("Starting content scraping...");
    //     let content = self.scrape_content().await?;
    //     println!("Scraped {} items", content.len());
    //
    //     let json = serde_json::to_string(&content)?;
    //     println!("JSON serialized, size: {} bytes", json.len());
    //
    //     let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    //     encoder.write_all(json.as_bytes())?;
    //     let compressed = encoder.finish()?;
    //     println!("Compressed size: {} bytes", compressed.len());
    //
    //     // Create a blob client for our file
    //     let blob_name = "latest.json.gz";
    //     let blob_client = self.blob_client.blob_client(blob_name);
    //
    //     println!("Attempting to upload blob: {}", blob_name);
    //
    //     let mut retry_count = 0;
    //     let max_retries = 3;
    //     let mut last_error = None;
    //
    //     // Create metadata using standard Headers
    //     use azure_core::headers::Headers;
    //     let mut metadata = Headers::new();
    //     metadata.insert("x-ms-meta-encoding", "gzip");
    //     metadata.insert("x-ms-meta-items", content.len().to_string());
    //
    //     while retry_count < max_retries {
    //         println!("Upload attempt {} of {}", retry_count + 1, max_retries);
    //
    //         match blob_client.put_block_blob(compressed.clone())
    //             .content_type("application/gzip")
    //             .metadata(&metadata)
    //             .await
    //         {
    //             Ok(_) => {
    //                 println!("Successfully uploaded blob: {}", blob_name);
    //
    //                 // Verify the upload
    //                 match blob_client.get_properties().await {
    //                     Ok(props) => {
    //                         let size = props.blob.properties.content_length;
    //                         println!("Verified blob exists with size: {} bytes", size);
    //
    //                         // Update cache only after successful upload and verification
    //                         let mut cache = self.cache.write();
    //                         cache.data.insert("latest".to_string(), content);
    //                         cache.last_updated = chrono::Utc::now();
    //                         println!("Cache updated");
    //                         return Ok(());
    //                     },
    //                     Err(e) => {
    //                         println!("Warning: Upload appeared successful but verification failed: {}", e);
    //                         // Continue with retry since verification failed
    //                     }
    //                 }
    //             },
    //             Err(e) => {
    //                 println!("Failed to upload blob (attempt {}): {}", retry_count + 1, e);
    //                 println!("Error details: {:?}", e);
    //                 last_error = Some(e);
    //             }
    //         }
    //
    //         retry_count += 1;
    //         if retry_count < max_retries {
    //             let delay = std::time::Duration::from_secs(2u64.pow(retry_count as u32));
    //             println!("Waiting {:?} before retry...", delay);
    //             tokio::time::sleep(delay).await;
    //         }
    //     }
    //
    //     if let Some(e) = last_error {
    //         eprintln!("Failed to upload blob after {} attempts", max_retries);
    //         eprintln!("Final error: {}", e);
    //         return Err(anyhow::anyhow!("Failed to upload blob: {}", e));
    //     }
    //
    //     Ok(())
    // }

    // async fn get_recommendations(&self, prefs: &UserPreferences) -> Result<Vec<Content>> {
    //     // First try cache
    //     {
    //         let cache = self.cache.read();
    //         if let Some(content) = cache.data.get("latest") {
    //             println!("Returning recommendations from cache");
    //             return Ok(content.iter()
    //                 .filter(|c| {
    //                     c.rating.unwrap_or(0.0) >= prefs.minimum_rating &&
    //                         c.genre.iter().any(|g| prefs.favorite_genres.contains(g))
    //                 })
    //                 .cloned()
    //                 .collect());
    //         }
    //     }
    //
    //     // If not in cache, try to load from blob
    //     let blob_name = "latest.json.gz";
    //     let blob_client = self.blob_client.blob_client(blob_name);
    //
    //     println!("Checking if blob exists...");
    //     match blob_client.get_properties().await {
    //         Ok(props) => {
    //             println!("Found existing blob, downloading content...");
    //             let mut stream = blob_client.get().into_stream();
    //             let mut data = Vec::new();
    //
    //             use futures_util::StreamExt;
    //             while let Some(chunk) = stream.next().await {
    //                 match chunk {
    //                     Ok(chunk) => {
    //                         let bytes = chunk.data.collect().await?;
    //                         data.extend(bytes);
    //                     },
    //                     Err(e) => {
    //                         println!("Error reading chunk: {}", e);
    //                         break;
    //                     }
    //                 }
    //             }
    //
    //             if !data.is_empty() {
    //                 println!("Downloaded {} bytes", data.len());
    //                 match self.process_blob_data(&data) {
    //                     Ok(content) => {
    //                         println!("Successfully processed blob data with {} items", content.len());
    //                         return Ok(content.into_iter()
    //                             .filter(|c| {
    //                                 c.rating.unwrap_or(0.0) >= prefs.minimum_rating &&
    //                                     c.genre.iter().any(|g| prefs.favorite_genres.contains(g))
    //                             })
    //                             .collect());
    //                     },
    //                     Err(e) => {
    //                         println!("Error processing blob data: {}, falling back to fresh fetch", e);
    //                     }
    //                 }
    //             } else {
    //                 println!("Downloaded data was empty, falling back to fresh fetch");
    //             }
    //         },
    //         Err(e) => {
    //             if e.to_string().contains("404") {
    //                 println!("No existing blob found (404), will fetch fresh content");
    //             } else {
    //                 println!("Error checking blob properties: {}, falling back to fresh fetch", e);
    //             }
    //         }
    //     }
    //
    //     // If we couldn't load from blob storage, fetch fresh content
    //     println!("Starting fresh content fetch...");
    //     match self.scrape_content().await {
    //         Ok(content) => {
    //             println!("Successfully fetched {} items, saving to blob storage...", content.len());
    //
    //             // Save the content to blob storage
    //             if let Err(e) = self.save_to_blob(&content).await {
    //                 println!("Warning: Failed to save to blob storage: {}", e);
    //             } else {
    //                 println!("Successfully saved to blob storage");
    //             }
    //
    //             // Update cache with fresh content
    //             let mut cache = self.cache.write();
    //             cache.data.insert("latest".to_string(), content.clone());
    //             cache.last_updated = chrono::Utc::now();
    //
    //             Ok(content.into_iter()
    //                 .filter(|c| {
    //                     c.rating.unwrap_or(0.0) >= prefs.minimum_rating &&
    //                         c.genre.iter().any(|g| prefs.favorite_genres.contains(g))
    //                 })
    //                 .collect())
    //         },
    //         Err(e) => {
    //             eprintln!("Failed to fetch fresh content: {}", e);
    //             Ok(vec![])
    //         }
    //     }
    // }

    async fn update_content(&self) -> Result<()> {
        // Check if update is needed
        {
            let cache = self.cache.read();
            if !cache.needs_update() {
                return Ok(());
            }
        }

        println!("Starting content scraping...");
        let content = self.scrape_content().await?;
        println!("Scraped {} items", content.len());

        // Create cache data outside the lock
        let cache_data = {
            let mut cache = self.cache.write();
            cache.data.insert("latest".to_string(), content);
            cache.used_recommendations.clear();
            cache.last_updated = chrono::Utc::now();

            CacheData {
                content: cache.data.get("latest").cloned().unwrap_or_default(),
                used_recommendations: cache.used_recommendations.clone(),
                last_updated: cache.last_updated,
            }
        }; // Lock is dropped here

        // Save to blob after releasing the lock
        self.save_to_blob(&cache_data).await?;

        Ok(())
    }

    async fn save_to_blob(&self, cache_data: &CacheData) -> Result<()> {
        let json = serde_json::to_string(cache_data)?;
        println!("JSON serialized, size: {} bytes", json.len());

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(json.as_bytes())?;
        let compressed = encoder.finish()?;
        println!("Compressed size: {} bytes", compressed.len());

        // Create a blob client for our file
        let blob_name = "latest.json.gz";
        let blob_client = self.blob_client.blob_client(blob_name);

        println!("Attempting to upload blob: {}", blob_name);

        // Create metadata using standard Headers
        use azure_core::headers::Headers;
        let mut metadata = Headers::new();
        metadata.insert("encoding", "gzip");
        metadata.insert("items", &cache_data.content.len().to_string());
        metadata.insert("last-updated", &cache_data.last_updated.to_rfc3339());

        // Try to upload with retries
        let mut retry_count = 0;
        let max_retries = 3;
        let mut last_error = None;

        while retry_count < max_retries {
            match blob_client.put_block_blob(compressed.clone())
                .content_type("application/gzip")
                .metadata(&metadata)
                .await
            {
                Ok(_) => {
                    println!("Successfully uploaded blob: {}", blob_name);
                    return Ok(());
                },
                Err(e) => {
                    println!("Upload attempt {} failed: {}", retry_count + 1, e);
                    last_error = Some(e);
                    retry_count += 1;

                    if retry_count < max_retries {
                        let delay = std::time::Duration::from_secs(2u64.pow(retry_count as u32));
                        println!("Waiting {:?} before retry...", delay);
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        if let Some(e) = last_error {
            println!("Failed to upload blob after {} attempts", max_retries);
            return Err(anyhow::anyhow!("Failed to upload blob after {} attempts: {}", max_retries, e));
        }

        Ok(())
    }

    fn process_blob_data(&self, data: &[u8]) -> Result<CacheData> {
        let mut decoder = flate2::read::GzDecoder::new(data);
        let mut decompressed = String::new();
        std::io::Read::read_to_string(&mut decoder, &mut decompressed)?;

        let cache_data: CacheData = serde_json::from_str(&decompressed)?;
        Ok(cache_data)
    }

    async fn get_recommendations(&self, prefs: &UserPreferences) -> Result<Vec<Content>> {
        println!("ContentService: Processing recommendation request");
        let user_key = self.generate_user_key(prefs);

        // Try to load from cache first
        let content = {
            let cache = self.cache.read();
            if !cache.needs_update() {
                cache.data.get("latest").cloned()
            } else {
                None
            }
        };

        let recommendations = if let Some(content) = content {
            // Use cached content
            self.filter_recommendations(content, prefs, &user_key)?
        } else {
            // Fetch fresh content
            println!("Starting fresh content fetch");
            let content = self.scrape_content().await?;

            // Update cache
            {
                let mut cache = self.cache.write();
                cache.data.insert("latest".to_string(), content.clone());
                cache.used_recommendations.clear();
                cache.last_updated = chrono::Utc::now();

                // Create cache data and drop lock before saving
                let cache_data = CacheData {
                    content: cache.data.get("latest").cloned().unwrap_or_default(),
                    used_recommendations: cache.used_recommendations.clone(),
                    last_updated: cache.last_updated,
                };
                drop(cache);

                // Save to blob outside the lock
                self.save_to_blob(&cache_data).await?;
            }

            // Filter recommendations
            self.filter_recommendations(content, prefs, &user_key)?
        };

        Ok(recommendations)
    }

    fn filter_recommendations(&self, content: Vec<Content>, prefs: &UserPreferences, user_key: &str) -> Result<Vec<Content>> {
        println!("Starting content filtering with {} items", content.len());

        // Filter content before taking the lock
        let mut available: Vec<_> = content.into_iter()
            .filter(|c| {
                c.rating.unwrap_or(0.0) >= prefs.minimum_rating &&
                    c.genre.iter().any(|g| prefs.favorite_genres.contains(g))
            })
            .collect();

        println!("Found {} items matching rating and genre criteria", available.len());

        // Take a write lock only when needed
        {
            let mut cache = self.cache.write();
            let used_recs = cache.used_recommendations
                .entry(user_key.to_string())
                .or_insert_with(HashSet::new);

            // Filter out used recommendations
            available.retain(|c| !used_recs.contains(&c.title));
            println!("After filtering used recommendations: {} items remain", available.len());

            // Reset if running low
            if available.len() < 10 {
                println!("Running low on recommendations, resetting for user");
                used_recs.clear();
                drop(cache);

                let cache_read = self.cache.read();
                if let Some(latest_content) = cache_read.data.get("latest") {
                    available = latest_content.iter()
                        .filter(|c| {
                            c.rating.unwrap_or(0.0) >= prefs.minimum_rating &&
                                c.genre.iter().any(|g| prefs.favorite_genres.contains(g))
                        })
                        .cloned()
                        .collect();
                }
            }
        }

        // Shuffle and select recommendations
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();
        available.shuffle(&mut rng);

        let recommendations: Vec<_> = available.into_iter().take(20).collect();
        println!("Selected {} recommendations", recommendations.len());

        // Mark selected items as used
        {
            let mut cache = self.cache.write();
            let used_recs = cache.used_recommendations
                .entry(user_key.to_string())
                .or_insert_with(HashSet::new);

            for content in &recommendations {
                used_recs.insert(content.title.clone());
            }
        }

        Ok(recommendations)
    }
}

async fn get_recommendations(
    prefs: web::Json<UserPreferences>,
    service: web::Data<ContentService>,
) -> HttpResponse {
    println!("Received recommendation request with preferences: {:?}", prefs);

    match service.get_recommendations(&prefs).await {
        Ok(content) => {
            println!("Returning {} recommendations to frontend", content.len());
            // Don't save to blob here since we already did in get_recommendations
            HttpResponse::Ok()
                .content_type("application/json")
                .json(content)
        },
        Err(e) => {
            eprintln!("Error getting recommendations: {}", e);
            HttpResponse::InternalServerError()
                .content_type("application/json")
                .json(json!({
                    "error": format!("Failed to get recommendations: {}", e)
                }))
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
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .max_age(3600);

        App::new()
            .wrap(cors)
            .app_data(service.clone())
            .route("/recommendations", web::post().to(get_recommendations))
    })
        .bind("0.0.0.0:8080")?
        .run()
        .await?;
    Ok(())
}