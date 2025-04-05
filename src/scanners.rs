use async_graphql::SimpleObject;
use async_trait::async_trait;
use duckdb::DuckdbConnectionManager;
use rand::seq::SliceRandom;
use regex::Regex;
use std::{
    collections::HashMap,
    env, fs,
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{process::Command, sync::Mutex};

use crate::{scans::Scan, AssetsDir};

// Mock scanner constants
const MOCK_SCANNER_NAME: &str = "mock:scanner";
const MOCK_SCANNER_DESCRIPTION: &str = "Mock Scanner for Development";

#[derive(Debug, Clone, SimpleObject)]
pub struct ScannerInfo {
    name: String,
    description: String,
}

// Define the common trait for scanner managers
#[async_trait]
pub trait ScannerProvider {
    async fn last_refreshed(&self) -> Instant;
    async fn force_list_scanners(&self) -> Vec<ScannerInfo>;
    async fn list_scanners(&self) -> Vec<ScannerInfo>;
    async fn complete_scan(
        &self,
        scan_id: i32,
        name: &str,
        scan_arguments: HashMap<String, String>,
        pool: &r2d2::Pool<DuckdbConnectionManager>,
        assets_dir: &AssetsDir,
    ) -> i32;
}

// Define an enum that can hold either scanner implementation
#[derive(Clone)]
pub enum ScannerManagerKind {
    Real(RealScannerManager),
    Mock(MockScannerManager),
}

// Real scanner implementation
#[derive(Clone)]
pub struct RealScannerManager {
    cached: Arc<Mutex<Vec<ScannerInfo>>>,
    last_refreshed: Arc<Mutex<Instant>>,
}

// Mock scanner implementation
#[derive(Clone)]
pub struct MockScannerManager {
    cached: Arc<Mutex<Vec<ScannerInfo>>>,
    last_refreshed: Arc<Mutex<Instant>>,
}

// Implementation for real scanners
#[async_trait]
impl ScannerProvider for RealScannerManager {
    async fn last_refreshed(&self) -> Instant {
        *self.last_refreshed.lock().await
    }

    async fn force_list_scanners(&self) -> Vec<ScannerInfo> {
        let re = Regex::new(r"device `([^']*)' is a (.*)$").unwrap();
        let output = Command::new("scanimage")
            .arg("--list-devices")
            .output()
            .await
            .unwrap();

        let stdout = String::from_utf8(output.stdout).unwrap();

        let mut results = vec![];

        for line in stdout.lines() {
            if let Some(captures) = re.captures(line) {
                results.push(ScannerInfo {
                    name: captures[1].to_string(),
                    description: captures[2].to_string(),
                });
            }
        }

        *self.cached.lock().await = results.clone();
        *self.last_refreshed.lock().await = Instant::now();

        results
    }

    async fn list_scanners(&self) -> Vec<ScannerInfo> {
        let last_refreshed = *self.last_refreshed.lock().await;
        // FIXME: Come up with something even better, but this is fine for now
        if last_refreshed.elapsed() > Duration::from_secs(60 * 10) {
            self.force_list_scanners().await
        } else {
            self.cached.lock().await.clone()
        }
    }

    async fn complete_scan(
        &self,
        scan_id: i32,
        name: &str,
        scan_arguments: HashMap<String, String>,
        pool: &r2d2::Pool<DuckdbConnectionManager>,
        assets_dir: &AssetsDir,
    ) -> i32 {
        let mut scan = Scan::load(scan_id, pool).unwrap();

        // Generate a unique filename that doesn't exist on disk
        let mut counter = 0;
        let mut file_path;

        // For rescans, we want to keep the same base name but modify the suffix
        // so we check if this is a rescan by looking at the original_path
        let base_name = if scan.original_path.is_some() {
            // This is a rescan, get the original file base name
            let original_path = scan.original_path.as_ref().unwrap();
            let original_path_str = original_path.as_relative_path();
            let filename = original_path_str.split('/').last().unwrap();
            let parts: Vec<&str> = filename.split('.').collect();
            parts.get(0).unwrap().to_string()
        } else {
            // This is a new scan, use the scan ID as the base name
            scan_id.to_string()
        };

        loop {
            let filename = if counter == 0 {
                format!("{}.png", base_name)
            } else {
                format!("{}_{}.png", base_name, counter)
            };

            file_path = Path::new("scans")
                .join(&filename)
                .as_os_str()
                .to_str()
                .unwrap()
                .to_string();

            // Check if the file exists on disk
            let full_path = Path::new(&assets_dir.0).join(&file_path);
            if !full_path.exists() {
                break;
            }

            // If it exists, increment counter and try again
            counter += 1;
        }

        // If this is the first scan, set the original_path
        if scan.original_path.is_none() {
            scan.original_path = Some(file_path.clone().into());
        }

        // Update the path for the current scan
        scan.path = file_path.into();
        scan.save(pool).unwrap();

        Self::do_scan(scan, name, scan_arguments, pool, assets_dir).await
    }
}

// Implementation for the mock scanner
#[async_trait]
impl ScannerProvider for MockScannerManager {
    async fn last_refreshed(&self) -> Instant {
        *self.last_refreshed.lock().await
    }

    async fn force_list_scanners(&self) -> Vec<ScannerInfo> {
        let mock_scanner = ScannerInfo {
            name: MOCK_SCANNER_NAME.to_string(),
            description: MOCK_SCANNER_DESCRIPTION.to_string(),
        };
        let results = vec![mock_scanner];

        *self.cached.lock().await = results.clone();
        *self.last_refreshed.lock().await = Instant::now();

        results
    }

    async fn list_scanners(&self) -> Vec<ScannerInfo> {
        let last_refreshed = *self.last_refreshed.lock().await;
        if last_refreshed.elapsed() > Duration::from_secs(60 * 10) {
            self.force_list_scanners().await
        } else {
            self.cached.lock().await.clone()
        }
    }

    async fn complete_scan(
        &self,
        scan_id: i32,
        name: &str,
        scan_arguments: HashMap<String, String>,
        pool: &r2d2::Pool<DuckdbConnectionManager>,
        assets_dir: &AssetsDir,
    ) -> i32 {
        let mut scan = Scan::load(scan_id, pool).unwrap();

        // Generate a unique filename that doesn't exist on disk
        let mut counter = 0;
        let mut file_path;

        // For rescans, we want to keep the same base name but modify the suffix
        let base_name = if scan.original_path.is_some() {
            // This is a rescan, get the original file base name
            let original_path = scan.original_path.as_ref().unwrap();
            let original_path_str = original_path.as_relative_path();
            let filename = original_path_str.split('/').last().unwrap();
            let parts: Vec<&str> = filename.split('.').collect();
            parts.get(0).unwrap().to_string()
        } else {
            // This is a new scan, use the scan ID as the base name
            scan_id.to_string()
        };

        loop {
            let filename = if counter == 0 {
                format!("{}.png", base_name)
            } else {
                format!("{}_{}.png", base_name, counter)
            };

            file_path = Path::new("scans")
                .join(&filename)
                .as_os_str()
                .to_str()
                .unwrap()
                .to_string();

            // Check if the file exists on disk
            let full_path = Path::new(&assets_dir.0).join(&file_path);
            if !full_path.exists() {
                break;
            }

            counter += 1;
        }

        // If this is the first scan, set the original_path
        if scan.original_path.is_none() {
            scan.original_path = Some(file_path.clone().into());
        }

        // Update the path for the current scan
        scan.path = file_path.into();
        scan.save(pool).unwrap();

        Self::do_mock_scan(scan, pool, assets_dir).await
    }
}

// Implement ScannerProvider for the enum
#[async_trait]
impl ScannerProvider for ScannerManagerKind {
    async fn last_refreshed(&self) -> Instant {
        match self {
            ScannerManagerKind::Real(real) => real.last_refreshed().await,
            ScannerManagerKind::Mock(mock) => mock.last_refreshed().await,
        }
    }

    async fn force_list_scanners(&self) -> Vec<ScannerInfo> {
        match self {
            ScannerManagerKind::Real(real) => real.force_list_scanners().await,
            ScannerManagerKind::Mock(mock) => mock.force_list_scanners().await,
        }
    }

    async fn list_scanners(&self) -> Vec<ScannerInfo> {
        match self {
            ScannerManagerKind::Real(real) => real.list_scanners().await,
            ScannerManagerKind::Mock(mock) => mock.list_scanners().await,
        }
    }

    async fn complete_scan(
        &self,
        scan_id: i32,
        name: &str,
        scan_arguments: HashMap<String, String>,
        pool: &r2d2::Pool<DuckdbConnectionManager>,
        assets_dir: &AssetsDir,
    ) -> i32 {
        match self {
            ScannerManagerKind::Real(real) => {
                real.complete_scan(scan_id, name, scan_arguments, pool, assets_dir)
                    .await
            }
            ScannerManagerKind::Mock(mock) => {
                mock.complete_scan(scan_id, name, scan_arguments, pool, assets_dir)
                    .await
            }
        }
    }
}

// Implementation for RealScannerManager
impl RealScannerManager {
    pub fn new() -> Self {
        Self {
            cached: Arc::new(Mutex::new(vec![])),
            last_refreshed: Arc::new(Mutex::new(
                Instant::now() - SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
            )),
        }
    }

    async fn do_scan(
        mut scan: Scan,
        name: &str,
        scan_arguments: HashMap<String, String>,
        pool: &r2d2::Pool<DuckdbConnectionManager>,
        assets_dir: &AssetsDir,
    ) -> i32 {
        let scan_path = scan.path.as_disk_path(&assets_dir.0);
        let mut output_status;
        let mut attempts = 0;

        loop {
            attempts += 1;

            println!(
                "Running command: {:?}",
                Command::new("scanimage")
                    .arg("--format")
                    .arg("png")
                    .arg("-d")
                    .arg(name)
                    .args(scan_arguments.iter().flat_map(|(k, v)| vec![k, v]))
                    .arg("-o")
                    .arg(scan_path.clone())
            );
            let output = Command::new("scanimage")
                .arg("--format")
                .arg("png")
                .arg("-d")
                .arg(name)
                .args(scan_arguments.iter().flat_map(|(k, v)| vec![k, v]))
                .arg("-o")
                .arg(scan_path.clone())
                .spawn()
                .ok()
                .unwrap()
                .wait_with_output()
                .await
                .unwrap();

            output_status = output.status.code().unwrap();

            println!(
                "{}, {:?}, {:?}",
                output.status, output.stdout, output.stderr
            );

            if (output_status == 0) || (attempts >= 3) {
                break;
            }

            println!("Retrying scan");
        }

        if output_status != 0 {
            scan.status = "FAILED".to_string();
        } else {
            scan.status = "COMPLETE".to_string();
        }

        scan.save(pool).unwrap();
        scan.id.unwrap()
    }
}

// Implementation for MockScannerManager
impl MockScannerManager {
    pub fn new() -> Self {
        Self {
            cached: Arc::new(Mutex::new(vec![])),
            last_refreshed: Arc::new(Mutex::new(
                Instant::now() - SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
            )),
        }
    }

    async fn do_mock_scan(
        mut scan: Scan,
        pool: &r2d2::Pool<DuckdbConnectionManager>,
        assets_dir: &AssetsDir,
    ) -> i32 {
        let scan_path = scan.path.as_disk_path(&assets_dir.0);

        // Simulate scanning delay
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Get a random sample image from the mock_scanner_samples directory
        let mock_samples_dir = Path::new(&assets_dir.0).join("mock_scanner_samples");

        // Only try to copy if the directory exists and has files
        let result = match fs::read_dir(&mock_samples_dir) {
            Ok(entries) => {
                let entries: Vec<_> = entries.filter_map(Result::ok).collect();
                if entries.is_empty() {
                    println!("Warning: No sample images found in {:?}", mock_samples_dir);
                    // Create an empty file as fallback
                    fs::File::create(&scan_path).ok();
                    Ok(())
                } else {
                    // Select a random sample image
                    if let Some(entry) = entries.choose(&mut rand::thread_rng()) {
                        let sample_path = entry.path();
                        println!("Using mock sample: {:?}", sample_path);
                        fs::copy(sample_path, &scan_path).map(|_| ())
                    } else {
                        println!("Failed to select a random sample image");
                        fs::File::create(&scan_path).map(|_| ())
                    }
                }
            }
            Err(e) => {
                println!("Warning: Could not read mock samples directory: {:?}", e);
                // Create an empty file as fallback
                fs::File::create(&scan_path).map(|_| ())
            }
        };

        if result.is_ok() {
            scan.status = "COMPLETE".to_string();
        } else {
            scan.status = "FAILED".to_string();
        }

        scan.save(pool).unwrap();
        scan.id.unwrap()
    }
}

// For backward compatibility, maintain the old struct name but delegate to the new implementation
pub struct ScannerManager {
    inner: ScannerManagerKind,
}

impl Clone for ScannerManager {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl ScannerManager {
    pub fn new() -> Self {
        // Create the appropriate scanner manager based on the environment variable
        let inner = if env::var("MOCK_SCANNER").unwrap_or_default() == "true" {
            println!("Using mock scanner for development");
            ScannerManagerKind::Mock(MockScannerManager::new())
        } else {
            ScannerManagerKind::Real(RealScannerManager::new())
        };

        Self { inner }
    }

    pub async fn last_refreshed(&self) -> Instant {
        self.inner.last_refreshed().await
    }

    pub async fn force_list_scanners(&self) -> Vec<ScannerInfo> {
        self.inner.force_list_scanners().await
    }

    pub async fn list_scanners(&self) -> Vec<ScannerInfo> {
        self.inner.list_scanners().await
    }

    pub async fn complete_scan(
        &self,
        scan_id: i32,
        name: &str,
        scan_arguments: HashMap<String, String>,
        pool: &r2d2::Pool<DuckdbConnectionManager>,
        assets_dir: &AssetsDir,
    ) -> i32 {
        self.inner
            .complete_scan(scan_id, name, scan_arguments, pool, assets_dir)
            .await
    }
}
