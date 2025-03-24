use async_graphql::SimpleObject;
use duckdb::DuckdbConnectionManager;
use regex::Regex;
use std::{
    collections::HashMap,
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{process::Command, sync::Mutex};

use crate::{scans::Scan, AssetsDir};

#[derive(Debug, Clone, SimpleObject)]
pub struct ScannerInfo {
    name: String,
    description: String,
}

pub struct ScannerManager {
    cached: Arc<Mutex<Vec<ScannerInfo>>>,
    last_refreshed: Arc<Mutex<Instant>>,
}

// Add Clone implementation for ScannerManager
impl Clone for ScannerManager {
    fn clone(&self) -> Self {
        Self {
            cached: self.cached.clone(),
            last_refreshed: self.last_refreshed.clone(),
        }
    }
}

// TODO, have this just refresh scanner data in the background on an interval
impl ScannerManager {
    pub fn new() -> Self {
        Self {
            cached: Arc::new(Mutex::new(vec![])),
            last_refreshed: Arc::new(Mutex::new(
                Instant::now() - SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
            )),
        }
    }

    pub async fn last_refreshed(&self) -> Instant {
        *self.last_refreshed.lock().await
    }

    pub async fn force_list_scanners(&self) -> Vec<ScannerInfo> {
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

    pub async fn list_scanners(&self) -> Vec<ScannerInfo> {
        let last_refreshed = *self.last_refreshed.lock().await;
        // FIXME: Come up with something even better, but this is fine for now
        if last_refreshed.elapsed() > Duration::from_secs(60 * 10) {
            self.force_list_scanners().await
        } else {
            self.cached.lock().await.clone()
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
        return scan.id.unwrap();
    }

    // New method to complete a scan that has already been created
    pub async fn complete_scan(
        &self,
        scan_id: i32,
        name: &str,
        scan_arguments: HashMap<String, String>,
        pool: &r2d2::Pool<DuckdbConnectionManager>,
        assets_dir: &AssetsDir,
    ) -> i32 {
        let mut scan = Scan::load(scan_id, pool).unwrap();

        // For testing purposes, simulate a delay
        tokio::time::sleep(Duration::from_secs(10)).await;

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

        ScannerManager::do_scan(scan, name, scan_arguments, pool, assets_dir).await
    }
}
