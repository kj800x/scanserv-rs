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

use crate::AssetsDir;

#[derive(Debug, Clone, SimpleObject)]
pub struct ScannerInfo {
    name: String,
    description: String,
}

pub struct ScannerManager {
    cached: Arc<Mutex<Vec<ScannerInfo>>>,
    last_refreshed: Arc<Mutex<Instant>>,
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
        if last_refreshed.elapsed() > Duration::from_secs(60) {
            self.force_list_scanners().await
        } else {
            self.cached.lock().await.clone()
        }
    }

    pub async fn scan(
        &self,
        name: &str,
        scan_arguments: HashMap<String, String>,
        pool: &r2d2::Pool<DuckdbConnectionManager>,
        assets_dir: &AssetsDir,
    ) -> i32 {
        std::fs::create_dir_all(&assets_dir.0).unwrap();
        std::fs::create_dir_all(&Path::new(&assets_dir.0).join("scans")).unwrap();

        let mut scan = crate::scans::Scan::new(
            "PENDING".to_string(),
            Path::new("scans")
                .join("tmp.png")
                .as_os_str()
                .to_str()
                .unwrap()
                .to_string(),
            name.to_string(),
            scan_arguments.clone(),
            chrono::Utc::now(),
        );

        scan.save(pool).unwrap();
        scan.path = Path::new("scans")
            .join(format!("{}.png", scan.id.unwrap()))
            .as_os_str()
            .to_str()
            .unwrap()
            .to_string()
            .into();
        scan.save(pool).unwrap();

        let scan_path = scan.path.as_disk_path(&assets_dir.0);

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
            .arg(scan_path)
            .spawn()
            .ok()
            .unwrap()
            .wait_with_output()
            // .output()
            .await
            .unwrap();

        println!(
            "{}, {:?}, {:?}",
            output.status, output.stdout, output.stderr
        );

        if output.status.code().unwrap() != 0 {
            scan.status = "FAILED".to_string();
        } else {
            scan.status = "COMPLETE".to_string();
        }

        scan.save(pool).unwrap();
        return scan.id.unwrap();
    }
}
