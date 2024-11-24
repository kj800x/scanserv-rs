use std::collections::HashMap;

use async_graphql::SimpleObject;
use chrono::{DateTime, Utc};
use duckdb::Result;
use duckdb::{params, DuckdbConnectionManager};

use crate::asset_path::AssetPath;

#[derive(Debug, Clone, SimpleObject)]
pub struct Scan {
    pub id: Option<i32>,
    pub status: String,
    pub scanned_at: DateTime<Utc>,
    pub scanner: String,
    pub scan_parameters: HashMap<String, String>,
    #[graphql(flatten)]
    pub path: AssetPath,
}

impl Scan {
    pub fn new(
        status: String,
        path: String,
        scanner: String,
        scan_parameters: HashMap<String, String>,
        scanned_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id: None,
            status,
            path: AssetPath::from_relative_path(path),
            scanner,
            scan_parameters,
            scanned_at,
        }
    }

    pub fn load(id: i32, pool: &r2d2::Pool<DuckdbConnectionManager>) -> Result<Self> {
        let conn = pool.get().unwrap();

        conn.query_row(
            "SELECT id, status, path, scanner, scan_parameters, scanned_at FROM scans WHERE id = ?",
            params![id],
            |row| {
                Ok(Self {
                    id: Some(row.get(0)?),
                    status: row.get(1)?,
                    path: row.get::<usize, String>(2)?.into(),
                    scanner: row.get(3)?,
                    scan_parameters: serde_json::from_str(&row.get::<usize, String>(4)?).unwrap(),
                    scanned_at: row.get(5)?,
                })
            },
        )
    }
    pub fn save(&mut self, pool: &r2d2::Pool<DuckdbConnectionManager>) -> Result<i32> {
        let conn = pool.get().unwrap();

        let scan_parameters_str = serde_json::to_string(&self.scan_parameters).unwrap();

        Ok(match self.id {
            Some(id) => {
                conn.execute(
                    "INSERT OR REPLACE INTO scans (id, status, path, scanner, scan_parameters, scanned_at) VALUES (?, ?, ?, ?, ?, ?)",
                    params![id, self.status, self.path.as_relative_path(), self.scanner, scan_parameters_str, self.scanned_at],
                )?;
                id
            }
            None => {
                let id: i32 = conn.query_row(
                    "INSERT INTO scans (status, path, scanner, scan_parameters, scanned_at) VALUES (?, ?, ?, ?, ?) RETURNING id",
                    params![self.status, self.path.as_relative_path(), self.scanner, scan_parameters_str, self.scanned_at, ],
                    |row| row.get(0),
                )?;
                self.id = Some(id);
                id
            }
        })
    }
}
