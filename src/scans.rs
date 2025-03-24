use std::collections::HashMap;

use async_graphql::SimpleObject;
use chrono::{DateTime, Utc};
use duckdb::Result;
use duckdb::{params, DuckdbConnectionManager};
use serde::{Deserialize, Serialize};

use crate::asset_path::AssetPath;

#[derive(Debug, Clone, SimpleObject)]
pub struct ScanGroup {
    pub id: i32,
    pub title: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub status: String,
    pub comment: String,
    pub tags: Vec<String>,
}

impl ScanGroup {
    pub fn create(title: String, status: String) -> Self {
        let now = Utc::now();
        Self {
            id: 0, // Will be set on save
            title,
            created_at: now,
            updated_at: now,
            status,
            comment: String::new(),
            tags: Vec::new(),
        }
    }

    pub fn load(id: i32, pool: &r2d2::Pool<DuckdbConnectionManager>) -> Result<Self> {
        let conn = pool.get().unwrap();

        conn.query_row(
            "SELECT id, title, created_at, updated_at, status, comment, tags FROM scan_groups WHERE id = ?",
            params![id],
            |row| {
                let tags_json: String = row.get(6)?;
                let tags: Vec<String> = serde_json::from_str(&tags_json).unwrap_or_default();

                Ok(Self {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    created_at: row.get(2)?,
                    updated_at: row.get(3)?,
                    status: row.get(4)?,
                    comment: row.get(5)?,
                    tags,
                })
            },
        )
    }

    pub fn save(&mut self, pool: &r2d2::Pool<DuckdbConnectionManager>) -> Result<i32> {
        let conn = pool.get().unwrap();
        self.updated_at = Utc::now();

        let tags_json = serde_json::to_string(&self.tags).unwrap_or_else(|_| "[]".to_string());

        if self.id == 0 {
            // New record
            let id: i32 = conn.query_row(
                "INSERT INTO scan_groups (title, created_at, updated_at, status, comment, tags)
                 VALUES (?, ?, ?, ?, ?, ?) RETURNING id",
                params![
                    self.title,
                    self.created_at,
                    self.updated_at,
                    self.status,
                    self.comment,
                    tags_json
                ],
                |row| row.get(0),
            )?;
            self.id = id;
            Ok(id)
        } else {
            // Update existing record
            conn.execute(
                "UPDATE scan_groups SET title = ?, updated_at = ?, status = ?, comment = ?, tags = ? WHERE id = ?",
                params![self.title, self.updated_at, self.status, self.comment, tags_json, self.id],
            )?;
            Ok(self.id)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CropCoordinates {
    pub x: f32,
    pub y: f32,
    pub width: f32,
    pub height: f32,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct Scan {
    pub id: Option<i32>,
    pub status: String,
    pub scanned_at: DateTime<Utc>,
    pub scanner: String,
    pub scan_parameters: HashMap<String, String>,
    #[graphql(flatten)]
    pub path: AssetPath,
    pub group: Option<ScanGroup>,
    pub rotation: i32,
    pub crop_coordinates: Option<String>,
    pub original_path: Option<AssetPath>,
    pub edited_path: Option<AssetPath>,
}

impl Scan {
    pub fn new(
        status: String,
        path: String,
        scanner: String,
        scan_parameters: HashMap<String, String>,
        scanned_at: DateTime<Utc>,
    ) -> Self {
        let asset_path = AssetPath::from_relative_path(path.clone());

        Self {
            id: None,
            status,
            path: asset_path.clone(),
            scanner,
            scan_parameters,
            scanned_at,
            group: None,
            rotation: 0,
            crop_coordinates: None,
            original_path: Some(asset_path),
            edited_path: None,
        }
    }

    pub fn load(id: i32, pool: &r2d2::Pool<DuckdbConnectionManager>) -> Result<Self> {
        let conn = pool.get().unwrap();

        conn.query_row(
            "SELECT id, status, path, scanner, scan_parameters, scanned_at, scan_group_id,
                    rotation, crop_coordinates, original_path, edited_path
             FROM scans WHERE id = ?",
            params![id],
            |row| {
                let path: String = row.get(2)?;
                let original_path: Option<String> = row.get(9)?;
                let edited_path: Option<String> = row.get(10)?;

                Ok(Self {
                    id: Some(row.get(0)?),
                    status: row.get(1)?,
                    path: path.into(),
                    scanner: row.get(3)?,
                    scan_parameters: serde_json::from_str(&row.get::<usize, String>(4)?).unwrap(),
                    scanned_at: row.get(5)?,
                    group: if let Some(group_id) = row.get::<usize, Option<i32>>(6)? {
                        Some(ScanGroup::load(group_id, pool)?)
                    } else {
                        None
                    },
                    rotation: row.get(7)?,
                    crop_coordinates: row.get(8)?,
                    original_path: original_path.map(|p| p.into()),
                    edited_path: edited_path.map(|p| p.into()),
                })
            },
        )
    }

    pub fn save(&mut self, pool: &r2d2::Pool<DuckdbConnectionManager>) -> Result<i32> {
        let conn = pool.get().unwrap();

        let scan_parameters_str = serde_json::to_string(&self.scan_parameters).unwrap();
        let original_path = self.original_path.as_ref().map(|p| p.as_relative_path());
        let edited_path = self.edited_path.as_ref().map(|p| p.as_relative_path());

        Ok(match self.id {
            Some(id) => {
                conn.execute(
                    "UPDATE scans SET
                     status = ?,
                     path = ?,
                     scanner = ?,
                     scan_parameters = ?,
                     scanned_at = ?,
                     rotation = ?,
                     crop_coordinates = ?,
                     original_path = ?,
                     edited_path = ?
                     WHERE id = ?",
                    params![
                        self.status,
                        self.path.as_relative_path(),
                        self.scanner,
                        scan_parameters_str,
                        self.scanned_at,
                        self.rotation,
                        self.crop_coordinates,
                        original_path,
                        edited_path,
                        id
                    ],
                )?;
                id
            }
            None => {
                let id: i32 = conn.query_row(
                    "INSERT INTO scans (
                        status,
                        path,
                        scanner,
                        scan_parameters,
                        scanned_at,
                        rotation,
                        crop_coordinates,
                        original_path,
                        edited_path
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    RETURNING id",
                    params![
                        self.status,
                        self.path.as_relative_path(),
                        self.scanner,
                        scan_parameters_str,
                        self.scanned_at,
                        self.rotation,
                        self.crop_coordinates,
                        original_path,
                        edited_path,
                    ],
                    |row| row.get(0),
                )?;
                self.id = Some(id);
                id
            }
        })
    }

    pub fn set_group(
        &mut self,
        group_id: i32,
        pool: &r2d2::Pool<DuckdbConnectionManager>,
    ) -> Result<()> {
        if let Some(id) = self.id {
            let conn = pool.get().unwrap();
            conn.execute(
                "UPDATE scans SET scan_group_id = ? WHERE id = ?",
                params![group_id, id],
            )?;
            self.group = Some(ScanGroup::load(group_id, pool)?);
            Ok(())
        } else {
            // Create a generic error result
            Err(duckdb::Error::ToSqlConversionFailure(Box::new(
                std::io::Error::new(std::io::ErrorKind::Other, "Scan not saved yet"),
            )))
        }
    }
}
