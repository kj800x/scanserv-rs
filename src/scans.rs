use async_graphql::SimpleObject;
use chrono::{DateTime, Utc};
use duckdb::Result;
use duckdb::{params, DuckdbConnectionManager};

#[derive(Debug, Clone, SimpleObject)]
pub struct Scan {
    pub id: Option<i32>,
    pub status: String,
    pub path: String,
    // #[graphql(skip)]
    pub scanned_at: DateTime<Utc>,
}

impl Scan {
    pub fn new(status: String, path: String, scanned_at: DateTime<Utc>) -> Self {
        Self {
            id: None,
            status,
            path,
            scanned_at,
        }
    }

    // pub fn load(id: i32, pool: &r2d2::Pool<DuckdbConnectionManager>) -> Result<Self> {
    //     let conn = pool.get().unwrap();

    //     let (status, path, scanned_at) = conn.query_row(
    //         "SELECT status, path, scanned_at FROM scans WHERE id = ?",
    //         params![id],
    //         |row| {
    //             Ok((
    //                 row.get(0)?,
    //                 row.get(1)?,
    //                 row.get(2)?,
    //             ))
    //         },
    //     )?;

    //     Ok(Self {
    //         id: Some(id),
    //         status,
    //         path,
    //         scanned_at,
    //     })
    // }

    pub fn save(&self, pool: &r2d2::Pool<DuckdbConnectionManager>) -> Result<i32> {
        let conn = pool.get().unwrap();

        Ok(match self.id {
            Some(id) => {
                conn.execute(
                    "INSERT OR REPLACE INTO scans (id, status, path, scanned_at) VALUES (?, ?, ?, ?)",
                    params![id, self.status, self.path, self.scanned_at],
                )?;
                id
            }
            None => {
                let id: i32 = conn.query_row(
                    "INSERT INTO scans (status, path, scanned_at) VALUES (?, ?, ?) RETURNING id",
                    params![self.status, self.path, self.scanned_at],
                    |row| row.get(0),
                )?;
                id
            }
        })
    }
}
