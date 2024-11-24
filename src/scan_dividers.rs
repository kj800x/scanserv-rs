use async_graphql::SimpleObject;
use chrono::{DateTime, Utc};
use duckdb::Result;
use duckdb::{params, DuckdbConnectionManager};

#[derive(Debug, Clone, SimpleObject)]
pub struct ScanDivider {
    pub id: Option<i32>,
    pub ts: DateTime<Utc>,
}

impl ScanDivider {
    pub fn new(ts: DateTime<Utc>) -> Self {
        Self { id: None, ts }
    }

    pub fn save(&mut self, pool: &r2d2::Pool<DuckdbConnectionManager>) -> Result<i32> {
        let conn = pool.get().unwrap();

        Ok(match self.id {
            Some(id) => {
                conn.execute(
                    "INSERT OR REPLACE INTO scan_dividers (id, ts) VALUES (?, ?)",
                    params![id, self.ts],
                )?;
                id
            }
            None => {
                let id: i32 = conn.query_row(
                    "INSERT INTO scan_dividers (ts) VALUES (?) RETURNING id",
                    params![self.ts],
                    |row| row.get(0),
                )?;
                self.id = Some(id);
                id
            }
        })
    }
}
