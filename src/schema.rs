use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use crate::{
    scanners::{ScannerInfo, ScannerManager},
    scans::{self, CropCoordinates, Scan, ScanGroup},
    simple_broker::SimpleBroker,
    AssetsDir,
};
use async_graphql::{Context, Enum, Object, Result, Schema, Subscription, ID};
use duckdb::params;
use futures_util::{lock::Mutex, Stream, StreamExt};
use slab::Slab;

pub type BooksSchema = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;

#[derive(Clone)]
pub struct Book {
    id: ID,
    name: String,
    author: String,
}

#[Object]
impl Book {
    async fn id(&self) -> &str {
        &self.id
    }

    async fn name(&self) -> &str {
        &self.name
    }

    async fn author(&self) -> &str {
        &self.author
    }
}

pub type Storage = Arc<Mutex<Slab<Book>>>;

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn books(&self, ctx: &Context<'_>) -> Vec<Book> {
        let books = ctx.data_unchecked::<Storage>().lock().await;
        books.iter().map(|(_, book)| book).cloned().collect()
    }

    async fn scanners(&self, ctx: &Context<'_>) -> Vec<ScannerInfo> {
        let scanner_manager = ctx.data_unchecked::<ScannerManager>();
        scanner_manager.list_scanners().await
    }

    async fn staleness(&self, ctx: &Context<'_>) -> u64 {
        let scanner_manager = ctx.data_unchecked::<ScannerManager>();
        let last_refreshed = scanner_manager.last_refreshed().await;
        last_refreshed.elapsed().as_millis() as u64
    }

    async fn scans(&self, ctx: &Context<'_>) -> Vec<crate::scans::Scan> {
        let pool = ctx.data_unchecked::<r2d2::Pool<crate::DuckdbConnectionManager>>();
        let conn = pool.get().unwrap();

        let mut stmt = conn
            .prepare("SELECT id, status, path, scanner, scan_parameters, scanned_at, scan_group_id, rotation, crop_coordinates, original_path, edited_path FROM scans")
            .unwrap();

        let scans = stmt
            .query_map([], |row| {
                let scan_parameters: HashMap<String, String> =
                    serde_json::from_str(&row.get::<usize, String>(4)?.to_owned()).unwrap();

                Ok(scans::Scan {
                    id: row.get(0)?,
                    status: row.get(1)?,
                    path: row.get::<usize, String>(2)?.into(),
                    scanner: row.get(3)?,
                    scan_parameters,
                    scanned_at: row.get(5)?,
                    group: if row.get::<usize, Option<i32>>(6)?.is_some() {
                        Some(crate::scans::ScanGroup::load(row.get(6)?, &pool).unwrap())
                    } else {
                        None
                    },
                    rotation: row.get(7)?,
                    crop_coordinates: row.get(8)?,
                    original_path: row.get::<usize, Option<String>>(9)?.map(|p| p.into()),
                    edited_path: row.get::<usize, Option<String>>(10)?.map(|p| p.into()),
                })
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();

        scans
    }

    async fn dividers(&self, ctx: &Context<'_>) -> Vec<crate::scan_dividers::ScanDivider> {
        let pool = ctx.data_unchecked::<r2d2::Pool<crate::DuckdbConnectionManager>>();
        let conn = pool.get().unwrap();

        let mut stmt = conn.prepare("SELECT id, ts FROM scan_dividers").unwrap();

        let dividers = stmt
            .query_map([], |row| {
                Ok(crate::scan_dividers::ScanDivider {
                    id: row.get(0)?,
                    ts: row.get(1)?,
                })
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();

        dividers
    }

    async fn groups(
        &self,
        ctx: &Context<'_>,
        status: Option<String>,
    ) -> Vec<crate::scans::ScanGroup> {
        let pool = ctx.data_unchecked::<r2d2::Pool<crate::DuckdbConnectionManager>>();
        let conn = pool.get().unwrap();

        let sql = match status {
            Some(_) => "SELECT id, title, created_at, updated_at, status, comment, tags FROM scan_groups WHERE status = ?",
            None => "SELECT id, title, created_at, updated_at, status, comment, tags FROM scan_groups",
        };

        let mut stmt = conn.prepare(sql).unwrap();

        let row_mapper = |row: &duckdb::Row| -> duckdb::Result<crate::scans::ScanGroup> {
            let tags_json: String = row.get(6)?;
            let tags: Vec<String> = serde_json::from_str(&tags_json).unwrap_or_default();

            Ok(crate::scans::ScanGroup {
                id: row.get(0)?,
                title: row.get(1)?,
                created_at: row.get(2)?,
                updated_at: row.get(3)?,
                status: row.get(4)?,
                comment: row.get(5)?,
                tags,
            })
        };

        let groups = if let Some(status_val) = status {
            stmt.query_map([status_val], row_mapper)
        } else {
            stmt.query_map([], row_mapper)
        }
        .unwrap()
        .map(Result::unwrap)
        .collect();

        groups
    }

    async fn group_by_id(&self, ctx: &Context<'_>, id: i32) -> Option<crate::scans::ScanGroup> {
        let pool = ctx.data_unchecked::<r2d2::Pool<crate::DuckdbConnectionManager>>();
        match crate::scans::ScanGroup::load(id, &pool) {
            Ok(group) => Some(group),
            Err(_) => None,
        }
    }

    async fn scans_by_group(&self, ctx: &Context<'_>, group_id: i32) -> Vec<crate::scans::Scan> {
        let pool = ctx.data_unchecked::<r2d2::Pool<crate::DuckdbConnectionManager>>();
        let conn = pool.get().unwrap();

        let mut stmt = conn
            .prepare("SELECT id, status, path, scanner, scan_parameters, scanned_at, scan_group_id, rotation, crop_coordinates, original_path, edited_path FROM scans WHERE scan_group_id = ?")
            .unwrap();

        let scans = stmt
            .query_map([group_id], |row| {
                let scan_parameters: HashMap<String, String> =
                    serde_json::from_str(&row.get::<usize, String>(4)?.to_owned()).unwrap();

                Ok(scans::Scan {
                    id: row.get(0)?,
                    status: row.get(1)?,
                    path: row.get::<usize, String>(2)?.into(),
                    scanner: row.get(3)?,
                    scan_parameters,
                    scanned_at: row.get(5)?,
                    group: Some(crate::scans::ScanGroup::load(row.get(6)?, &pool).unwrap()),
                    rotation: row.get(7)?,
                    crop_coordinates: row.get(8)?,
                    original_path: row.get::<usize, Option<String>>(9)?.map(|p| p.into()),
                    edited_path: row.get::<usize, Option<String>>(10)?.map(|p| p.into()),
                })
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();

        scans
    }
}

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn create_book(&self, ctx: &Context<'_>, name: String, author: String) -> ID {
        let mut books = ctx.data_unchecked::<Storage>().lock().await;
        let entry = books.vacant_entry();
        let id: ID = entry.key().into();
        let book = Book {
            id: id.clone(),
            name,
            author,
        };
        entry.insert(book);
        SimpleBroker::publish(BookChanged {
            mutation_type: MutationType::Created,
            id: id.clone(),
        });
        id
    }

    async fn delete_book(&self, ctx: &Context<'_>, id: ID) -> Result<bool> {
        let mut books = ctx.data_unchecked::<Storage>().lock().await;
        let id = id.parse::<usize>()?;
        if books.contains(id) {
            books.remove(id);
            SimpleBroker::publish(BookChanged {
                mutation_type: MutationType::Deleted,
                id: id.into(),
            });
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn scan(
        &self,
        ctx: &Context<'_>,
        name: String,
        parameters: String,
        group_id: Option<i32>,
    ) -> i32 {
        // Clone all context data to ensure 'static lifetimes for the async task
        let scanner_manager = ctx.data_unchecked::<ScannerManager>().clone();
        let pool = ctx
            .data_unchecked::<r2d2::Pool<crate::DuckdbConnectionManager>>()
            .clone();
        let assets_dir = ctx.data_unchecked::<AssetsDir>().clone();
        let parameters: HashMap<String, String> = serde_json::from_str(&parameters).unwrap();

        // First step: create the scan with a placeholder path
        std::fs::create_dir_all(&assets_dir.0).unwrap();
        std::fs::create_dir_all(&Path::new(&assets_dir.0).join("scans")).unwrap();

        let mut scan = Scan::new(
            "PENDING".to_string(),
            Path::new("scans")
                .join("tmp.png")
                .as_os_str()
                .to_str()
                .unwrap()
                .to_string(),
            name.clone(),
            parameters.clone(),
            chrono::Utc::now(),
        );

        // Save scan to get an ID
        scan.save(&pool).unwrap();
        let scan_id = scan.id.unwrap();

        // If a group_id is provided, immediately associate the scan with the group
        if let Some(group_id) = group_id {
            scan.set_group(group_id, &pool).unwrap();
        }

        // Create clones for the async task
        let name_clone = name.clone();
        let parameters_clone = parameters.clone();
        let assets_dir_clone = assets_dir.clone();
        let pool_clone = pool.clone();
        let scanner_manager_clone = scanner_manager.clone();

        // Start the actual scanning process in the background
        tokio::spawn(async move {
            scanner_manager_clone
                .complete_scan(
                    scan_id,
                    &name_clone,
                    parameters_clone,
                    &pool_clone,
                    &assets_dir_clone,
                )
                .await;
        });

        // Return the scan ID immediately to the client
        scan_id
    }

    async fn retry_scan(
        &self,
        ctx: &Context<'_>,
        name: String,
        parameters: String,
        scan_id: i32,
    ) -> i32 {
        // Clone all context data to ensure 'static lifetimes for the async task
        let scanner_manager = ctx.data_unchecked::<ScannerManager>().clone();
        let pool = ctx
            .data_unchecked::<r2d2::Pool<crate::DuckdbConnectionManager>>()
            .clone();
        let assets_dir = ctx.data_unchecked::<AssetsDir>().clone();
        let parameters: HashMap<String, String> = serde_json::from_str(&parameters).unwrap();

        // Load the existing scan
        let mut scan = Scan::load(scan_id, &pool).unwrap();

        // Update the scan with PENDING status and the new scanner name
        scan.status = "PENDING".to_string();
        scan.scanner = name.clone();
        scan.scan_parameters = parameters.clone();
        scan.save(&pool).unwrap();

        // Create clones for the async task
        let name_clone = name.clone();
        let parameters_clone = parameters.clone();
        let assets_dir_clone = assets_dir.clone();
        let pool_clone = pool.clone();
        let scanner_manager_clone = scanner_manager.clone();

        // Start the scanning process in the background with the existing scan ID
        tokio::spawn(async move {
            scanner_manager_clone
                .complete_scan(
                    scan_id,
                    &name_clone,
                    parameters_clone,
                    &pool_clone,
                    &assets_dir_clone,
                )
                .await;
        });

        // Return the same scan ID
        scan_id
    }

    async fn add_divider(&self, ctx: &Context<'_>) -> i32 {
        let pool = ctx.data_unchecked::<r2d2::Pool<crate::DuckdbConnectionManager>>();

        let ts = chrono::Utc::now();

        crate::scan_dividers::ScanDivider::new(ts)
            .save(&pool)
            .unwrap()
    }

    async fn create_group(&self, ctx: &Context<'_>, title: String, status: String) -> i32 {
        let pool = ctx.data_unchecked::<r2d2::Pool<crate::DuckdbConnectionManager>>();

        let mut group = ScanGroup::create(title, status);
        group.save(&pool).unwrap()
    }

    async fn update_group(
        &self,
        ctx: &Context<'_>,
        id: i32,
        title: Option<String>,
        status: Option<String>,
        comment: Option<String>,
        tags: Option<Vec<String>>,
    ) -> bool {
        let pool = ctx.data_unchecked::<r2d2::Pool<crate::DuckdbConnectionManager>>();

        match ScanGroup::load(id, &pool) {
            Ok(mut group) => {
                if let Some(title) = title {
                    group.title = title;
                }

                if let Some(status) = status {
                    group.status = status;
                }

                if let Some(comment) = comment {
                    group.comment = comment;
                }

                if let Some(tags) = tags {
                    group.tags = tags;
                }

                group.save(&pool).unwrap();
                true
            }
            Err(_) => false,
        }
    }

    async fn commit_group(&self, ctx: &Context<'_>, scan_ids: Vec<i32>, title: String) -> i32 {
        let pool = ctx.data_unchecked::<r2d2::Pool<crate::DuckdbConnectionManager>>();
        let conn = pool.get().unwrap();

        // Calculate the group_id by checking if all scans have the same group
        let mut common_group_id: Option<i32> = None;
        let mut all_same_group = true;

        for scan_id in &scan_ids {
            let scan = Scan::load(*scan_id, &pool).unwrap();
            if let Some(scan_group) = &scan.group {
                if let Some(existing_id) = common_group_id {
                    if existing_id != scan_group.id {
                        all_same_group = false;
                        break;
                    }
                } else {
                    common_group_id = Some(scan_group.id);
                }
            } else {
                all_same_group = false;
                break;
            }
        }

        let group_id = if all_same_group && common_group_id.is_some() {
            // Update existing group to finalized status
            let group_id = common_group_id.unwrap();
            conn.execute(
                "UPDATE scan_groups SET title = ?, status = 'finalized', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                params![title, group_id],
            ).unwrap();
            group_id
        } else {
            // Create a new group if the scans don't share a common group
            let id: i32 = conn
                .query_row(
                    "INSERT INTO scan_groups (title, status) VALUES (?, 'finalized') RETURNING id",
                    params![title, "finalized"],
                    |row| row.get(0),
                )
                .unwrap();

            // Update scan_group_id for all scans
            for scan_id in &scan_ids {
                conn.execute(
                    "UPDATE scans SET scan_group_id = ? WHERE id = ?",
                    params![id, scan_id],
                )
                .unwrap();
            }
            id
        };

        group_id
    }

    async fn add_scan_to_group(&self, ctx: &Context<'_>, scan_id: i32, group_id: i32) -> bool {
        let pool = ctx.data_unchecked::<r2d2::Pool<crate::DuckdbConnectionManager>>();

        match Scan::load(scan_id, &pool) {
            Ok(mut scan) => match scan.set_group(group_id, &pool) {
                Ok(_) => true,
                Err(_) => false,
            },
            Err(_) => false,
        }
    }

    async fn rotate_scan(&self, ctx: &Context<'_>, scan_id: i32, rotation: i32) -> bool {
        let pool = ctx.data_unchecked::<r2d2::Pool<crate::DuckdbConnectionManager>>();

        match Scan::load(scan_id, &pool) {
            Ok(mut scan) => {
                // Ensure rotation is in 90-degree increments (0, 90, 180, 270)
                let normalized_rotation = (rotation % 360 + 360) % 360;
                scan.rotation = normalized_rotation;
                scan.save(&pool).unwrap();
                true
            }
            Err(_) => false,
        }
    }

    async fn crop_scan(
        &self,
        ctx: &Context<'_>,
        scan_id: i32,
        x: f32,
        y: f32,
        width: f32,
        height: f32,
    ) -> bool {
        let pool = ctx.data_unchecked::<r2d2::Pool<crate::DuckdbConnectionManager>>();

        match Scan::load(scan_id, &pool) {
            Ok(mut scan) => {
                let crop = CropCoordinates {
                    x,
                    y,
                    width,
                    height,
                };
                let crop_json = serde_json::to_string(&crop).unwrap();
                scan.crop_coordinates = Some(crop_json);
                scan.save(&pool).unwrap();
                true
            }
            Err(_) => false,
        }
    }
}

#[derive(Enum, Eq, PartialEq, Copy, Clone)]
enum MutationType {
    Created,
    Deleted,
}

#[derive(Clone)]
struct BookChanged {
    mutation_type: MutationType,
    id: ID,
}

#[Object]
impl BookChanged {
    async fn mutation_type(&self) -> MutationType {
        self.mutation_type
    }

    async fn id(&self) -> &ID {
        &self.id
    }

    async fn book(&self, ctx: &Context<'_>) -> Result<Option<Book>> {
        let books = ctx.data_unchecked::<Storage>().lock().await;
        let id = self.id.parse::<usize>()?;
        Ok(books.get(id).cloned())
    }
}

pub struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
    async fn interval(&self, #[graphql(default = 1)] n: i32) -> impl Stream<Item = i32> {
        let mut value = 0;
        async_stream::stream! {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                value += n;
                yield value;
            }
        }
    }

    async fn books(&self, mutation_type: Option<MutationType>) -> impl Stream<Item = BookChanged> {
        SimpleBroker::<BookChanged>::subscribe().filter(move |event| {
            let res = if let Some(mutation_type) = mutation_type {
                event.mutation_type == mutation_type
            } else {
                true
            };
            async move { res }
        })
    }
}
