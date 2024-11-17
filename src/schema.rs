use std::{sync::Arc, time::Duration};

use crate::{
    scanners::{ScannerInfo, ScannerManager},
    scans,
    simple_broker::SimpleBroker,
    AssetsDir,
};
use async_graphql::{Context, Enum, Object, Result, Schema, Subscription, ID};
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
            .prepare("SELECT id, status, path, scanned_at FROM scans")
            .unwrap();

        let scans = stmt
            .query_map([], |row| {
                Ok(scans::Scan {
                    id: row.get(0)?,
                    status: row.get(1)?,
                    path: row.get(2)?,
                    scanned_at: row.get(3)?,
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

    async fn scan(&self, ctx: &Context<'_>, name: String) -> i32 {
        let scanner_manager = ctx.data_unchecked::<ScannerManager>();
        let pool = ctx.data_unchecked::<r2d2::Pool<crate::DuckdbConnectionManager>>();
        let assets_dir = ctx.data_unchecked::<AssetsDir>();
        scanner_manager.scan(&name, &pool, &assets_dir).await
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
                futures_timer::Delay::new(Duration::from_secs(1)).await;
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
