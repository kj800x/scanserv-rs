mod migrations;
mod scanners;
mod scans;
mod schema;
mod simple_broker;

use std::env;

use async_graphql::http::GraphiQLSource;
use async_graphql_poem::{GraphQL, GraphQLSubscription};
use duckdb::{DuckdbConnectionManager, Result};
use migrations::migrate;
use poem::{
    endpoint::StaticFilesEndpoint,
    get, handler,
    listener::TcpListener,
    web::{Html, Path},
    IntoResponse, Route, Server,
};
use scanners::ScannerManager;
use schema::{BooksSchema, MutationRoot, QueryRoot, Storage, SubscriptionRoot};

pub struct AssetsDir(String);

#[handler]
async fn graphiql() -> impl IntoResponse {
    Html(
        GraphiQLSource::build()
            .endpoint("/api/graphql")
            .subscription_endpoint("/api/graphql/ws")
            .finish(),
    )
}

#[handler]
fn hello(Path(name): Path<String>) -> String {
    format!("hello: {}", name)
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    println!("Starting up...");

    let manager = DuckdbConnectionManager::file("./db.duckdb").unwrap();
    let pool = r2d2::Pool::builder().max_size(15).build(manager).unwrap();
    let assets_dir = env::var("ASSETS_DIR").unwrap_or("./assets".to_string());

    migrate(&pool).await;

    println!("Waiting for scanners to be loaded...");

    let scanner_manager = ScannerManager::new();
    let scanners = scanner_manager.list_scanners().await;

    let schema = BooksSchema::build(QueryRoot, MutationRoot, SubscriptionRoot)
        .data(Storage::default())
        .data(scanner_manager)
        .data(pool)
        .data(AssetsDir(assets_dir.clone()))
        .finish();

    let app = Route::new()
        .at("/api/hello/:name", get(hello))
        .at(
            "/api/graphql",
            get(graphiql).post(GraphQL::new(schema.clone())),
        )
        .at("/api/graphql/ws", get(GraphQLSubscription::new(schema)))
        .nest(
            "/assets",
            StaticFilesEndpoint::new(assets_dir)
                .show_files_listing()
                .index_file("index.html"),
        );

    println!("Scanners: {:?}", scanners);
    println!("GraphiQL IDE: http://localhost:8080/api/graphql");

    Server::new(TcpListener::bind("0.0.0.0:8080"))
        .run(app)
        .await
}
