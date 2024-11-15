mod schema;
mod simple_broker;

use async_graphql::http::GraphiQLSource;
use async_graphql_poem::{GraphQL, GraphQLSubscription};
use poem::{
    get, handler,
    listener::TcpListener,
    web::{Html, Path},
    IntoResponse, Route, Server,
};
use schema::{BooksSchema, MutationRoot, QueryRoot, Storage, SubscriptionRoot};

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
    let schema = BooksSchema::build(QueryRoot, MutationRoot, SubscriptionRoot)
        .data(Storage::default())
        .finish();

    let app = Route::new()
        .at("/api/hello/:name", get(hello))
        .at(
            "/api/graphql",
            get(graphiql).post(GraphQL::new(schema.clone())),
        )
        .at("/api/graphql/ws", get(GraphQLSubscription::new(schema)));

    println!("GraphiQL IDE: http://localhost:8080/api/graphql");

    Server::new(TcpListener::bind("0.0.0.0:8080"))
        .run(app)
        .await
}
