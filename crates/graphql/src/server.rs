use std::future::Future;
use std::net::SocketAddr;

use async_graphql::dynamic::Schema;
use async_graphql::http::GraphiQLSource;
use async_graphql::Request;
use async_graphql_warp::graphql_subscription;
use tokio::sync::broadcast::Receiver;
use torii_storage::Storage;
use warp::{Filter, Rejection, Reply};

use super::schema::build_schema;

pub async fn new(
    mut shutdown_rx: Receiver<()>,
    storage: impl Storage + Clone + 'static,
) -> (SocketAddr, impl Future<Output = ()> + 'static) {
    let schema = build_schema(storage).await.unwrap();
    let routes = graphql_filter(schema);
    warp::serve(routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async move {
        shutdown_rx.recv().await.ok();
    })
}

fn graphql_filter(schema: Schema) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    let graphql_post = async_graphql_warp::graphql(schema.clone()).and_then(
        move |(schema, request): (Schema, Request)| async move {
            // Execute query
            let response = schema.execute(request).await;
            // Return result
            Ok::<_, Rejection>(warp::reply::json(&response))
        },
    );

    let playground_filter = warp::path("graphql").map(move || {
        warp::reply::html(
            GraphiQLSource::build()
                .plugins(&[async_graphql::http::graphiql_plugin_explorer()])
                .subscription_endpoint("/ws")
                // we patch the generated source to use the current URL instead of the origin
                // for hosted services like SLOT
                .finish()
                // we need to patch how we set up the graphiql
                .replace("@17", "@18")
                .replace(
                    "ReactDOM.render(",
                    "ReactDOM.createRoot(document.getElementById(\"graphiql\")).render(",
                )
                .replace(
                    "new URL(endpoint, window.location.origin);",
                    "new URL(window.location.href.trimEnd('/') + endpoint)",
                )
                // the playground source from async-graphql is broken as a new graphiql version has been released.
                // we patch the source to use a working version.
                .replace(
                    "https://unpkg.com/@graphiql/plugin-explorer/dist/style.css",
                    "https://unpkg.com/@graphiql/plugin-explorer@5.0.0-rc.1/dist/style.css",
                )
                .replace(
                    "https://unpkg.com/@graphiql/plugin-explorer/dist/index.umd.js",
                    "https://unpkg.com/@graphiql/plugin-explorer@5.0.0-rc.1/dist/index.umd.js",
                )
                .replace(
                    "https://unpkg.com/graphiql/graphiql.min.js",
                    "https://unpkg.com/graphiql@5.0.0-rc.1/graphiql.min.js",
                )
                .replace(
                    "https://unpkg.com/graphiql/graphiql.min.css",
                    "https://unpkg.com/graphiql@5.0.0-rc.1/graphiql.min.css",
                ),
        )
    });

    graphql_subscription(schema)
        .or(graphql_post)
        .or(playground_filter)
}
