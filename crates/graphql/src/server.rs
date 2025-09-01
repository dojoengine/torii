use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use async_graphql::dynamic::Schema;
use async_graphql::Request;
use async_graphql_warp::graphql_subscription;
use sqlx::{Pool, Sqlite};
use starknet::providers::Provider;
use tokio::sync::broadcast::Receiver;
use torii_messaging::Messaging;
use torii_storage::ReadOnlyStorage;
use warp::{Filter, Rejection, Reply};

use crate::playground::{graphiql::GraphiQLSource, graphiql_plugin::GraphiQLPlugin};

use super::schema::build_schema;

pub async fn new<P: Provider + Sync + Send + Clone + 'static>(
    mut shutdown_rx: Receiver<()>,
    pool: &Pool<Sqlite>,
    messaging: Arc<Messaging<P>>,
    storage: Arc<dyn ReadOnlyStorage>,
) -> (SocketAddr, impl Future<Output = ()> + 'static) {
    let schema = build_schema(pool, messaging, storage).await.unwrap();
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
                .version("6.0.0-canary-d779fd3f.0")
                .plugins(&[GraphiQLPlugin::explorer("6.0.0-canary-d779fd3f.0")])
                .subscription_endpoint("/ws")
                .title("Torii GraphQL Playground")
                // we patch the generated source to use the current URL instead of the origin
                // for hosted services like SLOT
                .finish(),
        )
    });

    graphql_subscription(schema)
        .or(graphql_post)
        .or(playground_filter)
}
