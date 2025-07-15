use std::collections::HashMap;

use handlebars::Handlebars;
use serde::Serialize;

use crate::playground::graphiql_plugin::GraphiQLPlugin;

/// Indicates whether the user agent should send or receive user credentials
/// (cookies, basic http auth, etc.) from the other domain in the case of
/// cross-origin requests.
#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum Credentials {
    /// Send user credentials if the URL is on the same origin as the calling
    /// script. This is the default value.
    #[default]
    SameOrigin,
    /// Always send user credentials, even for cross-origin calls.
    Include,
    /// Never send or receive user credentials.
    Omit,
}

#[derive(Serialize)]
struct GraphiQLVersion<'a>(&'a str);

impl Default for GraphiQLVersion<'_> {
    fn default() -> Self {
        Self("4")
    }
}

/// A builder for constructing a GraphiQL (v2) HTML page.
///
/// # Example
///
/// ```rust
/// use async_graphql::http::*;
///
/// GraphiQLSource::build()
///     .endpoint("/")
///     .subscription_endpoint("/ws")
///     .header("Authorization", "Bearer [token]")
///     .ws_connection_param("token", "[token]")
///     .credentials(Credentials::Include)
///     .finish();
/// ```
#[derive(Default, Serialize)]
pub struct GraphiQLSource<'a> {
    endpoint: &'a str,
    subscription_endpoint: Option<&'a str>,
    version: GraphiQLVersion<'a>,
    headers: Option<HashMap<&'a str, &'a str>>,
    ws_connection_params: Option<HashMap<&'a str, &'a str>>,
    title: Option<&'a str>,
    plugins: &'a [GraphiQLPlugin],
    credentials: Credentials,
}

impl<'a> GraphiQLSource<'a> {
    /// Creates a builder for constructing a GraphiQL (v2) HTML page.
    pub fn build() -> GraphiQLSource<'a> {
        Default::default()
    }

    /// Sets the endpoint of the server GraphiQL will connect to.
    #[must_use]
    pub fn endpoint(self, endpoint: &'a str) -> GraphiQLSource<'a> {
        GraphiQLSource { endpoint, ..self }
    }

    /// Sets the subscription endpoint of the server GraphiQL will connect to.
    pub fn subscription_endpoint(self, endpoint: &'a str) -> GraphiQLSource<'a> {
        GraphiQLSource {
            subscription_endpoint: Some(endpoint),
            ..self
        }
    }

    /// Sets a header to be sent with requests GraphiQL will send.
    pub fn header(self, name: &'a str, value: &'a str) -> GraphiQLSource<'a> {
        let mut headers = self.headers.unwrap_or_default();
        headers.insert(name, value);
        GraphiQLSource {
            headers: Some(headers),
            ..self
        }
    }

    /// Sets the version of GraphiQL to be fetched.
    pub fn version(self, value: &'a str) -> GraphiQLSource<'a> {
        GraphiQLSource {
            version: GraphiQLVersion(value),
            ..self
        }
    }

    /// Sets a WS connection param to be sent during GraphiQL WS connections.
    pub fn ws_connection_param(self, name: &'a str, value: &'a str) -> GraphiQLSource<'a> {
        let mut ws_connection_params = self.ws_connection_params.unwrap_or_default();
        ws_connection_params.insert(name, value);
        GraphiQLSource {
            ws_connection_params: Some(ws_connection_params),
            ..self
        }
    }

    /// Sets the html document title.
    pub fn title(self, title: &'a str) -> GraphiQLSource<'a> {
        GraphiQLSource {
            title: Some(title),
            ..self
        }
    }

    /// Sets plugins
    pub fn plugins(self, plugins: &'a [GraphiQLPlugin]) -> GraphiQLSource<'a> {
        GraphiQLSource { plugins, ..self }
    }

    /// Sets credentials option for the fetch requests.
    pub fn credentials(self, credentials: Credentials) -> GraphiQLSource<'a> {
        GraphiQLSource {
            credentials,
            ..self
        }
    }

    /// Returns a GraphiQL (v2) HTML page.
    pub fn finish(self) -> String {
        let mut handlebars = Handlebars::new();
        handlebars
            .register_template_string("graphiql_source", include_str!("./graphiql_source.hbs"))
            .expect("Failed to register template");

        handlebars
            .render("graphiql_source", &self)
            .expect("Failed to render template")
    }
}

#[cfg(test)]
mod tests {
    use crate::playground::graphiql_plugin::GraphiQLPlugin;

    use super::*;

    #[test]
    fn test_with_only_url() {
        let graphiql_source = GraphiQLSource::build().endpoint("/").finish();

        assert_eq!(
            graphiql_source,
            r#"<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="robots" content="noindex">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta name="referrer" content="origin">

    <title>GraphiQL IDE</title>

    <style>
      body {
        height: 100%;
        margin: 0;
        width: 100%;
        overflow: hidden;
      }

      #graphiql {
        height: 100vh;
      }
    </style>
    <script
      crossorigin
      src="https://unpkg.com/react@18/umd/react.development.js"
    ></script>
    <script
      crossorigin
      src="https://unpkg.com/react-dom@18/umd/react-dom.development.js"
    ></script>
    <link rel="icon" href="https://graphql.org/favicon.ico">
    <link rel="stylesheet" href="https://unpkg.com/graphiql@4/graphiql.min.css" />
  </head>

  <body>
    <div id="graphiql">Loading...</div>
    <script>
      // Configure Monaco Editor environment before GraphiQL loads
      window.MonacoEnvironment = {
        getWorker: async function (workerId, label) {
          console.info('setup-workers/graphiql', { label });
          
          let workerUrl;
          switch (label) {
            case 'json':
              workerUrl = 'https://unpkg.com/graphiql@4/dist/workers/json.worker.js';
              break;
            case 'graphql':
              workerUrl = 'https://unpkg.com/graphiql@4/dist/workers/graphql.worker.js';
              break;
            default:
              workerUrl = 'https://unpkg.com/graphiql@4/dist/workers/editor.worker.js';
          }
          
          try {
            // Fetch worker code and create blob URL to avoid CORS issues
            const response = await fetch(workerUrl);
            let workerCode = await response.text();
            
            // Remove source map references to avoid blob URL issues
            workerCode = workerCode.replace(/\/\/# sourceMappingURL=.*$/gm, '');
            
            const blob = new Blob([workerCode], { type: 'application/javascript' });
            const blobUrl = URL.createObjectURL(blob);
            
            // Create module worker since GraphiQL workers are ES modules
            return new Worker(blobUrl, { type: 'module' });
          } catch (error) {
            console.warn('Failed to load worker from CDN, falling back to URL:', error);
            // Try as module worker first, then regular worker
            try {
              return new Worker(workerUrl, { type: 'module' });
            } catch (moduleError) {
              return new Worker(workerUrl);
            }
          }
        }
      };
    </script>
    <script
      src="https://unpkg.com/graphiql@4/dist/index.umd.js"
      type="application/javascript"
    ></script>
    <script>
      customFetch = (url, opts = {}) => {
        return fetch(url, {...opts, credentials: 'same-origin'})
      }

      createUrl = (endpoint, subscription = false) => {
        const url = new URL(window.location.href.trimEnd('/') + endpoint);
        if (subscription) {
          url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
        }
        return url.toString();
      }

      ReactDOM.createRoot(document.getElementById("graphiql")).render(
        React.createElement(GraphiQL, {
          fetcher: GraphiQL.createFetcher({
            url: createUrl('/'),
            fetch: customFetch,
          }),
          defaultEditorToolsVisibility: true,
        })
      );
    </script>
  </body>
</html>"#
        )
    }

    #[test]
    fn test_with_both_urls() {
        let graphiql_source = GraphiQLSource::build()
            .endpoint("/")
            .subscription_endpoint("/ws")
            .finish();

        assert_eq!(
            graphiql_source,
            r#"<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="robots" content="noindex">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta name="referrer" content="origin">

    <title>GraphiQL IDE</title>

    <style>
      body {
        height: 100%;
        margin: 0;
        width: 100%;
        overflow: hidden;
      }

      #graphiql {
        height: 100vh;
      }
    </style>
    <script
      crossorigin
      src="https://unpkg.com/react@18/umd/react.development.js"
    ></script>
    <script
      crossorigin
      src="https://unpkg.com/react-dom@18/umd/react-dom.development.js"
    ></script>
    <link rel="icon" href="https://graphql.org/favicon.ico">
    <link rel="stylesheet" href="https://unpkg.com/graphiql@4/graphiql.min.css" />
  </head>

  <body>
    <div id="graphiql">Loading...</div>
    <script>
      // Configure Monaco Editor environment before GraphiQL loads
      window.MonacoEnvironment = {
        getWorker: async function (workerId, label) {
          console.info('setup-workers/graphiql', { label });
          
          let workerUrl;
          switch (label) {
            case 'json':
              workerUrl = 'https://unpkg.com/graphiql@4/dist/workers/json.worker.js';
              break;
            case 'graphql':
              workerUrl = 'https://unpkg.com/graphiql@4/dist/workers/graphql.worker.js';
              break;
            default:
              workerUrl = 'https://unpkg.com/graphiql@4/dist/workers/editor.worker.js';
          }
          
          try {
            // Fetch worker code and create blob URL to avoid CORS issues
            const response = await fetch(workerUrl);
            let workerCode = await response.text();
            
            // Remove source map references to avoid blob URL issues
            workerCode = workerCode.replace(/\/\/# sourceMappingURL=.*$/gm, '');
            
            const blob = new Blob([workerCode], { type: 'application/javascript' });
            const blobUrl = URL.createObjectURL(blob);
            
            // Create module worker since GraphiQL workers are ES modules
            return new Worker(blobUrl, { type: 'module' });
          } catch (error) {
            console.warn('Failed to load worker from CDN, falling back to URL:', error);
            // Try as module worker first, then regular worker
            try {
              return new Worker(workerUrl, { type: 'module' });
            } catch (moduleError) {
              return new Worker(workerUrl);
            }
          }
        }
      };
    </script>
    <script
      src="https://unpkg.com/graphiql@4/dist/index.umd.js"
      type="application/javascript"
    ></script>
    <script>
      customFetch = (url, opts = {}) => {
        return fetch(url, {...opts, credentials: 'same-origin'})
      }

      createUrl = (endpoint, subscription = false) => {
        const url = new URL(window.location.href.trimEnd('/') + endpoint);
        if (subscription) {
          url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
        }
        return url.toString();
      }

      ReactDOM.createRoot(document.getElementById("graphiql")).render(
        React.createElement(GraphiQL, {
          fetcher: GraphiQL.createFetcher({
            url: createUrl('/'),
            fetch: customFetch,
            subscriptionUrl: createUrl('/ws', true),
          }),
          defaultEditorToolsVisibility: true,
        })
      );
    </script>
  </body>
</html>"#
        )
    }

    #[test]
    fn test_with_all_options() {
        let graphiql_source = GraphiQLSource::build()
            .endpoint("/")
            .subscription_endpoint("/ws")
            .header("Authorization", "Bearer [token]")
            .version("3.9.0")
            .ws_connection_param("token", "[token]")
            .title("Awesome GraphiQL IDE Test")
            .credentials(Credentials::Include)
            .plugins(&[GraphiQLPlugin::explorer("3.9.0")])
            .finish();

        assert_eq!(
            graphiql_source,
            r#"<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="robots" content="noindex">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta name="referrer" content="origin">

    <title>Awesome GraphiQL IDE Test</title>

    <style>
      body {
        height: 100%;
        margin: 0;
        width: 100%;
        overflow: hidden;
      }

      #graphiql {
        height: 100vh;
      }
    </style>
    <script
      crossorigin
      src="https://unpkg.com/react@18/umd/react.development.js"
    ></script>
    <script
      crossorigin
      src="https://unpkg.com/react-dom@18/umd/react-dom.development.js"
    ></script>
    <link rel="icon" href="https://graphql.org/favicon.ico">
    <link rel="stylesheet" href="https://unpkg.com/graphiql@3.9.0/graphiql.min.css" />
    <link rel="stylesheet" href="https://unpkg.com/@graphiql/plugin-explorer@3.9.0/dist/style.css" />
  </head>

  <body>
    <div id="graphiql">Loading...</div>
    <script>
      // Configure Monaco Editor environment before GraphiQL loads
      window.MonacoEnvironment = {
        getWorker: async function (workerId, label) {
          console.info('setup-workers/graphiql', { label });
          
          let workerUrl;
          switch (label) {
            case 'json':
              workerUrl = 'https://unpkg.com/graphiql@3.9.0/dist/workers/json.worker.js';
              break;
            case 'graphql':
              workerUrl = 'https://unpkg.com/graphiql@3.9.0/dist/workers/graphql.worker.js';
              break;
            default:
              workerUrl = 'https://unpkg.com/graphiql@3.9.0/dist/workers/editor.worker.js';
          }
          
          try {
            // Fetch worker code and create blob URL to avoid CORS issues
            const response = await fetch(workerUrl);
            let workerCode = await response.text();
            
            // Remove source map references to avoid blob URL issues
            workerCode = workerCode.replace(/\/\/# sourceMappingURL=.*$/gm, '');
            
            const blob = new Blob([workerCode], { type: 'application/javascript' });
            const blobUrl = URL.createObjectURL(blob);
            
            // Create module worker since GraphiQL workers are ES modules
            return new Worker(blobUrl, { type: 'module' });
          } catch (error) {
            console.warn('Failed to load worker from CDN, falling back to URL:', error);
            // Try as module worker first, then regular worker
            try {
              return new Worker(workerUrl, { type: 'module' });
            } catch (moduleError) {
              return new Worker(workerUrl);
            }
          }
        }
      };
    </script>
    <script
      src="https://unpkg.com/graphiql@3.9.0/dist/index.umd.js"
      type="application/javascript"
    ></script>
    <script src="https://unpkg.com/@graphiql/plugin-explorer@3.9.0/dist/index.umd.js" crossorigin></script>
    <script>
      customFetch = (url, opts = {}) => {
        return fetch(url, {...opts, credentials: 'include'})
      }

      createUrl = (endpoint, subscription = false) => {
        const url = new URL(window.location.href.trimEnd('/') + endpoint);
        if (subscription) {
          url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
        }
        return url.toString();
      }

      const plugins = [];
      plugins.push(GraphiQLPluginExplorer.explorerPlugin());

      ReactDOM.createRoot(document.getElementById("graphiql")).render(
        React.createElement(GraphiQL, {
          fetcher: GraphiQL.createFetcher({
            url: createUrl('/'),
            fetch: customFetch,
            subscriptionUrl: createUrl('/ws', true),
            headers: {
              'Authorization': 'Bearer [token]',
            },
            wsConnectionParams: {
              'token': '[token]',
            },
          }),
          defaultEditorToolsVisibility: true,
          plugins,
        })
      );
    </script>
  </body>
</html>"#
        )
    }
}
