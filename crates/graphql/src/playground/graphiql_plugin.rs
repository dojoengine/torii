//! A simplified html for GraphiQL v2 with explorer plugin
//!
//! ```html
//! <!doctype html>
//! <html lang="en">
//!   <head>
//!     <title>GraphiQL</title>
//!     <!-- simplified ... -->
//!     <!-- [head_assets]
//!       These are imports for the GraphIQL Explorer plugin.
//!      -->
//!     <link rel="stylesheet" href="https://unpkg.com/@graphiql/plugin-explorer/dist/style.css" />
//!     <!-- END [head_assets] -->
//!   </head>
//!   <body>
//!     <!-- simplified ... -->
//!
//!     <!-- [body_assets]
//!       These are imports for the GraphIQL Explorer plugin.
//!     -->
//!     <script
//!       src="https://unpkg.com/@graphiql/plugin-explorer/dist/index.umd.js"
//!       crossorigin
//!     ></script>
//!     <!-- END [body_assets] -->
//!
//!     <script>
//!       <!-- simplified ... -->
//!
//!       <!-- plugins block -->
//!       const plugins = []
//!
//!       <!-- [pre_configs] -->
//!       <!-- END [ppre_configs] -->
//!
//!       <!-- [constructor and props] -->
//!       For explorerPlugin without any props:
//!       const explorerPlugin = GraphiQLPluginExplorer.explorerPlugin();
//!       where `GraphiQLPluginExplorer.explorerPlugin` is the constructor of plugin
//!
//!       For explorerPlugin with props `{hideActions: false}`:
//!       const explorerPlugin = GraphiQLPluginExplorer.explorerPlugin({hideActions: false});
//!       -->
//!       plugins.push(GraphiQLPluginExplorer.explorerPlugin());
//!       <!-- END [constructor and props] -->
//!       <!-- END plugins block -->
//!
//!       root.render(
//!         React.createElement(GraphiQL, {
//!           fetcher,
//!           defaultEditorToolsVisibility: true,
//!           plugins,
//!         }),
//!       );
//!     </script>
//!   </body>
//! </html>
//! ```
//!
//! Example for explorer plugin
//!
//! ```rust, ignore
//! GraphiQLPlugin {
//!     name: "GraphiQLPluginExplorer",
//!     constructor: "GraphiQLPluginExplorer.explorerPlugin",
//!     head_assets: Some(
//!         r#"<link rel="stylesheet" href="https://unpkg.com/@graphiql/plugin-explorer/dist/style.css" />"#,
//!     ),
//!     body_assets: Some(
//!         r#"<script
//!   src="https://unpkg.com/@graphiql/plugin-explorer/dist/index.umd.js"
//!   crossorigin
//! ></script>"#,
//!     ),
//!     ..Default::default()
//! }
//! ```

use serde::Serialize;

#[allow(missing_docs)]
#[derive(Debug, Default, Serialize)]
pub struct GraphiQLPlugin {
    pub name: String,
    pub constructor: String,
    /// assets which would be placed in head
    pub head_assets: Option<String>,
    /// assets which would be placed in body
    pub body_assets: Option<String>,
    /// related configs which would be placed before loading plugin
    pub pre_configs: Option<String>,
    /// props which would be passed to the plugin's constructor
    pub props: Option<String>,
}

impl GraphiQLPlugin {
    pub fn explorer(version: &str) -> GraphiQLPlugin {
        GraphiQLPlugin {
            name: "GraphiQLPluginExplorer".to_string(),
            constructor: "GraphiQLPluginExplorer.explorerPlugin".to_string(),
            head_assets: Some(
                format!("<link rel=\"stylesheet\" href=\"https://unpkg.com/@graphiql/plugin-explorer@{version}/dist/style.css\" />"),
            ),
            body_assets: Some(
                format!("<script src=\"https://unpkg.com/@graphiql/plugin-explorer@{version}/dist/index.umd.js\" crossorigin></script>"),
            ),
            ..Default::default()
        }
    }
}
