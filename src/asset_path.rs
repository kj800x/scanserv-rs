use async_graphql::Object;

#[derive(Debug, Clone)]
pub struct AssetPath(String);

impl AssetPath {
    pub fn as_disk_path(&self, assets_dir: &str) -> String {
        format!("{}/{}", assets_dir, self.0)
    }

    pub fn as_web_path(&self) -> String {
        format!("/assets/{}", self.0)
    }

    pub fn as_relative_path(&self) -> String {
        self.0.clone()
    }

    pub fn from_relative_path(path: String) -> Self {
        Self(path)
    }
}

impl From<String> for AssetPath {
    fn from(path: String) -> Self {
        AssetPath::from_relative_path(path)
    }
}
impl Into<String> for AssetPath {
    fn into(self) -> String {
        self.as_relative_path()
    }
}

#[Object]
impl AssetPath {
    async fn path(&self) -> String {
        self.as_web_path()
    }
}
