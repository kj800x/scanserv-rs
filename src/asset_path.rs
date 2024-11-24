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

    // FIXME: Should use Path operations
    pub fn as_revised_path(&self) -> Self {
        let filename = self.0.split('/').last().unwrap();
        let parts: Vec<&str> = filename.split('.').collect();
        let first_part = parts.get(0).unwrap();

        let updated_first_part = match first_part.split('_').collect::<Vec<_>>()[..] {
            [] => {
                panic!("Invalid filename: {}", filename);
            }
            [prefix] => {
                format!("{}_1", prefix)
            }
            [prefix, suffix] => {
                let new_suffix = suffix.parse::<i32>().unwrap() + 1;
                format!("{}_{}", prefix, new_suffix)
            }
            [_, _, ..] => {
                panic!("Invalid filename: {}", filename);
            }
        };

        let updated_filename = format!("{}.{}", updated_first_part, parts.get(1).unwrap());
        let updated_path = self.0.replace(filename, &updated_filename);
        Self(updated_path)
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
