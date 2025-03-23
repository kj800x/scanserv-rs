use async_graphql::Object;
use std::path::Path;

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

    // Get the next available path by checking if files exist on disk
    pub fn get_next_available_path(&self, assets_dir: &str) -> Self {
        let filename = self.0.split('/').last().unwrap();
        let dir_part = self.0.rsplit_once('/').map(|(dir, _)| dir).unwrap_or("");
        let parts: Vec<&str> = filename.split('.').collect();
        let first_part = parts.get(0).unwrap();
        let extension = parts.get(1).unwrap_or(&"png");

        let mut counter = 1;
        loop {
            let next_filename = if first_part.contains('_') {
                // If already has an underscore, try to update the counter
                let name_parts: Vec<&str> = first_part.split('_').collect();

                if name_parts.len() == 2 && name_parts[1].parse::<i32>().is_ok() {
                    let prefix = name_parts[0];
                    format!("{}_{}.{}", prefix, counter, extension)
                } else {
                    // Just add a counter if we can't parse existing counter
                    format!("{}_1.{}", first_part, extension)
                }
            } else {
                // Add _1 to the filename
                format!("{}_1.{}", first_part, extension)
            };

            let next_path = if dir_part.is_empty() {
                next_filename.clone()
            } else {
                format!("{}/{}", dir_part, next_filename)
            };

            let full_path = Path::new(assets_dir).join(&next_path);

            if !full_path.exists() {
                return Self(next_path);
            }

            counter += 1;
        }
    }

    // FIXME: Should use Path operations
    // This is kept for backward compatibility
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
