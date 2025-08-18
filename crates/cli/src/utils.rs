use std::path::PathBuf;

pub trait ExpandPath {
    fn expand_path(&self) -> Self;
}

impl ExpandPath for PathBuf {
    fn expand_path(&self) -> Self {
        let path_as_str = self.to_string_lossy();
        PathBuf::from(
            shellexpand::full(&path_as_str)
                .expect("Invalid path expansion")
                .into_owned(),
        )
    }
}
