use crate::util::verify_password;
use anyhow::anyhow;
use std::fs;
use std::path::PathBuf;
use tracing::debug;

pub struct ServerData {
    hash_str: String,
}

impl ServerData {
    pub fn new(passfile_path: &PathBuf) -> anyhow::Result<Self> {
        if !passfile_path.exists() {
            return Err(anyhow!(
                "Passfile {} does not exist. Please run the client with --set-password first.",
                passfile_path.display()
            ));
        } else if passfile_path.is_dir() {
            return Err(anyhow!(
                "Passfile {} is a directory. Please specify the valid passfile.",
                passfile_path.display()
            ));
        }
        debug!("Loading passfile {}", passfile_path.display());
        let hash_str = fs::read_to_string(&passfile_path)?;
        Ok(Self { hash_str })
    }

    pub fn verify_password(&self, password: &str) -> anyhow::Result<bool> {
        verify_password(password, &self.hash_str)
    }
}
