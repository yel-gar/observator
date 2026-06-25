use crate::errors::ConnectionLimitError;
use crate::server::{MAX_CONNECTIONS_PER_IP, MAX_TOTAL_CONNECTIONS};
use crate::util::verify_password;
use anyhow::anyhow;
use std::collections::HashMap;
use std::fs;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Mutex, RwLock};
use tracing::debug;

pub struct ServerData {
    hash_str: String,
    connections_count: AtomicUsize,
    connections_map: Mutex<HashMap<IpAddr, usize>>,
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
        Ok(Self {
            hash_str,
            connections_count: AtomicUsize::new(0),
            connections_map: Mutex::new(HashMap::new()),
        })
    }

    pub fn verify_password(&self, password: &str) -> anyhow::Result<bool> {
        verify_password(password, &self.hash_str)
    }

    pub async fn inc_connections(&self, ip: &IpAddr) -> Result<(), ConnectionLimitError> {
        if self.connections_count.load(Ordering::Relaxed) >= MAX_TOTAL_CONNECTIONS {
            return Err(ConnectionLimitError::Global);
        }
        self.connections_count.fetch_add(1, Ordering::Relaxed);

        let mut map = self.connections_map.lock().await;
        let count = map.entry(*ip).or_insert(0);

        if *count >= MAX_CONNECTIONS_PER_IP {
            return Err(ConnectionLimitError::Individual);
        }
        *count += 1;

        Ok(())
    }

    pub async fn dec_connections(&self, ip: &IpAddr) {
        self.connections_count.fetch_sub(1, Ordering::Relaxed);
        let mut map = self.connections_map.lock().await;
        if let Some(count) = map.get_mut(ip) {
            *count -= 1;
            if *count <= 0 {
                map.remove(ip);
            }
        }
    }
}
