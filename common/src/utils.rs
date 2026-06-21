use rustls::pki_types::CertificateDer;
use sha2::{Digest, Sha256};
use tracing::info;

pub fn log_cert_fingerprint(der: &CertificateDer) {
    let hash = Sha256::digest(der);
    info!("Certificate hash: {}", hex::encode(hash));
}
