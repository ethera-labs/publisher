//! Self-signed TLS configuration for QUIC endpoints.

use std::sync::Arc;

use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

use crate::error::TransportError;

const ALPN_ETHERA_QUIC: &[u8] = b"ethera-quic";

fn generate_self_signed(
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), TransportError> {
    let cert = generate_simple_self_signed(vec!["localhost".to_string()])
        .map_err(|e| TransportError::Tls(e.to_string()))?;

    let cert_der = CertificateDer::from(cert.cert);
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()));

    Ok((vec![cert_der], key_der))
}

pub fn self_signed_server_config() -> Result<Arc<rustls::ServerConfig>, TransportError> {
    let (certs, key) = generate_self_signed()?;
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| TransportError::Tls(e.to_string()))?;
    config.alpn_protocols = vec![ALPN_ETHERA_QUIC.to_vec()];
    Ok(Arc::new(config))
}
