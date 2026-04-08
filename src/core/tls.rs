use anyhow::Result;
use rcgen::generate_simple_self_signed;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, ServerConfig, SignatureScheme};
use std::sync::Arc;
use tokio_rustls::{TlsAcceptor, TlsConnector};

const CERT_PATH: &str = "saturn-cert.pem";
const KEY_PATH:  &str = "saturn-key.pem";

// ── Peer connector (client side) ──────────────────────────────────────────────

/// TLS connector for outbound peer connections.
/// Certificate verification is intentionally skipped — the cluster_secret
/// in the HELLO handshake (sent after TLS) handles peer authentication.
/// This still encrypts all cluster traffic against passive eavesdropping.
pub fn make_peer_connector() -> Result<TlsConnector> {
    let config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerifier))
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(config)))
}

/// Returns the server name to use for peer TLS SNI.
/// Since we skip cert verification, this just needs to be a valid hostname.
pub fn peer_server_name() -> ServerName<'static> {
    ServerName::try_from("saturn-peer").unwrap().to_owned()
}

// ── NoVerifier ────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct NoVerifier;

impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity:    &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name:   &ServerName<'_>,
        _ocsp_response: &[u8],
        _now:           UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self, _message: &[u8], _cert: &CertificateDer<'_>, _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self, _message: &[u8], _cert: &CertificateDer<'_>, _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        // Broad list so the TLS handshake succeeds regardless of what the
        // peer's cert uses. We verify nothing anyway.
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
        ]
    }
}

// ── Server acceptor ───────────────────────────────────────────────────────────

pub fn make_acceptor() -> Result<TlsAcceptor> {
    let (cert_pem, key_pem) = load_or_generate_cert()?;

    let certs = rustls_pemfile::certs(&mut cert_pem.as_bytes())
        .collect::<std::result::Result<Vec<CertificateDer>, _>>()?;

    let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())?
        .ok_or_else(|| anyhow::anyhow!("no private key found"))?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, PrivateKeyDer::try_from(key.secret_der().to_vec())
            .map_err(|e| anyhow::anyhow!("{e}"))?)?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn load_or_generate_cert() -> Result<(String, String)> {
    if std::path::Path::new(CERT_PATH).exists() && std::path::Path::new(KEY_PATH).exists() {
        let cert = std::fs::read_to_string(CERT_PATH)?;
        let key  = std::fs::read_to_string(KEY_PATH)?;
        return Ok((cert, key));
    }

    println!("generating self-signed TLS certificate...");
    let cert = generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])?;
    let cert_pem = cert.cert.pem();
    let key_pem  = cert.key_pair.serialize_pem();

    std::fs::write(CERT_PATH, &cert_pem)?;
    std::fs::write(KEY_PATH,  &key_pem)?;
    println!("certificate saved → {CERT_PATH}");

    Ok((cert_pem, key_pem))
}
