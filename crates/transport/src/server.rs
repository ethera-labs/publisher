//! QUIC server with connection registry, identification handshake,
//! and length-prefixed message dispatch.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use quinn::Endpoint;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::error::TransportError;
use crate::framing::LengthPrefixCodec;
use crate::tls;

pub type MessageHandler = Arc<dyn Fn(String, Vec<u8>) + Send + Sync + 'static>;
pub type ConnectionHandler = Arc<dyn Fn(String) + Send + Sync + 'static>;

#[derive(Debug, Default)]
struct ConnectionRegistry {
    connections: HashMap<String, quinn::Connection>,
}

pub struct QuicServer {
    listen_addr: String,
    codec: LengthPrefixCodec,
    registry: Arc<RwLock<ConnectionRegistry>>,
    endpoint: OnceLock<Endpoint>,
}

impl std::fmt::Debug for QuicServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicServer")
            .field("listen_addr", &self.listen_addr)
            .finish()
    }
}

impl QuicServer {
    pub fn new(listen_addr: String, max_message_size: usize) -> Self {
        Self {
            listen_addr,
            codec: LengthPrefixCodec::new(max_message_size),
            registry: Arc::new(RwLock::new(ConnectionRegistry::default())),
            endpoint: OnceLock::new(),
        }
    }

    pub fn start(
        &self,
        on_message: MessageHandler,
        on_connect: Option<ConnectionHandler>,
        on_disconnect: Option<ConnectionHandler>,
    ) -> Result<JoinHandle<()>, TransportError> {
        let tls_config = tls::self_signed_server_config()?;
        let quic_config = quinn::crypto::rustls::QuicServerConfig::try_from(tls_config)
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        let server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_config));

        let addr: std::net::SocketAddr = self
            .listen_addr
            .parse()
            .map_err(|e: std::net::AddrParseError| TransportError::Other(e.to_string()))?;

        let socket = crate::socket::build_udp_socket(addr)?;
        let runtime = quinn::default_runtime()
            .ok_or_else(|| TransportError::Other("no async runtime available".into()))?;
        let endpoint = Endpoint::new(
            quinn::EndpointConfig::default(),
            Some(server_config),
            socket,
            runtime,
        )?;

        self.endpoint
            .set(endpoint.clone())
            .map_err(|_| TransportError::Other("server already started".into()))?;

        info!(addr = %self.listen_addr, "QUIC server listening");

        let registry = self.registry.clone();
        let codec = self.codec.clone();

        let handle = tokio::spawn(async move {
            accept_loop(
                endpoint,
                registry,
                codec,
                on_message,
                on_connect,
                on_disconnect,
            )
            .await;
        });

        Ok(handle)
    }

    pub async fn send_raw(&self, client_id: &str, data: &[u8]) -> Result<(), TransportError> {
        let conn = {
            let reg = self.registry.read().await;
            reg.connections
                .get(client_id)
                .cloned()
                .ok_or_else(|| TransportError::ClientNotFound(client_id.to_string()))?
        };
        send_frame(&conn, &self.codec, data).await
    }

    pub async fn broadcast_raw(&self, data: &[u8], exclude: &str) -> Result<(), TransportError> {
        let connections: Vec<(String, quinn::Connection)> = {
            let reg = self.registry.read().await;
            reg.connections
                .iter()
                .filter(|(id, _)| *id != exclude)
                .map(|(id, conn)| (id.clone(), conn.clone()))
                .collect()
        };

        for (client_id, conn) in connections {
            if let Err(e) = send_frame(&conn, &self.codec, data).await {
                warn!(client_id = %client_id, error = %e, "Failed to send to client");
            }
        }
        Ok(())
    }

    pub async fn connection_count(&self) -> usize {
        self.registry.read().await.connections.len()
    }

    pub fn close(&self) {
        if let Some(ep) = self.endpoint.get() {
            ep.close(0u32.into(), b"server shutting down");
        }
    }
}

async fn send_frame(
    conn: &quinn::Connection,
    codec: &LengthPrefixCodec,
    data: &[u8],
) -> Result<(), TransportError> {
    let (mut send, _recv) = conn
        .open_bi()
        .await
        .map_err(|e| TransportError::Quic(e.to_string()))?;
    let frame = codec.encode(data)?;
    send.write_all(&frame)
        .await
        .map_err(|e| TransportError::Quic(e.to_string()))?;
    send.finish()
        .map_err(|e| TransportError::Quic(e.to_string()))?;
    Ok(())
}

async fn accept_loop(
    endpoint: Endpoint,
    registry: Arc<RwLock<ConnectionRegistry>>,
    codec: LengthPrefixCodec,
    on_message: MessageHandler,
    on_connect: Option<ConnectionHandler>,
    on_disconnect: Option<ConnectionHandler>,
) {
    while let Some(incoming) = endpoint.accept().await {
        let registry = registry.clone();
        let codec = codec.clone();
        let on_message = on_message.clone();
        let on_connect = on_connect.clone();
        let on_disconnect = on_disconnect.clone();

        tokio::spawn(async move {
            match incoming.await {
                Ok(conn) => {
                    if let Err(e) = handle_connection(
                        conn,
                        registry,
                        codec,
                        on_message,
                        on_connect,
                        on_disconnect,
                    )
                    .await
                    {
                        warn!(error = %e, "Connection handler error");
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Failed to accept incoming connection");
                }
            }
        });
    }
}

/// The first bi-stream from each client carries a length-prefixed client ID.
/// All subsequent bi-streams carry length-prefixed protobuf messages.
async fn handle_connection(
    conn: quinn::Connection,
    registry: Arc<RwLock<ConnectionRegistry>>,
    codec: LengthPrefixCodec,
    on_message: MessageHandler,
    on_connect: Option<ConnectionHandler>,
    on_disconnect: Option<ConnectionHandler>,
) -> Result<(), TransportError> {
    let (_send, mut recv) = conn
        .accept_bi()
        .await
        .map_err(|e| TransportError::Quic(e.to_string()))?;

    let mut header = [0u8; 4];
    recv.read_exact(&mut header)
        .await
        .map_err(|e| TransportError::Quic(e.to_string()))?;
    let len = codec.decode_length(&header)?;
    let mut id_buf = vec![0u8; len];
    recv.read_exact(&mut id_buf)
        .await
        .map_err(|e| TransportError::Quic(e.to_string()))?;

    let client_id = String::from_utf8_lossy(&id_buf).into_owned();
    info!(client_id = %client_id, remote = %conn.remote_address(), "Sidecar connected");

    {
        let mut reg = registry.write().await;
        if let Some(old) = reg.connections.insert(client_id.clone(), conn.clone()) {
            warn!(client_id = %client_id, "Replaced existing connection");
            old.close(0u32.into(), b"replaced");
        }
    }

    if let Some(cb) = &on_connect {
        cb(client_id.clone());
    }

    loop {
        let (_send, mut recv) = match conn.accept_bi().await {
            Ok(streams) => streams,
            Err(quinn::ConnectionError::ApplicationClosed { .. })
            | Err(quinn::ConnectionError::ConnectionClosed(_))
            | Err(quinn::ConnectionError::LocallyClosed) => {
                break;
            }
            Err(e) => {
                warn!(client_id = %client_id, error = %e, "accept_bi failed");
                break;
            }
        };

        let mut header = [0u8; 4];
        if let Err(e) = recv.read_exact(&mut header).await {
            warn!(client_id = %client_id, error = %e, "Failed to read frame header");
            continue;
        }
        let len = match codec.decode_length(&header) {
            Ok(len) => len,
            Err(e) => {
                warn!(client_id = %client_id, error = %e, "Invalid frame length");
                continue;
            }
        };
        let mut payload = vec![0u8; len];
        if let Err(e) = recv.read_exact(&mut payload).await {
            warn!(client_id = %client_id, error = %e, "Failed to read frame payload");
            continue;
        }

        on_message(client_id.clone(), payload);
    }

    info!(client_id = %client_id, "Sidecar disconnected");
    {
        let mut reg = registry.write().await;
        if let Some(stored) = reg.connections.get(&client_id) {
            if stored.stable_id() == conn.stable_id() {
                reg.connections.remove(&client_id);
            }
        }
    }

    if let Some(cb) = &on_disconnect {
        cb(client_id);
    }

    Ok(())
}
