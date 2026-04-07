//! Inbound message dispatch.

use std::sync::Arc;

use compose_spec::ChainId;
use compose_spec_proto::{Message, Payload};
use prost::Message as _;
use tracing::{error, info, warn};

use crate::coordinator::Coordinator;

pub async fn dispatch(coordinator: Arc<Coordinator>, client_id: String, data: Vec<u8>) {
    coordinator.inc_messages();

    let msg = match Message::decode(data.as_slice()) {
        Ok(m) => m,
        Err(e) => {
            error!(client_id, error = %e, "Failed to decode message");
            return;
        }
    };

    let Some(payload) = msg.payload else {
        warn!(client_id, "Empty payload");
        return;
    };

    match payload {
        Payload::Vote(vote) => {
            coordinator
                .handle_vote(
                    &client_id,
                    &vote.instance_id,
                    ChainId::new(vote.chain_id),
                    vote.vote,
                )
                .await;
        }
        Payload::XtRequest(xt_req) => {
            coordinator.handle_xt_request(client_id, xt_req).await;
        }
        Payload::Ping(ping) => {
            coordinator.handle_ping(&client_id, ping.timestamp).await;
        }
        Payload::HandshakeRequest(req) => {
            handle_handshake(coordinator, &client_id, &req).await;
        }
        Payload::MailboxMessage(mb) => {
            coordinator.handle_mailbox_relay(&mb).await;
        }
        other => {
            warn!(client_id, payload_type = ?std::mem::discriminant(&other), "Unhandled payload");
        }
    }
}

async fn handle_handshake(
    coordinator: Arc<Coordinator>,
    client_id: &str,
    req: &compose_spec_proto::HandshakeRequest,
) {
    info!(client_id, requested_id = %req.client_id, "Handshake received");

    if !req.client_id.is_empty() {
        let chain_id = parse_chain_id(&req.client_id);
        coordinator.register_chain(client_id, chain_id).await;
    }

    let resp = compose_spec_proto::Message {
        sender_id: "publisher".into(),
        payload: Some(Payload::HandshakeResponse(
            compose_spec_proto::HandshakeResponse {
                accepted: true,
                error: String::new(),
                session_id: client_id.to_string(),
            },
        )),
    };
    let data = resp.encode_to_vec();
    if let Err(e) = coordinator.server().send_raw(client_id, &data).await {
        warn!(client_id, error = %e, "Failed to send handshake response");
    }
}

fn parse_chain_id(client_id: &str) -> ChainId {
    let num_str: String = client_id
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    let id = num_str.parse::<u64>().unwrap_or(0);
    ChainId::new(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_chain_id_numeric_prefix() {
        assert_eq!(parse_chain_id("77777"), ChainId::new(77777));
        assert_eq!(parse_chain_id("88888-sidecar"), ChainId::new(88888));
        assert_eq!(parse_chain_id("abc"), ChainId::new(0));
        assert_eq!(parse_chain_id(""), ChainId::new(0));
    }
}
