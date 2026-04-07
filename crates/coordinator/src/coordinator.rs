//! Core coordinator state and public API.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use compose_spec::{ChainId, PeriodId, SequenceNumber, SuperblockNumber, XtRequest};
use compose_spec_sbcp::generate_instance_id;
use prost::Message;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use publisher_metrics::PublisherMetrics;
use publisher_transport::server::QuicServer;

const MAX_PENDING_QUEUE_SIZE: usize = 100;

#[derive(Debug)]
pub(crate) struct ActiveXt {
    chains: Vec<ChainId>,
    votes: HashMap<ChainId, bool>,
    start_time: Instant,
}

#[derive(Debug, Clone)]
pub(crate) struct PendingEntry {
    pub xt_request: compose_spec_proto::XtRequest,
    pub chains: Vec<ChainId>,
}

#[derive(Debug)]
pub(crate) struct CoordinatorState {
    pub chain_to_client: HashMap<ChainId, String>,
    pub client_to_chain: HashMap<String, ChainId>,
    pub active_xts: HashMap<String, ActiveXt>,
    pub active_chains: HashMap<ChainId, String>,
    pub pending_queue: Vec<PendingEntry>,
    pub current_period_id: PeriodId,
    pub next_sequence_num: SequenceNumber,
}

impl CoordinatorState {
    fn new() -> Self {
        Self {
            chain_to_client: HashMap::new(),
            client_to_chain: HashMap::new(),
            active_xts: HashMap::new(),
            active_chains: HashMap::new(),
            pending_queue: Vec::new(),
            current_period_id: PeriodId(0),
            next_sequence_num: SequenceNumber(1),
        }
    }

    fn register_chain(&mut self, client_id: &str, chain_id: ChainId) {
        if let Some(old_chain) = self.client_to_chain.get(client_id) {
            self.chain_to_client.remove(old_chain);
        }
        self.chain_to_client.insert(chain_id, client_id.to_string());
        self.client_to_chain.insert(client_id.to_string(), chain_id);
    }

    fn has_overlap(&self, chains: &[ChainId]) -> bool {
        chains.iter().any(|c| self.active_chains.contains_key(c))
    }

    fn reserve_chains(&mut self, xt_id: &str, chains: &[ChainId]) {
        for &chain_id in chains {
            self.active_chains.insert(chain_id, xt_id.to_string());
        }
    }

    fn release_chains(&mut self, xt_id: &str) {
        if let Some(xt) = self.active_xts.get(xt_id) {
            for chain_id in &xt.chains {
                if self.active_chains.get(chain_id).map(String::as_str) == Some(xt_id) {
                    self.active_chains.remove(chain_id);
                }
            }
        }
    }

    fn check_decision(&self, xt_id: &str) -> Option<bool> {
        let xt = self.active_xts.get(xt_id)?;
        if xt.votes.len() < xt.chains.len() {
            return None;
        }
        Some(xt.votes.values().all(|&v| v))
    }

    /// Mutates state and returns the encoded message ready to broadcast
    /// outside the lock.
    fn prepare_xt(
        &mut self,
        xt_req: &compose_spec_proto::XtRequest,
        chains: &[ChainId],
    ) -> (String, Vec<u8>) {
        let compose_req = proto_to_spec_xt(xt_req);
        let seq_num = self.next_sequence_num;
        let period_id = self.current_period_id;
        let instance_id = generate_instance_id(period_id, seq_num, &compose_req);
        let xt_id = instance_id.to_string();
        self.next_sequence_num = SequenceNumber(seq_num.get() + 1);

        self.reserve_chains(&xt_id, chains);
        self.active_xts.insert(
            xt_id.clone(),
            ActiveXt {
                chains: chains.to_vec(),
                votes: HashMap::new(),
                start_time: Instant::now(),
            },
        );

        let msg = compose_spec_proto::Message {
            sender_id: "publisher".into(),
            payload: Some(compose_spec_proto::Payload::StartInstance(
                compose_spec_proto::StartInstance {
                    instance_id: instance_id.as_bytes().to_vec(),
                    period_id: period_id.get(),
                    sequence_number: seq_num.get(),
                    xt_request: Some(xt_req.clone()),
                },
            )),
        };

        info!(
            xt_id,
            period_id = %period_id,
            seq_num = %seq_num,
            chains = chains.len(),
            "XT prepared"
        );

        (xt_id, msg.encode_to_vec())
    }

    /// Returns `Some((decision, latency_secs, encoded))` when quorum is
    /// reached (or any vote is false), `None` otherwise.
    fn record_vote(
        &mut self,
        xt_id: &str,
        instance_id_bytes: &[u8],
        chain_id: ChainId,
        vote: bool,
    ) -> Option<(bool, f64, Vec<u8>)> {
        let xt = self.active_xts.get_mut(xt_id)?;
        xt.votes.insert(chain_id, vote);

        let decision = if !vote {
            Some(false)
        } else {
            self.check_decision(xt_id)
        };

        let decision = decision?;

        let latency = self
            .active_xts
            .get(xt_id)
            .map(|x| x.start_time.elapsed().as_secs_f64())
            .unwrap_or(0.0);

        self.release_chains(xt_id);
        self.active_xts.remove(xt_id);

        let msg = compose_spec_proto::Message {
            sender_id: "publisher".into(),
            payload: Some(compose_spec_proto::Payload::Decided(
                compose_spec_proto::Decided {
                    instance_id: instance_id_bytes.to_vec(),
                    decision,
                },
            )),
        };

        Some((decision, latency, msg.encode_to_vec()))
    }

    fn take_next_ready(&mut self) -> Option<PendingEntry> {
        let idx = self
            .pending_queue
            .iter()
            .position(|e| !self.has_overlap(&e.chains))?;
        Some(self.pending_queue.remove(idx))
    }
}

pub struct Coordinator {
    pub(crate) state: Arc<RwLock<CoordinatorState>>,
    pub(crate) server: Arc<QuicServer>,
    pub(crate) metrics: Option<Arc<PublisherMetrics>>,
    messages_processed: AtomicU64,
    broadcasts_sent: AtomicU64,
    start_time: Instant,
}

impl std::fmt::Debug for Coordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Coordinator").finish()
    }
}

impl Coordinator {
    pub fn new(server: Arc<QuicServer>, metrics: Option<Arc<PublisherMetrics>>) -> Self {
        Self {
            state: Arc::new(RwLock::new(CoordinatorState::new())),
            server,
            metrics,
            messages_processed: AtomicU64::new(0),
            broadcasts_sent: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    pub fn server(&self) -> &Arc<QuicServer> {
        &self.server
    }

    pub fn inc_messages(&self) {
        self.messages_processed.fetch_add(1, Ordering::Relaxed);
        if let Some(m) = &self.metrics {
            m.messages_received_total.inc();
        }
    }

    fn inc_broadcasts(&self) {
        self.broadcasts_sent.fetch_add(1, Ordering::Relaxed);
        if let Some(m) = &self.metrics {
            m.broadcasts_sent_total.inc();
        }
    }

    pub async fn register_chain(&self, client_id: &str, chain_id: ChainId) {
        let mut state = self.state.write().await;
        state.register_chain(client_id, chain_id);
        info!(client_id, chain_id = %chain_id, "Chain registered");
    }

    pub async fn start_period(
        &self,
        period_id: PeriodId,
        superblock_num: SuperblockNumber,
    ) -> Result<(), publisher_transport::error::TransportError> {
        {
            let mut state = self.state.write().await;
            state.current_period_id = period_id;
            state.next_sequence_num = SequenceNumber(1);
        }

        let msg = compose_spec_proto::Message {
            sender_id: "publisher".into(),
            payload: Some(compose_spec_proto::Payload::StartPeriod(
                compose_spec_proto::StartPeriod {
                    period_id: period_id.get(),
                    superblock_number: superblock_num.get(),
                },
            )),
        };
        let data = msg.encode_to_vec();

        info!(period_id = %period_id, superblock_num = %superblock_num, "Broadcasting period");
        self.inc_broadcasts();
        if let Some(m) = &self.metrics {
            m.period_broadcast_total.inc();
        }
        self.server.broadcast_raw(&data, "").await
    }

    pub(crate) async fn handle_xt_request(
        &self,
        client_id: String,
        xt_req: compose_spec_proto::XtRequest,
    ) {
        let chains = extract_chains(&xt_req);

        let broadcast = {
            let mut state = self.state.write().await;

            if state.has_overlap(&chains) {
                if state.pending_queue.len() >= MAX_PENDING_QUEUE_SIZE {
                    warn!(client_id, "XT queue full, rejecting");
                    return;
                }
                state.pending_queue.push(PendingEntry {
                    xt_request: xt_req,
                    chains,
                });
                if let Some(m) = &self.metrics {
                    m.xt_queued_total.inc();
                }
                None
            } else {
                Some(state.prepare_xt(&xt_req, &chains))
            }
        };

        if let Some((_xt_id, data)) = broadcast {
            self.inc_broadcasts();
            if let Some(m) = &self.metrics {
                m.xt_started_total.inc();
            }
            if let Err(e) = self.server.broadcast_raw(&data, "").await {
                error!(error = %e, "Failed to broadcast XT start");
            }
        }
    }

    pub(crate) async fn handle_vote(
        &self,
        _client_id: &str,
        instance_id_bytes: &[u8],
        chain_id: ChainId,
        vote: bool,
    ) {
        let xt_id = hex::encode(instance_id_bytes);
        info!(xt_id, chain_id = %chain_id, vote, "Vote received");

        let result = {
            let mut state = self.state.write().await;
            state.record_vote(&xt_id, instance_id_bytes, chain_id, vote)
        };

        if let Some((decision, latency, data)) = result {
            info!(
                xt_id,
                decision,
                latency_ms = (latency * 1000.0) as u64,
                "Decision reached"
            );
            if let Some(m) = &self.metrics {
                if decision {
                    m.xt_decided_commit_total.inc();
                } else {
                    m.xt_decided_abort_total.inc();
                }
                m.xt_decision_latency_seconds.observe(latency);
            }

            self.inc_broadcasts();
            if let Err(e) = self.server.broadcast_raw(&data, "").await {
                error!(xt_id, error = %e, "Failed to broadcast decision");
            }

            self.drain_queue().await;
        }
    }

    pub(crate) async fn handle_mailbox_relay(&self, mailbox: &compose_spec_proto::MailboxMessage) {
        let dest_chain = ChainId::new(mailbox.destination_chain);

        let client_id = {
            let state = self.state.read().await;
            state.chain_to_client.get(&dest_chain).cloned()
        };

        let Some(client_id) = client_id else {
            warn!(dest_chain = %dest_chain, "No sidecar for destination chain");
            return;
        };

        let msg = compose_spec_proto::Message {
            sender_id: "publisher".into(),
            payload: Some(compose_spec_proto::Payload::MailboxMessage(mailbox.clone())),
        };
        let data = msg.encode_to_vec();
        self.inc_broadcasts();

        if let Err(e) = self.server.send_raw(&client_id, &data).await {
            warn!(client_id, error = %e, "Failed to relay mailbox");
        }
    }

    pub async fn broadcast_rollback(
        &self,
        period_id: u64,
        last_sb_num: u64,
        last_sb_hash: &[u8],
    ) -> Result<(), publisher_transport::error::TransportError> {
        let msg = compose_spec_proto::Message {
            sender_id: "publisher".into(),
            payload: Some(compose_spec_proto::Payload::Rollback(
                compose_spec_proto::Rollback {
                    period_id,
                    last_finalized_superblock_number: last_sb_num,
                    last_finalized_superblock_hash: last_sb_hash.to_vec(),
                },
            )),
        };
        let data = msg.encode_to_vec();

        info!(period_id, last_sb_num, "Broadcasting rollback");
        self.inc_broadcasts();
        self.server.broadcast_raw(&data, "").await
    }

    pub(crate) async fn handle_ping(&self, client_id: &str, timestamp: i64) {
        let msg = compose_spec_proto::Message {
            sender_id: "publisher".into(),
            payload: Some(compose_spec_proto::Payload::Pong(
                compose_spec_proto::Pong { timestamp },
            )),
        };
        let data = msg.encode_to_vec();
        if let Err(e) = self.server.send_raw(client_id, &data).await {
            warn!(client_id, error = %e, "Failed to send pong");
        }
    }

    async fn drain_queue(&self) {
        loop {
            let broadcast = {
                let mut state = self.state.write().await;
                let Some(entry) = state.take_next_ready() else {
                    return;
                };
                Some(state.prepare_xt(&entry.xt_request, &entry.chains))
            };

            if let Some((_xt_id, data)) = broadcast {
                self.inc_broadcasts();
                if let Some(m) = &self.metrics {
                    m.xt_started_total.inc();
                }
                if let Err(e) = self.server.broadcast_raw(&data, "").await {
                    error!(error = %e, "Failed to broadcast queued XT");
                }
            }
        }
    }

    pub async fn stats(&self) -> serde_json::Value {
        let state = self.state.read().await;
        serde_json::json!({
            "active_connections": self.server.connection_count().await,
            "registered_chains": state.chain_to_client.len(),
            "active_2pc_transactions": state.active_xts.len(),
            "active_chains": state.active_chains.len(),
            "queued_xts": state.pending_queue.len(),
            "messages_processed": self.messages_processed.load(Ordering::Relaxed),
            "broadcasts_sent": self.broadcasts_sent.load(Ordering::Relaxed),
            "uptime_seconds": self.start_time.elapsed().as_secs_f64(),
        })
    }
}

fn extract_chains(req: &compose_spec_proto::XtRequest) -> Vec<ChainId> {
    let mut seen = std::collections::HashSet::new();
    let mut chains = Vec::new();
    for tr in &req.transaction_requests {
        let cid = ChainId::new(tr.chain_id);
        if seen.insert(cid) {
            chains.push(cid);
        }
    }
    chains
}

fn proto_to_spec_xt(req: &compose_spec_proto::XtRequest) -> XtRequest {
    XtRequest {
        transactions: req
            .transaction_requests
            .iter()
            .map(|tr| compose_spec::TransactionRequest {
                chain_id: ChainId::new(tr.chain_id),
                transactions: tr.transaction.clone(),
            })
            .collect(),
    }
}
