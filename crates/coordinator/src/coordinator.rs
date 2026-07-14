//! Core coordinator state and public API.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::l1_submit::L1Submitter;
use crate::proof_types::ProofData;

use ethera_spec::{
    ChainId, Instance, InstanceId, PeriodId, SequenceNumber, SuperblockNumber, XtRequest,
};
use ethera_spec_sbcp::generate_instance_id;
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
    pub xt_request: ethera_spec_proto::XtRequest,
    pub chains: Vec<ChainId>,
}

#[derive(Debug)]
pub(crate) struct CoordinatorState {
    pub chain_to_client: HashMap<ChainId, String>,
    pub client_to_chain: HashMap<String, ChainId>,
    pub active_xts: HashMap<InstanceId, ActiveXt>,
    pub active_chains: HashMap<ChainId, InstanceId>,
    pub pending_queue: Vec<PendingEntry>,
    pub current_period_id: PeriodId,
    pub next_sequence_num: SequenceNumber,
    pub next_superblock_number: SuperblockNumber,
    pub last_finalized_superblock_number: u64,
    pub last_finalized_superblock_hash: Vec<u8>,
    /// Latest proof per chain. op-succinct reports chain-local `end_block`,
    /// while the publisher owns the global superblock number used for settlement.
    pending_proofs: HashMap<u64, ProofData>,
    proof_collection_started: Option<Instant>,
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
            next_superblock_number: SuperblockNumber::new(1),
            last_finalized_superblock_number: 0,
            last_finalized_superblock_hash: Vec::new(),
            pending_proofs: HashMap::new(),
            proof_collection_started: None,
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

    fn reserve_chains(&mut self, id: InstanceId, chains: &[ChainId]) {
        for &chain_id in chains {
            self.active_chains.insert(chain_id, id);
        }
    }

    fn release_chains(&mut self, id: InstanceId) {
        if let Some(xt) = self.active_xts.get(&id) {
            for chain_id in &xt.chains {
                if self.active_chains.get(chain_id) == Some(&id) {
                    self.active_chains.remove(chain_id);
                }
            }
        }
    }

    fn prepare_xt(
        &mut self,
        xt_req: &ethera_spec_proto::XtRequest,
        chains: &[ChainId],
    ) -> (InstanceId, Vec<u8>) {
        let seq_num = self.next_sequence_num;
        let period_id = self.current_period_id;
        let spec_req = XtRequest::from(xt_req);
        let instance = Instance {
            id: generate_instance_id(period_id, seq_num, &spec_req),
            period_id,
            sequence_number: seq_num,
            xt_request: spec_req,
        };
        let id = instance.id;
        self.next_sequence_num = seq_num + 1;

        self.reserve_chains(id, chains);
        self.active_xts.insert(
            id,
            ActiveXt {
                chains: chains.to_vec(),
                votes: HashMap::new(),
                start_time: Instant::now(),
            },
        );

        let msg = ethera_spec_proto::Message {
            sender_id: "publisher".into(),
            payload: Some(ethera_spec_proto::Payload::StartInstance(
                (&instance).into(),
            )),
        };

        info!(
            xt_id = %id,
            period_id = %period_id,
            seq_num = %seq_num,
            chains = chains.len(),
            "XT prepared"
        );

        (id, msg.encode_to_vec())
    }

    fn record_vote(
        &mut self,
        id: InstanceId,
        chain_id: ChainId,
        vote: bool,
    ) -> Option<(bool, f64, Vec<u8>)> {
        let xt = self.active_xts.get_mut(&id)?;

        if !xt.chains.contains(&chain_id) {
            warn!(xt_id = %id, chain_id = %chain_id, "Ignoring vote from non-participant chain");
            return None;
        }

        if xt.votes.contains_key(&chain_id) {
            warn!(xt_id = %id, chain_id = %chain_id, "Ignoring duplicate vote");
            return None;
        }

        xt.votes.insert(chain_id, vote);

        // A single reject decides false immediately and removes the xT, so by the
        // time every chain has voted here, all recorded votes are `true` -- the
        // decision is unanimous commit without re-scanning the map.
        let decision = if !vote {
            false
        } else if xt.votes.len() == xt.chains.len() {
            true
        } else {
            return None;
        };
        let latency = xt.start_time.elapsed().as_secs_f64();

        self.release_chains(id);
        self.active_xts.remove(&id);

        let msg = ethera_spec_proto::Message {
            sender_id: "publisher".into(),
            payload: Some(ethera_spec_proto::Payload::Decided(
                ethera_spec_proto::Decided {
                    instance_id: id.as_bytes().to_vec(),
                    decision,
                },
            )),
        };

        Some((decision, latency, msg.encode_to_vec()))
    }

    /// Finds timed-out xTs and produces `Decided(false)` messages for each.
    fn reap_timed_out(&mut self, timeout: Duration) -> Vec<(InstanceId, Vec<u8>)> {
        let now = Instant::now();
        let expired: Vec<InstanceId> = self
            .active_xts
            .iter()
            .filter(|(_, xt)| now.duration_since(xt.start_time) >= timeout)
            .map(|(id, _)| *id)
            .collect();

        let mut results = Vec::with_capacity(expired.len());
        for id in expired {
            self.release_chains(id);
            self.active_xts.remove(&id);

            let msg = ethera_spec_proto::Message {
                sender_id: "publisher".into(),
                payload: Some(ethera_spec_proto::Payload::Decided(
                    ethera_spec_proto::Decided {
                        instance_id: id.as_bytes().to_vec(),
                        decision: false,
                    },
                )),
            };
            results.push((id, msg.encode_to_vec()));
        }
        results
    }

    /// Returns true if proof collection has expired and clears collected proofs.
    fn reap_expired_proofs(&mut self, proof_window: Duration) -> bool {
        if let Some(started) = self.proof_collection_started {
            if Instant::now().duration_since(started) >= proof_window {
                self.pending_proofs.clear();
                self.proof_collection_started = None;
                return true;
            }
        }
        false
    }

    fn take_next_ready(&mut self) -> Option<PendingEntry> {
        let idx = self
            .pending_queue
            .iter()
            .position(|e| !self.has_overlap(&e.chains))?;
        Some(self.pending_queue.remove(idx))
    }

    fn is_chain_registered(&self, chain_id: ChainId) -> bool {
        self.chain_to_client.contains_key(&chain_id)
    }
}

pub struct Coordinator {
    pub(crate) state: Arc<RwLock<CoordinatorState>>,
    pub(crate) server: Arc<QuicServer>,
    pub(crate) metrics: Option<Arc<PublisherMetrics>>,
    pub(crate) l1_submitter: Option<Arc<L1Submitter>>,
    proof_mode: ProofMode,
    scp_timeout: Duration,
    proof_window: Duration,
    messages_processed: AtomicU64,
    broadcasts_sent: AtomicU64,
    protocol_ready: AtomicBool,
    start_time: Instant,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum ProofMode {
    #[default]
    Real,
    Mock,
}

impl std::fmt::Debug for Coordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Coordinator").finish()
    }
}

impl Coordinator {
    pub fn new(
        server: Arc<QuicServer>,
        metrics: Option<Arc<PublisherMetrics>>,
        scp_timeout: Duration,
        proof_window: Duration,
    ) -> Self {
        Self {
            state: Arc::new(RwLock::new(CoordinatorState::new())),
            server,
            metrics,
            l1_submitter: None,
            proof_mode: ProofMode::Real,
            scp_timeout,
            proof_window,
            messages_processed: AtomicU64::new(0),
            broadcasts_sent: AtomicU64::new(0),
            protocol_ready: AtomicBool::new(false),
            start_time: Instant::now(),
        }
    }

    pub fn with_l1_submitter(mut self, submitter: L1Submitter) -> Self {
        self.l1_submitter = Some(Arc::new(submitter));
        self
    }

    pub fn with_mock_proofs(mut self) -> Self {
        self.proof_mode = ProofMode::Mock;
        self
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

    pub async fn wait_for_chains(&self, required: &[u64], timeout: Duration) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let ready = {
                let state = self.state.read().await;
                required
                    .iter()
                    .all(|chain_id| state.is_chain_registered(ChainId::new(*chain_id)))
            };
            if ready {
                return Ok(());
            }
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "required chains did not reconnect before recovery timeout"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    pub async fn broadcast_recovery_rollback(
        &self,
        period_id: PeriodId,
    ) -> Result<(), publisher_transport::error::TransportError> {
        let (superblock_number, superblock_hash) = {
            let state = self.state.read().await;
            (
                state.last_finalized_superblock_number,
                state.last_finalized_superblock_hash.clone(),
            )
        };
        let msg = ethera_spec_proto::Message {
            sender_id: "publisher".into(),
            payload: Some(ethera_spec_proto::Payload::Rollback(
                ethera_spec_proto::Rollback {
                    period_id: period_id.get(),
                    last_finalized_superblock_number: superblock_number,
                    last_finalized_superblock_hash: superblock_hash,
                },
            )),
        };

        warn!(
            period_id = period_id.get(),
            last_finalized_superblock = superblock_number,
            "Broadcasting startup recovery rollback"
        );
        self.inc_broadcasts();
        self.server.broadcast_raw(&msg.encode_to_vec(), "").await
    }

    pub fn activate_protocol(&self) {
        self.protocol_ready.store(true, Ordering::Release);
        info!("Protocol recovery completed");
    }

    /// Initializes superblock state from L1 on startup - must be called before
    /// the period loop starts so `next_superblock_number` and `parent_hash` are
    /// correct after a restart.
    pub async fn init_from_l1(&self) -> anyhow::Result<()> {
        if let Some(submitter) = &self.l1_submitter {
            match submitter.fetch_latest_superblock_state().await? {
                Some((sb_num, sb_hash)) => {
                    let mut state = self.state.write().await;
                    state.last_finalized_superblock_number = sb_num;
                    state.last_finalized_superblock_hash = sb_hash.to_vec();
                    state.next_superblock_number = SuperblockNumber::new(sb_num + 1);
                    info!(
                        last_finalized = sb_num,
                        next = sb_num + 1,
                        "Initialized superblock state from L1"
                    );
                }
                None => {
                    info!("No seeded superblock hash on L1 yet - starting from genesis");
                }
            }
        }
        Ok(())
    }

    pub async fn start_period(
        &self,
        period_id: PeriodId,
    ) -> Result<(), publisher_transport::error::TransportError> {
        let superblock_num = {
            let mut state = self.state.write().await;
            if period_id <= state.current_period_id {
                return Ok(());
            }
            state.current_period_id = period_id;
            state.next_sequence_num = SequenceNumber(1);
            state.next_superblock_number
        };

        let msg = ethera_spec_proto::Message {
            sender_id: "publisher".into(),
            payload: Some(ethera_spec_proto::Payload::StartPeriod(
                ethera_spec_proto::StartPeriod {
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
        xt_req: ethera_spec_proto::XtRequest,
    ) {
        if !self.protocol_ready.load(Ordering::Acquire) {
            warn!(
                client_id,
                "Rejecting XT while protocol recovery is in progress"
            );
            return;
        }
        let chains = extract_chains(&xt_req);

        if chains.len() < 2 {
            warn!(
                client_id,
                chains = chains.len(),
                "Rejecting XT: must span at least 2 chains"
            );
            return;
        }

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
        let Ok(id_bytes) = <[u8; 32]>::try_from(instance_id_bytes) else {
            warn!(
                len = instance_id_bytes.len(),
                chain_id = %chain_id,
                "Ignoring vote: instance id must be 32 bytes"
            );
            return;
        };
        let id = InstanceId::new(id_bytes);
        info!(xt_id = %id, chain_id = %chain_id, vote, "Vote received");

        let result = {
            let mut state = self.state.write().await;
            state.record_vote(id, chain_id, vote)
        };

        if let Some((decision, latency, data)) = result {
            info!(
                xt_id = %id,
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
                error!(xt_id = %id, error = %e, "Failed to broadcast decision");
            }

            self.drain_queue().await;
        }
    }

    pub(crate) async fn handle_mailbox_relay(&self, mailbox: &ethera_spec_proto::MailboxMessage) {
        let dest_chain = ChainId::new(mailbox.destination_chain);

        let client_id = {
            let state = self.state.read().await;
            state.chain_to_client.get(&dest_chain).cloned()
        };

        let Some(client_id) = client_id else {
            warn!(dest_chain = %dest_chain, "No sidecar for destination chain");
            return;
        };

        let msg = ethera_spec_proto::Message {
            sender_id: "publisher".into(),
            payload: Some(ethera_spec_proto::Payload::MailboxMessage(mailbox.clone())),
        };
        let data = msg.encode_to_vec();
        self.inc_broadcasts();

        if let Err(e) = self.server.send_raw(&client_id, &data).await {
            warn!(client_id, error = %e, "Failed to relay mailbox");
        }
    }

    pub(crate) async fn handle_ping(&self, client_id: &str, timestamp: i64) {
        let msg = ethera_spec_proto::Message {
            sender_id: "publisher".into(),
            payload: Some(ethera_spec_proto::Payload::Pong(ethera_spec_proto::Pong {
                timestamp,
            })),
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

    pub async fn reap_timed_out_xts(&self) {
        let timed_out = {
            let mut state = self.state.write().await;
            state.reap_timed_out(self.scp_timeout)
        };

        for (id, data) in &timed_out {
            warn!(xt_id = %id, "SCP timeout - deciding false");
            if let Some(m) = &self.metrics {
                m.xt_decided_abort_total.inc();
            }
            self.inc_broadcasts();
            if let Err(e) = self.server.broadcast_raw(data, "").await {
                error!(xt_id = %id, error = %e, "Failed to broadcast timeout decision");
            }
        }

        if !timed_out.is_empty() {
            self.drain_queue().await;
        }
    }

    pub async fn reap_expired_proofs(&self) {
        let expired = {
            let mut state = self.state.write().await;
            state.reap_expired_proofs(self.proof_window)
        };

        if expired {
            warn!("Proof window expired - triggering rollback");

            let (period_id, last_sb_num, last_sb_hash) = {
                let mut state = self.state.write().await;
                state.next_superblock_number =
                    SuperblockNumber::new(state.last_finalized_superblock_number + 1);
                (
                    state.current_period_id.get(),
                    state.last_finalized_superblock_number,
                    state.last_finalized_superblock_hash.clone(),
                )
            };

            let msg = ethera_spec_proto::Message {
                sender_id: "publisher".into(),
                payload: Some(ethera_spec_proto::Payload::Rollback(
                    ethera_spec_proto::Rollback {
                        period_id,
                        last_finalized_superblock_number: last_sb_num,
                        last_finalized_superblock_hash: last_sb_hash,
                    },
                )),
            };
            let data = msg.encode_to_vec();

            info!(period_id, last_sb_num, "Broadcasting rollback");
            self.inc_broadcasts();
            if let Err(e) = self.server.broadcast_raw(&data, "").await {
                error!(error = %e, "Failed to broadcast rollback");
            }
        }
    }

    pub async fn is_chain_registered(&self, chain_id: ChainId) -> bool {
        let state = self.state.read().await;
        state.is_chain_registered(chain_id)
    }

    pub async fn chain_for_client(&self, client_id: &str) -> Option<ChainId> {
        let state = self.state.read().await;
        state.client_to_chain.get(client_id).copied()
    }

    pub async fn current_superblock_number(&self) -> u64 {
        let state = self.state.read().await;
        state.next_superblock_number.get()
    }

    /// Collects the latest proof from each chain and submits once all chains report.
    /// The incoming `superblock_number` is chain-local provenance; settlement uses
    /// the publisher's current global superblock number.
    pub async fn receive_proof(&self, superblock_number: u64, chain_id: u64, data: ProofData) {
        if !self.protocol_ready.load(Ordering::Acquire) {
            warn!(
                chain_id,
                "Ignoring proof while protocol recovery is in progress"
            );
            return;
        }
        let (collected, total, ready_proofs, submit_sb_number) = {
            let mut state = self.state.write().await;
            let total = state.chain_to_client.len();

            if state.pending_proofs.contains_key(&chain_id) {
                warn!(
                    chain_id,
                    superblock_number, "Replacing existing proof for chain"
                );
            }

            if state.proof_collection_started.is_none() {
                state.proof_collection_started = Some(Instant::now());
            }

            state.pending_proofs.insert(chain_id, data);
            let collected = state.pending_proofs.len();

            if total > 0 && collected >= total {
                let proofs: HashMap<u64, ProofData> = state.pending_proofs.drain().collect();
                let sb = state.next_superblock_number.get();
                state.proof_collection_started = None;
                (collected, total, Some(proofs), sb)
            } else {
                (collected, total, None, 0)
            }
        };

        if let Some(proofs) = ready_proofs {
            info!(
                superblock_number = submit_sb_number,
                collected, "All chains submitted proofs"
            );

            if let Some(submitter) = self.l1_submitter.clone() {
                let state = self.state.clone();
                let proof_mode = self.proof_mode;
                tokio::spawn(async move {
                    let result = match proof_mode {
                        ProofMode::Mock => submitter.submit_mock(submit_sb_number, &proofs).await,
                        ProofMode::Real => {
                            warn!(
                                superblock_number = submit_sb_number,
                                "Skipping L1 submission without an aggregated proof"
                            );
                            return;
                        }
                    };

                    match result {
                        Ok(()) => {
                            let mut s = state.write().await;
                            s.last_finalized_superblock_number = submit_sb_number;
                            s.next_superblock_number = SuperblockNumber::new(submit_sb_number + 1);
                            info!(
                                superblock_number = submit_sb_number,
                                "L1 submission succeeded, advancing state"
                            );
                        }
                        Err(e) => {
                            warn!(superblock_number = submit_sb_number, error = %e, "L1 submission failed");
                        }
                    }
                });
            }
        } else {
            info!(
                superblock_number,
                chain_id, collected, total, "Proof received"
            );
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
            "pending_proof_superblocks": state.pending_proofs.len(),
            "current_period_id": state.current_period_id.get(),
            "next_superblock_number": state.next_superblock_number.get(),
            "last_finalized_superblock": state.last_finalized_superblock_number,
            "messages_processed": self.messages_processed.load(Ordering::Relaxed),
            "broadcasts_sent": self.broadcasts_sent.load(Ordering::Relaxed),
            "uptime_seconds": self.start_time.elapsed().as_secs_f64(),
        })
    }
}

/// Returns the unique participating chains in first-seen order.
///
/// Reads chain ids straight from the wire request; converting to the domain
/// `XtRequest` first would clone every transaction payload.
fn extract_chains(req: &ethera_spec_proto::XtRequest) -> Vec<ChainId> {
    // The participating chain set is tiny (typically 2-3), so a linear scan to
    // dedup is cheaper than allocating a `HashSet`.
    let mut chains: Vec<ChainId> = Vec::new();
    for tr in &req.transaction_requests {
        let chain = ChainId::new(tr.chain_id);
        if !chains.contains(&chain) {
            chains.push(chain);
        }
    }
    chains
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proto_xt(chain_ids: &[u64]) -> ethera_spec_proto::XtRequest {
        ethera_spec_proto::XtRequest {
            transaction_requests: chain_ids
                .iter()
                .map(|&id| ethera_spec_proto::TransactionRequest {
                    chain_id: id,
                    transaction: vec![vec![0x01]],
                })
                .collect(),
        }
    }

    #[test]
    fn unanimous_yes_commits_and_releases_chains() {
        let mut state = CoordinatorState::new();
        let chains = vec![ChainId::new(1), ChainId::new(2)];
        let (id, _) = state.prepare_xt(&proto_xt(&[1, 2]), &chains);

        assert!(state.record_vote(id, ChainId::new(1), true).is_none());
        let (decision, _, _) = state.record_vote(id, ChainId::new(2), true).unwrap();
        assert!(decision);
        assert!(!state.active_xts.contains_key(&id));
        assert!(
            state.active_chains.is_empty(),
            "chains released on decision"
        );
    }

    #[test]
    fn any_no_aborts_immediately() {
        let mut state = CoordinatorState::new();
        let chains = vec![ChainId::new(1), ChainId::new(2), ChainId::new(3)];
        let (id, _) = state.prepare_xt(&proto_xt(&[1, 2, 3]), &chains);

        // One reject decides false without waiting for the remaining votes.
        let (decision, _, _) = state.record_vote(id, ChainId::new(2), false).unwrap();
        assert!(!decision);
        assert!(!state.active_xts.contains_key(&id));
    }

    #[test]
    fn ignores_non_participant_and_duplicate_votes() {
        let mut state = CoordinatorState::new();
        let chains = vec![ChainId::new(1), ChainId::new(2)];
        let (id, _) = state.prepare_xt(&proto_xt(&[1, 2]), &chains);

        assert!(state.record_vote(id, ChainId::new(99), true).is_none());
        assert!(state.record_vote(id, ChainId::new(1), true).is_none());
        // Duplicate vote from chain 1 must not count toward the quorum.
        assert!(state.record_vote(id, ChainId::new(1), true).is_none());
        let (decision, _, _) = state.record_vote(id, ChainId::new(2), true).unwrap();
        assert!(decision);
    }
}
