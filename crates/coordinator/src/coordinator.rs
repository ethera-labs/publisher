//! Coordinator wiring the spec SCP/SBCP state machines to QUIC transport,
//! scheduling, and L1 settlement.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::bridge::{Outbound, OutboundSink};
use crate::l1_submit::L1Submitter;
use crate::proof_types::ProofData;

use ethera_spec::{
    chains_from_request, ChainId, DecisionState, Instance, InstanceId, PeriodId, SuperblockHash,
    SuperblockNumber, XtRequest,
};
use ethera_spec_sbcp::PublisherError;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info, warn};

use publisher_metrics::PublisherMetrics;
use publisher_transport::server::QuicServer;

const MAX_PENDING_QUEUE_SIZE: usize = 100;

type SbcpPublisher = ethera_spec_sbcp::Publisher<OutboundSink, OutboundSink, OutboundSink>;
type ScpInstance = ethera_spec_scp::PublisherInstance<OutboundSink>;

struct InFlightXt {
    instance: Arc<ScpInstance>,
    chains: Vec<ChainId>,
    started: Instant,
}

#[derive(Debug, Default)]
struct ChainRegistry {
    chain_to_client: HashMap<ChainId, String>,
    client_to_chain: HashMap<String, ChainId>,
}

impl ChainRegistry {
    fn register(&mut self, client_id: &str, chain_id: ChainId) {
        if let Some(old_chain) = self.client_to_chain.get(client_id) {
            self.chain_to_client.remove(old_chain);
        }
        self.chain_to_client.insert(chain_id, client_id.to_string());
        self.client_to_chain.insert(client_id.to_string(), chain_id);
    }

    fn chains(&self) -> HashSet<ChainId> {
        self.chain_to_client.keys().copied().collect()
    }
}

/// Returned by [`Coordinator::receive_chain_proof`] when no terminated
/// superblock is awaiting proofs yet.
#[derive(Debug)]
pub struct NoTerminatedSuperblock;

impl std::fmt::Display for NoTerminatedSuperblock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("no terminated superblock awaiting proofs")
    }
}

impl std::error::Error for NoTerminatedSuperblock {}

pub struct Coordinator {
    sbcp: SbcpPublisher,
    in_flight: RwLock<HashMap<InstanceId, InFlightXt>>,
    registry: RwLock<ChainRegistry>,
    pending_queue: Mutex<VecDeque<XtRequest>>,
    sink: OutboundSink,
    outbound_rx: std::sync::Mutex<Option<UnboundedReceiver<Outbound>>>,
    pub(crate) server: Arc<QuicServer>,
    pub(crate) metrics: Option<Arc<PublisherMetrics>>,
    l1_submitter: Option<Arc<L1Submitter>>,
    scp_timeout: Duration,
    proof_window: Duration,
    proof_timer: std::sync::Mutex<Option<Instant>>,
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
    pub fn new(
        server: Arc<QuicServer>,
        metrics: Option<Arc<PublisherMetrics>>,
        scp_timeout: Duration,
        proof_window: Duration,
        proof_window_periods: u64,
        last_finalized_superblock_number: u64,
        last_finalized_superblock_hash: [u8; 32],
    ) -> anyhow::Result<Self> {
        let (sink, outbound_rx) = OutboundSink::channel();
        let sbcp = ethera_spec_sbcp::Publisher::new(
            sink.clone(),
            sink.clone(),
            sink.clone(),
            PeriodId(0),
            SuperblockNumber(last_finalized_superblock_number),
            SuperblockNumber(last_finalized_superblock_number),
            SuperblockHash(last_finalized_superblock_hash),
            proof_window_periods,
            HashSet::new(),
        )?;

        Ok(Self {
            sbcp,
            in_flight: RwLock::new(HashMap::new()),
            registry: RwLock::new(ChainRegistry::default()),
            pending_queue: Mutex::new(VecDeque::new()),
            sink,
            outbound_rx: std::sync::Mutex::new(Some(outbound_rx)),
            server,
            metrics,
            l1_submitter: None,
            scp_timeout,
            proof_window,
            proof_timer: std::sync::Mutex::new(None),
            messages_processed: AtomicU64::new(0),
            broadcasts_sent: AtomicU64::new(0),
            start_time: Instant::now(),
        })
    }

    pub fn with_l1_submitter(mut self, submitter: L1Submitter) -> Self {
        self.l1_submitter = Some(Arc::new(submitter));
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
        let chains = {
            let mut registry = self.registry.write().await;
            registry.register(client_id, chain_id);
            registry.chains()
        };
        self.sbcp.update_chains(chains);
        info!(client_id, chain_id = %chain_id, "Chain registered");
    }

    /// Drains the outbound channel produced by the spec state machines and
    /// performs the actual QUIC broadcasts and L1 submissions.
    pub async fn run_outbound(self: Arc<Self>) {
        let rx = self.outbound_rx.lock().unwrap().take();
        let Some(mut rx) = rx else {
            error!("Outbound receiver already taken");
            return;
        };

        while let Some(event) = rx.recv().await {
            match event {
                Outbound::Broadcast(data) => self.broadcast(&data).await,
                Outbound::Rollback(data) => {
                    self.abandon_in_flight().await;
                    self.broadcast(&data).await;
                    self.drain_queue().await;
                }
                Outbound::SubmitProof {
                    superblock_number,
                    proofs,
                } => self.spawn_l1_submit(superblock_number, proofs),
            }
        }
    }

    async fn broadcast(&self, data: &[u8]) {
        self.inc_broadcasts();
        if let Err(e) = self.server.broadcast_raw(data, "").await {
            error!(error = %e, "Failed to broadcast");
        }
    }

    /// Rollback invalidates every reservation the spec held, so pending SCP
    /// instances can never decide; drop them and let sidecars reset.
    async fn abandon_in_flight(&self) {
        let abandoned = std::mem::take(&mut *self.in_flight.write().await);
        if !abandoned.is_empty() {
            warn!(
                count = abandoned.len(),
                "Abandoning in-flight xTs due to rollback"
            );
        }
    }

    fn spawn_l1_submit(
        self: &Arc<Self>,
        superblock_number: SuperblockNumber,
        proofs: Vec<ProofData>,
    ) {
        let Some(submitter) = self.l1_submitter.clone() else {
            warn!(
                superblock_number = superblock_number.get(),
                "No L1 submitter configured - dropping superblock proof"
            );
            return;
        };

        let coordinator = Arc::clone(self);
        tokio::spawn(async move {
            match submitter.submit(superblock_number.get(), &proofs).await {
                Ok(superblock_hash) => {
                    *coordinator.proof_timer.lock().unwrap() = None;
                    if let Err(e) = coordinator
                        .sbcp
                        .advance_settled_state(superblock_number, SuperblockHash(superblock_hash.0))
                    {
                        warn!(
                            superblock_number = superblock_number.get(),
                            error = %e,
                            "Failed to advance settled state"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        superblock_number = superblock_number.get(),
                        error = %e,
                        "L1 submission failed - rolling back"
                    );
                    coordinator.sbcp.rollback();
                }
            }
        });
    }

    pub fn start_period(&self) {
        match self.sbcp.start_period() {
            Ok(()) => {
                if let Some(m) = &self.metrics {
                    m.period_broadcast_total.inc();
                }
            }
            Err(e) => warn!(error = %e, "Skipping period start"),
        }
    }

    pub(crate) async fn handle_xt_request(
        &self,
        client_id: String,
        xt_req: &ethera_spec_proto::XtRequest,
    ) {
        let request = XtRequest::from(xt_req);
        let chains = chains_from_request(&request);

        if chains.len() < 2 {
            warn!(
                client_id,
                chains = chains.len(),
                "Rejecting XT: must span at least 2 chains"
            );
            return;
        }

        match self.sbcp.start_instance(request.clone()) {
            Ok(instance) => self.launch_instance(instance).await,
            Err(PublisherError::CannotStartInstance) => {
                {
                    let mut queue = self.pending_queue.lock().await;
                    if queue.len() >= MAX_PENDING_QUEUE_SIZE {
                        warn!(client_id, "XT queue full, rejecting");
                        return;
                    }
                    queue.push_back(request);
                }
                if let Some(m) = &self.metrics {
                    m.xt_queued_total.inc();
                }
                // The blocking instance may have decided between the failed
                // start and the queue push; retry instead of stranding it.
                self.drain_queue().await;
            }
            Err(e) => warn!(client_id, error = %e, "Rejecting XT"),
        }
    }

    async fn launch_instance(&self, instance: Instance) {
        let id = instance.id;
        let chains = instance.chains();
        let scp = Arc::new(ScpInstance::new(instance, self.sink.clone()));

        self.in_flight.write().await.insert(
            id,
            InFlightXt {
                instance: Arc::clone(&scp),
                chains,
                started: Instant::now(),
            },
        );

        if let Some(m) = &self.metrics {
            m.xt_started_total.inc();
        }
        scp.run();
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

        let scp = {
            let in_flight = self.in_flight.read().await;
            in_flight.get(&id).map(|xt| Arc::clone(&xt.instance))
        };
        let Some(scp) = scp else {
            warn!(xt_id = %id, chain_id = %chain_id, "Vote for unknown or finished xT");
            return;
        };

        // Duplicate / non-participant votes are logged and rejected by the spec.
        let _ = scp.process_vote(chain_id, vote);

        if scp.decision_state() != DecisionState::Pending {
            self.finalize_instance(id).await;
        }
    }

    async fn finalize_instance(&self, id: InstanceId) {
        let Some(xt) = self.in_flight.write().await.remove(&id) else {
            return;
        };

        let decision = xt.instance.decision_state() == DecisionState::Accepted;
        let latency = xt.started.elapsed().as_secs_f64();
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

        if let Err(e) = self.sbcp.decide_instance(&xt.instance.instance()) {
            warn!(xt_id = %id, error = %e, "Failed to release instance chains");
        }

        self.drain_queue().await;
    }

    async fn drain_queue(&self) {
        loop {
            let launched = {
                let mut queue = self.pending_queue.lock().await;
                let mut launched = None;
                let mut idx = 0;
                while idx < queue.len() {
                    match self.sbcp.start_instance(queue[idx].clone()) {
                        Ok(instance) => {
                            queue.remove(idx);
                            launched = Some(instance);
                            break;
                        }
                        Err(PublisherError::CannotStartInstance) => idx += 1,
                        Err(e) => {
                            warn!(error = %e, "Dropping queued XT");
                            queue.remove(idx);
                        }
                    }
                }
                launched
            };

            match launched {
                Some(instance) => self.launch_instance(instance).await,
                None => return,
            }
        }
    }

    pub async fn reap_timed_out_xts(&self) {
        let now = Instant::now();
        let expired: Vec<(InstanceId, Arc<ScpInstance>)> = {
            let in_flight = self.in_flight.read().await;
            in_flight
                .iter()
                .filter(|(_, xt)| now.duration_since(xt.started) >= self.scp_timeout)
                .map(|(id, xt)| (*id, Arc::clone(&xt.instance)))
                .collect()
        };

        for (id, scp) in expired {
            warn!(xt_id = %id, "SCP timeout - deciding false");
            scp.timeout();
            self.finalize_instance(id).await;
        }
    }

    /// Drives the spec's proof-timeout: the timer arms once a terminated
    /// superblock awaits proofs (settlement pipeline started) and triggers a
    /// rollback when `proof_window` elapses without settlement.
    pub fn reap_expired_proofs(&self) {
        let target = self.sbcp.target_superblock_number();
        let finalized = self.sbcp.last_finalized_superblock_number();
        let settling = target.get() >= finalized.get() + 2;

        let mut timer = self.proof_timer.lock().unwrap();
        if !settling {
            *timer = None;
            return;
        }

        match *timer {
            None => *timer = Some(Instant::now()),
            Some(started) if started.elapsed() >= self.proof_window => {
                *timer = None;
                drop(timer);
                warn!("Proof window expired - triggering rollback");
                self.sbcp.proof_timeout();
            }
            Some(_) => {}
        }
    }

    pub fn receive_proof(
        &self,
        period_id: u64,
        superblock_number: u64,
        chain_id: u64,
        data: &ProofData,
    ) {
        match serde_json::to_vec(data) {
            Ok(blob) => self.sbcp.receive_proof(
                PeriodId(period_id),
                SuperblockNumber(superblock_number),
                blob,
                ChainId(chain_id),
            ),
            Err(e) => error!(chain_id, error = %e, "Failed to encode proof data"),
        }
    }

    /// Ingests an op-succinct proof, which reports chain-local block heights
    /// and knows nothing about SBCP numbering: the proof is mapped onto the
    /// superblock currently awaiting settlement (`last_finalized + 1`).
    pub fn receive_chain_proof(
        &self,
        chain_id: u64,
        data: &ProofData,
    ) -> Result<(), NoTerminatedSuperblock> {
        let target = self.sbcp.target_superblock_number();
        let finalized = self.sbcp.last_finalized_superblock_number();
        if target.get() < finalized.get() + 2 {
            return Err(NoTerminatedSuperblock);
        }

        let superblock = finalized + 1;
        let period = self.sbcp.period_id() - (target - superblock).get();
        self.receive_proof(period.get(), superblock.get(), chain_id, data);
        Ok(())
    }

    pub(crate) async fn handle_mailbox_relay(&self, mailbox: &ethera_spec_proto::MailboxMessage) {
        let dest_chain = ChainId::new(mailbox.destination_chain);

        let client_id = {
            let registry = self.registry.read().await;
            registry.chain_to_client.get(&dest_chain).cloned()
        };

        let Some(client_id) = client_id else {
            warn!(dest_chain = %dest_chain, "No sidecar for destination chain");
            return;
        };

        let data = crate::bridge::encode_message(ethera_spec_proto::Payload::MailboxMessage(
            mailbox.clone(),
        ));
        self.inc_broadcasts();

        if let Err(e) = self.server.send_raw(&client_id, &data).await {
            warn!(client_id, error = %e, "Failed to relay mailbox");
        }
    }

    pub(crate) async fn handle_ping(&self, client_id: &str, timestamp: i64) {
        let data = crate::bridge::encode_message(ethera_spec_proto::Payload::Pong(
            ethera_spec_proto::Pong { timestamp },
        ));
        if let Err(e) = self.server.send_raw(client_id, &data).await {
            warn!(client_id, error = %e, "Failed to send pong");
        }
    }

    pub async fn is_chain_registered(&self, chain_id: ChainId) -> bool {
        let registry = self.registry.read().await;
        registry.chain_to_client.contains_key(&chain_id)
    }

    pub async fn chain_for_client(&self, client_id: &str) -> Option<ChainId> {
        let registry = self.registry.read().await;
        registry.client_to_chain.get(client_id).copied()
    }

    #[cfg(test)]
    fn take_outbound_rx(&self) -> UnboundedReceiver<Outbound> {
        self.outbound_rx.lock().unwrap().take().unwrap()
    }

    pub async fn stats(&self) -> serde_json::Value {
        let finalized = self.sbcp.last_finalized_superblock_number();
        let pending_proofs = self
            .sbcp
            .proofs_for(finalized + 1)
            .map_or(0, |proofs| proofs.len());

        let (active_xts, active_chains) = {
            let in_flight = self.in_flight.read().await;
            let chains: usize = in_flight.values().map(|xt| xt.chains.len()).sum();
            (in_flight.len(), chains)
        };

        serde_json::json!({
            "active_connections": self.server.connection_count().await,
            "registered_chains": self.registry.read().await.chain_to_client.len(),
            "active_2pc_transactions": active_xts,
            "active_chains": active_chains,
            "queued_xts": self.pending_queue.lock().await.len(),
            "pending_proof_superblocks": pending_proofs,
            "current_period_id": self.sbcp.period_id().get(),
            "next_superblock_number": self.sbcp.target_superblock_number().get(),
            "last_finalized_superblock": finalized.get(),
            "messages_processed": self.messages_processed.load(Ordering::Relaxed),
            "broadcasts_sent": self.broadcasts_sent.load(Ordering::Relaxed),
            "uptime_seconds": self.start_time.elapsed().as_secs_f64(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message as _;
    use tokio::sync::mpsc::error::TryRecvError;

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

    fn test_coordinator(
        scp_timeout: Duration,
        proof_window: Duration,
    ) -> (Coordinator, UnboundedReceiver<Outbound>) {
        let server = Arc::new(QuicServer::new("127.0.0.1:0".into(), 4 * 1024 * 1024));
        let coordinator =
            Coordinator::new(server, None, scp_timeout, proof_window, 0, 0, [0; 32]).unwrap();
        let rx = coordinator.take_outbound_rx();
        (coordinator, rx)
    }

    fn decode_payload(event: Outbound) -> ethera_spec_proto::Payload {
        let data = match event {
            Outbound::Broadcast(data) | Outbound::Rollback(data) => data,
            Outbound::SubmitProof { .. } => panic!("expected broadcast, got proof submission"),
        };
        ethera_spec_proto::Message::decode(data.as_slice())
            .unwrap()
            .payload
            .unwrap()
    }

    fn recv_start_instance(rx: &mut UnboundedReceiver<Outbound>) -> Vec<u8> {
        match decode_payload(rx.try_recv().unwrap()) {
            ethera_spec_proto::Payload::StartInstance(si) => si.instance_id,
            other => panic!("expected StartInstance, got {other:?}"),
        }
    }

    fn recv_decided(rx: &mut UnboundedReceiver<Outbound>) -> ethera_spec_proto::Decided {
        match decode_payload(rx.try_recv().unwrap()) {
            ethera_spec_proto::Payload::Decided(d) => d,
            other => panic!("expected Decided, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unanimous_yes_commits_and_releases_chains() {
        let (c, mut rx) = test_coordinator(Duration::from_secs(60), Duration::from_secs(7200));
        c.handle_xt_request("client".into(), &proto_xt(&[1, 2]))
            .await;
        let id = recv_start_instance(&mut rx);

        c.handle_vote("client", &id, ChainId::new(1), true).await;
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        c.handle_vote("client", &id, ChainId::new(2), true).await;
        let decided = recv_decided(&mut rx);
        assert!(decided.decision);
        assert_eq!(decided.instance_id, id);
        assert!(c.in_flight.read().await.is_empty());

        // Chains released: the same chains start immediately instead of queueing.
        c.handle_xt_request("client".into(), &proto_xt(&[1, 2]))
            .await;
        recv_start_instance(&mut rx);
        assert!(c.pending_queue.lock().await.is_empty());
    }

    #[tokio::test]
    async fn any_no_aborts_immediately() {
        let (c, mut rx) = test_coordinator(Duration::from_secs(60), Duration::from_secs(7200));
        c.handle_xt_request("client".into(), &proto_xt(&[1, 2, 3]))
            .await;
        let id = recv_start_instance(&mut rx);

        c.handle_vote("client", &id, ChainId::new(2), false).await;
        let decided = recv_decided(&mut rx);
        assert!(!decided.decision);
        assert!(c.in_flight.read().await.is_empty());
    }

    #[tokio::test]
    async fn ignores_non_participant_and_duplicate_votes() {
        let (c, mut rx) = test_coordinator(Duration::from_secs(60), Duration::from_secs(7200));
        c.handle_xt_request("client".into(), &proto_xt(&[1, 2]))
            .await;
        let id = recv_start_instance(&mut rx);

        c.handle_vote("client", &id, ChainId::new(99), true).await;
        c.handle_vote("client", &id, ChainId::new(1), true).await;
        c.handle_vote("client", &id, ChainId::new(1), true).await;
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        c.handle_vote("client", &id, ChainId::new(2), true).await;
        assert!(recv_decided(&mut rx).decision);
    }

    #[tokio::test]
    async fn overlapping_xt_queues_then_drains_after_decision() {
        let (c, mut rx) = test_coordinator(Duration::from_secs(60), Duration::from_secs(7200));
        c.handle_xt_request("client".into(), &proto_xt(&[1, 2]))
            .await;
        let id = recv_start_instance(&mut rx);

        c.handle_xt_request("client".into(), &proto_xt(&[2, 3]))
            .await;
        assert_eq!(c.pending_queue.lock().await.len(), 1);
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        c.handle_vote("client", &id, ChainId::new(1), true).await;
        c.handle_vote("client", &id, ChainId::new(2), true).await;
        assert!(recv_decided(&mut rx).decision);

        // Queue drained: the second xT starts.
        recv_start_instance(&mut rx);
        assert!(c.pending_queue.lock().await.is_empty());
    }

    #[tokio::test]
    async fn single_chain_xt_rejected() {
        let (c, mut rx) = test_coordinator(Duration::from_secs(60), Duration::from_secs(7200));
        c.handle_xt_request("client".into(), &proto_xt(&[1])).await;
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
        assert!(c.in_flight.read().await.is_empty());
    }

    #[tokio::test]
    async fn timed_out_xt_decides_false_and_releases() {
        let (c, mut rx) = test_coordinator(Duration::ZERO, Duration::from_secs(7200));
        c.handle_xt_request("client".into(), &proto_xt(&[1, 2]))
            .await;
        recv_start_instance(&mut rx);

        c.reap_timed_out_xts().await;
        assert!(!recv_decided(&mut rx).decision);
        assert!(c.in_flight.read().await.is_empty());

        c.handle_xt_request("client".into(), &proto_xt(&[1, 2]))
            .await;
        recv_start_instance(&mut rx);
    }

    #[tokio::test]
    async fn proofs_from_all_chains_trigger_l1_submission() {
        let (c, mut rx) = test_coordinator(Duration::from_secs(60), Duration::from_secs(7200));
        c.register_chain("1-sidecar", ChainId::new(1)).await;
        c.register_chain("2-sidecar", ChainId::new(2)).await;

        // No terminated superblock yet.
        assert!(c.receive_chain_proof(1, &ProofData::default()).is_err());

        c.start_period();
        c.start_period();
        recv_start_period(&mut rx);
        recv_start_period(&mut rx);

        c.receive_chain_proof(1, &ProofData::default()).unwrap();
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        c.receive_chain_proof(2, &ProofData::default()).unwrap();
        match rx.try_recv().unwrap() {
            Outbound::SubmitProof {
                superblock_number,
                proofs,
            } => {
                assert_eq!(superblock_number, SuperblockNumber(1));
                assert_eq!(proofs.len(), 2);
            }
            other => panic!("expected SubmitProof, got {other:?}"),
        }
    }

    fn recv_start_period(rx: &mut UnboundedReceiver<Outbound>) -> (u64, u64) {
        match decode_payload(rx.try_recv().unwrap()) {
            ethera_spec_proto::Payload::StartPeriod(sp) => (sp.period_id, sp.superblock_number),
            other => panic!("expected StartPeriod, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn expired_proof_window_triggers_rollback() {
        let (c, mut rx) = test_coordinator(Duration::from_secs(60), Duration::ZERO);
        c.start_period();
        c.start_period();
        recv_start_period(&mut rx);
        recv_start_period(&mut rx);

        // First tick arms the timer, second tick fires it.
        c.reap_expired_proofs();
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
        c.reap_expired_proofs();

        match decode_payload(rx.try_recv().unwrap()) {
            ethera_spec_proto::Payload::Rollback(rb) => {
                assert_eq!(rb.last_finalized_superblock_number, 0);
            }
            other => panic!("expected Rollback, got {other:?}"),
        }
        assert_eq!(c.sbcp.target_superblock_number(), SuperblockNumber(1));
    }

    #[tokio::test]
    async fn period_start_advances_target_superblock() {
        let (c, mut rx) = test_coordinator(Duration::from_secs(60), Duration::from_secs(7200));
        c.start_period();
        assert_eq!(recv_start_period(&mut rx), (1, 1));
        c.start_period();
        assert_eq!(recv_start_period(&mut rx), (2, 2));
    }
}
