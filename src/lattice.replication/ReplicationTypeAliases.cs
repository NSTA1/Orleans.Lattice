using Orleans.Lattice.BPlusTree.Grains;
namespace Orleans.Lattice.Replication;

/// <summary>
/// Centralised Orleans serialization alias constants for every type
/// that participates in the replication wire format. Each alias is a
/// short, fixed string that provides a stable wire-format identity
/// independent of CLR type names. Replication aliases use the
/// <c>olr.</c> prefix to avoid collision with core <c>Orleans.Lattice</c>
/// aliases (which use <c>ol.</c>).
/// </summary>
public static class ReplicationTypeAliases
{
    // The WalRecord, LatticeMergeMode, IWalShardGrain,
    // WalShardSequencedEntry, and WalShardPage aliases moved to the
    // core Orleans.Lattice.TypeAliases table when the WAL adapters
    // were promoted into the core library so single-cluster hosts
    // could land durability without a hard reference on this package.
    // The wire-format string values were preserved verbatim
    // (olr.re, olr.rm, olr.gw, olr.we, olr.wp) so rolling
    // upgrade peers continue to interoperate. The former WalOp enum
    // (alias olr.ro) was collapsed into Orleans.Lattice.MutationKind
    // during the same move; that alias slot is intentionally retired.

    // Per-origin high-water-mark types

    /// <summary>Alias for the per-origin HWM grain interface.</summary>
    internal const string IReplicationHighWaterMarkGrain = "olr.gh";

    /// <summary>Alias for the per-origin HWM persistent state class.</summary>
    internal const string ReplicationHighWaterMarkState = "olr.hs";

    /// <summary>Alias for the bootstrap drop floor persisted on the high-water-mark state (issue #4549).</summary>
    internal const string ReplicationBootstrapFloor = "olr.hf";

    /// <summary>Alias for the high-water-mark grain's per-origin admission read (issue #4549).</summary>
    internal const string ReplicationApplyAdmission = "olr.hm";

    // Inbound apply pipeline

    /// <summary>Alias for the apply-result return value.</summary>
    internal const string ApplyResult = "olr.ar";

    /// <summary>Alias for <see cref="Replication.ReplicationAck"/>.</summary>
    internal const string ReplicationAck = "olr.ak";

    // Typed CRDT deltas moved to core Orleans.Lattice.TypeAliases when
    // the public delta DTOs were promoted into the core library. The
    // wire-format alias strings changed at the same time (ol.* prefix);
    // see TypeAliases.{LwwRegisterDelta, OrSetDelta, OrSetDeltaDot,
    // PnCounterDelta, VersionVectorDelta, MvRegisterDelta, OrMapDelta}.

    // Transport-side resume token

    /// <summary>Alias for <see cref="WalResumeToken"/>.</summary>
    internal const string WalResumeToken = "olr.wt";

    // Wire envelope (binary-framing seam)

    /// <summary>Alias for <see cref="ReplicationBatchEnvelope"/>.</summary>
    internal const string ReplicationBatchEnvelope = "olr.be";

    // Dead-letter queue (poison-entry park)

    /// <summary>Alias for <see cref="Replication.DeadLetterEntry"/>.</summary>
    internal const string DeadLetterEntry = "olr.dl";
    /// <summary>Alias for <see cref="Replication.ReplicationDeadLetterQueueFullException"/>.</summary>
    internal const string ReplicationDeadLetterQueueFullException = "olr.qf";
    /// <summary>Alias for <see cref="Replication.CausalDependencyVerdict"/>.</summary>
    internal const string CausalDependencyVerdict = "olr.dv";

    /// <summary>Alias for the per-tree dead-letter queue grain interface.</summary>
    internal const string IReplicationDeadLetterGrain = "olr.gd";

    // Snapshot / bootstrap protocol

    /// <summary>Alias for <see cref="Replication.SnapshotEntry"/>.</summary>
    internal const string SnapshotEntry = "olr.se";

    /// <summary>Alias for <see cref="Replication.RemoteSnapshotMetadata"/>.</summary>
    internal const string RemoteSnapshotMetadata = "olr.sm";

    /// <summary>
    /// Alias for <see cref="Replication.RemoteSnapshotMetadataRequest"/>
    /// - the request DTO for the gRPC <c>GetMetadata</c> RPC defined in
    /// <c>Orleans.Lattice.Replication.Grpc</c>.
    /// </summary>
    internal const string RemoteSnapshotMetadataRequest = "olr.sr";

    /// <summary>
    /// Alias for <see cref="Replication.RemoteSnapshotStreamItem"/>
    /// - the per-message DTO for the gRPC server-streaming
    /// <c>RequestSnapshot</c> RPC defined in
    /// <c>Orleans.Lattice.Replication.Grpc</c>.
    /// </summary>
    internal const string RemoteSnapshotStreamItem = "olr.si";

    /// <summary>Alias for the per-tree bootstrap coordinator grain interface.</summary>
    internal const string ILatticeBootstrapCoordinatorGrain = "olr.gb";

    /// <summary>Alias for <see cref="Grains.BootstrapCoordinatorState"/>.</summary>
    internal const string BootstrapCoordinatorState = "olr.bs";

    /// <summary>Alias for <see cref="Replication.BootstrapCoordinatorStatus"/>.</summary>
    internal const string BootstrapCoordinatorStatus = "olr.bx";

    /// <summary>Alias for <see cref="Replication.SnapshotSourceGeneration"/>.</summary>
    internal const string SnapshotSourceGeneration = "olr.sg";
    internal const string SnapshotSourceFrontier = "olr.sx";

    // Production replication drivers

    /// <summary>Alias for the per-(tree, peer) outbound shipper grain interface.</summary>
    internal const string IReplicationShipperGrain = "olr.gs";

    /// <summary>Alias for the per-(tree, peer) shipper grain persistent state class.</summary>
    internal const string ReplicationShipperState = "olr.ss";

    /// <summary>Alias for a saga the shipper withholds from its peer after a prepare was dead-lettered.</summary>
    internal const string PoisonedSaga = "olr.sp";

    /// <summary>Alias for the per-tree maintenance grain interface.</summary>
    internal const string IReplicationMaintenanceGrain = "olr.gm";

    /// <summary>Alias for the per-tree maintenance grain persistent state class.</summary>
    internal const string ReplicationMaintenanceState = "olr.ms";

    /// <summary>Alias for <see cref="Grains.ICausalApplyBufferGrain"/>.</summary>
    internal const string ICausalApplyBufferGrain = "olr.gk";

    /// <summary>Alias for <see cref="Grains.CausalApplyBufferState"/>.</summary>
    internal const string CausalApplyBufferState = "olr.cb";

    /// <summary>Alias for <see cref="Grains.ParkedCausalEntry"/>.</summary>
    internal const string ParkedCausalEntry = "olr.cr";

    /// <summary>Alias for <see cref="Grains.IReceiverSagaPoisonGrain"/>.</summary>
    internal const string IReceiverSagaPoisonGrain = "olr.yg";

    /// <summary>Alias for <see cref="Grains.ReceiverSagaPoisonState"/>.</summary>
    internal const string ReceiverSagaPoisonState = "olr.ys";

    /// <summary>Alias for <see cref="Grains.ReceiverSagaPoisonRecord"/>.</summary>
    internal const string ReceiverSagaPoisonRecord = "olr.yr";

    /// <summary>Alias for <see cref="Grains.ReceiverSagaPoisonClassification"/>.</summary>
    internal const string ReceiverSagaPoisonClassification = "olr.yq";

    /// <summary>Alias for <see cref="Replication.ReplicationContactDirection"/>.</summary>
    internal const string ReplicationContactDirection = "olr.cd";

    // Anti-entropy peer digest probe (detect stage)

    /// <summary>Alias for <see cref="Replication.DigestProbeRequest"/>.</summary>
    internal const string DigestProbeRequest = "olr.dq";

    /// <summary>Alias for <see cref="Replication.DigestProbeResponse"/>.</summary>
    internal const string DigestProbeResponse = "olr.dp";

    /// <summary>Alias for the per-tree digest-probe scheduler grain interface.</summary>
    internal const string IReplicationDigestProbeGrain = "olr.gp";

    /// <summary>Alias for the per-tree digest-probe scheduler grain persistent state class.</summary>
    internal const string ReplicationDigestProbeState = "olr.ps";

    // Anti-entropy Merkle-walk drift localisation (localise stage)

    /// <summary>Alias for <see cref="Replication.MerkleWalkProbeRequest"/>.</summary>
    internal const string MerkleWalkProbeRequest = "olr.mq";

    /// <summary>Alias for <see cref="Replication.MerkleWalkProbeResponse"/>.</summary>
    internal const string MerkleWalkProbeResponse = "olr.mp";

    // Anti-entropy targeted leaf re-replay (repair stage)

    /// <summary>Alias for <see cref="Replication.LeafReReplayRange"/>.</summary>
    internal const string LeafReReplayRange = "olr.rr";

    // Content-hash payload-elision round trip (sender manifest / receiver pull-missing)

    /// <summary>Alias for <see cref="Replication.ContentManifestEntry"/>.</summary>
    internal const string ContentManifestEntry = "olr.ce";

    /// <summary>Alias for <see cref="Replication.ContentManifestRequest"/>.</summary>
    internal const string ContentManifestRequest = "olr.cq";

    /// <summary>Alias for <see cref="Replication.ContentManifestResponse"/>.</summary>
    internal const string ContentManifestResponse = "olr.cp";

    // Content-fingerprint guard in shared-dictionary negotiation

    /// <summary>Alias for <see cref="Replication.AdvertisedCompressionDictionary"/>.</summary>
    internal const string AdvertisedCompressionDictionary = "olr.ad";

    // Self-distributing shared-dictionary pull round trip (receiver pulls
    // the bytes behind a peer-advertised id it does not yet hold)

    /// <summary>Alias for <see cref="Replication.CompressionDictionaryPullRequest"/>.</summary>
    internal const string CompressionDictionaryPullRequest = "olr.kq";

    /// <summary>Alias for <see cref="Replication.CompressionDictionaryPullResponse"/>.</summary>
    internal const string CompressionDictionaryPullResponse = "olr.kp";

    // Anti-entropy peer high-water-mark probe (re-replay bound) - the gRPC
    // binding's GetPeerHighWaterMark RPC request/response pair.

    /// <summary>Alias for <see cref="Replication.PeerHighWaterMarkRequest"/>.</summary>
    internal const string PeerHighWaterMarkRequest = "olr.hq";

    /// <summary>Alias for <see cref="Replication.PeerHighWaterMarkResponse"/>.</summary>
    internal const string PeerHighWaterMarkResponse = "olr.hp";

    // Cross-cluster saga control channel - the gRPC binding's
    // orleans.lattice.replication.LatticeSaga service request/response
    // pair, reused across the Prepare/Commit/Abort/GetStatus RPCs.

    /// <summary>Alias for <see cref="Replication.SagaControlRequest"/>.</summary>
    internal const string SagaControlRequest = "olr.sq";

    /// <summary>Alias for <see cref="Replication.SagaControlResponse"/>.</summary>
    internal const string SagaControlResponse = "olr.sv";

    // Durable cross-cluster saga coordinator + participant model. The
    // coordinator lifecycle phase / outcome / dialled decision, the
    // coordinator and participant grain interfaces, and their persisted
    // state and per-participant records. All use previously-unclaimed
    // olr.z* codes.

    /// <summary>Alias for <see cref="CrossClusterSagaPhase"/>.</summary>
    internal const string CrossClusterSagaPhase = "olr.zp";

    /// <summary>Alias for <see cref="CrossClusterSagaOutcome"/>.</summary>
    internal const string CrossClusterSagaOutcome = "olr.zo";

    /// <summary>Alias for <see cref="CrossClusterSagaDecision"/>.</summary>
    internal const string CrossClusterSagaDecision = "olr.zd";

    /// <summary>Alias for the cross-cluster saga coordinator grain interface.</summary>
    internal const string ICrossClusterSagaCoordinatorGrain = "olr.zg";

    /// <summary>Alias for <see cref="Grains.CrossClusterSagaCoordinatorState"/>.</summary>
    internal const string CrossClusterSagaCoordinatorState = "olr.zc";

    /// <summary>Alias for <see cref="Grains.CrossClusterSagaParticipantRef"/>.</summary>
    internal const string CrossClusterSagaParticipantRef = "olr.zr";

    /// <summary>Alias for the cross-cluster saga participant grain interface.</summary>
    internal const string ICrossClusterSagaParticipantGrain = "olr.zn";

    /// <summary>Alias for <see cref="Grains.CrossClusterSagaParticipantState"/>.</summary>
    internal const string CrossClusterSagaParticipantState = "olr.zs";

    // Durable per-tree write-fence and shipping-pause primitive engaged for the
    // duration of a cross-cluster saga cutover. The saga-scoped, group-atomic
    // fence grain, its persisted state and lifecycle phase, and the per-tree
    // inbound receive-fence grain and its state. All use previously-unclaimed
    // olr.f* codes.

    /// <summary>Alias for the saga write-fence grain interface.</summary>
    internal const string ISagaWriteFenceGrain = "olr.fg";

    /// <summary>Alias for <see cref="Grains.SagaWriteFenceState"/>.</summary>
    internal const string SagaWriteFenceState = "olr.fs";

    /// <summary>Alias for <see cref="SagaWriteFencePhase"/>.</summary>
    internal const string SagaWriteFencePhase = "olr.fp";

    /// <summary>Alias for <see cref="SagaWriteFenceRequest"/>.</summary>
    internal const string SagaWriteFenceRequest = "olr.fr";

    /// <summary>Alias for <see cref="SagaWriteFenceSnapshot"/>.</summary>
    internal const string SagaWriteFenceSnapshot = "olr.fn";

    /// <summary>Alias for the per-tree inbound receive-fence grain interface.</summary>
    internal const string ITreeReceiveFenceGrain = "olr.fc";

    /// <summary>Alias for <see cref="Grains.TreeReceiveFenceState"/>.</summary>
    internal const string TreeReceiveFenceState = "olr.ft";

    /// <summary>Alias for <see cref="ReceiveFenceObservation"/> (issue #4593).</summary>
    internal const string ReceiveFenceObservation = "olr.fo";

    // Runtime per-tree replication configuration (the sys-replication-config
    // CRDT tree). The composite OR-Map value record carrying a tree's
    // enablement flag and declared wire merge mode.

    /// <summary>Alias for <see cref="LatticeReplicationConfigEntry"/>.</summary>
    internal const string LatticeReplicationConfigEntry = "olr.rc";

    /// <summary>Alias for <see cref="LatticeReplicationPreconditionFailedException"/>.</summary>
    internal const string LatticeReplicationPreconditionFailedException = "olr.rp";

    /// <summary>Alias for <see cref="LatticeReplicationModeChangeRejectedException"/>.</summary>
    internal const string LatticeReplicationModeChangeRejectedException = "olr.rj";

    // Peer-status read path: the per-silo grain service the replication status
    // facade fans out to, and the bounded read it answers.

    /// <summary>Alias for <see cref="Replication.IReplicationPeerStatusGrainService"/>.</summary>
    internal const string IReplicationPeerStatusGrainService = "olr.pg";

    /// <summary>Alias for <see cref="Replication.ReplicationPeerStatusRow"/>.</summary>
    internal const string ReplicationPeerStatusRow = "olr.pw";

    /// <summary>Alias for <see cref="Replication.ReplicationPeerStatusReadRequest"/>.</summary>
    internal const string ReplicationPeerStatusReadRequest = "olr.pq";

    /// <summary>Alias for <see cref="Replication.ReplicationPeerStatusCursor"/>.</summary>
    internal const string ReplicationPeerStatusCursor = "olr.pc";

    // Snapshot export epoch (#4534): a per-tree counter advanced by every full
    // export at its registry snap0, so a shipper can tell a peer's re-seed
    // happened after it took the peer off the log.
    internal const string IReplicationExportEpochGrain = "olr.xg";
    internal const string ReplicationExportEpochState = "olr.xs";

    // Receiver per-origin causal frontier (#4586): the origin's shipped low
    // watermark, and the writes held here without being applied.
    internal const string IReplicationOriginFrontierGrain = "olr.og";
    internal const string ReplicationOriginFrontierState = "olr.os";

    // The sender's applied low watermark for a receiver's tree (#4586 part 2b).
    internal const string ReplicationSourceFrontier = "olr.sf";

    // Receiver per-tree causal frontier (#4586 part 2b).
    internal const string IReplicationTreeFrontierGrain = "olr.tf";
    internal const string ReplicationTreeFrontierState = "olr.ts";
    internal const string ReplicationTreeOriginFrontier = "olr.to";
    internal const string ReplicationTreeFrontierSnapshot = "olr.tn";

    // The sender's per-peer aggregate of its trees' applied low watermarks (#4586 part 2b).
    internal const string IReplicationSourceFrontierAggregateGrain = "olr.fa";
    internal const string ReplicationSourceFrontierAggregateState = "olr.fv";
    internal const string SourceFrontierShipperState = "olr.fw";
    internal const string SourceFrontierPrepare = "olr.fx";

    // A receiver's record of the source lineage it last drained (#4673).
    internal const string ReplicationDrainedLineage = "olr.dn";

    // Origin cross-tree decision purge hold (#4684).
    internal const string ICrossTreeHoldTrackerGrain = "olr.ch";
    internal const string CrossTreeHoldTrackerState = "olr.cs";
    internal const string CrossTreeHoldBoundary = "olr.ck";
    internal const string CrossTreeHoldSnapshot = "olr.cn";
    internal const string ICrossTreePeerEnrolmentGrain = "olr.pe";
    internal const string CrossTreePeerEnrolmentState = "olr.pn";
    internal const string ReplicationAckedPositions = "olr.ap";
    internal const string CrossTreeSiblingBoundary = "olr.sb";

    // The source lineage a sender stamped on an entry, carried with it into the
    // causal-apply buffer and the dead-letter queue (#4707).
    internal const string ReplicationSourceLineageStamp = "olr.ls";
}
