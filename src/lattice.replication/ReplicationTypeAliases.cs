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

    // Production replication drivers

    /// <summary>Alias for the per-(tree, peer) outbound shipper grain interface.</summary>
    internal const string IReplicationShipperGrain = "olr.gs";

    /// <summary>Alias for the per-(tree, peer) shipper grain persistent state class.</summary>
    internal const string ReplicationShipperState = "olr.ss";

    /// <summary>Alias for the per-tree maintenance grain interface.</summary>
    internal const string IReplicationMaintenanceGrain = "olr.gm";

    /// <summary>Alias for the per-tree maintenance grain persistent state class.</summary>
    internal const string ReplicationMaintenanceState = "olr.ms";

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

}
