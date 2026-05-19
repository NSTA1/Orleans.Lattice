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

    // Typed CRDT deltas (commit-time wire payloads for replicable primitives)

    /// <summary>Alias for <see cref="LwwRegisterDelta"/>.</summary>
    internal const string LwwRegisterDelta = "olr.ld";

    /// <summary>Alias for <see cref="OrSetDelta"/>.</summary>
    internal const string OrSetDelta = "olr.od";

    /// <summary>Alias for <see cref="OrSetDot"/>.</summary>
    internal const string OrSetDot = "olr.dt";

    /// <summary>Alias for <see cref="PnCounterDelta"/>.</summary>
    internal const string PnCounterDelta = "olr.pd";

    /// <summary>Alias for <see cref="VersionVectorDelta"/>.</summary>
    internal const string VersionVectorDelta = "olr.vd";

    /// <summary>Alias for <see cref="MvRegisterDelta"/>.</summary>
    internal const string MvRegisterDelta = "olr.md";

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

}
