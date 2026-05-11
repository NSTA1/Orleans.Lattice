using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Persistent state for IReplicationShipperGrain. Tracks the per-peer
/// cursor and lightweight metadata that survive across silo restart.
/// Operational state (pending tasks, in-flight count, jitter RNG) lives
/// in transient grain fields and is reconstructed on activation.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationShipperState)]
internal sealed class ReplicationShipperState
{
    /// <summary>
    /// Highest HLC of any successfully shipped and acknowledged entry
    /// for this (tree, peer). The cursor advances strictly to
    /// ReplicationAck.HighestAppliedHlc on a positive ack and is the
    /// authoritative resume point on activation.
    /// </summary>
    [Id(0)]
    public HybridLogicalClock Cursor { get; set; } = HybridLogicalClock.Zero;

    /// <summary>
    /// Number of consecutive transport failures since the last
    /// successful send. Reset to zero on the first successful ack.
    /// Used to size the next backoff delay via doubling.
    /// </summary>
    /// <remarks>
    /// <strong>Best-effort durability.</strong> The field is declared
    /// at <c>[Id(1)]</c> so it round-trips through Orleans serialization
    /// when state is persisted, but the shipper writes state through
    /// only on a successful cursor advance — a backoff path does not
    /// itself flush state. A silo crash mid-backoff therefore loses
    /// the failure count and a freshly-activated shipper resumes from
    /// <c>0</c>, paying one full ramp through the exponential backoff
    /// schedule before settling. The trade-off is intentional: writing
    /// state on every failure would amplify a transient outage into a
    /// per-failure storage write storm. Operators monitor the
    /// <c>orleans.lattice.replication.consecutive_errors</c> metric
    /// for steady-state visibility rather than relying on this field
    /// surviving migration.
    /// </remarks>
    [Id(1)]
    public int ConsecutiveFailures { get; set; }

    /// <summary>
    /// Per-partition resume cursors keyed by partition index. Each value
    /// is the next unread WAL sequence number for that partition — i.e.
    /// the value to pass as <c>fromSequence</c> on the next call to
    /// <see cref="Grains.IWalShardGrain.ReadAsync"/>
    /// </summary>
    /// <remarks>
    /// <para>
    /// Sequence-based, not HLC-based, because the WAL partitions are
    /// dense append-only logs (zero gaps, monotonically increasing) and
    /// resuming by sequence converts every pump tick from an O(N)
    /// rescan-from-zero walk into an O(page) read past the last
    /// successfully shipped offset. The HLC <see cref="Cursor"/> is
    /// retained alongside because it is the contract surface the
    /// <see cref="IWalCursorRegistry"/> / WAL GC
    /// predicate consume; partition cursors are private shipper state.
    /// </para>
    /// <para>
    /// <strong>Wire-compat additive.</strong> Legacy persisted state
    /// without an <c>[Id(2)]</c> slot decodes to an empty map and
    /// produces the same cold-start behaviour as a freshly activated
    /// shipper (every partition reads from sequence <c>0</c>); the
    /// HLC <see cref="Cursor"/> filters the rescan to entries the
    /// peer has not seen, so an upgrade is observable as a single
    /// extra rescan on first pump tick after activation.
    /// </para>
    /// <para>
    /// Persisted on every cursor advance via the same
    /// <see cref="IPersistentState{TState}.WriteStateAsync"/> call
    /// that flushes the HLC cursor — one round-trip, atomic across
    /// the two slots. A failed write rolls back both, preserving the
    /// pre-existing pump-side guarantee that a transient storage
    /// failure leaves the shipper at the prior durable resume point.
    /// </para>
    /// </remarks>
    [Id(2)]
    public Dictionary<int, long> PartitionCursors { get; set; } = new();
}