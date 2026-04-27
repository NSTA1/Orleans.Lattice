using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Registry of WAL-consumer cursor positions used by the WAL garbage
/// collector to compute a safe trim point. Every active consumer of
/// the per-shard write-ahead log (an outbound replication ship loop,
/// a future local materialiser, an in-process bridge, ...) reports the
/// highest <see cref="HybridLogicalClock"/> it has fully consumed for
/// a given <c>treeName</c>; the <see cref="ILatticeReplicationGc"/>
/// then trims entries with <see cref="ReplogEntry.Timestamp"/> at or
/// below the minimum reported cursor.
/// <para>
/// The registry is consumer-neutral: a <c>consumerId</c> may be a
/// remote peer cluster id, an internal materialiser handle, a custom
/// bridge name, or any other stable string. The garbage collector
/// pins the WAL to the slowest consumer of the lot, which mirrors the
/// "min(cursor across IChangeFeed subscribers)" predicate the
/// replication design requires for v1 and the future log-first model.
/// </para>
/// <para>
/// Implementations must be safe for concurrent use; the default
/// <see cref="InMemoryReplicationCursorRegistry"/> guards its
/// per-tree maps under a single lock.
/// </para>
/// </summary>
public interface ILatticeReplicationCursorRegistry
{
    /// <summary>
    /// Reports the current cursor position for <paramref name="consumerId"/>
    /// against <paramref name="treeName"/>. The reported cursor is the
    /// highest <see cref="HybridLogicalClock"/> the consumer has fully
    /// consumed; the GC predicate trims entries with
    /// <c>entry.Timestamp &lt;= min(reported cursors)</c>.
    /// <para>
    /// Cursor reports are monotonically non-decreasing per
    /// <c>(treeName, consumerId)</c>: a report whose cursor is less
    /// than a previously-reported cursor for the same pair is silently
    /// coalesced into the existing entry rather than rolling the
    /// cursor backwards.
    /// </para>
    /// </summary>
    /// <param name="treeName">Logical tree id whose cursor is being reported. Must not be <see langword="null"/> or whitespace.</param>
    /// <param name="consumerId">Stable identifier for the reporting consumer. Must not be <see langword="null"/> or whitespace.</param>
    /// <param name="cursor">Highest HLC the consumer has fully consumed. Must be strictly greater than <see cref="HybridLogicalClock.Zero"/>; consumers that have not yet observed any entries should not register at all.</param>
    /// <param name="cancellationToken">Cancellation token observed before any state mutation.</param>
    Task ReportCursorAsync(
        string treeName,
        string consumerId,
        HybridLogicalClock cursor,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes <paramref name="consumerId"/>'s registration from the
    /// per-tree map. Called when a consumer goes away (peer removed
    /// from topology, materialiser stopped) so it no longer pins the
    /// log. Idempotent: unregistering a consumer that is not registered
    /// is a no-op.
    /// </summary>
    Task UnregisterAsync(
        string treeName,
        string consumerId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the minimum cursor across every registered consumer for
    /// <paramref name="treeName"/>, or <see langword="null"/> when no
    /// consumer has reported a cursor yet. The returned value is the
    /// "trim by cursor" half of the GC predicate; the GC additionally
    /// applies <see cref="LatticeReplicationOptions.WalRetention"/> as
    /// an optional hard ceiling.
    /// </summary>
    Task<HybridLogicalClock?> GetMinCursorAsync(
        string treeName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns a point-in-time snapshot of every registered consumer's
    /// cursor for <paramref name="treeName"/>. Useful for diagnostics,
    /// the back-pressure health check (later phase), and for asserting
    /// on registry state in tests.
    /// </summary>
    Task<IReadOnlyList<ReplicationCursorSnapshot>> SnapshotAsync(
        string treeName,
        CancellationToken cancellationToken = default);
}
