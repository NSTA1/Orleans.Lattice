using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Pure-pull, cursor-driven subscriber API over the per-shard
/// write-ahead log. Lets in-process consumers (the outbound ship loop
/// in later phases, custom bridges, integration tests) read every
/// captured <see cref="ReplogEntry"/> for a tree without touching the
/// primary state and without depending on transport-shaped acks.
/// <para>
/// The contract is deliberately neutral: there is no peer id, no
/// per-call ack envelope, no notion of "live" vs. "snapshot" mode.
/// Consumers pass an <see cref="HybridLogicalClock"/> cursor on each
/// call and receive every entry they have not yet seen, in HLC
/// ascending order. To stream forward, a consumer remembers the
/// timestamp of the last entry it observed and re-subscribes with
/// that value as the new cursor.
/// </para>
/// </summary>
public interface IChangeFeed
{
    /// <summary>
    /// Yields every captured <see cref="ReplogEntry"/> for
    /// <paramref name="treeName"/> with
    /// <see cref="ReplogEntry.Timestamp"/> strictly greater than
    /// <paramref name="cursor"/>. Entries are emitted in HLC ascending
    /// order; ties are broken by the order in which the merge consumes
    /// them across partitions and is therefore unspecified - consumers
    /// must treat the feed as a multiset under equal HLCs.
    /// <para>
    /// The enumeration takes a snapshot of the WAL at call time and
    /// completes once that snapshot is exhausted. To pick up entries
    /// committed after the call, a consumer re-subscribes with an
    /// updated cursor; this matches the cursor-driven, pure-pull model
    /// described in the replication design.
    /// </para>
    /// </summary>
    /// <param name="treeName">
    /// Logical tree id whose change feed is being consumed. Only
    /// entries with <see cref="ReplogEntry.TreeId"/> equal to this
    /// value are yielded. Must not be <see langword="null"/>.
    /// </param>
    /// <param name="cursor">
    /// Strict lower-bound timestamp; the feed yields entries with
    /// <c>entry.Timestamp &gt; cursor</c>. Pass
    /// <see cref="HybridLogicalClock.Zero"/> to read from the start of
    /// the WAL.
    /// </param>
    /// <param name="includeLocalOrigin">
    /// When <see langword="true"/> (the default), entries authored by
    /// the local cluster are included in the stream. When
    /// <see langword="false"/>, entries whose
    /// <see cref="ReplogEntry.OriginClusterId"/> matches the configured
    /// local <see cref="LatticeReplicationOptions.ClusterId"/> are
    /// filtered out - the cursor-driven cycle-break used by remote
    /// shippers. Defaults to <see langword="true"/> because the future
    /// local materialiser (and any in-process projection) needs to
    /// observe local-origin mutations.
    /// </param>
    /// <param name="cancellationToken">Cancellation token observed between every page read and every yielded entry.</param>
    IAsyncEnumerable<ReplogEntry> Subscribe(
        string treeName,
        HybridLogicalClock cursor,
        bool includeLocalOrigin = true,
        CancellationToken cancellationToken = default);
}
