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
    /// Blocked-floor overload of <see cref="ReportCursorAsync(string, string, HybridLogicalClock, CancellationToken)"/>
    /// that additionally reports the consumer's atomic-batch staging
    /// buffer pin (<paramref name="blockedAtHlc"/>). The
    /// <see cref="ILatticeReplicationGc"/> AND-s a strict-less
    /// <c>entry.Timestamp &lt; blockedFloor</c> clause into its trim
    /// predicate so the producer cannot trim past an entry the
    /// receiver still needs to recover from buffer state, where
    /// <c>blockedFloor = min(BlockedAtHlc across consumers that have
    /// reported a non-null pin)</c>.
    /// <para>
    /// The cursor contract is partially relaxed: this overload accepts
    /// <see cref="HybridLogicalClock.Zero"/> for <paramref name="cursor"/>
    /// (a "blocked-floor-only" registration). Such consumers are
    /// excluded from <see cref="GetMinCursorAsync"/> so they do not
    /// pin the WAL on the cursor side; they contribute only to the
    /// blocked-floor meet via <see cref="GetBlockedFloorAsync"/>.
    /// Strictly negative cursors are rejected as before.
    /// </para>
    /// <para>
    /// <paramref name="blockedAtHlc"/> uses replace semantics: the
    /// consumer is the authority on its own pin and each call
    /// replaces the previous value, including transitioning back to
    /// <see langword="null"/> when the buffer drains. A
    /// <see langword="null"/> report contributes nothing to the
    /// blocked-floor meet — only consumers that report a non-null
    /// pin participate.
    /// </para>
    /// </summary>
    /// <param name="treeName">Logical tree id whose state is being reported. Must not be <see langword="null"/> or whitespace.</param>
    /// <param name="consumerId">Stable identifier for the reporting consumer. Must not be <see langword="null"/> or whitespace.</param>
    /// <param name="cursor">Highest HLC the consumer has fully consumed, or <see cref="HybridLogicalClock.Zero"/> when the consumer is reporting only a blocked-floor pin and has no cursor of its own.</param>
    /// <param name="blockedAtHlc">Lowest HLC of any partially-buffered atomic batch the consumer is currently holding, or <see langword="null"/> when the consumer's buffer is empty.</param>
    /// <param name="cancellationToken">Cancellation token observed before any state mutation.</param>
    Task ReportCursorAsync(
        string treeName,
        string consumerId,
        HybridLogicalClock cursor,
        HybridLogicalClock? blockedAtHlc,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Causal+ overload of <see cref="ReportCursorAsync(string, string, HybridLogicalClock, CancellationToken)"/>.
    /// In addition to the highest HLC the consumer has fully consumed,
    /// the consumer reports the per-origin <see cref="VersionVector"/>
    /// frontier it has fully applied. The garbage collector uses the
    /// pointwise minimum of every reported vector — the
    /// <em>causal-stable frontier</em> — as the dominating-clock half
    /// of its trim predicate, AND-ed with the existing HLC cursor
    /// predicate for safety.
    /// <para>
    /// The HLC <paramref name="cursor"/> contract is identical to the
    /// HLC-only overload (monotonic non-decreasing, strictly greater
    /// than <see cref="HybridLogicalClock.Zero"/>). The
    /// <paramref name="vector"/> is treated as advisory: a stale or
    /// concurrent VC report is coalesced into the existing entry by
    /// taking the pointwise maximum (per-origin
    /// <see cref="HybridLogicalClock"/> max) so the registry is
    /// monotonically non-decreasing across both axes.
    /// </para>
    /// <para>
    /// Consumers that only report through the HLC-only overload
    /// continue to pin the HLC half of the predicate but are excluded
    /// from the causal-stable frontier computation — when no consumer
    /// has reported a vector, <see cref="GetCausalStableAsync"/>
    /// returns <see langword="null"/> and the GC degrades to the
    /// HLC-only predicate.
    /// </para>
    /// </summary>
    /// <param name="treeName">Logical tree id whose cursor is being reported. Must not be <see langword="null"/> or whitespace.</param>
    /// <param name="consumerId">Stable identifier for the reporting consumer. Must not be <see langword="null"/> or whitespace.</param>
    /// <param name="cursor">Highest HLC the consumer has fully consumed.</param>
    /// <param name="vector">Per-origin <see cref="VersionVector"/> frontier the consumer has fully applied. The registry stores a defensive clone so subsequent caller-side mutation cannot poison cached state.</param>
    /// <param name="cancellationToken">Cancellation token observed before any state mutation.</param>
    Task ReportCursorAsync(
        string treeName,
        string consumerId,
        HybridLogicalClock cursor,
        VersionVector vector,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Causal+ × blocked-floor overload that combines the per-origin
    /// <paramref name="vector"/> (pointwise-max coalescing) with the
    /// blocked-floor pin (<paramref name="blockedAtHlc"/>, replace
    /// semantics). Cursor contract matches the VC-shaped overload —
    /// must be strictly greater than <see cref="HybridLogicalClock.Zero"/>
    /// for VC-reporting consumers (the blocked-floor-only relaxation
    /// is reserved for the HLC-shape overload because a VC-reporting
    /// consumer is by definition not a blocked-floor-only consumer).
    /// </summary>
    /// <param name="treeName">Logical tree id whose state is being reported. Must not be <see langword="null"/> or whitespace.</param>
    /// <param name="consumerId">Stable identifier for the reporting consumer. Must not be <see langword="null"/> or whitespace.</param>
    /// <param name="cursor">Highest HLC the consumer has fully consumed.</param>
    /// <param name="vector">Per-origin <see cref="VersionVector"/> frontier the consumer has fully applied.</param>
    /// <param name="blockedAtHlc">Lowest HLC of any partially-buffered atomic batch the consumer is currently holding, or <see langword="null"/> when the consumer's buffer is empty.</param>
    /// <param name="cancellationToken">Cancellation token observed before any state mutation.</param>
    Task ReportCursorAsync(
        string treeName,
        string consumerId,
        HybridLogicalClock cursor,
        VersionVector vector,
        HybridLogicalClock? blockedAtHlc,
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
    /// <para>
    /// Consumers registered with <see cref="HybridLogicalClock.Zero"/>
    /// for cursor (blocked-floor-only registrations) are
    /// excluded from the meet so a buffer-only consumer does not
    /// disable the cursor branch of the GC predicate.
    /// </para>
    /// </summary>
    Task<HybridLogicalClock?> GetMinCursorAsync(
        string treeName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the causal-stable frontier for <paramref name="treeName"/>:
    /// the pointwise minimum <see cref="VersionVector"/> across every
    /// consumer that has reported a vector through the causal+ overload
    /// of <see cref="ReportCursorAsync(string, string, HybridLogicalClock, VersionVector, CancellationToken)"/>.
    /// An origin is included in the meet only when every reporting
    /// consumer has named that origin; origins missing from any
    /// consumer are excluded so the frontier is a strict lower bound.
    /// <para>
    /// Returns <see langword="null"/> when no consumer has reported a
    /// vector yet (the registry is empty for the tree, or every
    /// consumer reported HLC-only). When the result is
    /// <see langword="null"/> the GC skips the causal-stable half of
    /// its predicate and degrades to the HLC cursor /
    /// <see cref="LatticeReplicationOptions.WalRetention"/> branches.
    /// </para>
    /// <para>
    /// Implementations are expected to cache the computed frontier and
    /// recompute it only on consumer mutation (a new report or an
    /// unregister), so a high-frequency GC pass that observes a stable
    /// registry is O(1) per call.
    /// </para>
    /// </summary>
    /// <param name="treeName">Logical tree id whose causal-stable frontier is being read. Must not be <see langword="null"/> or whitespace.</param>
    /// <param name="cancellationToken">Cancellation token observed before any state mutation.</param>
    Task<VersionVector?> GetCausalStableAsync(
        string treeName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the blocked-floor for <paramref name="treeName"/>: the
    /// pointwise minimum <see cref="HybridLogicalClock"/> across every
    /// consumer that has reported a non-<see langword="null"/>
    /// <c>BlockedAtHlc</c> through the blocked-floor overloads of
    /// <see cref="ReportCursorAsync(string, string, HybridLogicalClock, HybridLogicalClock?, CancellationToken)"/>.
    /// Consumers that have never reported a non-null pin (the
    /// majority — leaf materialisers, peer ship loops) are excluded,
    /// and a consumer that previously reported a non-null pin and
    /// later cleared it via a null report no longer contributes.
    /// <para>
    /// Returns <see langword="null"/> when no consumer currently
    /// reports a buffer pin. When the result is <see langword="null"/>
    /// the GC skips the blocked-floor half of its predicate and
    /// degrades cleanly to the cursor / TTL / causal-stable
    /// branches.
    /// </para>
    /// <para>
    /// Implementations are expected to cache the computed floor
    /// behind the same per-tree generation counter that gates the
    /// causal-stable cache, so a high-frequency GC pass that
    /// observes a stable registry is O(1) per call.
    /// </para>
    /// </summary>
    /// <param name="treeName">Logical tree id whose blocked-floor is being read. Must not be <see langword="null"/> or whitespace.</param>
    /// <param name="cancellationToken">Cancellation token observed before any state mutation.</param>
    Task<HybridLogicalClock?> GetBlockedFloorAsync(
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
