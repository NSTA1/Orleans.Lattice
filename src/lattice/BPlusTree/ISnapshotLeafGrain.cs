using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Transient per-shard snapshot leaf grain used by zero-observable-
/// writes snapshot cursors. Materialises a read-only view of one
/// shard's projection by replaying the per-shard write-ahead log up
/// to a captured offset (see
/// <see cref="LatticeSnapshotCoordinate.PerShardWalOffsets"/>), then
/// exposes range-scan helpers for the cursor's
/// <c>NextKeysAsync</c> / <c>NextEntriesAsync</c> calls.
/// <para>
/// In-memory only - no <c>IPersistentState</c>, no WAL of its own.
/// A grain reactivation (silo failover, idle eviction) re-runs the
/// same replay; replay is deterministic and idempotent by
/// construction. Grain key:
/// <c>{treeId}/{shardIndex}/{snapshotCoordinateHash}</c>.
/// </para>
/// </summary>
[Alias(TypeAliases.ISnapshotLeafGrain)]
internal interface ISnapshotLeafGrain : IGrainWithStringKey
{
    /// <summary>
    /// Materialises the snapshot leaf by replaying the per-shard WAL
    /// prefix across every WAL partition. Idempotent: a second call
    /// with the same <paramref name="capturedOffsetsByPartition"/>
    /// sequence is a no-op once the first call has completed; a call
    /// with a different sequence throws
    /// <see cref="InvalidOperationException"/> (snapshot leaves are
    /// coordinate-keyed - a different captured frontier belongs to a
    /// different grain activation).
    /// </summary>
    /// <param name="treeId">Tree the snapshot belongs to.</param>
    /// <param name="shardIndex">Virtual shard this leaf materialises.</param>
    /// <param name="capturedOffsetsByPartition">
    /// Per-WAL-partition upper-bound (exclusive) WAL offsets captured
    /// at open time. Indexed by partition number; the snapshot leaf
    /// replays records <c>(empty, capturedOffsetsByPartition[p])</c>
    /// for each partition <c>p</c> and merges the results under the
    /// two-pass (Set/Delete/prepare first, then TxCommit/TxAbort/
    /// DeleteRange) replay strategy so saga atomicity and range-
    /// tombstone ordering are preserved across partition boundaries.
    /// On single-partition trees the list has a single element and
    /// the two-pass collapses to a single forward walk for
    /// behavioural parity with the legacy scalar-offset shape.
    /// </param>
    /// <param name="ownedVirtualSlots">
    /// The sorted, ascending set of virtual slots the pinned snapshot
    /// shard map (<see cref="LatticeSnapshotCoordinate.PinnedShardMap"/>)
    /// assigns to this leaf's <paramref name="shardIndex"/>. When
    /// non-null the leaf surfaces a replayed key only when its virtual
    /// slot (computed with <paramref name="virtualShardCount"/>) is a
    /// member of this set; keys whose slot the pinned map assigns to a
    /// different shard are donor orphans left behind by an adaptive shard
    /// split and are dropped. <see langword="null"/> disables the filter
    /// (single-shard snapshot, or a legacy coordinate that captured no
    /// pinned map) and the leaf surfaces every replayed key.
    /// </param>
    /// <param name="virtualShardCount">
    /// The pinned map's virtual shard count, used to recompute each key's
    /// virtual slot for the ownership check. Ignored when
    /// <paramref name="ownedVirtualSlots"/> is <see langword="null"/>.
    /// </param>
    /// <param name="baselineToken">
    /// The cursor's per-open frozen-baseline token
    /// (<see cref="LatticeSnapshotCoordinate.SnapshotBaselineToken"/>). When
    /// non-empty the leaf seeds its projection from the durable per-shard
    /// <see cref="Grains.SnapshotShardBaseline"/> captured at open time
    /// (through the same <paramref name="ownedVirtualSlots"/> ownership filter)
    /// and performs <b>no</b> WAL replay, so a later WAL GC cannot perturb the
    /// view. <see cref="Guid.Empty"/> selects the legacy from-zero WAL-replay
    /// path for coordinates persisted before the frozen-baseline store existed.
    /// </param>
    /// <param name="cancellationToken">Cancels the replay loop between slices.</param>
    Task OpenAsync(string treeId, int shardIndex, IReadOnlyList<long> capturedOffsetsByPartition, IReadOnlyList<int>? ownedVirtualSlots, int virtualShardCount, Guid baselineToken, CancellationToken cancellationToken);

    /// <summary>
    /// Returns the sorted list of keys this snapshot leaf observes in
    /// the optional [<paramref name="startInclusive"/>,
    /// <paramref name="endExclusive"/>) range. Mirrors the live
    /// <c>IBPlusLeafGrain.GetKeysAsync</c> filter contract (after /
    /// before exclusive bounds for continuation-token pagination)
    /// but serves them off the snapshot leaf's replayed projection
    /// state rather than the live <c>Entries</c> dictionary.
    /// <para>
    /// When <paramref name="reverse"/> is <see langword="true"/> the fetch returns
    /// the <b>largest</b> <paramref name="limit"/> keys in range (still sorted
    /// ascending) rather than the smallest, so the snapshot cursor's reverse k-way
    /// merge - which walks each per-shard slice from its high end - sees the correct
    /// top-of-range candidates. A forward fetch returns the smallest
    /// <paramref name="limit"/> as before.
    /// </para>
    /// </summary>
    Task<List<string>> GetKeysAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null, int limit = int.MaxValue, LatticePredicateNode? predicate = null, bool reverse = false);

    /// <summary>
    /// Returns the sorted list of live key-value pairs this snapshot
    /// leaf observes in the optional [<paramref name="startInclusive"/>,
    /// <paramref name="endExclusive"/>) range. Same filter contract
    /// as <see cref="GetKeysAsync"/>, including the
    /// <paramref name="limit"/> truncation knob, the optional
    /// server-side <paramref name="predicate"/>, and the
    /// <paramref name="reverse"/> top-of-range selection.
    /// </summary>
    Task<List<KeyValuePair<string, byte[]>>> GetEntriesAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null, int limit = int.MaxValue, LatticePredicateNode? predicate = null, bool reverse = false);
}
