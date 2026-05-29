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
    /// <param name="cancellationToken">Cancels the replay loop between slices.</param>
    Task OpenAsync(string treeId, int shardIndex, IReadOnlyList<long> capturedOffsetsByPartition, CancellationToken cancellationToken);

    /// <summary>
    /// Returns the sorted list of keys this snapshot leaf observes in
    /// the optional [<paramref name="startInclusive"/>,
    /// <paramref name="endExclusive"/>) range. Mirrors the live
    /// <c>IBPlusLeafGrain.GetKeysAsync</c> filter contract (after /
    /// before exclusive bounds for continuation-token pagination)
    /// but serves them off the snapshot leaf's replayed projection
    /// state rather than the live <c>Entries</c> dictionary.
    /// <para>
    /// When <paramref name="limit"/> is non-negative the result is
    /// truncated to at most that many keys, allowing the snapshot
    /// cursor's per-page k-way merge to bound its per-shard fetch
    /// cost. <c>0</c> returns an empty list; the default
    /// <see cref="int.MaxValue"/> preserves the unbounded contract.
    /// </para>
    /// </summary>
    Task<List<string>> GetKeysAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null, int limit = int.MaxValue);

    /// <summary>
    /// Returns the sorted list of live key-value pairs this snapshot
    /// leaf observes in the optional [<paramref name="startInclusive"/>,
    /// <paramref name="endExclusive"/>) range. Same filter contract
    /// as <see cref="GetKeysAsync"/>, including the
    /// <paramref name="limit"/> truncation knob.
    /// </summary>
    Task<List<KeyValuePair<string, byte[]>>> GetEntriesAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null, int limit = int.MaxValue);
}
