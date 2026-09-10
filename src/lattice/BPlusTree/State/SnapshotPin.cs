namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Per-pin tombstone-retention record persisted by
/// <see cref="Orleans.Lattice.BPlusTree.Grains.TxRegistryGrain"/> against a point-in-time cursor's
/// saga-decision snapshot. Keeps every txid the snapshot referenced
/// exempt from tombstone expiry for the lifetime of the pin even as
/// concurrent sagas call <c>ForgetAsync</c>, so the registry's own
/// <c>GetStatusAsync</c> keeps answering with the recorded
/// <see cref="TxStatus.Committed"/> / <see cref="TxStatus.Aborted"/>
/// outcome instead of degrading to <see cref="TxStatus.InFlight"/>,
/// and so a reading that had aged out to
/// <see cref="TxStatus.Indeterminate"/> - a stored decision row whose
/// tombstone has expired - is restored while pinned, because the
/// retention mask is pin-aware.
/// <para>
/// WHAT THIS PIN DOES NOT DO, because the distinction has already been
/// got wrong once (issue #2325). It is <b>not</b> what makes a cursor's
/// own <c>Next*Async</c> step snapshot-consistent, and it could not be.
/// That step runs inside a
/// <see cref="LatticeRegistrySnapshotContext"/> scope carrying the
/// dictionary the cursor materialised at open, and
/// <c>BPlusLeafGrain.ResolvePendingStatusAsync</c> answers from that
/// dictionary and returns <b>without contacting the registry at all</b>.
/// There is therefore no registry lookup on that path for a pin to
/// influence: delete every pin and a scoped step takes exactly the same
/// readings. What the pin protects is every read that really does reach
/// the registry - an unscoped read of the same keys, a re-derived view,
/// and the cursor's own reading of an entry the snapshot captured as
/// <see cref="TxStatus.Indeterminate"/>. The precedence rule the cursor
/// guarantee actually rests on is pinned by
/// <c>BPlusLeafGrainTests.GetAsync_under_a_snapshot_scope_answers_from_the_snapshot_without_consulting_the_registry</c>
/// and its <c>GetManyAsync</c> counterpart - one per short-circuit
/// site, because the single-key and batched resolutions are separate
/// code - so removing either short-circuit fails the build rather than silently
/// removing the guarantee while this type stays in place and appears to
/// cover it.
/// </para>
/// <para>
/// One <see cref="SnapshotPin"/> entry lives in the registry's
/// <c>SnapshotPins</c> map per active <see cref="LatticeCursorSpec.PointInTime"/>
/// cursor, keyed by a server-assigned <c>Guid pinId</c>. The cursor
/// grain refreshes its pin on every step; the registry independently
/// expires pins past <see cref="ExpiresAt"/> via the same prune pass
/// that handles tombstone expiry.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.SnapshotPin)]
internal sealed class SnapshotPin
{
    /// <summary>
    /// The set of saga txids this pin holds against the registry. Every
    /// entry was present in the cursor's captured snapshot (with a
    /// non-<see cref="TxStatus.InFlight"/> outcome) at the moment the
    /// pin was installed. Stored as <see cref="HashSet{T}"/> for fast
    /// union/intersection during prune and footprint accounting.
    /// </summary>
    [Id(0)] public HashSet<Guid> Txids { get; set; } = [];

    /// <summary>
    /// Wall-clock instant past which the registry may evict this pin
    /// from its <c>SnapshotPins</c> map even without an explicit
    /// <c>UnpinSnapshotAsync</c>. Refreshed by
    /// <c>RefreshPinAsync</c> on every cursor step. A subsequent
    /// cursor step that finds its pin missing throws
    /// <see cref="LatticeCursorSnapshotExpiredException"/>.
    /// </summary>
    [Id(1)] public DateTimeOffset ExpiresAt { get; set; }
}