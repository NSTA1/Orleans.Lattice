namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Outcome of a single bounded
/// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.CompactTombstonesAsync"/>
/// turn: how many entries it physically reaped, and whether it got to the end
/// of the leaf before its work budget was spent.
/// <para>
/// <b><see cref="Completed"/> is the load-bearing field, and it fails
/// closed.</b> The compaction pass is bounded so one call returns comfortably
/// inside the Orleans request timeout (issue 4135), which means a
/// tombstone-heavy leaf legitimately returns with work still outstanding. A
/// caller that treated every successful return as "this leaf is drained" would
/// then clear the leaf's shard-root dirty mark with condemned entries still in
/// place - the "improves every observable while degrading the thing being
/// measured" trade issue 2926 refuses. Reading <see cref="Completed"/> is what
/// keeps that from happening.
/// </para>
/// <para>
/// <b>Nothing is persisted to record that a pass was partial, deliberately.</b>
/// A durability marker would need a coordinator state write, and that write
/// fails under precisely the write pressure that causes the truncation, so the
/// marker would be unavailable in exactly the case it exists for. Instead
/// incomplete is the default on both sides and only a landed write claims
/// completeness: the leaf stamps <c>LastCompactionVersion</c> only when it
/// finished, and the coordinator calls
/// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.ClearDirtyLeavesUpToAsync"/>
/// only when every leaf it walked finished. Not stamping and not clearing both
/// require no write, so neither can fail.
/// </para>
/// <para>
/// <b>A truncated pass loses no work.</b> Each reaped entry is committed to the
/// WAL before it leaves the in-memory cache, so reaped entries stay reaped
/// across the turn boundary and every re-scan finds strictly less than the one
/// before it. The cost of truncation is a re-scan, not lost progress, and that
/// monotone decrease is what guarantees a leaf eventually drains rather than
/// re-attempting the same unbounded work forever.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafCompactionResult)]
[Immutable]
internal readonly record struct LeafCompactionResult
{
    /// <summary>
    /// Number of entries - condemned tombstones plus TTL-expired live entries -
    /// this turn physically removed from the leaf. Counts what was actually
    /// reaped, not what the scan condemned, so a turn truncated part-way
    /// through its removal loop reports the smaller, true figure.
    /// </summary>
    [Id(0)] public int EntriesRemoved { get; init; }

    /// <summary>
    /// Whether this turn reached the end of the leaf before its work budget was
    /// spent. <see langword="false"/> means the pass was truncated and the leaf
    /// still holds condemned entries, so its dirty mark must survive and a
    /// later pass must re-nominate it. See the type remarks.
    /// </summary>
    [Id(1)] public bool Completed { get; init; }

    /// <summary>
    /// A completed turn that reaped <paramref name="entriesRemoved"/> entries.
    /// </summary>
    internal static LeafCompactionResult Complete(int entriesRemoved) =>
        new() { EntriesRemoved = entriesRemoved, Completed = true };

    /// <summary>
    /// A turn truncated by its work budget after reaping
    /// <paramref name="entriesRemoved"/> entries, with condemned entries still
    /// outstanding in the leaf.
    /// </summary>
    internal static LeafCompactionResult Truncated(int entriesRemoved) =>
        new() { EntriesRemoved = entriesRemoved, Completed = false };
}
