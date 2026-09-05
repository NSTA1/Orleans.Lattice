namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A point-in-time snapshot of a child node's contribution to its parent
/// internal node's subtree fold. Carried by
/// <see cref="IBPlusInternalGrain.OnChildDigestPublishedAsync"/> so the
/// parent can XOR the old contribution out and the new contribution in
/// without re-walking every sibling. Defined as a serialisable value
/// type because the propagation hook is a cross-grain RPC.
/// <para>
/// The <see cref="Hash"/> field is the child's 16-byte XOR-fold
/// projection hash (a leaf's <c>state.State.ProjectionHash</c>, or an
/// internal node's <c>SubtreeProjectionHash</c>). The
/// <see cref="EntryCount"/> field is the sum of live and tombstoned
/// entries in the child's subtree, and <see cref="CheckpointOffset"/>
/// is the highest <c>ProjectionCheckpointOffset</c> across descendant
/// leaves (max-reduced upward, not summed, so two silos at the same
/// applied-prefix observe the same value regardless of how the chain
/// is sharded).
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ChildDigestSnapshot)]
[Immutable]
internal readonly record struct ChildDigestSnapshot
{
    /// <summary>The child's 16-byte XOR-fold projection hash (may be <see langword="null"/> when the child has never published).</summary>
    [Id(0)] public byte[]? Hash { get; init; }

    /// <summary>Total entry count folded into the subtree.</summary>
    [Id(1)] public long EntryCount { get; init; }

    /// <summary>Highest projection-checkpoint offset across descendant leaves.</summary>
    [Id(2)] public long CheckpointOffset { get; init; }

    /// <summary>
    /// Monotonic, per-publisher-activation stamp identifying the order in
    /// which this snapshot was produced relative to the publishing node's
    /// other snapshots. The parent's fold
    /// (<c>BPlusInternalGrain.ApplyChildSnapshotAsync</c>) drops any
    /// snapshot whose sequence is <em>strictly lower</em> than the one
    /// already folded for the same child, so a late publish that raced a
    /// fresher one and arrived out of order cannot overwrite the newer
    /// value for a still-owned child. The hazard it closes: under the
    /// <c>[AlwaysInterleave]</c> leaf mutation surface a coalesced
    /// per-write publish can read a child's pre-split (pre-trim) entry
    /// count and land after the split's post-trim inline publish,
    /// permanently inflating the chained-fold count. A default value of
    /// <c>0</c> (used by direct unit-test pushes and the range/partial
    /// digest computations that never reach the parent fold) is treated as
    /// "unsequenced" and always accepted, preserving last-write-wins
    /// semantics for callers that do not stamp a sequence.
    /// </summary>
    [Id(3)] public long PublishSequence { get; init; }

    /// <summary>
    /// Lowest key inclusively covered by the child's subtree, or
    /// <see langword="null"/> when the subtree is empty. Structural-only:
    /// populated by snapshot producers and stored verbatim in the parent's
    /// per-child table, but never folded into the digest arithmetic.
    /// </summary>
    [Id(4)] public string? LowKeyInclusive { get; init; }

    /// <summary>
    /// Exclusive upper bound of the key range the child's subtree covers,
    /// or <see langword="null"/> when unbounded/empty. Structural-only.
    /// </summary>
    [Id(5)] public string? HighKeyExclusive { get; init; }

    /// <summary>Count of live (non-tombstoned, unexpired) entries in the child's subtree. Structural-only.</summary>
    [Id(6)] public long LiveCount { get; init; }

    /// <summary>Count of tombstoned entries retained in the child's subtree. Structural-only.</summary>
    [Id(7)] public long TombstoneCount { get; init; }

    /// <summary>Height of the child's subtree: <c>1</c> for a leaf, <c>1 + max(child depth)</c> for an internal node. Structural-only.</summary>
    [Id(8)] public int SubtreeDepth { get; init; }

    /// <summary>Number of immediate children: <c>0</c> for a leaf, <c>Children.Count</c> for an internal node. Structural-only.</summary>
    [Id(9)] public int ChildFanout { get; init; }

    /// <summary>
    /// Compares two snapshots by value: every scalar field plus the
    /// <see cref="Hash"/> bytes compared by content. The compiler-generated
    /// record-struct equality compares <see cref="Hash"/> with
    /// <see cref="EqualityComparer{T}.Default"/>, which for a <see cref="byte"/>
    /// array is reference equality, so two structurally identical snapshots would
    /// otherwise never compare equal - and a snapshot that round-trips through
    /// serialization would never equal its pre-serialization self, defeating the
    /// content-digest comparison this type exists for.
    /// </summary>
    /// <param name="other">The snapshot to compare against.</param>
    public bool Equals(ChildDigestSnapshot other) =>
        EntryCount == other.EntryCount
        && CheckpointOffset == other.CheckpointOffset
        && PublishSequence == other.PublishSequence
        && string.Equals(LowKeyInclusive, other.LowKeyInclusive, StringComparison.Ordinal)
        && string.Equals(HighKeyExclusive, other.HighKeyExclusive, StringComparison.Ordinal)
        && LiveCount == other.LiveCount
        && TombstoneCount == other.TombstoneCount
        && SubtreeDepth == other.SubtreeDepth
        && ChildFanout == other.ChildFanout
        && (Hash is null ? other.Hash is null : other.Hash is not null && Hash.AsSpan().SequenceEqual(other.Hash));

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(EntryCount);
        hash.Add(CheckpointOffset);
        hash.Add(PublishSequence);
        hash.Add(LowKeyInclusive, StringComparer.Ordinal);
        hash.Add(HighKeyExclusive, StringComparer.Ordinal);
        hash.Add(LiveCount);
        hash.Add(TombstoneCount);
        hash.Add(SubtreeDepth);
        hash.Add(ChildFanout);
        if (Hash is { } bytes)
        {
            hash.AddBytes(bytes);
        }

        return hash.ToHashCode();
    }
}
