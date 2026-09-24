using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Result returned when a leaf or internal node splits.
/// The parent node uses this to insert the promoted separator key and
/// the reference to the newly created sibling.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.SplitResult)]
[Immutable]
internal sealed record SplitResult
{
    /// <summary>The separator key promoted to the parent.</summary>
    [Id(0)] public required string PromotedKey { get; init; }

    /// <summary>The grain identity of the newly created right sibling.</summary>
    [Id(1)] public required GrainId NewSiblingId { get; init; }

    /// <summary>
    /// Whether <see cref="NewSiblingId"/> identifies a leaf grain (<c>true</c>)
    /// or an internal-node grain (<c>false</c>). Self-describes the sibling's
    /// node type so a deferred <see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.PendingPromotion"/>
    /// resume path can construct the new internal root with the correct
    /// <c>childrenAreLeaves</c> value without re-reading <c>RootIsLeaf</c>
    /// from shard-root state - a value an interleaved peer
    /// <c>SetManyAsync</c> turn may have already flipped after this
    /// <see cref="SplitResult"/> was produced. Captured at every
    /// construction site (leaf split, internal split, bulk-graft) and
    /// preserved across the <c>AcceptSplitAsync</c> bubble loop.
    /// </summary>
    [Id(2)] public bool ChildIsLeaf { get; init; }

    /// <summary>
    /// Further leaf splits produced by the same call, beyond this one. Null
    /// when the call produced a single split, which is the overwhelming case.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A leaf call can produce more than one split when it forwards part of
    /// its work to a sibling: a write that lands on a leaf mid-division, or
    /// on a key its declared span no longer covers, is forwarded to the
    /// right sibling, and that sibling can overflow and divide in turn. Each
    /// such division creates a new leaf that is spliced into the sibling
    /// chain and pinned, but that no descent reaches until a parent learns
    /// its separator - and the only party that can deliver the separator is
    /// the shard root that receives this result.
    /// </para>
    /// <para>
    /// The leaf API returns a single <see cref="SplitResult"/>, so these
    /// forwarded divisions used to be discarded, on the reasoning that the
    /// sibling's split "describes a split of the sibling". Every discarded
    /// one left an orphaned leaf and every key on it unreachable (issue
    /// #3523). They are carried here instead, flattened (no entry of
    /// <see cref="Additional"/> carries its own <see cref="Additional"/>),
    /// and the shard root links each by re-descending on its
    /// <see cref="PromotedKey"/> rather than reusing the path it captured
    /// for the leaf it called, because a forwarded sibling need not share
    /// that leaf's parent. Build combined values with
    /// <see cref="Combine(SplitResult?, SplitResult?)"/>.
    /// </para>
    /// <para>
    /// Every entry sits at the same level as the primary result. A leaf call
    /// combines leaf-level splits, so every entry has <see cref="ChildIsLeaf"/>
    /// set. An internal node combines the division it recovered from an
    /// interrupted split with any division the same call then made, both of
    /// its own level, so there no entry has it set.
    /// </para>
    /// </remarks>
    [Id(3)] public SplitResult[]? Additional { get; init; }

    /// <summary>
    /// Whether this result's own division (its <see cref="PromotedKey"/> and
    /// <see cref="NewSiblingId"/>) is of a sibling the called leaf forwarded
    /// work to, rather than of the called leaf itself (issue #3523).
    /// </summary>
    /// <remarks>
    /// A caller links a leaf's own split against the ancestor path it
    /// captured on the way down to that leaf. A forwarded sibling need not
    /// share that leaf's parent, so a forwarded division must never be
    /// linked against that path: its separator would reach a parent whose
    /// range need not cover it. A forwarded result is linked by re-descent
    /// on its promoted key instead, exactly as every entry of
    /// <see cref="Additional"/> is. Set only through
    /// <see cref="Forward(SplitResult?)"/>; entries of
    /// <see cref="Additional"/> never carry it, since they are all linked by
    /// re-descent regardless.
    /// </remarks>
    [Id(4)] public bool Forwarded { get; init; }

    /// <summary>
    /// Marks a split result a sibling returned to a forwarding leaf as
    /// forwarded, so the forwarding leaf's caller links it by re-descent
    /// rather than against the path it captured for the forwarding leaf
    /// (see <see cref="Forwarded"/>).
    /// </summary>
    /// <param name="forwarded">The sibling's split result, possibly null.</param>
    /// <returns><see langword="null"/> when <paramref name="forwarded"/> is null; otherwise the same result marked forwarded.</returns>
    public static SplitResult? Forward(SplitResult? forwarded) =>
        forwarded is null || forwarded.Forwarded ? forwarded : forwarded with { Forwarded = true };

    /// <summary>
    /// Combines two possibly-null split results from the same leaf call into
    /// one, so neither division is lost (issue #3523). The first non-null
    /// argument becomes the primary result; every other division, including
    /// any already carried in either argument's <see cref="Additional"/>,
    /// is flattened into the combined value's <see cref="Additional"/>.
    /// </summary>
    /// <param name="first">The first split result, typically the called leaf's own.</param>
    /// <param name="second">The second split result, typically a forwarded sibling's.</param>
    /// <returns>
    /// <see langword="null"/> when both are null; the non-null argument
    /// unchanged when only one is; otherwise a combined result.
    /// </returns>
    public static SplitResult? Combine(SplitResult? first, SplitResult? second)
    {
        if (first is null) return second;
        if (second is null) return first;

        // Keep a split of the called leaf itself as the primary whenever
        // there is one: only the primary may be linked against the ancestor
        // path the caller captured for that leaf.
        if (first.Forwarded && !second.Forwarded)
        {
            (first, second) = (second, first);
        }

        var count = 1 + (first.Additional?.Length ?? 0) + (second.Additional?.Length ?? 0);
        var additional = new SplitResult[count];
        var i = 0;
        if (first.Additional is not null)
        {
            foreach (var extra in first.Additional)
            {
                additional[i++] = extra;
            }
        }

        additional[i++] = second.Additional is null && !second.Forwarded
            ? second
            : second with { Additional = null, Forwarded = false };
        if (second.Additional is not null)
        {
            foreach (var extra in second.Additional)
            {
                additional[i++] = extra;
            }
        }

        return first with { Additional = additional };
    }
}
