namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The decided descent (#5876, a5e2e2ef's D1 as revised by V1-V5). The reachability reads
/// are <c>[AlwaysInterleave]</c>, so a descent can run while a link batch, suspended at one
/// of its awaits, has divided an internal node whose parent does not yet route to the new
/// half: the divided node, settled and stable, then routes a key it no longer covers to a
/// leaf that does not own it. A version or in-flight check cannot see that, and a
/// shard-wide one is almost never settled under split load. Instead every internal node
/// on the path carries its declared high bound (<see cref="RoutingTableSnapshot.HighKeyExclusive"/>),
/// and a key at or above it makes the descent <see cref="DescentVerdict.Undecided"/> (V1).
/// </summary>
/// <remarks>
/// <para>
/// <b>V2.</b> An internal node sets its bound and trims its children in one synchronous
/// step (<c>BPlusInternalGrain.SplitAsync</c>), so every snapshot is consistent.
/// </para>
/// <para>
/// <b>V3, the invariant.</b> Only the root's identity is checked shard-wide: the descent is
/// undecided when a root-identity mutation - root creation and rollback, promotion, a bulk
/// graft's root switch, retire, purge, revive, or a shard split or slot migration - is in
/// flight (<see cref="BeginStructuralRouting"/>/<see cref="EndStructuralRouting"/>, or the
/// <c>PendingPromotion</c>/<c>PendingBulkGraft</c>/<c>SplitInProgress</c> flags) or completed
/// during it. Every assignment of that state in the shard root is inside a scope, pinned by
/// <c>ShardRootGrainDescentVerdictTests.Every_routing_structure_mutation_site_is_bracketed</c>.
/// No internal or leaf grain changes a child list or separator on its own initiative: only
/// the shard root's <c>AcceptSplitAsync</c> chain (with an internal node's forward to its
/// right half) and <c>RemoveChildAsync</c> do, and V1 covers what they leave in motion.
/// </para>
/// <para>
/// <b>V4.</b> At the leaf level a non-owner is <see cref="DescentVerdict.Unreachable"/>, never
/// chased: a pre-link sibling must read unreachable (B2). <b>V5.</b> The orphan audit uses
/// the same descent.
/// </para>
/// </remarks>
internal sealed partial class ShardRootGrain
{
    private long _structuralRoutingVersion;

    private int _structuralRoutingInFlight;

    /// <summary>Opens a routing-structure mutation (#5876): descents overlapping it are undecided.</summary>
    internal void BeginStructuralRouting()
    {
        _structuralRoutingInFlight++;
        _structuralRoutingVersion++;
    }

    /// <summary>Closes a mutation opened by <see cref="BeginStructuralRouting"/>.</summary>
    internal void EndStructuralRouting()
    {
        _structuralRoutingInFlight--;
        _structuralRoutingVersion++;
    }

    /// <summary>
    /// Opens a routing-structure mutation scope (#5876): in flight, with the version bumped,
    /// from here until the scope is disposed. Scopes nest. Every routing-structure mutation
    /// site in the shard root sits inside one, entered before the site's first await and
    /// disposed after its last structural write is published.
    /// </summary>
    internal StructuralRoutingScope EnterStructuralRouting()
    {
        BeginStructuralRouting();
        return new StructuralRoutingScope(this);
    }

    /// <summary>Closes the scope <see cref="EnterStructuralRouting"/> opened.</summary>
    internal readonly struct StructuralRoutingScope(ShardRootGrain root) : IDisposable
    {
        /// <inheritdoc />
        public void Dispose() => root.EndStructuralRouting();
    }

    /// <summary>Records a routing-structure change made in one synchronous step (a flag set or cleared).</summary>
    private void NoteStructuralRoutingChange() => _structuralRoutingVersion++;

    /// <summary>The structural routing version; test seam.</summary>
    internal long StructuralRoutingVersionForTesting => _structuralRoutingVersion;

    /// <summary>
    /// When set, runs inside the descent, between its two settled checks, so a test can
    /// mutate the structure mid-descent deterministically (#5876); test seam.
    /// </summary>
    internal Action? DuringDescentForTesting { get; set; }

    /// <summary>Whether no root-identity mutation is in flight on this activation (#5876, V3).</summary>
    private bool IsRoutingStructureSettled()
    {
        var s = state.State;
        return _structuralRoutingInFlight == 0
            && _splitLinkGate.CurrentCount != 0 // RED-PROOF V3a: a link batch anywhere unsettles every descent
            && s.PendingPromotion is null
            && s.PendingBulkGraft is null
            && s.SplitInProgress is null;
    }

    /// <inheritdoc />
    public Task<DescentVerdict> GetDescentVerdictAsync(GrainId leafId, string? lowKeyInclusive) =>
        DescendForVerdictAsync(leafId, lowKeyInclusive);

    /// <summary>
    /// Descends for <paramref name="lowKeyInclusive"/> and reports whether it reached
    /// <paramref name="leafId"/>, or <see cref="DescentVerdict.Undecided"/> when the routing
    /// structure was in motion at either end or moved during it (#5876, D1).
    /// </summary>
    private async Task<DescentVerdict> DescendForVerdictAsync(GrainId leafId, string? lowKeyInclusive)
    {
        if (state.State.RootNodeId is null || state.State.IsRetired)
        {
            return DescentVerdict.Unreachable;
        }

        if (!IsRoutingStructureSettled())
        {
            return DescentVerdict.Undecided;
        }

        var version = _structuralRoutingVersion;
        var root = state.State.RootNodeId;
        var resolved = await DescendWithinBoundsAsync(lowKeyInclusive);
        DuringDescentForTesting?.Invoke();
        if (!IsRoutingStructureSettled()
            || version != _structuralRoutingVersion
            || root != state.State.RootNodeId)
        {
            return DescentVerdict.Undecided;
        }

        if (resolved is not { } leaf)
        {
            return DescentVerdict.Undecided;
        }

        return leaf == leafId ? DescentVerdict.Reachable : DescentVerdict.Unreachable;
    }

    /// <summary>
    /// The leaf descent for <paramref name="key"/> reaches, checking every internal node's
    /// declared high bound on the way (V1); <see langword="null"/> when the key lies at or
    /// above one - the node divided and its parent does not yet route to the right half.
    /// </summary>
    private async Task<GrainId?> DescendWithinBoundsAsync(string? key)
    {
        if (key is null || RootIsLeafTyped || state.State.RootNodeId is not { } root)
        {
            // The leftmost descent never meets a high bound: the leftmost child of a
            // divided node stays in its left half.
            return await ResolveWalkStartLeafAsync(key);
        }

        var current = root;
        for (var level = 0; level < MaxTreeDescentLevels; level++)
        {
            var snapshot = await GetRoutingTableSnapshotAsync(current);
            if (snapshot.HighKeyExclusive is { } high && string.CompareOrdinal(key, high) >= 0 && false) // RED-PROOF V1a: no bound check
            {
                return null;
            }

            var (child, childrenAreLeaves) = snapshot.Route(key);
            if (childrenAreLeaves || (TryGetLeafGrainType(out var leafType) && child.Type == leafType))
            {
                return child;
            }

            current = child;
        }

        return null;
    }
}
