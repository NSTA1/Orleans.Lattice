namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The decided descent (#5876, a5e2e2ef's D1). The reachability reads are
/// <c>[AlwaysInterleave]</c>, so a descent can run while a link batch, suspended at one of
/// its awaits, has divided an internal node whose parent does not yet route to the new
/// half: it then names a leaf that does not own the key, or misses the one that does. A
/// descent therefore answers only when the routing structure was settled before and after
/// it and its version did not move; otherwise it is <see cref="DescentVerdict.Undecided"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>The invariant.</b> Every mutation of the shard's routing structure runs inside
/// <see cref="BeginStructuralRouting"/>/<see cref="EndStructuralRouting"/> or behind a state
/// flag the settled check reads: the split-link gate's critical sections (every link,
/// internal division and the root promotions they bubble into), the promotion gate's
/// completion, a reclaim's routing retirement, a bulk graft (<c>PendingBulkGraft</c>, and
/// the root it installs) and a shard split (<c>SplitInProgress</c>). The set is pinned by
/// <c>ShardRootGrainDescentVerdictTests.Every_routing_structure_mutation_site_is_bracketed</c>.
/// </para>
/// <para>
/// This version is deliberately not <c>_routingEpoch</c>: that one moves on every
/// interleaved batch write, so under write load every descent would read undecided and no
/// leaf would ever prove reachable. The check is two reads of in-memory state before and
/// after the descent; no grain call, no allocation.
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

    /// <summary>Whether no routing-structure mutation is in flight on this activation (#5876).</summary>
    private bool IsRoutingStructureSettled()
    {
        var s = state.State;
        return _structuralRoutingInFlight == 0
            && _splitLinkGate.CurrentCount != 0
            && _promotionGate.CurrentCount != 0
            && s.PendingPromotion is null
            && s.PendingBulkGraft is null
            && s.PendingChildLinks.Count == 0
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
        var resolved = await ResolveWalkStartLeafAsync(lowKeyInclusive);
        DuringDescentForTesting?.Invoke();
        if (!IsRoutingStructureSettled()
            || version != _structuralRoutingVersion
            || root != state.State.RootNodeId)
        {
            return DescentVerdict.Undecided;
        }

        return resolved == leafId ? DescentVerdict.Reachable : DescentVerdict.Unreachable;
    }
}
