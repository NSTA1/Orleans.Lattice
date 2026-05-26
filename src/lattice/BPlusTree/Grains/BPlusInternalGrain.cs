using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Internal (non-leaf) node grain implementation. Stores separator keys and
/// child references. Splits when the child count exceeds the internal-sizing
/// pin in the tree registry.
/// </summary>
internal sealed partial class BPlusInternalGrain(
    IGrainContext context,
    [PersistentState("internal", LatticeOptions.StorageProviderName)] IPersistentState<InternalNodeState> state,
    IGrainFactory grainFactory,
    LatticeOptionsResolver optionsResolver) : IBPlusInternalGrain, IGrainBase
{
    /// <summary>
    /// Implements <see cref="IGrainBase.GrainContext"/> so the
    /// <c>Orleans.GrainExtensions.GetGrainId(IAddressable)</c> extension can
    /// resolve this grain's identity directly from the activation instance.
    /// In production, callers receive a grain reference proxy whose identity
    /// is intrinsic; this property is never reached. Test/bench harnesses
    /// that hand an IGrainFactory mock real grain instances back to callers
    /// (see Orleans.Lattice.Benchmark.Microbench) require this self-describing
    /// shape - exactly the pattern <see cref="BPlusLeafGrain"/> already
    /// follows. Adding <see cref="IGrainBase"/> here aligns the two grain
    /// classes with no production-runtime impact.
    /// </summary>
    public IGrainContext GrainContext => context;

    /// <summary>
    /// Per-activation gate that serialises every mutating entry into
    /// <see cref="AcceptSplitAsync"/> (and any future internal-node
    /// mutation that touches the split state machine).
    /// <para>
    /// U9p step 8c-c-iv-c2-vi: <see cref="IBPlusInternalGrain.AcceptSplitAsync"/>
    /// is marked <c>[AlwaysInterleave]</c> so concurrent overflow
    /// propagations from different leaves do not queue on the internal
    /// node's activation turn. The race window is the same shape as the
    /// c2-iii leaf race: two interleaved callers both reach the
    /// <see cref="Primitives.SplitState.SplitInProgress"/> recovery
    /// branch or both observe the <c>Children.Count &gt; MaxInternalChildren</c>
    /// predicate and both call <c>SplitAsync</c>, double-persisting
    /// <c>state.State</c> and corrupting the child chain. This gate
    /// serialises every <c>AcceptSplitAsync</c> body so the mutation /
    /// persist sequence is atomic per activation; the surrounding
    /// <c>[AlwaysInterleave]</c> still permits read-only RPCs
    /// (<c>RouteWithMetadataAsync</c>, <c>GetRoutingTableAsync</c>,
    /// etc.) to overlap, which is the actual throughput lever.
    /// </para>
    /// </summary>
    private readonly SemaphoreSlim _splitGate = new(1, 1);

    private ResolvedLatticeOptions? _options;
    private ValueTask<ResolvedLatticeOptions> GetOptionsAsync() =>
        _options is not null
            ? new ValueTask<ResolvedLatticeOptions>(_options)
            : ResolveOptionsSlowAsync();

    private async ValueTask<ResolvedLatticeOptions> ResolveOptionsSlowAsync() =>
        _options = await optionsResolver.ResolveAsync(state.State.TreeId ?? string.Empty);

    public async Task InitializeAsync(string separatorKey, GrainId leftChild, GrainId rightChild, bool childrenAreLeaves)
    {
        // Snapshot mutated fields BEFORE any in-memory change so a failing
        // WriteStateAsync below can revert the activation to the state every
        // peer (and any future reactivation) observes from storage. Without
        // this revert, a transient storage failure leaves this activation
        // routing against an initialised topology while every peer reads an
        // uninitialised one - the Class B "persisted / in-memory divergence
        // on write failure" anti-pattern.
        var childrenSnapshot = state.State.Children;
        var childrenAreLeavesSnapshot = state.State.ChildrenAreLeaves;
        var clockSnapshot = state.State.Clock;

        state.State.Children =
        [
            new ChildEntry { SeparatorKey = null, ChildId = leftChild },
            new ChildEntry { SeparatorKey = separatorKey, ChildId = rightChild }
        ];
        state.State.ChildrenAreLeaves = childrenAreLeaves;
        state.State.Clock = HybridLogicalClock.Tick(state.State.Clock);

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Children = childrenSnapshot;
            state.State.ChildrenAreLeaves = childrenAreLeavesSnapshot;
            state.State.Clock = clockSnapshot;
            throw;
        }

        // Wire the digest chain: tell both children this node is their
        // parent so a future mutation can publish a fresh
        // ChildDigestSnapshot upward. SetParentAsync is idempotent on
        // an unchanged parent id, so a re-call from a crash-recovery
        // path is a no-op aside from a refresher publish.
        await SeedChildParentAsync(leftChild, childrenAreLeaves);
        await SeedChildParentAsync(rightChild, childrenAreLeaves);
    }

    public async Task InitializeWithChildrenAsync(List<string?> separatorKeys, List<GrainId> childIds, bool childrenAreLeaves)
    {
        var children = new List<ChildEntry>(separatorKeys.Count);
        for (int i = 0; i < separatorKeys.Count; i++)
            children.Add(new ChildEntry { SeparatorKey = separatorKeys[i], ChildId = childIds[i] });

        // See InitializeAsync for the snapshot/restore rationale.
        var childrenSnapshot = state.State.Children;
        var childrenAreLeavesSnapshot = state.State.ChildrenAreLeaves;
        var clockSnapshot = state.State.Clock;

        state.State.Children = children;
        state.State.ChildrenAreLeaves = childrenAreLeaves;
        state.State.Clock = HybridLogicalClock.Tick(state.State.Clock);

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Children = childrenSnapshot;
            state.State.ChildrenAreLeaves = childrenAreLeavesSnapshot;
            state.State.Clock = clockSnapshot;
            throw;
        }

        // Seed the parent slot on every child so the digest-publication
        // chain is live from the first mutation that touches one of
        // them. See InitializeAsync for the chain rationale.
        foreach (var id in childIds)
        {
            await SeedChildParentAsync(id, childrenAreLeaves);
        }
    }

    /// <summary>
    /// Sets <paramref name="childId"/>'s persisted parent slot to this
    /// node's identity so the child publishes its
    /// <see cref="ChildDigestSnapshot"/> upward on the next
    /// digest-changing mutation. Dispatches to <c>IBPlusLeafGrain</c>
    /// or <c>IBPlusInternalGrain</c> based on <paramref name="childIsLeaf"/>.
    /// Idempotent (mirrors the per-grain <c>SetParentAsync</c> contract).
    /// <para>
    /// After persisting the parent slot, this method pulls the child's
    /// current <see cref="ChildDigestSnapshot"/> and folds it into this
    /// node's subtree aggregates via <see cref="ApplyChildSnapshotAsync"/>.
    /// The pull-then-fold shape avoids a reentrant callback into this
    /// non-reentrant internal grain that would otherwise deadlock when
    /// the child's <c>SetParentAsync</c> publishes back into our
    /// still-running mutation frame.
    /// </para>
    /// </summary>
    private async Task SeedChildParentAsync(GrainId childId, bool childIsLeaf)
    {
        var myId = context.GrainId;
        ChildDigestSnapshot snapshot;
        if (childIsLeaf)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(childId);
            await leaf.SetParentAsync(myId);
            snapshot = await leaf.GetChildDigestSnapshotAsync();
        }
        else
        {
            var inner = grainFactory.GetGrain<IBPlusInternalGrain>(childId);
            await inner.SetParentAsync(myId);
            snapshot = await inner.GetChildDigestSnapshotAsync();
        }
        await ApplyChildSnapshotAsync(childId, snapshot);
    }

    public Task<(GrainId ChildId, bool ChildrenAreLeaves)> RouteWithMetadataAsync(string key) =>
        Task.FromResult((state.State.Route(key), state.State.ChildrenAreLeaves));

    public Task<RoutingTableSnapshot> GetRoutingTableAsync()
    {
        var children = state.State.Children;
        var seps = new string?[children.Count];
        var ids = new GrainId[children.Count];
        for (int i = 0; i < children.Count; i++)
        {
            seps[i] = children[i].SeparatorKey;
            ids[i] = children[i].ChildId;
        }
        return Task.FromResult(new RoutingTableSnapshot
        {
            SeparatorKeys = seps,
            ChildIds = ids,
            ChildrenAreLeaves = state.State.ChildrenAreLeaves,
        });
    }

    public Task<GrainId> GetLeftmostChildAsync() =>
        Task.FromResult(state.State.Children[0].ChildId);

    public Task<GrainId> GetRightmostChildAsync() =>
        Task.FromResult(state.State.Children[^1].ChildId);

    public Task<(GrainId ChildId, bool ChildrenAreLeaves)> GetLeftmostChildWithMetadataAsync() =>
        Task.FromResult((state.State.Children[0].ChildId, state.State.ChildrenAreLeaves));

    public Task<(GrainId ChildId, bool ChildrenAreLeaves)> GetRightmostChildWithMetadataAsync() =>
        Task.FromResult((state.State.Children[^1].ChildId, state.State.ChildrenAreLeaves));

    public Task<bool> AreChildrenLeavesAsync() =>
        Task.FromResult(state.State.ChildrenAreLeaves);

    public async Task<SplitResult?> AcceptSplitAsync(string promotedKey, GrainId newChild)
    {
        // U9p step 8c-c-iv-c2-vi: the interface is marked
        // [AlwaysInterleave] so concurrent overflow propagations from
        // different leaves do not queue on the activation turn. The
        // body itself is non-reentrant - it mutates state.State and
        // persists - so the per-activation _splitGate serialises every
        // execution. The gate's contention is bounded by the number of
        // concurrent overflows targeting THIS internal node (typically
        // 1-2 in flight under steady load); the surrounding interleave
        // lets all read traffic (Route*, GetRoutingTable*) overlap.
        await _splitGate.WaitAsync().ConfigureAwait(true);
        try
        {
            return await AcceptSplitCoreAsync(promotedKey, newChild);
        }
        finally
        {
            _splitGate.Release();
        }
    }

    private async Task<SplitResult?> AcceptSplitCoreAsync(string promotedKey, GrainId newChild)
    {
        SplitResult? pendingRecovery = null;

        // Recovery: if a previous split was interrupted, complete it first.
        if (state.State.SplitState == Primitives.SplitState.SplitInProgress)
        {
            // Snapshot the recovery-mutated fields BEFORE CompleteSplitAsync
            // runs so that a failing WriteStateAsync below can revert the
            // activation to the pre-recovery state. Without this revert, the
            // recovery branch guard above (state.State.SplitState ==
            // SplitInProgress) would short-circuit every retry from this
            // activation - in-memory SplitState would be SplitComplete and
            // SplitRightChildren would be null, so the caller never
            // receives the prior split's pending SplitResult and the
            // structural promotion chain is silently dropped until the
            // activation is recycled. This is the Class B
            // "persisted / in-memory divergence on write failure"
            // anti-pattern, made high-priority by the guard short-circuit.
            var splitStateSnapshot = state.State.SplitState;
            var splitRightChildrenSnapshot = state.State.SplitRightChildren;

            pendingRecovery = await CompleteSplitAsync();
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                state.State.SplitState = splitStateSnapshot;
                state.State.SplitRightChildren = splitRightChildrenSnapshot;
                throw;
            }

            // Route the caller's promotion to the correct node after recovery.
            if (string.Compare(promotedKey, state.State.SplitKey!, StringComparison.Ordinal) >= 0)
            {
                // The promotion belongs to the new sibling - forward it there.
                var sibling = grainFactory.GetGrain<IBPlusInternalGrain>(state.State.SplitSiblingId!.Value);
                await sibling.AcceptSplitAsync(promotedKey, newChild);
                return pendingRecovery;
            }
            // Otherwise fall through to insert the promotion in THIS node.
        }

        // Idempotency check: if this separator+child pair already exists, this is a
        // duplicate delivery (e.g. crash recovery re-emit). Skip the insert.
        for (int i = 0; i < state.State.Children.Count; i++)
        {
            if (state.State.Children[i].SeparatorKey == promotedKey &&
                state.State.Children[i].ChildId == newChild)
            {
                return pendingRecovery;
            }
        }

        // Snapshot mutated fields BEFORE any in-memory change so that a
        // failing WriteStateAsync below can revert the activation to the
        // state every peer (and any future reactivation) observes from
        // storage. Without this revert, a transient storage failure leaves
        // this activation routing as though the split landed while every
        // peer routes against the unmodified topology - the Class B
        // "persisted / in-memory divergence on write failure" anti-pattern.
        var clockSnapshot = state.State.Clock;
        var childrenSnapshot = new List<ChildEntry>(state.State.Children);

        state.State.Clock = HybridLogicalClock.Tick(state.State.Clock);

        // Insert the new child at the correct sorted position.
        var entry = new ChildEntry { SeparatorKey = promotedKey, ChildId = newChild };
        int insertIndex = 1; // skip the leftmost null-separator child
        for (; insertIndex < state.State.Children.Count; insertIndex++)
        {
            if (string.Compare(promotedKey, state.State.Children[insertIndex].SeparatorKey, StringComparison.Ordinal) < 0)
                break;
        }
        state.State.Children.Insert(insertIndex, entry);

        SplitResult? splitResult = null;
        var splitRan = false;
        if (state.State.Children.Count > (await GetOptionsAsync()).MaxInternalChildren)
        {
            splitResult = await SplitAsync();
            splitRan = true;
        }

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            // Only revert the AcceptSplitAsync-local Clock/Children mutations
            // when SplitAsync did NOT run. When SplitAsync did run, its own
            // WriteStateAsync (BPlusInternalGrain.SplitAsync line ~179) has
            // already persisted Clock and Children at the post-mutation
            // values; rewinding them here would introduce a fresh divergence
            // (in-memory < persisted) and break the SplitInProgress recovery
            // branch which expects the post-SplitAsync state to match. The
            // Split-branch recovery is already covered by that path - on the
            // next activation, persisted SplitState=SplitInProgress drives
            // CompleteSplitAsync (idempotent) and the in-memory mutations
            // CompleteSplitAsync made (SplitState=SplitComplete,
            // SplitRightChildren=null) are reproduced.
            if (!splitRan)
            {
                state.State.Clock = clockSnapshot;
                state.State.Children = childrenSnapshot;
            }
            throw;
        }

        // The new child has just been grafted under this node, so its
        // parent slot must point at us. Idempotent - if the child was
        // already pointing here (e.g. crash recovery), this is a no-op
        // aside from a refresher publish.
        await SeedChildParentAsync(newChild, state.State.ChildrenAreLeaves);

        return pendingRecovery ?? splitResult;
    }

    public async Task SetTreeIdAsync(string treeId)
    {
        if (state.State.TreeId is not null) return;

        // Snapshot the pre-mutation TreeId. The idempotency guard above
        // means a failing WriteStateAsync that leaks the mutated TreeId in
        // memory would short-circuit every retry from the same activation,
        // permanently diverging in-memory from storage - the Class B
        // "persisted / in-memory divergence on write failure" anti-pattern.
        var treeIdSnapshot = state.State.TreeId;
        state.State.TreeId = treeId;

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.TreeId = treeIdSnapshot;
            throw;
        }
    }

    private async Task<SplitResult> SplitAsync()
    {
        // Phase 1: Persist the split intent before any cross-grain calls.
        int mid = state.State.Children.Count / 2;
        var promotedKey = state.State.Children[mid].SeparatorKey!;

        // Right half becomes a new internal node.
        var rightChildren = state.State.Children.GetRange(mid, state.State.Children.Count - mid);
        // The first entry in the right node becomes the new leftmost (null separator).
        rightChildren[0] = new ChildEntry { SeparatorKey = null, ChildId = rightChildren[0].ChildId };

        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitInProgress);
        state.State.SplitKey = promotedKey;
        state.State.SplitSiblingId = grainFactory.GetGrain<IBPlusInternalGrain>(Guid.NewGuid()).GetGrainId();
        state.State.SplitRightChildren = rightChildren;

        // Trim our children to the left half.
        state.State.Children.RemoveRange(mid, state.State.Children.Count - mid);
        await state.WriteStateAsync();

        // Phase 2: Execute cross-grain operations using the persisted identity.
        return await CompleteSplitAsync();
    }

    /// <summary>
    /// Completes (or resumes) a split whose intent has already been persisted.
    /// Safe to call multiple times - <see cref="IBPlusInternalGrain.InitializeAsync"/>
    /// overwrites the new sibling's state, and <see cref="IBPlusInternalGrain.AcceptSplitAsync"/>
    /// has its own idempotency guard.
    /// </summary>
    private async Task<SplitResult> CompleteSplitAsync()
    {
        var promotedKey = state.State.SplitKey!;
        var siblingId = state.State.SplitSiblingId!.Value;
        var rightChildren = state.State.SplitRightChildren!;

        var newInternal = grainFactory.GetGrain<IBPlusInternalGrain>(siblingId);
        await newInternal.SetTreeIdAsync(state.State.TreeId!);

        if (rightChildren.Count >= 2)
        {
            // Initialize with the leftmost and first real separator.
            await newInternal.InitializeAsync(
                rightChildren[1].SeparatorKey!,
                rightChildren[0].ChildId,
                rightChildren[1].ChildId,
                state.State.ChildrenAreLeaves);

            // Accept remaining children (idempotent per AcceptSplitAsync guard).
            for (int i = 2; i < rightChildren.Count; i++)
            {
                await newInternal.AcceptSplitAsync(
                    rightChildren[i].SeparatorKey!,
                    rightChildren[i].ChildId);
            }
        }

        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitComplete);
        state.State.SplitRightChildren = null;

        return new SplitResult
        {
            PromotedKey = promotedKey,
            NewSiblingId = siblingId,
            ChildIsLeaf = false,
        };
    }

    public Task<List<GrainId>> GetChildIdsAsync() =>
        Task.FromResult(state.State.Children.Select(c => c.ChildId).ToList());

    public async Task ClearGrainStateAsync()
    {
        await state.ClearStateAsync();
        context.Deactivate(new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "Tree purged"));
    }
}
