using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Internal (non-leaf) node grain implementation. Stores separator keys and
/// child references. Splits when the child count exceeds <see cref="LatticeOptions.MaxInternalChildren"/>.
/// </summary>
internal sealed class BPlusInternalGrain(
    [PersistentState("internal", "bplustree")] IPersistentState<InternalNodeState> state,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor) : IBPlusInternalGrain
{
    private LatticeOptions Options => optionsMonitor.Get(state.State.TreeId ?? string.Empty);
    public async Task InitializeAsync(string separatorKey, GrainId leftChild, GrainId rightChild, bool childrenAreLeaves)
    {
        state.State.Children =
        [
            new ChildEntry { SeparatorKey = null, ChildId = leftChild },
            new ChildEntry { SeparatorKey = separatorKey, ChildId = rightChild }
        ];
        state.State.ChildrenAreLeaves = childrenAreLeaves;
        state.State.Clock = HybridLogicalClock.Tick(state.State.Clock);
        await state.WriteStateAsync();
    }

    public Task<GrainId> RouteAsync(string key) =>
        Task.FromResult(state.State.Route(key));

    public Task<bool> AreChildrenLeavesAsync() =>
        Task.FromResult(state.State.ChildrenAreLeaves);

    public async Task<SplitResult?> AcceptSplitAsync(string promotedKey, GrainId newChild)
    {
        SplitResult? pendingRecovery = null;

        // Recovery: if a previous split was interrupted, complete it first.
        if (state.State.SplitState == Primitives.SplitState.SplitInProgress)
        {
            pendingRecovery = await CompleteSplitAsync();
            await state.WriteStateAsync();

            // Route the caller's promotion to the correct node after recovery.
            if (string.Compare(promotedKey, state.State.SplitKey!, StringComparison.Ordinal) >= 0)
            {
                // The promotion belongs to the new sibling — forward it there.
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
        if (state.State.Children.Count > Options.MaxInternalChildren)
        {
            splitResult = await SplitAsync();
        }

        await state.WriteStateAsync();
        return pendingRecovery ?? splitResult;
    }

    public async Task SetTreeIdAsync(string treeId)
    {
        if (state.State.TreeId is not null) return;
        state.State.TreeId = treeId;
        await state.WriteStateAsync();
    }

    private async Task<SplitResult> SplitAsync()
    {
        // Phase 1: Persist the split intent before any cross-grain calls.
        int mid = state.State.Children.Count / 2;
        var promotedKey = state.State.Children[mid].SeparatorKey!;

        // Right half becomes a new internal node.
        var rightChildren = state.State.Children.Skip(mid).ToList();
        // The first entry in the right node becomes the new leftmost (null separator).
        rightChildren[0] = new ChildEntry { SeparatorKey = null, ChildId = rightChildren[0].ChildId };

        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitInProgress);
        state.State.SplitKey = promotedKey;
        state.State.SplitSiblingId = grainFactory.GetGrain<IBPlusInternalGrain>(Guid.NewGuid()).GetGrainId();
        state.State.SplitRightChildren = rightChildren;

        // Trim our children to the left half.
        state.State.Children = state.State.Children.Take(mid).ToList();
        await state.WriteStateAsync();

        // Phase 2: Execute cross-grain operations using the persisted identity.
        return await CompleteSplitAsync();
    }

    /// <summary>
    /// Completes (or resumes) a split whose intent has already been persisted.
    /// Safe to call multiple times — <see cref="IBPlusInternalGrain.InitializeAsync"/>
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
            NewSiblingId = siblingId
        };
    }
}
