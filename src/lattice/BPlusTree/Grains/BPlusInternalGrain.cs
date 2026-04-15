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
    public async Task InitializeAsync(string separatorKey, GrainId leftChild, GrainId rightChild)
    {
        state.State.Children =
        [
            new ChildEntry { SeparatorKey = null, ChildId = leftChild },
            new ChildEntry { SeparatorKey = separatorKey, ChildId = rightChild }
        ];
        state.State.Clock = HybridLogicalClock.Tick(state.State.Clock);
        await state.WriteStateAsync();
    }

    public Task<GrainId> RouteAsync(string key) =>
        Task.FromResult(state.State.Route(key));

    public async Task<SplitResult?> AcceptSplitAsync(string promotedKey, GrainId newChild)
    {
        // Idempotency check: if this separator+child pair already exists, this is a
        // duplicate delivery (e.g. crash recovery re-emit). Skip the insert.
        for (int i = 0; i < state.State.Children.Count; i++)
        {
            if (state.State.Children[i].SeparatorKey == promotedKey &&
                state.State.Children[i].ChildId == newChild)
            {
                return null;
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
        return splitResult;
    }

    public async Task SetTreeIdAsync(string treeId)
    {
        if (state.State.TreeId is not null) return;
        state.State.TreeId = treeId;
        await state.WriteStateAsync();
    }

    private async Task<SplitResult> SplitAsync()
    {
        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitInProgress);

        int mid = state.State.Children.Count / 2;
        var promotedKey = state.State.Children[mid].SeparatorKey!;

        // Right half becomes a new internal node.
        var rightChildren = state.State.Children.Skip(mid).ToList();
        // The first entry in the right node becomes the new leftmost (null separator).
        rightChildren[0] = new ChildEntry { SeparatorKey = null, ChildId = rightChildren[0].ChildId };

        var newInternal = grainFactory.GetGrain<IBPlusInternalGrain>(Guid.NewGuid());
        await newInternal.SetTreeIdAsync(state.State.TreeId!);

        // Trim our children to the left half.
        state.State.Children = state.State.Children.Take(mid).ToList();
        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitComplete);

        return await InitializeNewInternalAsync(newInternal, rightChildren, promotedKey);
    }

    private async Task<SplitResult> InitializeNewInternalAsync(
        IBPlusInternalGrain newInternal,
        List<ChildEntry> rightChildren,
        string promotedKey)
    {
        if (rightChildren.Count >= 2)
        {
            // Initialize with the leftmost and first real separator.
            await newInternal.InitializeAsync(
                rightChildren[1].SeparatorKey!,
                rightChildren[0].ChildId,
                rightChildren[1].ChildId);

            // Accept remaining children.
            for (int i = 2; i < rightChildren.Count; i++)
            {
                await newInternal.AcceptSplitAsync(
                    rightChildren[i].SeparatorKey!,
                    rightChildren[i].ChildId);
            }
        }

        return new SplitResult
        {
            PromotedKey = promotedKey,
            NewSiblingId = newInternal.GetGrainId()
        };
    }
}
