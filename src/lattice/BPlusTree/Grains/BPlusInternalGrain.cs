using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Internal (non-leaf) node grain implementation. Stores separator keys and
/// child references. Splits when the child count exceeds <see cref="BPlusTreeOptions.MaxInternalChildren"/>.
/// </summary>
public sealed class BPlusInternalGrain(
    [PersistentState("internal", "bplustree")] IPersistentState<InternalNodeState> state,
    IGrainFactory grainFactory) : Grain, IBPlusInternalGrain
{
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
        if (state.State.Children.Count > BPlusTreeOptions.MaxInternalChildren)
        {
            splitResult = await SplitAsync();
        }

        await state.WriteStateAsync();
        return splitResult;
    }

    private Task<SplitResult> SplitAsync()
    {
        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitInProgress);

        int mid = state.State.Children.Count / 2;
        var promotedKey = state.State.Children[mid].SeparatorKey!;

        // Right half becomes a new internal node.
        var rightChildren = state.State.Children.Skip(mid).ToList();
        // The first entry in the right node becomes the new leftmost (null separator).
        rightChildren[0] = new ChildEntry { SeparatorKey = null, ChildId = rightChildren[0].ChildId };

        var newInternal = grainFactory.GetGrain<IBPlusInternalGrain>(Guid.NewGuid());

        // We can't call InitializeAsync directly since it only takes two children.
        // Instead, we'll initialize then accept remaining splits.
        // For now, we batch by initializing with the first two, then accepting the rest.
        // Actually, let's just set up the new node fully by initializing with
        // leftmost + first separator, then accepting the rest.

        // Trim our children to the left half.
        state.State.Children = state.State.Children.Take(mid).ToList();
        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitComplete);

        // Initialize the new node. We handle this by having the shard root do it
        // through the returned SplitResult. The new internal node state will be
        // populated by the shard root grain.
        // For simplicity, we store the right children on the split result isn't possible
        // with the current interface. So let's initialize the new node inline.

        return InitializeNewInternalAsync(newInternal, rightChildren, promotedKey);
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
