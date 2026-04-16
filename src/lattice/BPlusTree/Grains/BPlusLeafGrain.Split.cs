using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Leaf-node split logic: two-phase crash-safe split with deterministic sibling identity.
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    private async Task<SplitResult> SplitAsync()
    {
        var keys = state.State.Entries.Keys.ToList();
        int mid = keys.Count / 2;
        var splitKey = keys[mid];

        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitInProgress);
        state.State.SplitKey = splitKey;
        state.State.SplitSiblingId = grainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid()).GetGrainId();
        state.State.OldNextSibling = state.State.NextSibling;
        state.State.NextSibling = state.State.SplitSiblingId;
        await state.WriteStateAsync();

        return await CompleteSplitAsync();
    }

    /// <summary>
    /// Completes (or resumes) a split whose intent has already been persisted.
    /// Safe to call multiple times — MergeEntriesAsync is idempotent (LWW merge).
    /// </summary>
    private async Task<SplitResult> CompleteSplitAsync()
    {
        var splitKey = state.State.SplitKey!;
        var siblingId = state.State.SplitSiblingId!.Value;
        var newLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(siblingId);

        await newLeaf.SetTreeIdAsync(state.State.TreeId!);

        var rightEntries = new Dictionary<string, LwwValue<byte[]>>();
        foreach (var (key, lww) in state.State.Entries)
        {
            if (string.Compare(key, splitKey, StringComparison.Ordinal) >= 0)
            {
                rightEntries[key] = lww;
            }
        }

        if (rightEntries.Count > 0)
        {
            await newLeaf.MergeEntriesAsync(rightEntries);
        }

        var oldNextId = state.State.OldNextSibling;

        await newLeaf.SetNextSiblingAsync(oldNextId);
        await newLeaf.SetPrevSiblingAsync(context.GrainId);

        if (oldNextId is not null)
        {
            var oldNext = grainFactory.GetGrain<IBPlusLeafGrain>(oldNextId.Value);
            await oldNext.SetPrevSiblingAsync(siblingId);
        }

        foreach (var key in rightEntries.Keys)
        {
            state.State.Entries.Remove(key);
        }

        state.State.OldNextSibling = null;
        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitComplete);

        return new SplitResult
        {
            PromotedKey = splitKey,
            NewSiblingId = siblingId
        };
    }
}
