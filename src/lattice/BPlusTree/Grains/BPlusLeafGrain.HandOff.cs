using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The leaf's side of the split hand-off fence (epic #5605, member 2; design note
/// section 2, the hand-off table's split row, and the design owner's answers Q1 and
/// Q2).
/// </summary>
/// <remarks>
/// <para>
/// <b>Open.</b> The donor opens <c>split:{donor}</c> on the sibling's marker, with
/// the sibling's marker initialised, immediately before the sibling's birth write, so
/// the sibling is held from its first moment. A resume of the split re-runs it, so a
/// split begun while the gate was closed opens its marker when it resumes with the gate
/// open (rule 1 of "Hand-offs in flight across the gate").
/// </para>
/// <para>
/// <b>Close by fence.</b> Once the donor has narrowed its span (admission of moved keys
/// stops), it raises its own epoch and re-claims it at every partition it had claimed
/// (<see cref="LeafAppendEpochFence.BumpAndReclaimAsync"/>), and closes the sibling's
/// marker with the claim heads. Every append the donor stamped before the bump was
/// allocated below the heads or is rejected, self-fenced, and re-admitted by its
/// caller, whose re-admission forwards a moved key to the sibling.
/// </para>
/// <para>
/// <b>Whoever appends stamps with its own epoch (Q2).</b> The split's transfers are
/// appended by the sibling, which also applies them, so they are its own appends; the
/// marker is needed only for the records the donor appended for the moved keys.
/// </para>
/// <para>
/// <b>Re-drive.</b> A failed close leaves the marker open, which holds the sibling on
/// the replay-based advance (latency, never data); the donor re-drives it when the
/// shard root acknowledges the split's link, which it retries until it lands. Nothing
/// here arms a leaf: member 2 is inert.
/// </para>
/// </remarks>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>The token a split from <paramref name="donorLeafId"/> opens on its sibling's marker.</summary>
    internal static string SplitHandOffToken(Guid donorLeafId) => string.Concat("split:", donorLeafId.ToString("N"));

    /// <summary>The hand-off markers' storage, or <see langword="null"/> when the silo provides no fence.</summary>
    private IGrainStorage? HandOffMarkerStorage() =>
        context.ActivationServices?.GetService<IWalAppendFenceGate>() is not null
            ? WalAppendFenceStorage.Resolve(context.ActivationServices)
            : null;

    private bool IsHandOffGateOpen() =>
        context.ActivationServices?.GetService<IWalAppendFenceGate>() is { IsOpen: true };

    /// <summary>
    /// Opens this donor's split marker on <paramref name="siblingId"/>, initialising it,
    /// before the sibling's birth write. A no-op while the gate is closed: the sibling is
    /// then born with no marker, which holds it until its state is initialised.
    /// </summary>
    private async Task OpenSplitHandOffAsync(GrainId siblingId)
    {
        if (!IsHandOffGateOpen() || HandOffMarkerStorage() is not { } storage)
        {
            return;
        }

        var token = SplitHandOffToken(context.GrainId.GetGuidKey());
        var opened = false;
        await HandOffMarkerRows.UpdateLeafAsync(storage, siblingId.GetGuidKey(), marker =>
        {
            var changed = !marker.Initialised;
            marker.Initialised = true;
            opened = HandOffMarkerCore.Open(marker, token, null, null);
            return changed | opened;
        });

        if (opened)
        {
            LatticeMetrics.RecordHandOffMarker(LatticeMetrics.HandOffKindSplit, opened: true, state.State.TreeId ?? string.Empty);
        }
    }

    /// <summary>
    /// Closes this donor's split marker on <paramref name="siblingId"/> by fence, if it is
    /// open. Called once the donor's span has narrowed; idempotent. A failure is logged
    /// and leaves the marker open; the split-link acknowledgement re-drives it.
    /// </summary>
    private async Task CloseSplitHandOffAsync(GrainId siblingId)
    {
        if (HandOffMarkerStorage() is not { } storage || string.IsNullOrEmpty(state.State.TreeId))
        {
            return;
        }

        var treeId = state.State.TreeId;
        var siblingKey = siblingId.GetGuidKey();
        var token = SplitHandOffToken(context.GrainId.GetGuidKey());
        try
        {
            var marker = await HandOffMarkerRows.ReadLeafAsync(storage, siblingKey);
            if (!marker.State.Open.ContainsKey(token))
            {
                return;
            }

            var partitions = await optionsResolver.GetWalPartitionsAsync(treeId);
            var heads = _appendFence is { } fence
                ? await fence.BumpAndReclaimAsync(
                    partition => grainFactory.GetGrain<IWalShardGrain>($"{treeId}/{partition}"),
                    partitions,
                    CancellationToken.None)
                : null;

            // An activation that stamped nothing appended unstamped, which the
            // watermark reports; the allocator heads now bound everything it allocated.
            heads ??= await CaptureAllocatorHeadsAsync(grainFactory, treeId, partitions);
            var closed = false;
            await HandOffMarkerRows.UpdateLeafAsync(storage, siblingKey, m => closed = HandOffMarkerCore.Close(m, token, heads));
            if (closed)
            {
                LatticeMetrics.RecordHandOffMarker(LatticeMetrics.HandOffKindSplit, opened: false, treeId);
            }
        }
        catch (LeafRetiredException superseded) when (superseded.ActivationSuperseded)
        {
            // Superseded mid-close: the live activation re-drives the close on the
            // split-link acknowledgement.
            ResolveLogger()?.LogDebug(
                "Leaf {LeafId}: superseded while closing its split marker on {SiblingId} (epic #5605).",
                context.GrainId,
                siblingId);
        }
        catch (Exception ex)
        {
            ResolveLogger()?.LogWarning(
                ex,
                "Leaf {LeafId}: could not close its split marker on sibling {SiblingId}; the sibling stays on the replay-based advance until the split-link acknowledgement re-drives the close (epic #5605).",
                context.GrainId,
                siblingId);
        }
    }

    /// <summary>Deletes this leaf's hand-off marker row with the leaf; best effort.</summary>
    private async Task ClearHandOffMarkerAsync()
    {
        if (HandOffMarkerStorage() is not { } storage)
        {
            return;
        }

        try
        {
            await HandOffMarkerRows.ClearLeafAsync(storage, context.GrainId.GetGuidKey());
        }
        catch (Exception ex)
        {
            ResolveLogger()?.LogWarning(
                ex,
                "Leaf {LeafId}: could not delete its hand-off marker row; a leftover marker only holds a re-created leaf on the replay-based advance (epic #5605).",
                context.GrainId);
        }
    }

    /// <summary>
    /// The allocator's next offset at every WAL partition of <paramref name="treeId"/>,
    /// read after the hand-off's admission stopped: every append allocated before the
    /// read lies below it.
    /// </summary>
    internal static async Task<long[]> CaptureAllocatorHeadsAsync(IGrainFactory grainFactory, string treeId, int partitions)
    {
        var reads = new Task<long>[partitions];
        for (var p = 0; p < partitions; p++)
        {
            reads[p] = grainFactory.GetGrain<IWalShardGrain>($"{treeId}/{p}").GetNextSequenceAsync(CancellationToken.None).AsTask();
        }

        return await Task.WhenAll(reads);
    }
}
