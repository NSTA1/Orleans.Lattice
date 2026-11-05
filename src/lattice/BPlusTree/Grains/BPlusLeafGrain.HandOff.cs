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
/// <b>Close by fence.</b> Once the donor's narrowed span is durable (admission of moved
/// keys stops), it raises the leaf's epoch row and claims the raised epoch at every
/// partition (<see cref="LeafAppendEpochFence.FenceSplitAsync"/>, #5841), and closes the
/// sibling's marker with the claim heads. Every append any activation of the donor
/// stamped before the raise was allocated below the heads or is rejected; this
/// activation's own are self-fenced and re-admitted by their caller, whose re-admission
/// forwards a moved key to the sibling. In pre-arm mode the shard root runs the same
/// fence when it links the split, before its deferred close.
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

    /// <summary>
    /// The routing a stamped leaf method appends under (member 2): the shard-level
    /// generation it was called with, or <see cref="WalRoutingStamp.None"/>, which a
    /// leaf-to-leaf hand-off passes for a write the receiver appends and applies as its
    /// own (the design owner's answer Q2). A stamped method is called only by new code;
    /// a write reaching the leaf through an existing method appends unstamped (S3).
    /// Evaluated synchronously at the method's entry, before its span admission, it
    /// captures this activation's hand-off closes (#5664 (a)); a value carried from the
    /// caller is replaced. The fence is created with the commit-log writer, so it is
    /// resolved here first: a write admitted before this activation's first append is
    /// marked too, and a close made before that write dispatches still refuses it.
    /// </summary>
    private WalRoutingStamp StampedRouting(WalRoutingStamp routing)
    {
        ResolveCommitLogWriter();
        return routing with { AdmissionMark = _appendFence is { } fence ? fence.HandOffCloses + 1 : 0 };
    }

    /// <summary>
    /// Appends <paramref name="entry"/> under <paramref name="routing"/>: the leaf's
    /// epoch alone for <see cref="WalRoutingStamp.None"/>, extended by the routing
    /// generation when one is set, or unstamped for <see cref="WalRoutingStamp.Unstamped"/>.
    /// </summary>
    private static Task<long> AppendRoutedAsync(ICommitLogWriter writer, WalRecord entry, WalRoutingStamp routing) =>
        routing != default && writer is IRoutingStampedCommitLogWriter routed
            ? routed.AppendAsync(entry, routing, CancellationToken.None)
            : writer.AppendAsync(entry);

    /// <summary>The batched form of <see cref="AppendRoutedAsync"/>.</summary>
    private static Task<IReadOnlyList<long>> AppendManyRoutedAsync(ICommitLogWriter writer, IReadOnlyList<WalRecord> entries, WalRoutingStamp routing) =>
        routing != default && writer is IRoutingStampedCommitLogWriter routed
            ? routed.AppendManyAsync(entries, routing, CancellationToken.None)
            : writer.AppendManyAsync(entries);

    /// <summary>A single-key set under <paramref name="routing"/>, as <see cref="SetAsync(string, byte[])"/> runs it.</summary>
    private async Task<SplitResult?> SetRoutedAsync(string key, byte[] value, WalRoutingStamp routing)
    {
        await AwaitReplayBarrierAsync();
        return await SetCoreAsync(key, value, 0L, routing);
    }

    /// <summary>The hand-off markers' storage, or <see langword="null"/> when the silo provides no fence.</summary>
    private IGrainStorage? HandOffMarkerStorage() =>
        context.ActivationServices?.GetService<IWalAppendFenceGate>() is not null
            ? WalAppendFenceStorage.Resolve(context.ActivationServices)
            : null;

    private bool IsHandOffGateOpen() =>
        context.ActivationServices?.GetService<IWalAppendFenceGate>() is { IsOpen: true };

    private bool IsPreArmGateOpen() =>
        context.ActivationServices?.GetService<IWalAppendFenceGate>() is { IsOpen: true, IsPreArmOpen: true };

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
            if (IsPreArmGateOpen())
            {
                // #5833, revision 2, C6, and #5841: the shard root fences this donor's
                // duplicates and closes the sibling's marker by a deferred fence when it
                // links this split, in its own turn. This activation only counts the close,
                // so a write it admitted before its narrowing is self-fenced.
                // RED PROOF (#5841 as filed): only an armed donor bumps.
                if (_appendFence is { } armed && armed.HasClaimedAnywhere)
                {
                    await armed.FenceSplitAsync(
                        partition => grainFactory.GetGrain<IWalShardGrain>($"{treeId}/{partition}"),
                        partitions,
                        CancellationToken.None);
                }
                else
                {
                    _appendFence?.CountSplitClose();
                }

                return;
            }

            // #5841, rule A: the narrowed state is durable, so fence every activation of
            // this leaf that claimed before it, whether or not this one did.
            var heads = _appendFence is { } fence
                ? await fence.FenceSplitAsync(
                    partition => grainFactory.GetGrain<IWalShardGrain>($"{treeId}/{partition}"),
                    partitions,
                    CancellationToken.None)
                : await LeafSplitDuplicateFence.FenceAsync(
                    storage,
                    partition => grainFactory.GetGrain<IWalShardGrain>($"{treeId}/{partition}"),
                    partitions,
                    context.GrainId.GetGuidKey(),
                    onRaised: null,
                    onClaimed: null,
                    CancellationToken.None);

            // No activation ever claimed: every append was unstamped, which the watermark
            // reports; the allocator heads now bound everything allocated.
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
