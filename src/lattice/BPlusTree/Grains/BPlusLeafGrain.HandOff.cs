using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;
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
    /// <remarks>
    /// Returns the row as this open wrote it, with its ETag, or <see langword="null"/> when it
    /// wrote nothing (the token was already open, or the gate is closed) (#5837, E1).
    /// </remarks>
    private async Task<Orleans.GrainState<LeafHandOffState>?> OpenSplitHandOffAsync(GrainId siblingId)
    {
        if (!IsHandOffGateOpen() || HandOffMarkerStorage() is not { } storage)
        {
            return null;
        }

        var token = SplitHandOffToken(context.GrainId.GetGuidKey());
        var opened = false;
        var written = await HandOffMarkerRows.UpdateLeafRowAsync(storage, siblingId.GetGuidKey(), marker =>
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

        return opened ? written : null;
    }

    /// <summary>
    /// The sibling's marker row as this activation's own open wrote it in the split in
    /// flight, kept only once that split's intent persisted (#5837, E1, E4). The donor's
    /// close writes from it on its ETag, with no read; it is used once and cleared.
    /// </summary>
    private (GrainId Sibling, Orleans.GrainState<LeafHandOffState> Row)? _siblingMarkerRow;

    /// <summary>
    /// The routing generation this activation's span was established under at its split
    /// birth (#5837, T1), or unset: activation memory only, so a later activation appends
    /// its transfers unstamped.
    /// </summary>
    private WalRoutingStamp _spanRouting;

    /// <summary>
    /// The routing a split donor's transfer of <paramref name="count"/> records into this
    /// leaf is appended under (#5837, T1 and T3): the span's generation, when the call is
    /// marked as a transfer, the gate is in pre-arm, and every key is in this leaf's span
    /// now - checked in the synchronous step that dispatches the append. Otherwise unset,
    /// and the append goes as it always has.
    /// </summary>
    private WalRoutingStamp SplitTransferRoutingFor(WalRecord[] records, int count)
    {
        if (!_spanRouting.IsSet
            || BulkSeedScope.Value is not null
            || !SplitTransferScope.IsTransfer
            || !IsPreArmGateOpen())
        {
            return default;
        }

        var low = state.State.LowKeyInclusive;
        var high = state.State.HighKeyExclusive;
        for (var i = 0; i < count; i++)
        {
            var key = records[i].Key;
            if (key is null
                || (low is not null && string.CompareOrdinal(key, low) < 0)
                || (high is not null && string.CompareOrdinal(key, high) >= 0))
            {
                return default;
            }
        }

        return _spanRouting;
    }

    /// <summary>
    /// Appends a split transfer whose routed append was refused stale (#5851, a5e2e2ef's
    /// ruling 1): refreshes the span's generation (<see cref="TryRefreshSpanRoutingAsync"/>)
    /// and retries routed once, re-deciding the transfer class after the refresh. Only a
    /// refresh that fails, or a retry refused again, appends the transfer unstamped and
    /// clears the span's routing for the activation.
    /// </summary>
    private async Task<IReadOnlyList<long>> AppendTransferAfterStaleAsync(
        ICommitLogWriter writer, ArraySegment<WalRecord> segment, WalRecord[] records, int count, WalRoutingGenerationStaleException stale)
    {
        if (await TryRefreshSpanRoutingAsync(stale, records, count) && SplitTransferRoutingFor(records, count) is { IsSet: true } refreshed)
        {
            try
            {
                return await AppendManyRoutedAsync(writer, segment, refreshed);
            }
            catch (WalRoutingGenerationStaleException)
            {
            }
        }

        _spanRouting = default;
        return await writer.AppendManyAsync(segment);
    }

    /// <summary>
    /// Re-reads the shard's routing generation for this leaf's span after a stale
    /// refusal, as an admission would establish it (#5851, ruling 1): the generation is
    /// read fresh from the shard's marker, never from a cache, and is taken only when it
    /// is above both the refused stamp and the WAL's refusal and no shard hand-off over the
    /// span is open. Then descent, read after the generation, must route both the lowest
    /// and the highest key of the transfer to this leaf - or, before its link, both to the
    /// donor whose split created it (a5e2e2ef's F1): a stale activation whose upper keys
    /// a later split moved away fails the check, rather than appending a moved key under a
    /// generation its new owner's stale high does not count. Any later move of the keys
    /// raises the generation again, so the retry is refused.
    /// </summary>
    private async Task<bool> TryRefreshSpanRoutingAsync(WalRoutingGenerationStaleException stale, WalRecord[] records, int count)
    {
        if (!_spanRouting.IsSet
            || count <= 0
            || state.State.TreeId is not { } treeId
            || state.State.ShardIndex is not { } shardIndex
            || shardIndex != _spanRouting.Initiator.Index)
        {
            return false;
        }

        string? lowest = null;
        string? highest = null;
        for (var i = 0; i < count; i++)
        {
            if (records[i].Key is not { } key)
            {
                return false;
            }

            if (lowest is null || string.CompareOrdinal(key, lowest) < 0)
            {
                lowest = key;
            }

            if (highest is null || string.CompareOrdinal(key, highest) > 0)
            {
                highest = key;
            }
        }

        var shardRoot = LiveAdvanceShardRoot(treeId, shardIndex);
        var view = await shardRoot.GetLiveAdvanceViewAsync(context.GrainId, state.State.LowKeyInclusive, state.State.HighKeyExclusive);
        if (view.HeldOpen || view.RoutingGeneration <= Math.Max(_spanRouting.Generation, stale.Refusal - 1))
        {
            return false;
        }

        if (!await RoutesBothAsync(shardRoot, context.GrainId, lowest, highest)
            && !(state.State.PrevSibling is { } donor && await RoutesBothAsync(shardRoot, donor, lowest, highest)))
        {
            return false;
        }

        _spanRouting = WalRoutingStamp.ForShardRoot(shardIndex, view.RoutingGeneration);
        return true;
    }

    /// <summary>Whether descent routes both <paramref name="lowest"/> and <paramref name="highest"/> to <paramref name="leaf"/>.</summary>
    private static async Task<bool> RoutesBothAsync(IShardRootGrain shardRoot, GrainId leaf, string? lowest, string? highest) =>
        await shardRoot.IsReachableByDescentAsync(leaf, lowest)
        && (string.Equals(lowest, highest, StringComparison.Ordinal) || await shardRoot.IsReachableByDescentAsync(leaf, highest));

    /// <summary>The routing generation a split's sibling is told its span is established under (#5837, T1).</summary>
    private (int Shard, long Generation) SpanRoutingForSibling() =>
        IsPreArmGateOpen() && _appendFence is { } fence && fence.TryGetAdmittedRouting(out var shard, out var generation)
            ? (shard, generation)
            : (0, 0);

    /// <summary>
    /// Arms this activation's split window over the moved range <c>[splitKey, high)</c>
    /// before the split's heads are read (#5837, rule D). Only under the pre-arm gate,
    /// where the donor writes the sibling's close itself; otherwise a no-op.
    /// </summary>
    private void ArmSplitCloseWindow(string splitKey)
    {
        if (IsPreArmGateOpen() && _appendFence is { } fence && _options is { } options)
        {
            fence.ArmSplitWindow(splitKey, state.State.HighKeyExclusive, Math.Max(1, options.WalPartitions));
        }
    }

    /// <summary>
    /// The donor's own close of its sibling's birth window under the pre-arm gate (#5837,
    /// rules D, R, A and S). Once the narrow is durable it waits for every append it
    /// started before, fences this leaf's duplicate activations (#5841, rule A), and then,
    /// in one marker write, closes the window with its deferred bound: this leaf, this
    /// activation, and the routing generation the donor's writes were admitted under.
    /// <list type="bullet">
    /// <item>The split's own close (rule D) closes at one above its highest own moved-key
    /// record since <paramref name="splitHeads"/>, or at the heads when it wrote none; an
    /// append touching the window that failed without an offset closes at the allocator
    /// heads read after the wait instead.</item>
    /// <item>A resumed close - a recovered split, or the link acknowledgement's re-drive -
    /// has no window, so it closes at one above the highest offset any activation of this
    /// leaf allocated, never below the partition's restart or eviction floor (rule R), or at
    /// the allocator heads from a partition that cannot say. Its generation is the
    /// shard's current one, read now, when that is higher than any this activation saw.</item>
    /// </list>
    /// Returns <see langword="false"/>, leaving the window open, when no routing generation
    /// can be established.
    /// </summary>
    private async Task<bool> TryCloseWithDeferredBoundAsync(
        LeafAppendEpochFence fence,
        IGrainStorage storage,
        Guid siblingKey,
        string token,
        string treeId,
        int partitions,
        long[]? splitHeads,
        Orleans.GrainState<LeafHandOffState>? openedRow = null)
    {
        try
        {
            var windowed = splitHeads is not null
                && splitHeads.Length == partitions
                && fence.TryGetSplitWindow(out _, out _, out _, out _);
            await fence.DrainAsync();

            long[] movedOwnHigh = [];
            int shardIndex;
            long generation;
            var ambiguous = false;
            if (windowed)
            {
                windowed = fence.TryGetSplitWindow(out movedOwnHigh, out shardIndex, out generation, out ambiguous)
                    && movedOwnHigh.Length == partitions;
            }
            else
            {
                fence.TryGetAdmittedRouting(out shardIndex, out generation);
            }

            if ((!windowed || shardIndex < 0 || generation <= 0) && state.State.ShardIndex is { } currentShard)
            {
                // Rule R, and a split whose writes carried no generation: the shard's current
                // generation bounds every write admitted to it so far.
                var view = await LiveAdvanceShardRoot(treeId, currentShard)
                    .GetLiveAdvanceViewAsync(context.GrainId, state.State.LowKeyInclusive, state.State.HighKeyExclusive);
                if (shardIndex != currentShard)
                {
                    shardIndex = currentShard;
                    generation = 0;
                }

                generation = Math.Max(generation, view.RoutingGeneration);
            }

            if (shardIndex < 0 || generation <= 0)
            {
                return false;
            }

            await fence.FenceSplitAsync(
                partition => grainFactory.GetGrain<IWalShardGrain>($"{treeId}/{partition}"),
                partitions,
                CancellationToken.None);

            long[] heads;
            if (windowed && !ambiguous)
            {
                heads = new long[partitions];
                for (var p = 0; p < partitions; p++)
                {
                    heads[p] = Math.Max(splitHeads![p], movedOwnHigh[p] + 1);
                }
            }
            else if (windowed)
            {
                heads = await CaptureAllocatorHeadsAsync(grainFactory, treeId, partitions);
            }
            else
            {
                heads = await ResumedCloseHeadsAsync(treeId, partitions);
            }

            var donor = context.GrainId.GetGuidKey();
            var closed = false;
            Func<LeafHandOffState, bool> close = m =>
                closed = HandOffMarkerCore.CloseWithDeferredBound(m, token, heads, donor, fence.ActivationToken, shardIndex, generation);
            if (openedRow is not null)
            {
                // E1-E3: from the row this activation's own open wrote, on its ETag; any
                // miss re-reads, and a token no longer open is already settled.
                await HandOffMarkerRows.UpdateLeafFromAsync(storage, siblingKey, openedRow, close);
            }
            else
            {
                await HandOffMarkerRows.UpdateLeafAsync(storage, siblingKey, close);
            }
            if (closed)
            {
                LatticeMetrics.RecordHandOffMarker(LatticeMetrics.HandOffKindSplit, opened: false, treeId);
            }

            return true;
        }
        finally
        {
            fence.DisarmSplitWindow();
        }
    }

    /// <summary>
    /// A resumed close's heads (#5837, rule R): per partition, one above the highest offset
    /// any activation of this leaf allocated there - its forwards included - never below
    /// the partition's unattributed floor; the allocator heads when the partitions cannot
    /// say, a silo on an earlier build among them.
    /// </summary>
    private async Task<long[]> ResumedCloseHeadsAsync(string treeId, int partitions)
    {
        try
        {
            // No activation token: every activation's records are foreign to it.
            var everyActivation = new WalAppendStamp(context.GrainId.GetGuidKey(), 0, WalRoutingInitiator.None, 0);
            var reads = new Task<WalSplitCut>[partitions];
            for (var p = 0; p < partitions; p++)
            {
                reads[p] = grainFactory.GetGrain<IWalShardGrain>($"{treeId}/{p}").GetSplitCutAsync(everyActivation, CancellationToken.None);
            }

            var cut = await Task.WhenAll(reads);
            var heads = new long[partitions];
            for (var p = 0; p < partitions; p++)
            {
                heads[p] = Math.Max(0, cut[p].ForeignHigh + 1);
            }

            return heads;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ResolveLogger()?.LogDebug(
                ex,
                "Leaf {LeafId}: could not read its resumed split close's bounds; it closes at the allocator heads (#5837).",
                context.GrainId);
            return await CaptureAllocatorHeadsAsync(grainFactory, treeId, partitions);
        }
    }

    /// <summary>
    /// Closes this donor's split marker on <paramref name="siblingId"/> by fence, if it is
    /// open. Called once the donor's span has narrowed; idempotent. A failure is logged
    /// and leaves the marker open; the split-link acknowledgement re-drives it. Returns
    /// whether the window is settled: closed, or never open.
    /// </summary>
    private async Task<bool> CloseSplitHandOffAsync(GrainId siblingId, long[]? splitHeads = null)
    {
        if (HandOffMarkerStorage() is not { } storage || string.IsNullOrEmpty(state.State.TreeId))
        {
            return true;
        }

        var treeId = state.State.TreeId;
        var siblingKey = siblingId.GetGuidKey();
        var token = SplitHandOffToken(context.GrainId.GetGuidKey());

        // #5837, E1: the open's own row only for the split this activation began, never on a
        // recovery or a re-drive; taken once.
        var openedRow = splitHeads is not null && _siblingMarkerRow is { } kept && kept.Sibling == siblingId ? kept.Row : null;
        _siblingMarkerRow = null;
        try
        {
            if (openedRow is null)
            {
                var marker = await HandOffMarkerRows.ReadLeafAsync(storage, siblingKey);
                if (!marker.State.Open.ContainsKey(token))
                {
                    return true;
                }
            }

            var partitions = await optionsResolver.GetWalPartitionsAsync(treeId);
            if (IsPreArmGateOpen())
            {
                // #5837, rules S and R: this donor closes its sibling's window itself, with
                // its deferred bound; the shard root's link only raises its generation (C6).
                // This activation counts the close, so a write it admitted before its
                // narrowing is self-fenced. The fence is created lazily with the writer, so a
                // resumed activation resolves it first.
                _ = ResolveCommitLogWriter();
                if (_appendFence is not { } donorFence)
                {
                    return false;
                }

                donorFence.CountSplitClose();
                return await TryCloseWithDeferredBoundAsync(donorFence, storage, siblingKey, token, treeId, partitions, splitHeads, openedRow);
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

            return true;
        }
        catch (LeafRetiredException superseded) when (superseded.ActivationSuperseded)
        {
            // Superseded mid-close: the live activation re-drives the close on the
            // split-link acknowledgement.
            ResolveLogger()?.LogDebug(
                "Leaf {LeafId}: superseded while closing its split marker on {SiblingId} (epic #5605).",
                context.GrainId,
                siblingId);
            return false;
        }
        catch (Exception ex)
        {
            ResolveLogger()?.LogWarning(
                ex,
                "Leaf {LeafId}: could not close its split marker on sibling {SiblingId}; the sibling stays on the replay-based advance until the split-link acknowledgement re-drives the close (epic #5605).",
                context.GrainId,
                siblingId);
            return false;
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
