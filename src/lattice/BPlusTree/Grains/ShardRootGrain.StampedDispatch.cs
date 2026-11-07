using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The shard root's routing generation, stamped at admission (epic #5605, member 2;
/// design note section 2, "Stamped at admission", and section 5): every write the
/// shard root admits is captured with its routing generation in the same synchronous
/// step as its phase check (<c>ThrowIfRejectedForKey</c>), and reaches the leaf
/// through the leaf's stamped method, so a hand-off close's re-claim of the generation
/// fences every write admitted before it. A write rejected because its generation
/// was re-claimed is retried from here: the phase check re-runs, which re-routes a
/// slot that moved to another shard, and the write is re-admitted under the current
/// generation; it is never re-stamped at the leaf.
/// </summary>
/// <remarks>
/// <para>
/// <b>Cost.</b> One field read and one gate read per admission; the leaf dispatch
/// helpers use the pooling <see cref="ValueTask"/> builder, so a stamped dispatch adds
/// no allocation over the existing call. The generation is raised from 0 to 1 once per
/// shard root, durably, the first time it admits a write with the gate open; a
/// partition that has never seen it loads it when the leaf re-presents it.
/// </para>
/// <para>
/// With the gate closed, or before the generation is raised, the routing is
/// <see cref="WalRoutingStamp.Unstamped"/> and the existing leaf methods are called,
/// which append unstamped: always admitted and reported. A leaf hosted by an older
/// build answers a stamped method with the definite "method unknown" failure, and only
/// then is the existing method called instead.
/// </para>
/// </remarks>
internal sealed partial class ShardRootGrain
{
    private IWalAppendFenceGate? _routingFenceGate;
    private bool _routingFenceGateResolved;
    private Task? _routingGenerationEnsure;

    private bool RoutingFenceGateOpen()
    {
        if (!_routingFenceGateResolved)
        {
            _routingFenceGate = context.ActivationServices?.GetService<IWalAppendFenceGate>();
            _routingFenceGateResolved = true;
        }

        return _routingFenceGate is { IsOpen: true };
    }

    /// <summary>
    /// The routing a write admitted now carries: this shard root's current routing
    /// generation while the gate is open, otherwise <see cref="WalRoutingStamp.Unstamped"/>.
    /// Called in the same synchronous step as the phase check.
    /// </summary>
    internal WalRoutingStamp AdmissionRoutingStamp() =>
        _handOffMarker is { RoutingGeneration: > 0 } marker && RoutingFenceGateOpen()
            ? WalRoutingStamp.ForShardRoot(ShardIndex, marker.RoutingGeneration)
            : WalRoutingStamp.Unstamped;

    /// <summary>
    /// Ensures the routing generation is loaded and at least 1 while the gate is open;
    /// a completed task on every write after the first.
    /// </summary>
    private Task EnsureRoutingGenerationAsync() =>
        _handOffMarker is { RoutingGeneration: > 0 } || !RoutingFenceGateOpen()
            ? Task.CompletedTask
            : EnsureRoutingGenerationSingleFlightAsync();

    /// <summary>
    /// #5987: the writes that reach this shard root before its generation is seeded share
    /// one slow path, so a new tree's first touch reads the routing floor once per
    /// activation rather than once per concurrent write. A failed run is not cached: the
    /// next write runs it again.
    /// </summary>
    private Task EnsureRoutingGenerationSingleFlightAsync()
    {
        if (_routingGenerationEnsure is { } inFlight)
        {
            return inFlight;
        }

        var run = EnsureRoutingGenerationSlowAsync();
        if (!run.IsCompleted)
        {
            _routingGenerationEnsure = run;
            _ = ForgetRoutingGenerationEnsureAsync(run);
        }

        return run;
    }

    private async Task ForgetRoutingGenerationEnsureAsync(Task run)
    {
        try
        {
            await run;
        }
        catch (Exception)
        {
            // Observed here so the discarded task never surfaces an unobserved fault; every
            // caller awaits the run itself and sees its outcome (#5988 review, S1).
        }
        finally
        {
            if (ReferenceEquals(_routingGenerationEnsure, run))
            {
                _routingGenerationEnsure = null;
            }
        }
    }

    private async Task EnsureRoutingGenerationSlowAsync()
    {
        try
        {
            var marker = await LoadHandOffMarkerAsync();
            long? floor = null;
            if (marker.RoutingGeneration == 0)
            {
                // #5668: above this shard root's routing floor, so a re-created
                // incarnation never reuses a purged one's generation. Read after the
                // marker, never alongside it: the purge raises the floor before it
                // deletes the marker, so a marker read absent is followed by a floor
                // read that covers the purge (#5831 review, B1).
                floor = await ReadRoutingFloorAsync();
                var seeded = floor.Value + 1;

                // #5831, F4: with no purged incarnation to fence (floor 0), the seed and
                // rule 2's initialisation are one write, the rule evaluated on the state
                // the write is built from and proved current by its ETag condition. With
                // a floor above 0 they stay two writes, with the recreate fence between.
                var initialiseWithSeed = floor.Value == 0;
                marker = await UpdateHandOffMarkerAsync(m =>
                {
                    var changed = false;
                    if (m.RoutingGeneration == 0)
                    {
                        m.RoutingGeneration = seeded;
                        changed = true;
                    }

                    if (initialiseWithSeed && !m.Initialised && CanInitialiseHandOffMarker())
                    {
                        m.Initialised = true;
                        m.Sequence++;
                        changed = true;
                    }

                    return changed;
                });
            }

            if (!marker.Initialised)
            {
                // #5668: the first initialisation after a purge is a fence. The purge
                // retired every generation entry, and a straddling write of the purged
                // incarnation is refused at a partition only once a generation above its
                // stamp is claimed there, so every retired initiator claims its seeded
                // generation everywhere, and the heads are recorded, before the marker
                // reads initialised. A tree id never purged has floor 0 and claims nothing.
                floor ??= await ReadRoutingFloorAsync();
                if (floor.Value > 0)
                {
                    await FenceRecreatedIncarnationAsync(floor.Value, marker.RoutingGeneration);
                }
            }

            // Rule 2 ("absence never means closed"): the marker reads initialised
            // only once this shard root has no hand-off of its own in flight.
            if (!marker.Initialised)
            {
                await TryInitialiseHandOffMarkerAsync();
            }
        }
        catch (Exception ex)
        {
            // Fail safe: the shard root keeps admitting unstamped writes, and its marker
            // stays uninitialised, which holds its leaves (rule 2).
            logger.LogDebug(
                ex,
                "Shard root {TreeId}/{ShardIndex}: could not raise its routing generation; its writes stay unstamped (epic #5605).",
                TreeId,
                ShardIndex);
        }
    }

    /// <summary>This shard root's own routing floor (#5668): 0 for a tree id never purged.</summary>
    private Task<long> ReadRoutingFloorAsync() =>
        WalAppendFenceRows.ReadRoutingFloorAsync(HandOffStorage(), TreeId, WalRoutingInitiator.ShardRoot(ShardIndex));

    /// <summary>The close token the fence of the incarnation re-created above <paramref name="floor"/> records.</summary>
    internal static string RecreationFenceToken(long floor) =>
        string.Concat("recreate:", floor.ToString(System.Globalization.CultureInfo.InvariantCulture));

    /// <summary>
    /// The re-created incarnation's first-initialisation fence (#5668; design note
    /// section 2, "Purging a tree's fence rows"): drives the range-delete fence's
    /// first claim, claims this shard root's seeded <paramref name="generation"/> at
    /// every WAL partition, and records the higher of the two head vectors as a close
    /// over the shard's whole range. Idempotent: a re-claim of the same generation is
    /// accepted again, and a recorded close is not recorded twice.
    /// </summary>
    private async Task FenceRecreatedIncarnationAsync(long floor, long generation)
    {
        var fenceHeads = await grainFactory.GetGrain<ITreeRangeDeleteFenceGrain>(TreeId).EnsureInitialisedAsync();
        var ownHeads = await ClaimRoutingGenerationEverywhereAsync(generation);
        var heads = HandOffMarkerCore.MaxHeads(ownHeads, fenceHeads);
        await UpdateHandOffMarkerAsync(m => HandOffMarkerCore.Close(m, RecreationFenceToken(floor), heads));
    }

    /// <summary>
    /// Re-reads the marker after a stale-generation rejection, so a re-admission
    /// stamps the generation a close (possibly by another activation) raised.
    /// Returns whether the row was read and found absent.
    /// </summary>
    private async Task<bool> ReloadHandOffMarkerAsync()
    {
        try
        {
            var row = await HandOffMarkerRows.ReadShardAsync(HandOffStorage(), TreeId, ShardIndex);
            NoteHandOffMarkerRead(row);
            _handOffMarker = row.State;
            _handOffMarkerRow = row;
            return !row.RecordExists;
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Shard root {TreeId}/{ShardIndex}: could not reload its hand-off marker (epic #5605).", TreeId, ShardIndex);
            return false;
        }
    }

    /// <summary>
    /// The stale-generation retry step: waits out the seam's backoff (to the
    /// operation's deadline), reloads the marker, and returns the routing a
    /// re-admission carries. The caller re-runs its phase check between the reload
    /// and this capture.
    /// </summary>
    private async Task<WalFenceSeamRetry?> NextRoutingRetryAsync(WalFenceSeamRetry? retry, WalRoutingGenerationStaleException stale)
    {
        retry = await WalFenceSeamRetry.NextAsync(retry, WalFenceSeamRetry.CallerShardRoot, TreeId);
        await ThrowIfAdmittedUnderPurgedIncarnationAsync(stale);
        if (await ReloadHandOffMarkerAsync() && _handOffMarkerSeen && await IsRetiredByPurgeAsync())
        {
            // #5716 review, N1: the purge retired the marker after the floor check above.
            // Re-admitting would go unstamped, under a marker read absent.
            throw PurgedIncarnation();
        }

        await RaiseRoutingGenerationAboveRefusalAsync(stale);
        return retry;
    }

    /// <summary>
    /// #5705: a write admitted under a generation at or below its initiator's routing
    /// floor was admitted by an incarnation a purge has since retired. It is never
    /// re-stamped: neither raised above the floor's refusal (which would re-create the
    /// purged marker at floor + 1), nor re-admitted under a re-created incarnation's
    /// generation. Its retry surfaces the deleted-tree outcome. A live incarnation's
    /// generations are always above the floor, so this never refuses one of its writes.
    /// Reads the floor row on the stale-generation path only.
    /// </summary>
    private async Task ThrowIfAdmittedUnderPurgedIncarnationAsync(WalRoutingGenerationStaleException stale)
    {
        if (stale.Generation <= 0 || WalAppendFenceStorage.Resolve(context.ActivationServices) is not { } storage)
        {
            return;
        }

        var floor = await WalAppendFenceRows.ReadRoutingFloorAsync(storage, TreeId, stale.Initiator);
        if (stale.Generation <= floor)
        {
            throw PurgedIncarnation();
        }
    }

    /// <summary>
    /// The deleted-tree outcome a retired incarnation's operation surfaces (#5705): the
    /// existing exception and message, tagged in <see cref="Exception.Data"/> so a
    /// best-effort step on the retry path can tell it from a fault it may swallow.
    /// </summary>
    private static InvalidOperationException PurgedIncarnation()
    {
        var ex = new InvalidOperationException("This tree has been deleted and is no longer accessible.");
        ex.Data[PurgedIncarnationKey] = true;
        return ex;
    }

    private const string PurgedIncarnationKey = "lattice.purged-incarnation";

    /// <summary>Whether <paramref name="ex"/> is the purged-incarnation outcome (#5705), which is never swallowed.</summary>
    private static bool IsPurgedIncarnation(Exception ex) =>
        ex is InvalidOperationException && ex.Data.Contains(PurgedIncarnationKey);

    /// <summary>Runs one stale-generation retry step, as a write's seam does; test seam (#5705).</summary>
    internal Task RetryStaleRoutingForTestingAsync(WalRoutingGenerationStaleException stale) =>
        NextRoutingRetryAsync(null, stale);

    /// <summary>Loads this activation's marker as its first admission does; test seam (#5705).</summary>
    internal Task<State.ShardHandOffState> LoadHandOffMarkerForTestingAsync() => LoadHandOffMarkerAsync();

    /// <summary>
    /// #5663: a refusal above this shard root's stored generation means the
    /// marker is behind a partition's durable row (the marker row lost, reset or
    /// restored older), not that a close raised it: re-admitting under the stored
    /// value would be refused again until the operation's deadline. The marker is
    /// raised above the refusal, as a close raises it; leaves re-present the raised
    /// generation at each partition. Raising only fences this shard root's own writes
    /// admitted under the old generation, which retry, so it is always safe.
    /// </summary>
    private async Task RaiseRoutingGenerationAboveRefusalAsync(WalRoutingGenerationStaleException stale)
    {
        if (stale.Refusal <= 0
            || stale.Initiator != WalRoutingInitiator.ShardRoot(ShardIndex)
            || _handOffMarker is not { } marker
            || marker.RoutingGeneration >= stale.Refusal)
        {
            return;
        }

        try
        {
            await UpdateHandOffMarkerAsync(m =>
            {
                if (m.RoutingGeneration >= stale.Refusal)
                {
                    return false;
                }

                m.RoutingGeneration = stale.Refusal + 1;
                return true;
            });
            logger.LogWarning(
                "Shard root {TreeId}/{ShardIndex}: a WAL partition refused routing generation {Generation} at {Refusal}; the marker was behind it and is raised above it (#5663).",
                TreeId,
                ShardIndex,
                stale.Generation,
                stale.Refusal);
        }
        catch (Exception ex) when (!IsPurgedIncarnation(ex))
        {
            // #5716 review, N1: a purged incarnation's outcome is surfaced, never swallowed.
            logger.LogDebug(ex, "Shard root {TreeId}/{ShardIndex}: could not raise its routing generation above a refusal (epic #5605).", TreeId, ShardIndex);
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<SplitResult?> LeafSetAsync(IBPlusLeafGrain leaf, string key, byte[] value, long expiresAtTicks, bool withExpiry, WalRoutingStamp routing)
    {
        if (!routing.ForcesUnstamped)
        {
            try
            {
                return await leaf.SetStampedAsync(key, value, expiresAtTicks, routing);
            }
            catch (Exception ex) when (WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(ex))
            {
            }
        }

        return withExpiry ? await leaf.SetAsync(key, value, expiresAtTicks) : await leaf.SetAsync(key, value);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<GetOrSetResult> LeafGetOrSetAsync(IBPlusLeafGrain leaf, string key, byte[] value, WalRoutingStamp routing)
    {
        if (!routing.ForcesUnstamped)
        {
            try
            {
                return await leaf.GetOrSetStampedAsync(key, value, routing);
            }
            catch (Exception ex) when (WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(ex))
            {
            }
        }

        return await leaf.GetOrSetAsync(key, value);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<CasResult> LeafSetIfVersionAsync(IBPlusLeafGrain leaf, string key, byte[] value, HybridLogicalClock expectedVersion, WalRoutingStamp routing)
    {
        if (!routing.ForcesUnstamped)
        {
            try
            {
                return await leaf.SetIfVersionStampedAsync(key, value, expectedVersion, routing);
            }
            catch (Exception ex) when (WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(ex))
            {
            }
        }

        return await leaf.SetIfVersionAsync(key, value, expectedVersion);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<CrdtApplyResult> LeafApplyCrdtDeltaAsync(IBPlusLeafGrain leaf, string key, LatticeMergeMode mode, byte[] deltaBytes, long expiresAtTicks, WalRoutingStamp routing)
    {
        if (!routing.ForcesUnstamped)
        {
            try
            {
                return await leaf.ApplyCrdtDeltaStampedAsync(key, mode, deltaBytes, expiresAtTicks, routing);
            }
            catch (Exception ex) when (WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(ex))
            {
            }
        }

        return await leaf.ApplyCrdtDeltaAsync(key, mode, deltaBytes, expiresAtTicks);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<SplitResult?> LeafApplyCrdtDeltaManyAsync(IBPlusLeafGrain leaf, List<KeyValuePair<string, byte[]>> slice, LatticeMergeMode mode, WalRoutingStamp routing)
    {
        if (!routing.ForcesUnstamped)
        {
            try
            {
                return await leaf.ApplyCrdtDeltaManyStampedAsync(slice, mode, routing);
            }
            catch (Exception ex) when (WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(ex))
            {
            }
        }

        return await leaf.ApplyCrdtDeltaManyAsync(slice, mode);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<SplitResult?> LeafSetManyAsync(IBPlusLeafGrain leaf, List<KeyValuePair<string, byte[]>> slice, WalRoutingStamp routing)
    {
        if (!routing.ForcesUnstamped)
        {
            try
            {
                return await leaf.SetManyStampedAsync(slice, routing);
            }
            catch (Exception ex) when (WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(ex))
            {
            }
        }

        return await leaf.SetManyAsync(slice);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<ConditionalSetManyResult> LeafSetManyWherePredicateAsync(IBPlusLeafGrain leaf, List<KeyValuePair<string, byte[]>> slice, LatticePredicateNode predicate, WalRoutingStamp routing)
    {
        if (!routing.ForcesUnstamped)
        {
            try
            {
                return await leaf.SetManyWherePredicateStampedAsync(slice, predicate, routing);
            }
            catch (Exception ex) when (WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(ex))
            {
            }
        }

        return await leaf.SetManyWherePredicateAsync(slice, predicate);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<SplitResult?> LeafMergeManyAsync(IBPlusLeafGrain leaf, Dictionary<string, LwwValue<byte[]>> entries, bool isCrossShardMigration, WalRoutingStamp routing)
    {
        if (!routing.ForcesUnstamped)
        {
            try
            {
                return await leaf.MergeManyStampedAsync(entries, isCrossShardMigration, routing);
            }
            catch (Exception ex) when (WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(ex))
            {
            }
        }

        return await leaf.MergeManyAsync(entries, isCrossShardMigration);
    }
}
