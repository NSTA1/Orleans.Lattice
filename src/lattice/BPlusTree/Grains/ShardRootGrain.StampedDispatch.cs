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
            : EnsureRoutingGenerationSlowAsync();

    private async Task EnsureRoutingGenerationSlowAsync()
    {
        try
        {
            var marker = await LoadHandOffMarkerAsync();
            if (marker.RoutingGeneration == 0)
            {
                await UpdateHandOffMarkerAsync(m =>
                {
                    if (m.RoutingGeneration > 0)
                    {
                        return false;
                    }

                    m.RoutingGeneration = 1;
                    return true;
                });
            }

            // Rule 2 ("absence never means closed"): the marker reads initialised
            // only once this shard root has no hand-off of its own in flight.
            await TryInitialiseHandOffMarkerAsync();
        }
        catch (Exception ex)
        {
            // Fail safe: the shard root keeps admitting unstamped writes.
            logger.LogDebug(
                ex,
                "Shard root {TreeId}/{ShardIndex}: could not raise its routing generation; its writes stay unstamped (epic #5605).",
                TreeId,
                ShardIndex);
        }
    }

    /// <summary>
    /// Re-reads the marker after a stale-generation rejection, so a re-admission
    /// stamps the generation a close (possibly by another activation) raised.
    /// </summary>
    private async Task ReloadHandOffMarkerAsync()
    {
        try
        {
            var row = await HandOffMarkerRows.ReadShardAsync(HandOffStorage(), TreeId, ShardIndex);
            _handOffMarker = row.State;
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Shard root {TreeId}/{ShardIndex}: could not reload its hand-off marker (epic #5605).", TreeId, ShardIndex);
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
        await ReloadHandOffMarkerAsync();
        await RaiseRoutingGenerationAboveRefusalAsync(stale);
        return retry;
    }

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
        if (stale.Refusal <= long.MaxValue
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
        catch (Exception ex)
        {
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
