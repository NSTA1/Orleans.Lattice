namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Shared, bounded, one-sided emptiness probe used by the coordinator grains
/// whose empty-tree fast paths need to know only whether a tree holds any live
/// key.
/// <para>
/// Both <see cref="TreeReshardGrain"/> and <see cref="TreeResizeGrain"/>
/// previously answered that boolean with <see cref="ILattice.CountAsync"/>, a
/// strongly-consistent whole-tree fan-out that walks every leaf chain, then
/// discards its result and retries whenever the shard map moves under it,
/// giving up only once <see cref="LatticeOptions.MaxScanRetries"/> is
/// exhausted. Both call it at exactly the moment that map is most likely to be
/// churning, so the probe could consume the caller's entire response budget and
/// time the operation out before it had started.
/// </para>
/// <para>
/// This instead OR-s a short-circuiting <see cref="IShardRootGrain.AnyAsync"/>
/// across the tree's physical shards, and needs no stability loop at all. That
/// is the crux of the difference: a count must reconcile against a moving map
/// because a key migrating between shards is briefly visible on both the source
/// and the destination, which double-counts. An adaptive split only ever
/// <em>moves</em> keys - it never creates or destroys one, and never leaves one
/// present on neither side - so a key that exists is seen by at least one shard
/// no matter where the split has got to, and seeing it twice still just means
/// "a key exists". The OR is therefore correct against a map changing
/// underneath it, with no retry and no snapshot.
/// </para>
/// <para>
/// The answer is one-sided by design: it may report non-empty while the last
/// keys are migrating away, but it can never report empty while a key exists
/// anywhere. Only the "empty" answer unlocks a fast path (which repins registry
/// state without migrating anything), so the sole consequential direction is
/// the one that cannot be wrong. Every inconclusive outcome - the budget
/// elapsing, or any shard faulting - is likewise reported as "not empty" so the
/// caller simply proceeds down its normal coordinator path.
/// </para>
/// </summary>
internal static class TreeEmptinessProbe
{
    /// <summary>
    /// Returns <see langword="true"/> only when every physical shard of the
    /// tree was positively observed to hold no live key within
    /// <paramref name="budget"/>.
    /// </summary>
    /// <param name="grainFactory">Factory used to resolve the shard grains.</param>
    /// <param name="physicalTreeId">The resolved physical tree id the shards belong to.</param>
    /// <param name="physicalShards">The physical shard indices to probe.</param>
    /// <param name="budget">
    /// Ceiling on the whole probe, or <see cref="Timeout.InfiniteTimeSpan"/> to
    /// wait indefinitely.
    /// </param>
    /// <returns><see langword="true"/> only when the tree was positively observed to be empty.</returns>
    /// <remarks>
    /// Runs inside a grain turn, so the continuation model matters:
    /// <list type="bullet">
    /// <item>Every await that the caller's grain state depends on resuming from
    /// is pinned to the captured context with
    /// <see cref="ConfigureAwaitOptions.ContinueOnCapturedContext"/>, matching
    /// <see cref="ShardActivationRetry"/>, so control returns to the
    /// activation's task scheduler and the caller may touch grain state
    /// immediately after awaiting this method.</item>
    /// <item>The fire-and-forget fault observation for an abandoned probe is
    /// pinned to <see cref="TaskScheduler.Default"/> and deliberately does
    /// <em>not</em> use <c>ExecuteSynchronously</c>: it must never be inlined
    /// onto an Orleans scheduler thread nor consume a grain turn, since it can
    /// run long after the activation has moved on or been deactivated. Its body
    /// touches only the task, never grain state.</item>
    /// <item>The timeout timer is cancelled as soon as the probe wins, so the
    /// common fast path does not leave a timer pending for the whole
    /// budget.</item>
    /// </list>
    /// </remarks>
    internal static async Task<bool> IsObservablyEmptyAsync(
        IGrainFactory grainFactory,
        string physicalTreeId,
        IReadOnlyList<int> physicalShards,
        TimeSpan budget)
    {
        try
        {
            var probes = new Task<bool>[physicalShards.Count];
            for (int i = 0; i < physicalShards.Count; i++)
            {
                var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{physicalShards[i]}");
                probes[i] = ShardActivationRetry.RunAsync(shard.AnyAsync);
            }

            var all = Task.WhenAll(probes);

            if (budget != Timeout.InfiniteTimeSpan)
            {
                using var timeoutCts = new CancellationTokenSource();
                var winner = await Task.WhenAny(all, Task.Delay(budget, timeoutCts.Token))
                    .ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);

                if (!ReferenceEquals(winner, all))
                {
                    ObserveEventualFault(all);
                    return false;
                }

                // The probe won: stop the timer rather than leave it pending
                // for the remainder of the budget.
                await timeoutCts.CancelAsync().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
            }

            foreach (var shardHasKeys in await all.ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext))
            {
                if (shardHasKeys) return false;
            }
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // An inconclusive probe must never block the operation: fall
            // through to the coordinator path rather than fail outright.
            return false;
        }
    }

    // Observes an abandoned probe's eventual fault so it cannot surface as an
    // unobserved task exception. Pinned to the default scheduler (never the
    // activation's) and never inlined, so it cannot consume a grain turn or run
    // grain-affine code after the activation has moved on.
    private static void ObserveEventualFault(Task task) =>
        _ = task.ContinueWith(
            static t => _ = t.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted,
            TaskScheduler.Default);
}
