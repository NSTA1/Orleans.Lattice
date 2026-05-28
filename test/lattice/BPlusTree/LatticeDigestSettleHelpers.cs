using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Test helpers that read the per-shard projection digest in a
/// coalescing-window-agnostic way.
/// <para>
/// The leaf-side digest publish is deferred by
/// <see cref="LatticeOptions.DigestCoalescingWindowMs"/> (default 5 ms);
/// every prior oracle test that called
/// <see cref="ILattice.GetLeafProjectionDigestAsync"/> directly raced
/// the one-shot publish timer and would intermittently observe a
/// pre-mutation aggregate at the parent. These helpers replace the
/// bare API call with a converge-or-timeout poll loop so the tests
/// pass under <em>any</em> coalescing window from <c>0</c> to a
/// reasonable upper bound, without making the tests depend on a
/// specific default value.
/// </para>
/// <para>
/// The only test that should depend on the configured window is the
/// dedicated <c>DigestCoalescingWindowMs_default_*</c> defaults-pin
/// test - every other test should reach a stable observation via
/// these helpers.
/// </para>
/// </summary>
internal static class LatticeDigestSettleHelpers
{
    /// <summary>
    /// Default per-poll wait. Sized below the library default coalescing
    /// window (5 ms) so the loop typically completes in 1-2 iterations
    /// when the publish timer fires, and well above the synchronous
    /// publish cost when the window is 0.
    /// </summary>
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(2);

    /// <summary>
    /// Default ceiling on total settle wait time. Generous enough to
    /// absorb a few coalescing windows plus any per-call grain RPC
    /// scheduling overhead, but short enough that a genuine convergence
    /// failure surfaces as a test failure rather than a CI timeout.
    /// </summary>
    private static readonly TimeSpan SettleTimeout = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Polls <see cref="ILattice.GetLeafProjectionDigestAsync"/> for
    /// <paramref name="shardIndex"/> until the reported
    /// <see cref="LeafProjectionDigest.EntryCount"/> equals
    /// <paramref name="expectedEntryCount"/>, then returns the settled
    /// digest. Throws <see cref="TimeoutException"/> if the count never
    /// converges within <see cref="SettleTimeout"/>.
    /// <para>
    /// Use when the test has an authoritative ground-truth count (e.g.
    /// from a fresh chain walk) that the chained-fold aggregate must
    /// catch up to.
    /// </para>
    /// </summary>
    public static async Task<LeafProjectionDigest> AwaitDigestConvergesToAsync(
        ILattice tree,
        int shardIndex,
        long expectedEntryCount,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tree);
        var deadline = DateTime.UtcNow + (timeout ?? SettleTimeout);
        LeafProjectionDigest latest = default;

        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            latest = await tree.GetLeafProjectionDigestAsync(shardIndex);
            if (latest.EntryCount == expectedEntryCount)
            {
                return latest;
            }
            await Task.Delay(PollInterval, cancellationToken);
        }

        throw new TimeoutException(
            $"projection digest for shard {shardIndex} did not converge to " +
            $"{expectedEntryCount} entries within {(timeout ?? SettleTimeout).TotalMilliseconds} ms " +
            $"(last observation: {latest.EntryCount} entries)");
    }

    /// <summary>
    /// Polls <see cref="ILattice.GetLeafProjectionDigestAsync"/> across
    /// every shard until the sum of reported entry counts equals
    /// <paramref name="expectedTotalEntries"/>, then returns the
    /// per-shard digests in shard-index order. Throws
    /// <see cref="TimeoutException"/> on failure to converge.
    /// </summary>
    public static async Task<IReadOnlyList<LeafProjectionDigest>> AwaitAllShardDigestsConvergeAsync(
        ILattice tree,
        int shardCount,
        long expectedTotalEntries,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tree);
        var deadline = DateTime.UtcNow + (timeout ?? SettleTimeout);
        var latest = new LeafProjectionDigest[shardCount];

        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var total = 0L;
            for (var s = 0; s < shardCount; s++)
            {
                latest[s] = await tree.GetLeafProjectionDigestAsync(s);
                total += latest[s].EntryCount;
            }
            if (total == expectedTotalEntries)
            {
                return latest;
            }
            await Task.Delay(PollInterval, cancellationToken);
        }

        var lastTotal = 0L;
        foreach (var d in latest) lastTotal += d.EntryCount;
        throw new TimeoutException(
            $"per-shard projection digests across {shardCount} shards did not converge " +
            $"to a total of {expectedTotalEntries} entries within " +
            $"{(timeout ?? SettleTimeout).TotalMilliseconds} ms (last total: {lastTotal})");
    }

    /// <summary>
    /// Polls <see cref="ILattice.GetLeafProjectionDigestAsync"/> for
    /// <paramref name="shardIndex"/> until the reported hash equals
    /// <paramref name="expectedHash"/>, then returns the settled
    /// digest. Throws <see cref="TimeoutException"/> if the hash never
    /// converges within <see cref="SettleTimeout"/>.
    /// <para>
    /// Use when the test has an authoritative ground-truth hash (e.g.
    /// from a fresh chain walk recomputing the outer fold) that the
    /// chained-fold aggregate must catch up to.
    /// </para>
    /// </summary>
    public static async Task<LeafProjectionDigest> AwaitDigestHashConvergesToAsync(
        ILattice tree,
        int shardIndex,
        byte[] expectedHash,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(expectedHash);
        var deadline = DateTime.UtcNow + (timeout ?? SettleTimeout);
        LeafProjectionDigest latest = default;

        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            latest = await tree.GetLeafProjectionDigestAsync(shardIndex);
            if (latest.Hash is { } actual && actual.SequenceEqual(expectedHash))
            {
                return latest;
            }
            await Task.Delay(PollInterval, cancellationToken);
        }

        throw new TimeoutException(
            $"projection digest hash for shard {shardIndex} did not converge within " +
            $"{(timeout ?? SettleTimeout).TotalMilliseconds} ms");
    }
}
