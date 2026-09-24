using NSubstitute;
using NSubstitute.Core;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Stubs a substituted <see cref="IWalShardGrain"/> so a
/// <see cref="WalCommitLogWriter.AppendAsync"/> point append observes the
/// same outcome whichever grain overload
/// <see cref="LatticeOptions.WalBatchedSingleEntryAppends"/> routes it to.
/// </summary>
/// <remarks>
/// A writer-level test that stubs only <see cref="IWalShardGrain.AppendAsync"/>
/// silently stops exercising its stub once point appends are routed through
/// <see cref="IWalShardGrain.AppendBatchAsync"/>: the unstubbed batched call
/// returns an auto-value and the test's hang, fault, or cancellation never
/// reaches the writer. Stubbing both targets keeps such a test about the
/// writer behaviour it names rather than about the dispatch target. A test
/// that is <i>about</i> the dispatch target asserts it directly instead.
/// </remarks>
internal static class WalShardGrainPointAppendStubs
{
    /// <summary>
    /// Makes every point append on <paramref name="shard"/> complete with
    /// <paramref name="result"/>, on either grain overload.
    /// </summary>
    /// <param name="shard">The substituted shard grain.</param>
    /// <param name="result">The task each point append returns; a batched dispatch completes, faults, or cancels exactly as it does.</param>
    public static void StubPointAppend(this IWalShardGrain shard, Task<long> result)
        => shard.StubPointAppend(_ => result);

    /// <summary>
    /// Makes every point append on <paramref name="shard"/> complete with the
    /// task <paramref name="factory"/> returns, on either grain overload. The
    /// factory runs once per dispatch; <c>callInfo[1]</c> is the
    /// <see cref="CancellationToken"/> on both overloads.
    /// </summary>
    /// <param name="shard">The substituted shard grain.</param>
    /// <param name="factory">Produces the per-dispatch result task.</param>
    public static void StubPointAppend(this IWalShardGrain shard, Func<CallInfo, Task<long>> factory)
    {
        ArgumentNullException.ThrowIfNull(shard);
        ArgumentNullException.ThrowIfNull(factory);

        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => factory(callInfo));
        shard.AppendBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => AsBatchAsync(factory(callInfo)));
    }

    private static async Task<IReadOnlyList<long>> AsBatchAsync(Task<long> single)
        => new[] { await single.ConfigureAwait(false) };
}
