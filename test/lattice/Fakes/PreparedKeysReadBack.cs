using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Stubs the saga's original-prepare-stamp read-back (issue #4522) on a
/// substitute shard. An <c>AtomicWriteGrain</c> reads back every key it prepared
/// before its execute phase ends, and fails the batch when a key is unaccounted
/// for, so a substitute shard must report the saga's keys. The read-back runs
/// only once every entry has been dispatched (by this activation or an earlier
/// one), so every entry is reported as prepared and unmarked, which leaves the
/// committed-values backstop exactly as it was before #4522. Tests of the
/// read-back itself override the stub.
/// </summary>
internal static class PreparedKeysReadBack
{
    /// <summary>
    /// Has <paramref name="shard"/> report every key <paramref name="keys"/>
    /// yields, at call time, as prepared and unmarked.
    /// </summary>
    public static void Stub(IShardRootGrain shard, Func<IEnumerable<string>> keys) =>
        shard.GetOriginalPrepareStampsAsync(Arg.Any<Guid>(), Arg.Any<bool>())
            .Returns(_ => Task.FromResult(
                keys().Distinct(StringComparer.Ordinal)
                    .ToDictionary(k => k, _ => (HybridLogicalClock?)null, StringComparer.Ordinal)));
}
