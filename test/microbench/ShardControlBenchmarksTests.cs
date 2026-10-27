using Orleans.Lattice.Benchmark.Microbench;

namespace Orleans.Lattice.Benchmark.Microbench.Tests;

[TestFixture]
public sealed class ShardControlBenchmarksTests
{
    [TestCase(ShardControlCallBenchmarks.Site.SnapshotHead)]
    [TestCase(ShardControlCallBenchmarks.Site.MaterialiserLag)]
    public async Task Capture_models_preserve_results_and_dispatch_budgets(ShardControlCallBenchmarks.Site site)
    {
        var benchmark = new ShardControlCallBenchmarks
        {
            Path = site,
            Hop = LeafChainWalkElisionBenchmarks.HopModel.Yield,
        };
        benchmark.Setup();
        Assert.That(await benchmark.AfterAsync(), Is.EqualTo(await benchmark.BeforeAsync()));
    }

    [TestCase(16)]
    [TestCase(256)]
    [TestCase(4096)]
    public void Membership_benchmark_uses_equivalent_generation_checks(int shards)
    {
        var benchmark = new MergeShardMembershipBenchmarks { Shards = shards };
        benchmark.Setup();
        Assert.That(benchmark.After(), Is.EqualTo(benchmark.Before()));
    }
}
