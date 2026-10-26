using Orleans.Lattice.Benchmark.Microbench;

namespace Orleans.Lattice.Benchmark.Microbench.Tests;

[TestFixture]
public sealed class ReplayMaintenanceBenchmarksTests
{
    [TestCase(ReplayMaintenanceBenchmarks.Site.PrepareBatch)]
    [TestCase(ReplayMaintenanceBenchmarks.Site.PurgeProbe)]
    [TestCase(ReplayMaintenanceBenchmarks.Site.StampLookups)]
    public async Task Lanes_return_identical_results_and_validate_dispatch_budgets(ReplayMaintenanceBenchmarks.Site path)
    {
        var benchmark = new ReplayMaintenanceBenchmarks
        {
            Path = path,
            Hop = LeafChainWalkElisionBenchmarks.HopModel.Yield,
        };
        benchmark.Setup();
        Assert.That(await benchmark.AfterAsync(), Is.EqualTo(await benchmark.BeforeAsync()));
    }
}
