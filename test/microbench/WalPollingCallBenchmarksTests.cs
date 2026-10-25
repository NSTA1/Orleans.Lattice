using Orleans.Lattice.Benchmark.Microbench;

namespace Orleans.Lattice.Benchmark.Microbench.Tests;

[TestFixture]
public sealed class WalPollingCallBenchmarksTests
{
    [TestCase(WalPollingCallBenchmarks.Site.IdleDrain)]
    [TestCase(WalPollingCallBenchmarks.Site.RetentionTail)]
    [TestCase(WalPollingCallBenchmarks.Site.ViewAlias)]
    public async Task Lanes_return_identical_results_and_validate_dispatch_budgets(WalPollingCallBenchmarks.Site path)
    {
        var benchmark = new WalPollingCallBenchmarks
        {
            Path = path,
            Hop = LeafChainWalkElisionBenchmarks.HopModel.Yield,
        };
        benchmark.Setup();
        Assert.That(await benchmark.AfterAsync(), Is.EqualTo(await benchmark.BeforeAsync()));
    }
}
