using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Cross-silo correctness tests for the read facade. Each surface is exercised
/// against a three-silo cluster whose shard / internal / leaf grains are spread
/// across silos, proving the facade reconciles cluster-wide state rather than the
/// subset local to the serving silo. Later features append their own surface's
/// multi-silo coverage to this class.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed partial class MultiSiloStateApiIntegrationTests
{
    private readonly MultiSiloStateApiClusterFixture _fixture = new();

    [OneTimeSetUp]
    public Task OneTimeSetUp() => _fixture.InitializeAsync();

    [OneTimeTearDown]
    public Task OneTimeTearDown() => _fixture.DisposeAsync();

    [Test]
    public async Task GetTreeSummary_reconciles_total_live_keys_across_silos()
    {
        const string treeId = "multisilo-summary";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 120);

        var result = await _fixture.Query.GetTreeSummaryAsync(treeId);

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Summary!.ShardCount, Is.EqualTo(MultiSiloStateApiClusterFixture.ShardCount));
        Assert.That(result.Summary.TotalLiveKeys, Is.EqualTo(120),
            "the per-shard rollup must sum to the full key set even when shards are on different silos");
    }

    [Test]
    public async Task GetShardSummaries_sum_to_total_across_silos()
    {
        const string treeId = "multisilo-shards";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 96);

        var result = await _fixture.Query.GetShardSummariesAsync(treeId);

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Shards, Has.Count.EqualTo(MultiSiloStateApiClusterFixture.ShardCount));
        Assert.That(result.Shards.Select(s => s.ShardIndex), Is.Ordered.And.Unique);
        Assert.That(result.Shards.Sum(s => s.LiveKeys), Is.EqualTo(96),
            "every shard summary must be collected regardless of which silo hosts the shard");
    }

    [Test]
    public async Task Summary_served_by_a_non_originating_silo_is_consistent()
    {
        const string treeId = "multisilo-cross-serve";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 64);

        var fromFirst = await _fixture.Query.GetTreeSummaryAsync(treeId);
        var fromOther = await _fixture.QueryFromOtherSilo().GetTreeSummaryAsync(treeId);

        Assert.That(fromOther.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(fromOther.Summary!.TotalLiveKeys, Is.EqualTo(fromFirst.Summary!.TotalLiveKeys));
        Assert.That(fromOther.Summary.ShardCount, Is.EqualTo(fromFirst.Summary.ShardCount));
    }
}
