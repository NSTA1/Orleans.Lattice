using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Integration tests for the transport-agnostic <see cref="ILatticeStateQuery"/>
/// read facade over a real single-silo cluster.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeStateQueryIntegrationTests
{
    private readonly StateQueryClusterFixture _fixture = new();

    [OneTimeSetUp]
    public Task OneTimeSetUp() => _fixture.InitializeAsync();

    [OneTimeTearDown]
    public Task OneTimeTearDown() => _fixture.DisposeAsync();

    [Test]
    public async Task GetTreeSummaryAsync_reports_accurate_counts_and_shard_count()
    {
        const string treeId = "summary-accuracy";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 50);

        var result = await _fixture.Query.GetTreeSummaryAsync(treeId);

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Summary, Is.Not.Null);
        Assert.That(result.Summary!.TreeId, Is.EqualTo(treeId));
        Assert.That(result.Summary.ShardCount, Is.EqualTo(StateQueryClusterFixture.ShardCount));
        Assert.That(result.Summary.TotalLiveKeys, Is.EqualTo(50));
        Assert.That(result.Summary.Lifecycle, Is.EqualTo(TreeLifecycleState.Active));
        Assert.That(result.Summary.Config, Is.Not.Null);
        Assert.That(result.Summary.Config!.ShardCount, Is.EqualTo(StateQueryClusterFixture.ShardCount));
    }

    [Test]
    public async Task GetShardSummariesAsync_returns_per_shard_summaries_summing_to_total()
    {
        const string treeId = "shard-accuracy";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 40);

        var result = await _fixture.Query.GetShardSummariesAsync(treeId);

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Shards, Has.Count.EqualTo(StateQueryClusterFixture.ShardCount));
        Assert.That(result.Shards.Select(s => s.ShardIndex), Is.Ordered);
        Assert.That(result.Shards.Sum(s => s.LiveKeys), Is.EqualTo(40));
    }

    [Test]
    public async Task GetTreeSummaryAsync_returns_typed_not_found_for_unknown_tree()
    {
        var result = await _fixture.Query.GetTreeSummaryAsync("no-such-tree");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
        Assert.That(result.Summary, Is.Null);
        Assert.That(result.TreeId, Is.EqualTo("no-such-tree"));
    }

    [Test]
    public async Task GetShardSummariesAsync_returns_typed_not_found_for_unknown_tree()
    {
        var result = await _fixture.Query.GetShardSummariesAsync("no-such-tree");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
        Assert.That(result.Shards, Is.Empty);
    }

    [Test]
    public void GetTreeSummaryAsync_honours_cancellation()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await _fixture.Query.GetTreeSummaryAsync("any", cancellationToken: cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task GetTreeSummaryAsync_issues_bounded_grain_calls_independent_of_shard_count()
    {
        await _fixture.CreatePopulatedTreeAsync("bounded-1-shard", keyCount: 20, shardCount: 1);
        await _fixture.CreatePopulatedTreeAsync("bounded-8-shard", keyCount: 20, shardCount: 8);

        _fixture.CallCounter.Reset();
        await _fixture.Query.GetTreeSummaryAsync("bounded-1-shard");
        var oneShardCalls = _fixture.CallCounter.Count;

        _fixture.CallCounter.Reset();
        await _fixture.Query.GetTreeSummaryAsync("bounded-8-shard");
        var eightShardCalls = _fixture.CallCounter.Count;

        Assert.That(oneShardCalls, Is.EqualTo(eightShardCalls),
            "the facade must issue the same (bounded) number of ILattice calls regardless of shard count");
        Assert.That(eightShardCalls, Is.LessThanOrEqualTo(4),
            "the facade must issue only a small constant number of ILattice calls per tree summary");
    }

    [Test]
    public async Task Second_adapter_over_facade_returns_identical_results()
    {
        const string treeId = "mcp-reuse-parity";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 30);

        // A trivial second adapter over the same facade, simulating the
        // future MCP binding: it must observe identical results to a direct
        // facade call, proving transport neutrality.
        var mcpLikeAdapter = new ParityAdapter(_fixture.Query);

        var direct = await _fixture.Query.GetTreeSummaryAsync(treeId);
        var viaAdapter = await mcpLikeAdapter.SummariseAsync(treeId);

        Assert.That(viaAdapter, Is.Not.Null);
        Assert.That(viaAdapter!.TreeId, Is.EqualTo(direct.Summary!.TreeId));
        Assert.That(viaAdapter.ShardCount, Is.EqualTo(direct.Summary.ShardCount));
        Assert.That(viaAdapter.TotalLiveKeys, Is.EqualTo(direct.Summary.TotalLiveKeys));
    }

    private sealed class ParityAdapter(ILatticeStateQuery query)
    {
        public async Task<TreeStateSummary?> SummariseAsync(string treeId)
        {
            var result = await query.GetTreeSummaryAsync(treeId);
            return result.Summary;
        }
    }
}
