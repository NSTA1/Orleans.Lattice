using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Detectors for the resize's hold on shard migrations (issue #4452), driven
/// through the real grains end to end. The grain-level interlock tests stub
/// <see cref="ITreeResizeGrain.HoldsShardMigrationsAsync"/> and so test only the
/// callers; these run the resize coordinator's own decision, so removing the
/// hold, or its soft-delete-window arm, or the reshard's read of it, makes a
/// split, a consolidation or a reshard start where it must be refused
/// (shard-ownership review #4435, finding F3).
/// </summary>
[TestFixture]
[Category("Integration")]
public class ResizeMigrationHoldDetectorIntegrationTests
{
    private const string HeldMigrationRefusal = "resize of the tree is in progress or can still be undone";

    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    private async Task<string> CreatePopulatedTreeAsync(string prefix)
    {
        var treeId = $"{prefix}-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        for (var i = 0; i < 200; i++)
        {
            await tree.SetAsync($"key-{i:D4}", Encoding.UTF8.GetBytes($"value-{i}"));
        }

        return treeId;
    }

    private async Task StartResizeAsync(string treeId)
    {
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
        await resize.ResizeAsync(64, 64);
        Assert.That(await resize.IsIdleAsync(), Is.False, "precondition: the resize is in flight");
    }

    private async Task CompleteResizeAsync(string treeId)
    {
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
        await resize.ResizeAsync(64, 64);
        await resize.RunResizePassAsync();
        Assert.That(await resize.IsIdleAsync(), Is.True, "precondition: the resize completed and is in its soft-delete window");
    }

    private async Task AssertSplitRefusedAsync(string treeId)
    {
        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => split.SplitAsync(sourceShardIndex: 0));
        Assert.That(ex!.Message, Does.Contain("resize of the tree is in progress"));
        Assert.That(await split.IsIdleAsync(), Is.True, "a refused split must not have started");
    }

    private async Task AssertConsolidationRefusedAsync(string treeId)
    {
        var fold = _cluster.GrainFactory.GetGrain<ITreeShardConsolidationGrain>($"{treeId}/1");
        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => fold.StartAsync(survivorShardIndex: 0));
        Assert.That(ex!.Message, Does.Contain(HeldMigrationRefusal));
        Assert.That(await fold.IsIdleAsync(), Is.True, "a refused consolidation must not have started");
    }

    [Test]
    public async Task A_split_is_refused_by_the_resize_hold_while_the_resize_is_in_flight()
    {
        var treeId = await CreatePopulatedTreeAsync("hold-split-flight");
        await StartResizeAsync(treeId);

        await AssertSplitRefusedAsync(treeId);
    }

    [Test]
    public async Task A_split_is_refused_by_the_resize_hold_while_the_replaced_copy_still_mirrors()
    {
        var treeId = await CreatePopulatedTreeAsync("hold-split-window");
        await CompleteResizeAsync(treeId);

        await AssertSplitRefusedAsync(treeId);
    }

    [Test]
    public async Task A_consolidation_is_refused_by_the_resize_hold_while_the_resize_is_in_flight()
    {
        var treeId = await CreatePopulatedTreeAsync("hold-fold-flight");
        await StartResizeAsync(treeId);

        await AssertConsolidationRefusedAsync(treeId);
    }

    [Test]
    public async Task A_consolidation_is_refused_by_the_resize_hold_while_the_replaced_copy_still_mirrors()
    {
        var treeId = await CreatePopulatedTreeAsync("hold-fold-window");
        await CompleteResizeAsync(treeId);

        await AssertConsolidationRefusedAsync(treeId);
    }

    [Test]
    public async Task A_reshard_is_refused_by_the_resize_hold_while_the_replaced_copy_still_mirrors()
    {
        // The resize coordinator is idle here, so only the reshard's read of the
        // completed resize's hold refuses it; the in-flight check cannot.
        var treeId = await CreatePopulatedTreeAsync("hold-reshard-window");
        await CompleteResizeAsync(treeId);
        var reshard = _cluster.GrainFactory.GetGrain<ITreeReshardGrain>(treeId);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => reshard.ReshardAsync(FourShardClusterFixture.TestShardCount * 2));

        Assert.That(ex!.Message, Does.Contain("can still be undone"));
        Assert.That(await reshard.IsIdleAsync(), Is.True, "a refused reshard must not have started");
    }
}
