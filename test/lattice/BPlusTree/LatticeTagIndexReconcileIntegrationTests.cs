using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration coverage for the background tag-index reconciliation coordinator
/// against a live in-memory cluster: digest-gated sweeps remove orphan rows for
/// deleted keys, leave clean indexes untouched, and honour probe-only mode.
/// </summary>
[TestFixture]
[Category("Integration")]
public class LatticeTagIndexReconcileIntegrationTests
{
    private ClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    private ILattice Tree(string id) => _cluster.GrainFactory.GetGrain<ILattice>(id);

    private ITagIndexReconcileGrain Coordinator(string indexName)
        => _cluster.GrainFactory.GetGrain<ITagIndexReconcileGrain>(indexName);

    [Test]
    public async Task RunSweepAsync_removes_orphan_rows_for_deleted_keys()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var index = $"colors-{sfx}";
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("d", Bytes("1"));
        var idx = tree.TagIndex(_cluster.GrainFactory, index);
        await idx.Key("d").AddAsync(["red"]);

        await tree.DeleteAsync("d");
        var report = await Coordinator(index).RunSweepAsync();

        Assert.That(report.OrphanRowsRemoved, Is.GreaterThanOrEqualTo(1));
        Assert.That(await idx.WithAnyTags("red").CountAsync(), Is.Zero);
    }

    [Test]
    public async Task RunSweepAsync_leaves_clean_index_untouched()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var index = $"colors-{sfx}";
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        var idx = tree.TagIndex(_cluster.GrainFactory, index);
        await idx.Key("a").AddAsync(["red"]);

        // First sweep establishes the baseline; second sweep should be a no-op.
        await Coordinator(index).RunSweepAsync();
        var report = await Coordinator(index).RunSweepAsync();

        Assert.That(report.OrphanRowsRemoved, Is.Zero);
        Assert.That(await idx.WithAnyTags("red").CountAsync(), Is.EqualTo(1));
    }

    [Test]
    public async Task RunSweepAsync_is_idle_after_completion()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var index = $"colors-{sfx}";
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        var idx = tree.TagIndex(_cluster.GrainFactory, index);
        await idx.Key("a").AddAsync(["red"]);

        await Coordinator(index).RunSweepAsync();

        Assert.That(await Coordinator(index).IsIdleAsync(), Is.True);
    }

    [Test]
    public async Task RunSweepAsync_re_detects_divergence_after_baseline_established()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var index = $"colors-{sfx}";
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        await tree.SetAsync("b", Bytes("2"));
        var idx = tree.TagIndex(_cluster.GrainFactory, index);
        await idx.Key("a").AddAsync(["red"]);
        await idx.Key("b").AddAsync(["red"]);

        // First sweep captures a clean baseline.
        await Coordinator(index).RunSweepAsync();

        // A new deletion after the baseline must still be detected: the digest
        // gate must not suppress genuine post-baseline divergence.
        await tree.DeleteAsync("b");
        var report = await Coordinator(index).RunSweepAsync();

        Assert.That(report.OrphanRowsRemoved, Is.GreaterThanOrEqualTo(1));
        Assert.That(await idx.WithAnyTags("red").CountAsync(), Is.EqualTo(1));
    }

    [Test]
    public async Task RunSweepAsync_repairs_only_the_divergent_tree_across_covered_trees()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var index = $"colors-{sfx}";
        var treeA = Tree($"itemsA-{sfx}");
        var treeB = Tree($"itemsB-{sfx}");
        await treeA.SetAsync("a", Bytes("1"));
        await treeB.SetAsync("b", Bytes("2"));
        var idxA = treeA.TagIndex(_cluster.GrainFactory, index);
        var idxB = treeB.TagIndex(_cluster.GrainFactory, index);
        await idxA.Key("a").AddAsync(["red"]);
        await idxB.Key("b").AddAsync(["red"]);

        // Delete a key only in tree A; tree B stays live.
        await treeA.DeleteAsync("a");
        var report = await Coordinator(index).RunSweepAsync();

        Assert.That(report.OrphanRowsRemoved, Is.GreaterThanOrEqualTo(1));
        // The deleted key's row is gone, but tree B's live row survives.
        var multi = _cluster.GrainFactory.MultiTreeTagIndex(index);
        var remaining = new List<TaggedKey>();
        await foreach (var hit in multi.WithAnyTags("red"))
        {
            remaining.Add(hit);
        }
        Assert.That(remaining, Has.Count.EqualTo(1));
        Assert.That(remaining[0].Key, Is.EqualTo("b"));
    }
}
