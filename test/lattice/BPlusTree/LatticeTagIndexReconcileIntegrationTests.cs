using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Tests.Fakes;
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

    private ILatticeTagIndex TagIndex(ILattice tree, string name) =>
        new DefaultLatticeTagIndexFactory(_cluster.GrainFactory, FakeLatticeReplicationContext.Disabled).Create(tree, name);

    private ILatticeMultiTreeTagIndex MultiTreeTagIndex(string name) =>
        new DefaultLatticeTagIndexFactory(_cluster.GrainFactory, FakeLatticeReplicationContext.Disabled).CreateMultiTree(name);

    private ITagIndexReconcileGrain Coordinator(string indexName)
        => _cluster.GrainFactory.GetGrain<ITagIndexReconcileGrain>(indexName);

    [Test]
    public async Task RunSweepAsync_removes_orphan_rows_for_deleted_keys()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var index = $"colors-{sfx}";
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("d", Bytes("1"));
        var idx = TagIndex(tree, index);
        await idx.Key("d").AddAsync(["red"]);

        await tree.DeleteAsync("d");
        var report = await Coordinator(index).RunSweepAsync();

        Assert.That(report.OrphanRowsRemoved, Is.GreaterThanOrEqualTo(1));
        Assert.That(await idx.WithAnyTags("red").CountAsync(), Is.Zero);
    }

    [Test]
    public async Task ReconcileTreeAsync_reconciles_a_covered_tree_and_returns_true()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var index = $"colors-{sfx}";
        var treeId = $"items-{sfx}";
        var tree = Tree(treeId);
        await tree.SetAsync("d", Bytes("1"));
        var idx = TagIndex(tree, index);
        await idx.Key("d").AddAsync(["red"]);

        // Delete the key so its membership row is orphaned, then drive the reconcile
        // through the identity-swap trigger seam rather than a full manual sweep.
        await tree.DeleteAsync("d");
        var covered = await Coordinator(index).ReconcileTreeAsync(treeId);
        var count = await idx.WithAnyTags("red").CountAsync();

        Assert.Multiple(() =>
        {
            Assert.That(covered, Is.True, "The index covers the tree, so the reconcile must run.");
            Assert.That(count, Is.Zero,
                "The coverage-gated reconcile must remove the orphaned membership row.");
        });
    }

    [Test]
    public async Task ReconcileTreeAsync_returns_false_for_an_uncovered_tree()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var index = $"colors-{sfx}";
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("d", Bytes("1"));
        var idx = TagIndex(tree, index);
        await idx.Key("d").AddAsync(["red"]);

        // A tree this index never tagged is not covered, so the reconcile is a no-op.
        var covered = await Coordinator(index).ReconcileTreeAsync($"unrelated-{sfx}");
        var count = await idx.WithAnyTags("red").CountAsync();

        Assert.Multiple(() =>
        {
            Assert.That(covered, Is.False);
            Assert.That(count, Is.EqualTo(1),
                "An uncovered-tree reconcile must not touch the index.");
        });
    }

    [Test]
    public async Task RunSweepAsync_leaves_clean_index_untouched()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var index = $"colors-{sfx}";
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        var idx = TagIndex(tree, index);
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
        var idx = TagIndex(tree, index);
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
        var idx = TagIndex(tree, index);
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
        var idxA = TagIndex(treeA, index);
        var idxB = TagIndex(treeB, index);
        await idxA.Key("a").AddAsync(["red"]);
        await idxB.Key("b").AddAsync(["red"]);

        // Delete a key only in tree A; tree B stays live.
        await treeA.DeleteAsync("a");
        var report = await Coordinator(index).RunSweepAsync();

        Assert.That(report.OrphanRowsRemoved, Is.GreaterThanOrEqualTo(1));
        // The deleted key's row is gone, but tree B's live row survives.
        var multi = MultiTreeTagIndex(index);
        var remaining = new List<TaggedKey>();
        await foreach (var hit in multi.WithAnyTags("red"))
        {
            remaining.Add(hit);
        }
        Assert.That(remaining, Has.Count.EqualTo(1));
        Assert.That(remaining[0].Key, Is.EqualTo("b"));
    }
}
