using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

public partial class LatticeTagIndexRoundTripBatchingTests
{
    private static (CountingTree tree, ILatticeMultiTreeTagIndex index, ITagIndexReconcileGrain coordinator) CreateMultiTree()
    {
        var counting = new CountingTree();
        var coordinator = Substitute.For<ITagIndexReconcileGrain>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(counting.Lattice);
        grainFactory.GetGrain<ITagIndexReconcileGrain>(Arg.Any<string>()).Returns(coordinator);
        return (counting, LatticeTagIndexContext.CreateMultiTree(grainFactory, IndexName, allowlist: null), coordinator);
    }

    // ── Per-tree handles share the multi-tree index's caches ──

    [Test]
    public async Task Multi_tree_handles_share_existence_and_marker_caches()
    {
        var (tree, index, coordinator) = CreateMultiTree();

        await index.Tree(TreeId).Key("k1").AddAsync(["red"]);
        await index.Tree(TreeId).Key("k2").AddAsync(["red"]);
        await index.Tree(TreeId).Key("k3").AddAsync(["blue"]);

        Assert.Multiple(() =>
        {
            // Each Tree(t) handle used to start with empty caches, so every
            // handle re-probed existence, rewrote the covered marker and
            // re-ensured the reconcile schedule: three of each here.
            Assert.That(tree.TreeExistsAsyncCalls, Is.EqualTo(1));
            Assert.That(tree.SetAsyncCalls, Is.EqualTo(1), "the covered marker is written once");
            Assert.That(tree.SetManyAsyncCalls, Is.EqualTo(3));
        });
        await coordinator.Received(1).EnsureScheduleAsync();
    }

    [Test]
    public async Task Multi_tree_handles_still_probe_each_distinct_tree_once()
    {
        var (tree, index, _) = CreateMultiTree();

        await index.Tree("tree-a").Key("k1").AddAsync(["red"]);
        await index.Tree("tree-b").Key("k1").AddAsync(["red"]);
        await index.Tree("tree-a").Key("k2").AddAsync(["red"]);
        await index.Tree("tree-b").Key("k2").AddAsync(["red"]);

        Assert.Multiple(() =>
        {
            Assert.That(tree.TreeExistsAsyncCalls, Is.EqualTo(2));
            Assert.That(tree.SetAsyncCalls, Is.EqualTo(2), "one covered marker per distinct tree");
        });
    }

    [Test]
    public async Task Multi_tree_handle_rejects_an_unregistered_tree_without_caching_it()
    {
        var (tree, index, _) = CreateMultiTree();
        tree.TreeExists = false;

        Assert.That(
            async () => await index.Tree(TreeId).Key("k1").AddAsync(["red"]),
            Throws.InvalidOperationException);

        tree.TreeExists = true;
        await index.Tree(TreeId).Key("k1").AddAsync(["red"]);

        Assert.That(tree.TreeExistsAsyncCalls, Is.EqualTo(2), "a refused tree must be probed again");
    }

    // ── An eventual value+tags write needs no existence probe ──

    [Test]
    public async Task Eventual_value_with_tags_write_skips_the_existence_probe()
    {
        var (tree, index, _) = CreateMultiTree();

        await index.Tree(TreeId).SetValueWithTags("k1", [1, 2, 3], "red", "blue").CommitAsync();

        await Assert.MultipleAsync(async () =>
        {
            // The value write registers the tree, so the probe the tag write
            // used to issue straight after it could only answer "registered".
            Assert.That(tree.TreeExistsAsyncCalls, Is.Zero);
            Assert.That(await index.Tree(TreeId).Key("k1").GetAsync(), Is.EquivalentTo(new[] { "red", "blue" }));
        });
    }

    [Test]
    public async Task Eventual_value_with_tags_write_does_not_accept_the_tree_when_the_value_write_fails()
    {
        var (tree, index, _) = CreateMultiTree();
        tree.Lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("value write failed")));
        tree.TreeExists = false;

        Assert.That(
            async () => await index.Tree(TreeId).SetValueWithTags("k1", [1], "red").CommitAsync(),
            Throws.InvalidOperationException.With.Message.EqualTo("value write failed"));

        // The failed value write proved nothing, so a plain tag write still
        // probes - and is refused for the unregistered tree.
        Assert.That(
            async () => await index.Tree(TreeId).Key("k1").AddAsync(["red"]),
            Throws.InvalidOperationException.With.Message.Contains("is not registered"));
        Assert.That(tree.TreeExistsAsyncCalls, Is.EqualTo(1));
    }

    // ── SetAsync overlaps its add and its removal ──

    [Test]
    public async Task Set_issues_its_add_and_its_removal_concurrently()
    {
        var (tree, index) = Create();
        await index.Key("k1").AddAsync(["red", "green"]);

        // The add's batched write does not complete until a removal delete has
        // arrived. Run one after the other, the add would wait forever for a
        // removal that is only issued once it finishes.
        var deleteArrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        tree.DeleteGate = () =>
        {
            deleteArrived.TrySetResult();
            return Task.CompletedTask;
        };
        tree.SetManyGate = () => deleteArrived.Task;

        var set = index.Key("k1").SetAsync(["green", "blue"]);
        var finished = await Task.WhenAny(set, Task.Delay(TimeSpan.FromSeconds(30)));

        Assert.That(finished, Is.SameAs(set), "the add and the removal did not overlap");
        await set;
        tree.SetManyGate = null;
        tree.DeleteGate = null;

        Assert.That(await index.Key("k1").GetAsync(), Is.EquivalentTo(new[] { "green", "blue" }));
    }

    [Test]
    public async Task Set_rejects_an_invalid_new_tag_before_removing_any_row()
    {
        var (tree, index) = Create();
        await index.Key("k1").AddAsync(["red", "green"]);

        Assert.That(
            async () => await index.Key("k1").SetAsync(["green", "bl\0ue"]),
            Throws.ArgumentException);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(tree.DeleteAsyncCalls, Is.Zero);
            Assert.That(await index.Key("k1").GetAsync(), Is.EquivalentTo(new[] { "red", "green" }));
        });
    }

    [Test]
    public async Task Set_rejects_an_unregistered_tree_before_removing_any_row()
    {
        var (tree, index, _) = CreateMultiTree();
        await index.Tree(TreeId).Key("k1").AddAsync(["red", "green"]);

        // A fresh index has no cached acceptance, so the replacement must probe.
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(tree.Lattice);
        var fresh = LatticeTagIndexContext.CreateMultiTree(grainFactory, IndexName, allowlist: null);
        tree.TreeExists = false;

        Assert.That(
            async () => await fresh.Tree(TreeId).Key("k1").SetAsync(["green", "blue"]),
            Throws.InvalidOperationException);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(tree.DeleteAsyncCalls, Is.Zero);
            Assert.That(await index.Tree(TreeId).Key("k1").GetAsync(), Is.EquivalentTo(new[] { "red", "green" }));
        });
    }
}
