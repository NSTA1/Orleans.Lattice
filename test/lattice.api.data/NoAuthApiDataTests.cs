namespace Orleans.Lattice.Api.Data.Tests;

/// <summary>
/// Proves the data API is zero-cost and behaviour-identical when the auth add-on
/// is not registered: with no access gate active, every write, delete, atomic
/// batch, cross-tree batch, point read, and bounded range read just works for an
/// unauthenticated (anonymous) caller, exactly as the in-cluster client would
/// observe. The opt-in enforcement of the sibling auth fixture is entirely
/// absent here.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class NoAuthApiDataTests
{
    private NoAuthApiDataClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new NoAuthApiDataClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    [Test]
    public async Task set_then_get_round_trips_without_auth()
    {
        const string tree = "noauth-set-get";
        await _fixture.RegisterTreeAsync(tree);

        await _fixture.Api.SetAsync(tree, "k1", new byte[] { 1, 2, 3 });
        var result = await _fixture.Api.GetAsync(tree, "k1");

        Assert.Multiple(() =>
        {
            Assert.That(result.Found, Is.True);
            Assert.That(result.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
        });
    }

    [Test]
    public async Task delete_removes_the_value_without_auth()
    {
        const string tree = "noauth-delete";
        await _fixture.RegisterTreeAsync(tree);

        await _fixture.Api.SetAsync(tree, "k1", new byte[] { 9 });
        var removed = await _fixture.Api.DeleteAsync(tree, "k1");
        var afterwards = await _fixture.Api.GetAsync(tree, "k1");

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.True);
            Assert.That(afterwards.Found, Is.False);
        });
    }

    [Test]
    public async Task atomic_batch_commits_without_auth()
    {
        const string tree = "noauth-atomic";
        await _fixture.RegisterTreeAsync(tree);

        var batch = new DataAtomicBatch
        {
            Upserts =
            [
                new DataEntry { Key = "a", Value = new byte[] { 1 } },
                new DataEntry { Key = "b", Value = new byte[] { 2 } },
            ],
        };

        await _fixture.Api.SetManyAtomicAsync(tree, batch, "noauth-op-1");

        Assert.Multiple(() =>
        {
            Assert.That(_fixture.Api.GetAsync(tree, "a").Result.Value, Is.EqualTo(new byte[] { 1 }));
            Assert.That(_fixture.Api.GetAsync(tree, "b").Result.Value, Is.EqualTo(new byte[] { 2 }));
        });
    }

    [Test]
    public async Task cross_tree_atomic_commits_without_auth()
    {
        const string treeA = "noauth-xt-a";
        const string treeB = "noauth-xt-b";
        await _fixture.RegisterTreeAsync(treeA);
        await _fixture.RegisterTreeAsync(treeB);

        var batches = new List<DataTreeBatch>
        {
            new() { TreeId = treeA, Upserts = [new DataEntry { Key = "k", Value = new byte[] { 1 } }] },
            new() { TreeId = treeB, Upserts = [new DataEntry { Key = "k", Value = new byte[] { 2 } }] },
        };

        var outcome = await _fixture.Api.SetManyAtomicCrossTreeAsync(batches, "noauth-xt-op-1");

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
            Assert.That(_fixture.Api.GetAsync(treeA, "k").Result.Value, Is.EqualTo(new byte[] { 1 }));
            Assert.That(_fixture.Api.GetAsync(treeB, "k").Result.Value, Is.EqualTo(new byte[] { 2 }));
        });
    }

    [Test]
    public async Task get_on_unknown_tree_reports_miss_and_does_not_register_it()
    {
        // A read must never materialise a tree: probing an unknown tree must report
        // a clean miss without routing into the shard root (which would register the
        // tree and seed its shard roots as a write side-effect of a read).
        const string tree = "noauth-ghost-tree";
        var handle = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        var existedBefore = await handle.TreeExistsAsync();
        var result = await _fixture.Api.GetAsync(tree, "any-key");
        var existsAfter = await handle.TreeExistsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(existedBefore, Is.False);
            Assert.That(result.Found, Is.False, "an unknown tree reports a clean miss");
            Assert.That(existsAfter, Is.False, "a read must not auto-register the tree");
        });
    }

    [Test]
    public async Task bounded_range_read_returns_all_entries_without_auth()
    {
        const string tree = "noauth-range";
        await _fixture.RegisterTreeAsync(tree);

        await _fixture.Api.SetAsync(tree, "a", new byte[] { 1 });
        await _fixture.Api.SetAsync(tree, "b", new byte[] { 2 });
        await _fixture.Api.SetAsync(tree, "c", new byte[] { 3 });

        var page = await _fixture.Api.ReadRangeAsync(new DataRangeRequest { TreeId = tree, PageSize = 100 });

        Assert.That(page.Entries.Select(e => e.Key), Is.EquivalentTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public async Task bounded_range_delete_drains_the_whole_range_without_auth()
    {
        const string tree = "noauth-range-delete";
        await _fixture.RegisterTreeAsync(tree);

        // Seed more keys than a single step so the drain must loop.
        for (var i = 0; i < 10; i++)
        {
            await _fixture.Api.SetAsync(tree, $"k{i:D2}", new byte[] { (byte)i });
        }
        // A key outside the range must survive.
        await _fixture.Api.SetAsync(tree, "zzz", new byte[] { 99 });

        var result = await _fixture.Api.DeleteRangeAsync(
            new DataRangeDeleteRequest { TreeId = tree, StartInclusive = "k00", EndExclusive = "k99" });

        var page = await _fixture.Api.ReadRangeAsync(new DataRangeRequest { TreeId = tree, PageSize = 100 });

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo(tree));
            Assert.That(result.DeletedCount, Is.EqualTo(10));
            Assert.That(page.Entries.Select(e => e.Key), Is.EquivalentTo(new[] { "zzz" }));
        });
    }
}
