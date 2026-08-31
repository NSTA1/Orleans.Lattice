using Orleans.TestingHost;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration coverage for the tag index (associate tags with keys and query
/// by tag) against a live in-memory cluster: per-key tag CRUD, intersection /
/// union queries, combined value+tags writes (eventual and atomic), on-demand
/// reconcile, and the multi-tree surface.
/// </summary>
[TestFixture]
[Category("Integration")]
public class LatticeTagIndexIntegrationTests
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

    private ILatticeMultiTreeTagIndex MultiTreeTagIndex(string name, IReadOnlyCollection<string>? allowedTrees = null) =>
        new DefaultLatticeTagIndexFactory(_cluster.GrainFactory, FakeLatticeReplicationContext.Disabled).CreateMultiTree(name, allowedTrees);

    private static async Task<List<string>> Collect(ILatticeTagQuery query)
    {
        var list = new List<string>();
        await foreach (var key in query)
        {
            list.Add(key);
        }
        list.Sort(StringComparer.Ordinal);
        return list;
    }

    private static async Task<List<TaggedKey>> Collect(ILatticeMultiTreeTagQuery query)
    {
        var list = new List<TaggedKey>();
        await foreach (var key in query)
        {
            list.Add(key);
        }
        return list;
    }

    [Test]
    public async Task Add_and_query_single_tag_returns_the_key()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        var idx = TagIndex(tree, $"colors-{sfx}");

        await idx.Key("a").AddAsync(["red"]);

        Assert.That(await Collect(idx.WithAnyTags("red")), Is.EqualTo(new[] { "a" }));
    }

    [Test]
    public async Task WithAllTags_returns_only_keys_carrying_every_tag()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        await tree.SetAsync("b", Bytes("1"));
        var idx = TagIndex(tree, $"colors-{sfx}");

        await idx.Key("a").AddAsync(["red", "round"]);
        await idx.Key("b").AddAsync(["red"]);

        Assert.That(await Collect(idx.WithAllTags("red", "round")), Is.EqualTo(new[] { "a" }));
    }

    [Test]
    public async Task WithAnyTags_unions_and_dedupes()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        await tree.SetAsync("b", Bytes("1"));
        var idx = TagIndex(tree, $"colors-{sfx}");

        await idx.Key("a").AddAsync(["red", "round"]);
        await idx.Key("b").AddAsync(["round"]);

        Assert.That(await Collect(idx.WithAnyTags("red", "round")), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task Key_AddAsync_deduplicates_repeated_tags_in_one_write()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        var idx = TagIndex(tree, $"colors-{sfx}");

        // A single write carrying duplicate tags (and mixed order) must be
        // normalized to the distinct set - this exercises NormalizeTags, whose
        // small-set path dedups with an ordinal linear scan rather than a
        // HashSet. The stored, sorted result is the distinct set regardless of
        // input duplication or order.
        await idx.Key("a").AddAsync(["red", "round", "red", "round", "red"]);

        Assert.That(await idx.Key("a").GetAsync(), Is.EqualTo(new[] { "red", "round" }));
        Assert.That(await idx.WithAnyTags("red", "round").CountAsync(), Is.EqualTo(1));
    }

    [Test]
    public async Task Key_GetAsync_returns_current_tags_sorted()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        var idx = TagIndex(tree, $"colors-{sfx}");

        await idx.Key("a").AddAsync(["round", "red"]);

        Assert.That(await idx.Key("a").GetAsync(), Is.EqualTo(new[] { "red", "round" }));
    }

    [Test]
    public async Task Key_SetAsync_replaces_the_tag_set()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        var idx = TagIndex(tree, $"colors-{sfx}");

        await idx.Key("a").AddAsync(["red", "round"]);
        await idx.Key("a").SetAsync(["green"]);

        Assert.That(await idx.Key("a").GetAsync(), Is.EqualTo(new[] { "green" }));
        Assert.That(await idx.WithAnyTags("red", "round").CountAsync(), Is.Zero);
    }

    [Test]
    public async Task Key_RemoveAsync_removes_one_tag()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        var idx = TagIndex(tree, $"colors-{sfx}");

        await idx.Key("a").AddAsync(["red", "round"]);
        await idx.Key("a").RemoveAsync(["red"]);

        Assert.That(await idx.Key("a").GetAsync(), Is.EqualTo(new[] { "round" }));
    }

    [Test]
    public async Task CountAsync_counts_matches()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        await tree.SetAsync("b", Bytes("1"));
        var idx = TagIndex(tree, $"colors-{sfx}");

        await idx.Key("a").AddAsync(["red"]);
        await idx.Key("b").AddAsync(["red"]);

        Assert.That(await idx.WithAllTags("red").CountAsync(), Is.EqualTo(2));
    }

    [Test]
    public async Task SetValueWithTags_eventual_writes_value_and_tags()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        var idx = TagIndex(tree, $"colors-{sfx}");

        await idx.SetValueWithTags("c", Bytes("v"), "blue").CommitAsync();

        Assert.That(await tree.GetAsync("c"), Is.EqualTo(Bytes("v")));
        Assert.That(await idx.Key("c").GetAsync(), Is.EqualTo(new[] { "blue" }));
        Assert.That(await Collect(idx.WithAllTags("blue")), Is.EqualTo(new[] { "c" }));
    }

    [Test]
    public async Task SetValueWithTags_atomic_writes_value_and_tags()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        var idx = TagIndex(tree, $"colors-{sfx}");

        await idx.SetValueWithTags("c", Bytes("v"), "blue", "small").Atomic().CommitAsync();

        Assert.That(await tree.GetAsync("c"), Is.EqualTo(Bytes("v")));
        Assert.That(await idx.Key("c").GetAsync(), Is.EqualTo(new[] { "blue", "small" }));
    }

    [Test]
    public async Task ReconcileAsync_removes_orphan_rows_for_deleted_keys()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("d", Bytes("1"));
        var idx = TagIndex(tree, $"colors-{sfx}");
        await idx.Key("d").AddAsync(["red"]);

        await tree.DeleteAsync("d");
        var report = await idx.ReconcileAsync();

        Assert.That(report.OrphanRowsRemoved, Is.GreaterThanOrEqualTo(1));
        Assert.That(await idx.WithAnyTags("red").CountAsync(), Is.Zero);
    }

    [Test]
    public async Task ReconcileAsync_is_idempotent()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        var idx = TagIndex(tree, $"colors-{sfx}");
        await idx.Key("a").AddAsync(["red"]);

        await idx.ReconcileAsync();
        var second = await idx.ReconcileAsync();

        Assert.That(second.OrphanRowsRemoved, Is.Zero);
        Assert.That(await idx.WithAnyTags("red").CountAsync(), Is.EqualTo(1));
    }

    [Test]
    public async Task MultiTree_query_yields_tagged_keys_across_trees()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var indexName = $"colors-{sfx}";
        var t1 = $"items1-{sfx}";
        var t2 = $"items2-{sfx}";
        var tree1 = Tree(t1);
        var tree2 = Tree(t2);
        await tree1.SetAsync("a", Bytes("1"));
        await tree2.SetAsync("b", Bytes("1"));

        await TagIndex(tree1, indexName).Key("a").AddAsync(["red"]);
        await TagIndex(tree2, indexName).Key("b").AddAsync(["red"]);

        var multi = MultiTreeTagIndex(indexName);
        var hits = await Collect(multi.WithAnyTags("red"));

        Assert.That(hits, Does.Contain(new TaggedKey(t1, "a")));
        Assert.That(hits, Does.Contain(new TaggedKey(t2, "b")));
    }

    [Test]
    public async Task MultiTree_InTree_narrows_to_one_tree()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var indexName = $"colors-{sfx}";
        var t1 = $"items1-{sfx}";
        var t2 = $"items2-{sfx}";
        var tree1 = Tree(t1);
        var tree2 = Tree(t2);
        await tree1.SetAsync("a", Bytes("1"));
        await tree2.SetAsync("b", Bytes("1"));
        await TagIndex(tree1, indexName).Key("a").AddAsync(["red"]);
        await TagIndex(tree2, indexName).Key("b").AddAsync(["red"]);

        var multi = MultiTreeTagIndex(indexName);
        var hits = await Collect(multi.WithAnyTags("red").InTree(t1));

        Assert.That(hits, Is.EqualTo(new[] { new TaggedKey(t1, "a") }));
    }

    [Test]
    public async Task CoveredTreesAsync_includes_written_trees()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var indexName = $"colors-{sfx}";
        var t1 = $"items1-{sfx}";
        var tree1 = Tree(t1);
        await tree1.SetAsync("a", Bytes("1"));
        await TagIndex(tree1, indexName).Key("a").AddAsync(["red"]);

        var multi = MultiTreeTagIndex(indexName);
        Assert.That(await multi.CoveredTreesAsync(), Does.Contain(t1));
    }

    [Test]
    public void Tag_containing_nul_separator_is_rejected()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        var idx = TagIndex(tree, $"colors-{sfx}");
        Assert.That(() => idx.WithAllTags("bad\0tag"), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void AddAsync_on_unregistered_tree_in_open_mode_throws()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"never-written-{sfx}");
        var idx = TagIndex(tree, $"colors-{sfx}");
        Assert.That(async () => await idx.Key("z").AddAsync(["red"]), Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Closed_allowlist_rejects_unlisted_tree()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var treeId = $"items-{sfx}";
        Tree(treeId);
        var idx = MultiTreeTagIndex($"colors-{sfx}", allowedTrees: new[] { "some-other-tree" })
            .Tree(treeId);
        Assert.That(async () => await idx.Key("z").AddAsync(["red"]), Throws.InstanceOf<ArgumentException>());
    }

    private static async Task<List<string>> Collect(IAsyncEnumerable<string> tags)
    {
        var list = new List<string>();
        await foreach (var tag in tags)
        {
            list.Add(tag);
        }
        return list;
    }

    [Test]
    public async Task TagsAsync_lists_distinct_tags_in_subject_tree()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        await tree.SetAsync("b", Bytes("1"));
        var idx = TagIndex(tree, $"colors-{sfx}");

        await idx.Key("a").AddAsync(["red", "round"]);
        await idx.Key("b").AddAsync(["red", "blue"]);

        Assert.That(await Collect(idx.TagsAsync()), Is.EqualTo(new[] { "blue", "red", "round" }));
    }

    [Test]
    public async Task TagsAsync_excludes_tags_from_other_trees()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var indexName = $"colors-{sfx}";
        var t1 = $"items1-{sfx}";
        var t2 = $"items2-{sfx}";
        var tree1 = Tree(t1);
        var tree2 = Tree(t2);
        await tree1.SetAsync("a", Bytes("1"));
        await tree2.SetAsync("b", Bytes("1"));
        await TagIndex(tree1, indexName).Key("a").AddAsync(["red"]);
        await TagIndex(tree2, indexName).Key("b").AddAsync(["green"]);

        Assert.That(await Collect(TagIndex(tree1, indexName).TagsAsync()), Is.EqualTo(new[] { "red" }));
    }

    [Test]
    public async Task MultiTree_TagsAsync_lists_distinct_tags_across_trees()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var indexName = $"colors-{sfx}";
        var t1 = $"items1-{sfx}";
        var t2 = $"items2-{sfx}";
        var tree1 = Tree(t1);
        var tree2 = Tree(t2);
        await tree1.SetAsync("a", Bytes("1"));
        await tree2.SetAsync("b", Bytes("1"));
        await TagIndex(tree1, indexName).Key("a").AddAsync(["red"]);
        await TagIndex(tree2, indexName).Key("b").AddAsync(["green"]);

        var multi = MultiTreeTagIndex(indexName);
        var tags = await Collect(multi.TagsAsync());
        Assert.That(tags, Is.EqualTo(new[] { "green", "red" }));
    }

    [Test]
    public async Task ReconcileAsync_honors_key_range_bounds()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        await tree.SetAsync("m", Bytes("1"));
        var idx = TagIndex(tree, $"colors-{sfx}");
        await idx.Key("a").AddAsync(["red"]);
        await idx.Key("m").AddAsync(["red"]);

        // Delete both keys but reconcile only the ["a","b") range: only "a" is
        // in range, so only its orphan row is removed.
        await tree.DeleteAsync("a");
        await tree.DeleteAsync("m");
        var report = await idx.ReconcileAsync("a", "b");

        Assert.That(report.OrphanRowsRemoved, Is.EqualTo(1));
        Assert.That(await Collect(idx.WithAnyTags("red")), Is.EqualTo(new[] { "m" }));
    }

    [Test]
    public async Task ReconcileAsync_keeps_rows_for_live_keys_within_range()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        var idx = TagIndex(tree, $"colors-{sfx}");
        await idx.Key("a").AddAsync(["red"]);

        var report = await idx.ReconcileAsync();

        Assert.That(report.OrphanRowsRemoved, Is.Zero);
        Assert.That(await Collect(idx.WithAnyTags("red")), Is.EqualTo(new[] { "a" }));
    }

    [Test]
    public async Task MultiTree_CountAsync_counts_across_trees()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var indexName = $"colors-{sfx}";
        var t1 = $"items1-{sfx}";
        var t2 = $"items2-{sfx}";
        var tree1 = Tree(t1);
        var tree2 = Tree(t2);
        await tree1.SetAsync("a", Bytes("1"));
        await tree2.SetAsync("b", Bytes("1"));
        await TagIndex(tree1, indexName).Key("a").AddAsync(["red"]);
        await TagIndex(tree2, indexName).Key("b").AddAsync(["red"]);

        var multi = MultiTreeTagIndex(indexName);
        Assert.That(await multi.WithAnyTags("red").CountAsync(), Is.EqualTo(2));
    }

    [Test]
    public async Task SetValueWithTags_eventual_is_explicit_default()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        var idx = TagIndex(tree, $"colors-{sfx}");

        await idx.SetValueWithTags("c", Bytes("v"), "blue").Eventual().CommitAsync();

        Assert.That(await tree.GetAsync("c"), Is.EqualTo(Bytes("v")));
        Assert.That(await idx.Key("c").GetAsync(), Is.EqualTo(new[] { "blue" }));
    }

    [Test]
    public async Task MultiTree_Tree_narrows_back_to_single_tree_keys()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var indexName = $"colors-{sfx}";
        var t1 = $"items1-{sfx}";
        var t2 = $"items2-{sfx}";
        var tree1 = Tree(t1);
        var tree2 = Tree(t2);
        await tree1.SetAsync("a", Bytes("1"));
        await tree2.SetAsync("b", Bytes("1"));
        await TagIndex(tree1, indexName).Key("a").AddAsync(["red"]);
        await TagIndex(tree2, indexName).Key("b").AddAsync(["red"]);

        var multi = MultiTreeTagIndex(indexName);
        ILatticeTagIndex scoped = multi.Tree(t1);
        Assert.That(await Collect(scoped.WithAnyTags("red")), Is.EqualTo(new[] { "a" }));
    }

    [Test]
    public void MultiTree_Tree_rejects_tree_id_containing_nul()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var multi = MultiTreeTagIndex($"colors-{sfx}");
        Assert.That(() => multi.Tree("bad\0tree"), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void MultiTree_InTree_rejects_tree_id_containing_nul()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var multi = MultiTreeTagIndex($"colors-{sfx}");
        Assert.That(() => multi.WithAnyTags("red").InTree("bad\0tree"), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task CoveredTrees_survive_concurrent_first_writes_from_different_trees()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var indexName = $"colors-{sfx}";
        var ids = Enumerable.Range(0, 8).Select(i => $"items{i}-{sfx}").ToArray();

        // Drive the first tagging write for several distinct subject trees
        // concurrently: each registers its own covered-tree marker, so none may
        // be lost to a read-modify-write race on a shared hint blob.
        await Task.WhenAll(ids.Select(async id =>
        {
            var tree = Tree(id);
            await tree.SetAsync("a", Bytes("1"));
            await TagIndex(tree, indexName).Key("a").AddAsync(["red"]);
        }));

        var multi = MultiTreeTagIndex(indexName);
        var covered = await multi.CoveredTreesAsync();
        Assert.That(covered, Is.SupersetOf(ids));
    }

    [Test]
    public async Task Key_GetAsync_isolates_keys_that_share_a_prefix()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        await tree.SetAsync("ab", Bytes("1"));
        var idx = TagIndex(tree, $"colors-{sfx}");

        await idx.Key("a").AddAsync(["red"]);
        await idx.Key("ab").AddAsync(["blue", "green"]);

        // The key-major lookup for "a" must not leak the tags of "ab".
        Assert.That(await idx.Key("a").GetAsync(), Is.EqualTo(new[] { "red" }));
        Assert.That(await idx.Key("ab").GetAsync(), Is.EqualTo(new[] { "blue", "green" }));
    }

    [Test]
    public async Task Key_GetAsync_isolates_a_key_that_embeds_the_separator()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        await tree.SetAsync("a\0b", Bytes("1"));
        var idx = TagIndex(tree, $"colors-{sfx}");

        await idx.Key("a").AddAsync(["red"]);
        await idx.Key("a\0b").AddAsync(["blue"]);

        // "a\0b" falls inside the prefix range for "a"; the exact full-key
        // comparison must still keep their tag sets apart.
        Assert.That(await idx.Key("a").GetAsync(), Is.EqualTo(new[] { "red" }));
        Assert.That(await idx.Key("a\0b").GetAsync(), Is.EqualTo(new[] { "blue" }));
    }

    [Test]
    public async Task Reconcile_removes_both_directions_so_key_lookup_is_clean()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("d", Bytes("1"));
        var idx = TagIndex(tree, $"colors-{sfx}");
        await idx.Key("d").AddAsync(["red", "round"]);

        await tree.DeleteAsync("d");
        await idx.ReconcileAsync();

        // The orphan sweep must drop the key-major mirror too, not just the
        // tag-major rows, so a subsequent key lookup sees nothing.
        Assert.That(await idx.Key("d").GetAsync(), Is.Empty);
        Assert.That(await idx.WithAnyTags("red", "round").CountAsync(), Is.Zero);
    }

    // ── Flag membership single-cluster round-trips ───────────────────

    [Test]
    public async Task OrFlag_membership_add_query_and_remove_roundtrips()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        var idx = new DefaultLatticeTagIndexFactory(
            _cluster.GrainFactory,
            FakeLatticeReplicationContext.Enabled("site-a", LatticeMergeMode.OrFlag))
            .Create(tree, $"colors-{sfx}");

        await idx.Key("a").AddAsync(["red", "round"]);
        Assert.That(await Collect(idx.WithAnyTags("red", "round")), Is.EqualTo(new[] { "a" }));
        Assert.That(await idx.Key("a").GetAsync(), Is.EqualTo(new[] { "red", "round" }));

        await idx.Key("a").RemoveAsync(["red"]);
        // A disabled flag leaves a present-but-tombstoned row; the read path
        // must decode flag state and treat it as absent.
        Assert.That(await idx.Key("a").GetAsync(), Is.EqualTo(new[] { "round" }));
        Assert.That(await idx.WithAnyTags("red").CountAsync(), Is.Zero);
        Assert.That(await idx.WithAnyTags("round").CountAsync(), Is.EqualTo(1));
    }

    [Test]
    public async Task RwFlag_membership_add_query_and_remove_roundtrips()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        var idx = new DefaultLatticeTagIndexFactory(
            _cluster.GrainFactory,
            FakeLatticeReplicationContext.Enabled("site-a", LatticeMergeMode.RwFlag))
            .Create(tree, $"colors-{sfx}");

        await idx.Key("a").AddAsync(["red", "round"]);
        Assert.That(await Collect(idx.WithAnyTags("red", "round")), Is.EqualTo(new[] { "a" }));

        await idx.Key("a").RemoveAsync(["red"]);
        Assert.That(await idx.Key("a").GetAsync(), Is.EqualTo(new[] { "round" }));
        Assert.That(await idx.WithAnyTags("red").CountAsync(), Is.Zero);
    }

    [Test]
    public async Task OrFlag_membership_reconcile_removes_orphan_rows()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("d", Bytes("1"));
        var idx = new DefaultLatticeTagIndexFactory(
            _cluster.GrainFactory,
            FakeLatticeReplicationContext.Enabled("site-a", LatticeMergeMode.OrFlag))
            .Create(tree, $"colors-{sfx}");
        await idx.Key("d").AddAsync(["red", "round"]);

        await tree.DeleteAsync("d");
        var report = await idx.ReconcileAsync();

        Assert.That(report.OrphanRowsRemoved, Is.GreaterThanOrEqualTo(1));
        Assert.That(await idx.Key("d").GetAsync(), Is.Empty);
        Assert.That(await idx.WithAnyTags("red", "round").CountAsync(), Is.Zero);
    }

    [Test]
    public async Task OrFlag_membership_SetValueWithTags_eventual_writes_value_and_tags()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        var idx = new DefaultLatticeTagIndexFactory(
            _cluster.GrainFactory,
            FakeLatticeReplicationContext.Enabled("site-a", LatticeMergeMode.OrFlag))
            .Create(tree, $"colors-{sfx}");

        await idx.SetValueWithTags("c", Bytes("v"), "blue").CommitAsync();

        Assert.That(await tree.GetAsync("c"), Is.EqualTo(Bytes("v")));
        Assert.That(await idx.Key("c").GetAsync(), Is.EqualTo(new[] { "blue" }));
        Assert.That(await Collect(idx.WithAllTags("blue")), Is.EqualTo(new[] { "c" }));
    }

    [Test]
    public async Task OrFlag_membership_SetValueWithTags_atomic_couples_value_and_membership_rows()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var treeId = $"items-{sfx}";
        var indexName = $"colors-{sfx}";
        var tree = Tree(treeId);
        var idx = new DefaultLatticeTagIndexFactory(
            _cluster.GrainFactory,
            FakeLatticeReplicationContext.Enabled("site-a", LatticeMergeMode.OrFlag))
            .Create(tree, indexName);

        // Under a flag membership mode the atomic value+tags write couples the
        // value and its membership rows all-or-nothing: each membership row is
        // staged with a freshly minted flag-enable delta rather than a plain
        // presence write, so the value and tags land together.
        await idx.SetValueWithTags("c", Bytes("v"), "blue", "small").Atomic().CommitAsync();

        Assert.That(await tree.GetAsync("c"), Is.EqualTo(Bytes("v")));
        Assert.That(await idx.Key("c").GetAsync(), Is.EqualTo(new[] { "blue", "small" }));

        // The atomic path took the flag-delta route, not a plain presence write:
        // each membership row stores a serialised flag state carrying exactly one
        // enable dot authored by the local replica.
        var indexTree = Tree($"tag-{indexName}");
        var blueRow = await indexTree.OrFlag($"blue\0{treeId}\0c").GetAsync();
        Assert.That(blueRow.IsEnabled, Is.True);
        Assert.That(blueRow.Enables, Has.Count.EqualTo(1));
        Assert.That(blueRow.Enables[0].ReplicaId, Is.EqualTo("site-a"));
        var smallRow = await indexTree.OrFlag($"small\0{treeId}\0c").GetAsync();
        Assert.That(smallRow.IsEnabled, Is.True);
        Assert.That(smallRow.Enables, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task RwFlag_membership_SetValueWithTags_atomic_couples_value_and_membership_rows()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var treeId = $"items-{sfx}";
        var indexName = $"colors-{sfx}";
        var tree = Tree(treeId);
        var idx = new DefaultLatticeTagIndexFactory(
            _cluster.GrainFactory,
            FakeLatticeReplicationContext.Enabled("site-a", LatticeMergeMode.RwFlag))
            .Create(tree, indexName);

        await idx.SetValueWithTags("c", Bytes("v"), "blue", "small").Atomic().CommitAsync();

        Assert.That(await tree.GetAsync("c"), Is.EqualTo(Bytes("v")));
        Assert.That(await idx.Key("c").GetAsync(), Is.EqualTo(new[] { "blue", "small" }));

        var indexTree = Tree($"tag-{indexName}");
        var blueRow = await indexTree.RwFlag($"blue\0{treeId}\0c").GetAsync();
        Assert.That(blueRow.IsEnabled, Is.True);
        Assert.That(blueRow.Enables, Has.Count.EqualTo(1));
        Assert.That(blueRow.Enables[0].ReplicaId, Is.EqualTo("site-a"));
    }

    [Test]
    public async Task OrFlag_membership_atomic_then_remove_converges_enable_wins()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var treeId = $"items-{sfx}";
        var indexName = $"colors-{sfx}";
        var tree = Tree(treeId);
        var idx = new DefaultLatticeTagIndexFactory(
            _cluster.GrainFactory,
            FakeLatticeReplicationContext.Enabled("site-a", LatticeMergeMode.OrFlag))
            .Create(tree, indexName);

        // An atomic enable followed by a removal of one tag: the removed tag's
        // row decodes as absent (its enable dot is tombstoned) while the other
        // tag survives, proving the atomic enable and the eventual disable share
        // the same flag-CRDT lattice.
        await idx.SetValueWithTags("c", Bytes("v"), "blue", "small").Atomic().CommitAsync();
        await idx.Key("c").RemoveAsync(["blue"]);

        Assert.That(await idx.Key("c").GetAsync(), Is.EqualTo(new[] { "small" }));
        Assert.That(await idx.WithAnyTags("blue").CountAsync(), Is.Zero);
        Assert.That(await idx.WithAnyTags("small").CountAsync(), Is.EqualTo(1));
    }

    [Test]
    public async Task OrFlag_membership_concurrent_atomic_writers_converge_on_overlapping_tags()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var treeId = $"items-{sfx}";
        var indexName = $"colors-{sfx}";
        var tree = Tree(treeId);
        var idxA = new DefaultLatticeTagIndexFactory(
            _cluster.GrainFactory,
            FakeLatticeReplicationContext.Enabled("site-a", LatticeMergeMode.OrFlag))
            .Create(tree, indexName);
        var idxB = new DefaultLatticeTagIndexFactory(
            _cluster.GrainFactory,
            FakeLatticeReplicationContext.Enabled("site-b", LatticeMergeMode.OrFlag))
            .Create(tree, indexName);

        // Two writers author an overlapping tag ("shared") on the same key via
        // independent atomic writes. Within this single cluster the value write
        // is a plain last-writer-wins store, so the locally-retained "shared"
        // row keeps one writer's enable dot; cross-cluster delta replication is
        // what unions both replicas' dots in a real active-active deployment.
        // The enable-wins invariant the test pins is the one observable here: an
        // enable dot survives the race, so the membership row stays live and the
        // key is returned. The two non-overlapping tags each land exactly once.
        await Task.WhenAll(
            idxA.SetValueWithTags("c", Bytes("va"), "shared", "a-only").Atomic().CommitAsync(),
            idxB.SetValueWithTags("c", Bytes("vb"), "shared", "b-only").Atomic().CommitAsync());

        Assert.That(await idxA.WithAnyTags("shared").CountAsync(), Is.EqualTo(1));
        Assert.That(await idxA.Key("c").GetAsync(), Is.EqualTo(new[] { "a-only", "b-only", "shared" }));

        var indexTree = Tree($"tag-{indexName}");
        var sharedRow = await indexTree.OrFlag($"shared\0{treeId}\0c").GetAsync();
        Assert.That(sharedRow.IsEnabled, Is.True);
        Assert.That(sharedRow.Enables, Is.Not.Empty);
    }

    [Test]
    public async Task OrFlag_membership_repeated_atomic_enable_does_not_accumulate_duplicate_dots()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var treeId = $"items-{sfx}";
        var indexName = $"colors-{sfx}";
        var tree = Tree(treeId);
        var idx = new DefaultLatticeTagIndexFactory(
            _cluster.GrainFactory,
            FakeLatticeReplicationContext.Enabled("site-a", LatticeMergeMode.OrFlag))
            .Create(tree, indexName);

        // Each atomic enable mints a fresh monotonic dot from the local replica;
        // re-asserting the same tag advances the counter but never leaves stale
        // duplicate live dots once tombstones cancel superseded enables. The row
        // stays enabled and the membership read is stable across repeats.
        await idx.SetValueWithTags("c", Bytes("v1"), "blue").Atomic().CommitAsync();
        await idx.SetValueWithTags("c", Bytes("v2"), "blue").Atomic().CommitAsync();

        Assert.That(await tree.GetAsync("c"), Is.EqualTo(Bytes("v2")));
        Assert.That(await idx.Key("c").GetAsync(), Is.EqualTo(new[] { "blue" }));
        Assert.That(await idx.WithAnyTags("blue").CountAsync(), Is.EqualTo(1));

        var indexTree = Tree($"tag-{indexName}");
        var blueRow = await indexTree.OrFlag($"blue\0{treeId}\0c").GetAsync();
        Assert.That(blueRow.IsEnabled, Is.True);
        // Both enable dots are authored by the same replica with distinct
        // monotonic counters; no two enable dots collide.
        var distinctDots = blueRow.Enables.Distinct().Count();
        Assert.That(distinctDots, Is.EqualTo(blueRow.Enables.Count));
    }

    [Test]
    public async Task LwwRegister_membership_SetValueWithTags_atomic_is_unchanged()
    {
        // Regression: the LwwRegister (default) atomic path stages plain presence
        // writes exactly as before - no flag deltas, value and tags couple.
        var sfx = Guid.NewGuid().ToString("N");
        var tree = Tree($"items-{sfx}");
        var idx = TagIndex(tree, $"colors-{sfx}");

        await idx.SetValueWithTags("c", Bytes("v"), "blue", "small").Atomic().CommitAsync();

        Assert.That(await tree.GetAsync("c"), Is.EqualTo(Bytes("v")));
        Assert.That(await idx.Key("c").GetAsync(), Is.EqualTo(new[] { "blue", "small" }));
        Assert.That(await Collect(idx.WithAllTags("blue", "small")), Is.EqualTo(new[] { "c" }));
    }
}
