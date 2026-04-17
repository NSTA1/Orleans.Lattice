using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class BPlusTreeIntegrationTests
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
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task Set_and_Get_roundtrips_a_value()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("test-tree");
        var value = Encoding.UTF8.GetBytes("hello-world");

        await router.SetAsync("key1", value);
        var result = await router.GetAsync("key1");

        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("hello-world"));
    }

    [Test]
    public async Task Get_returns_null_for_missing_key()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("test-tree-miss");
        var result = await router.GetAsync("nonexistent");
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task Delete_returns_false_for_missing_key()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("test-tree-del-miss");
        var result = await router.DeleteAsync("nonexistent");
        Assert.That(result, Is.False);
    }

    [Test]
    public async Task Delete_removes_a_previously_set_key()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("test-tree-del");
        await router.SetAsync("to-delete", Encoding.UTF8.GetBytes("value"));

        var deleted = await router.DeleteAsync("to-delete");
        Assert.That(deleted, Is.True);

        var result = await router.GetAsync("to-delete");
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task Set_overwrites_existing_value()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("test-tree-overwrite");
        await router.SetAsync("k", Encoding.UTF8.GetBytes("v1"));
        await router.SetAsync("k", Encoding.UTF8.GetBytes("v2"));

        var result = await router.GetAsync("k");
        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("v2"));
    }

    [Test]
    public async Task Multiple_keys_in_same_shard_are_independent()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("test-tree-multi");
        await router.SetAsync("alpha", Encoding.UTF8.GetBytes("a"));
        await router.SetAsync("bravo", Encoding.UTF8.GetBytes("b"));
        await router.SetAsync("charlie", Encoding.UTF8.GetBytes("c"));

        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("alpha"))!), Is.EqualTo("a"));
        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("bravo"))!), Is.EqualTo("b"));
        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("charlie"))!), Is.EqualTo("c"));
    }

    [Test]
    public async Task ExistsAsync_returns_true_for_existing_key()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("test-exists-true");
        await router.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(await router.ExistsAsync("k1"), Is.True);
    }

    [Test]
    public async Task ExistsAsync_returns_false_for_missing_key()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("test-exists-false");

        Assert.That(await router.ExistsAsync("nonexistent"), Is.False);
    }

    [Test]
    public async Task ExistsAsync_returns_false_after_delete()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("test-exists-del");
        await router.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await router.DeleteAsync("k1");

        Assert.That(await router.ExistsAsync("k1"), Is.False);
    }

    [Test]
    public async Task GetManyAsync_returns_all_existing_keys()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("test-getmany");
        await router.SetAsync("a", Encoding.UTF8.GetBytes("va"));
        await router.SetAsync("b", Encoding.UTF8.GetBytes("vb"));
        await router.SetAsync("c", Encoding.UTF8.GetBytes("vc"));

        var result = await router.GetManyAsync(new List<string> { "a", "b", "c", "missing" });

        Assert.That(result.Count, Is.EqualTo(3));
        Assert.That(Encoding.UTF8.GetString(result["a"]), Is.EqualTo("va"));
        Assert.That(Encoding.UTF8.GetString(result["b"]), Is.EqualTo("vb"));
        Assert.That(Encoding.UTF8.GetString(result["c"]), Is.EqualTo("vc"));
        Assert.That(result.ContainsKey("missing"), Is.False);
    }

    [Test]
    public async Task GetManyAsync_returns_empty_for_no_matches()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("test-getmany-empty");

        var result = await router.GetManyAsync(new List<string> { "x", "y" });

        Assert.That(result, Is.Empty);
    }

    [Test]
    public async Task SetManyAsync_writes_and_reads_back()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("test-setmany");
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("s1", Encoding.UTF8.GetBytes("v1")),
            new("s2", Encoding.UTF8.GetBytes("v2")),
            new("s3", Encoding.UTF8.GetBytes("v3")),
        };

        await router.SetManyAsync(entries);

        var result = await router.GetManyAsync(new List<string> { "s1", "s2", "s3" });
        Assert.That(result.Count, Is.EqualTo(3));
        Assert.That(Encoding.UTF8.GetString(result["s1"]), Is.EqualTo("v1"));
        Assert.That(Encoding.UTF8.GetString(result["s2"]), Is.EqualTo("v2"));
        Assert.That(Encoding.UTF8.GetString(result["s3"]), Is.EqualTo("v3"));
    }

    [Test]
    public async Task SetManyAsync_overwrites_existing_values()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("test-setmany-overwrite");
        await router.SetAsync("k1", Encoding.UTF8.GetBytes("old"));

        await router.SetManyAsync(new List<KeyValuePair<string, byte[]>> { new("k1", Encoding.UTF8.GetBytes("new")) });

        var result = await router.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("new"));
    }

    // --- GetOrSetAsync ---

    [Test]
    public async Task GetOrSet_sets_value_when_key_absent()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("getorset-absent");
        var result = await tree.GetOrSetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(result, Is.Null);

        var stored = await tree.GetAsync("k1");
        Assert.That(stored, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(stored!), Is.EqualTo("v1"));
    }

    [Test]
    public async Task GetOrSet_returns_existing_value_without_overwriting()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("getorset-existing");
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("original"));

        var result = await tree.GetOrSetAsync("k1", Encoding.UTF8.GetBytes("ignored"));

        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("original"));

        // Confirm the value was not overwritten.
        var stored = await tree.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(stored!), Is.EqualTo("original"));
    }

    [Test]
    public async Task GetOrSet_sets_value_after_delete()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("getorset-deleted");
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("old"));
        await tree.DeleteAsync("k1");

        var result = await tree.GetOrSetAsync("k1", Encoding.UTF8.GetBytes("new"));

        Assert.That(result, Is.Null);

        var stored = await tree.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(stored!), Is.EqualTo("new"));
    }

    // --- CountAsync ---

    [Test]
    public async Task Count_returns_zero_for_empty_tree()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("count-empty");
        var count = await tree.CountAsync();
        Assert.That(count, Is.EqualTo(0));
    }

    [Test]
    public async Task Count_returns_total_live_keys()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("count-live");
        for (int i = 0; i < 10; i++)
            await tree.SetAsync($"k-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));

        var count = await tree.CountAsync();
        Assert.That(count, Is.EqualTo(10));
    }

    [Test]
    public async Task Count_excludes_deleted_keys()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("count-del");
        for (int i = 0; i < 5; i++)
            await tree.SetAsync($"k-{i}", Encoding.UTF8.GetBytes($"v-{i}"));

        await tree.DeleteAsync("k-2");
        await tree.DeleteAsync("k-4");

        var count = await tree.CountAsync();
        Assert.That(count, Is.EqualTo(3));
    }

    [Test]
    public async Task Count_works_after_bulk_load()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("count-bulk");
        var entries = Enumerable.Range(0, 50)
            .Select(i => KeyValuePair.Create($"b-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}")))
            .ToList();

        await tree.BulkLoadAsync(entries);

        var count = await tree.CountAsync();
        Assert.That(count, Is.EqualTo(50));
    }

    // --- CountPerShardAsync ---

    [Test]
    public async Task CountPerShard_returns_correct_shard_count()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("count-per-shard");
        for (int i = 0; i < 20; i++)
            await tree.SetAsync($"s-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));

        var counts = await tree.CountPerShardAsync();
        Assert.That(counts.Count, Is.EqualTo(64)); // default shard count
        Assert.That(counts.Sum(), Is.EqualTo(20));
    }

    [Test]
    public async Task CountPerShard_returns_all_zeros_for_empty_tree()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("count-per-shard-empty");
        var counts = await tree.CountPerShardAsync();
        Assert.That(counts.Count, Is.EqualTo(64));
        Assert.That(counts.Sum(), Is.EqualTo(0));
    }
}
