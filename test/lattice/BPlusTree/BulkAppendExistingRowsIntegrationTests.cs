using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// A bulk append meeting rows the shard already holds in the appended range
/// (issue #5610, epic #5605).
/// <para>
/// A bulk append grafts its leaves at the shard's right edge. When the shard
/// already holds rows at or above the first grafted separator, those rows used
/// to stay on the old rightmost leaf, which reads no longer reached. That
/// happens when a point write lands above the maximum between two chunks, or
/// when a chunk sorts below rows the shard already holds. The graft now
/// relocates those rows into the bulk leaves and resolves each contested key by
/// last-writer-wins. The acknowledged point write, versioned by a real clock,
/// beats the bulk entry, which is versioned from a zero clock. That is the value
/// every replica and view already returns (change 4 of the live-advance design
/// note). Nothing is refused: a chunk that succeeded before still succeeds.
/// </para>
/// <para>
/// Every assertion runs twice: once on the resident leaves, and once after every
/// leaf has been taken cold and has replayed. The owner and a WAL reader must
/// resolve a contested key to the same value (<c>ContestedKeyAgrees</c> in
/// <c>WalLiveAdvance.tla</c>), and a replay is the owner's own WAL read.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class BulkAppendExistingRowsIntegrationTests
{
    private SmallLeafClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SmallLeafClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task BulkAppend_after_a_point_write_above_the_maximum_keeps_the_point_value_and_every_chunk_key()
    {
        const string treeId = "bulk-existing-interleaved-point";
        var (shard, tree) = await SingleShardTreeAsync(treeId);

        await shard.BulkAppendAsync("chunk-1", Entries(0, 8, "b"));

        // Acknowledged between the chunks, above the first chunk's maximum and
        // inside the second chunk's range.
        await tree.SetAsync("k0015", Utf8("point"));

        // The second chunk carries the contested key as well.
        await shard.BulkAppendAsync("chunk-2", Entries(8, 12, "b"));

        var expected = Expected(0, 20, "b");
        expected["k0015"] = "point";

        await AssertTreeHoldsExactlyAsync(tree, expected, "resident");
        await DeactivateEveryLeafAsync(shard);
        await AssertTreeHoldsExactlyAsync(tree, expected, "after every leaf replayed");
    }

    [Test]
    public async Task BulkAppend_below_rows_the_shard_holds_relocates_them_and_refuses_nothing()
    {
        const string treeId = "bulk-existing-below";
        var (shard, tree) = await SingleShardTreeAsync(treeId);

        // Rows the shard already holds, above every key of the chunk below.
        for (var i = 100; i < 106; i++)
        {
            await tree.SetAsync(Key(i), Utf8("point"));
        }

        // A chunk that sorts entirely below them: resuming an import into a tree
        // that has since taken writes.
        await shard.BulkAppendAsync("below", Entries(0, 12, "b"));

        var expected = Expected(0, 12, "b");
        for (var i = 100; i < 106; i++)
        {
            expected[Key(i)] = "point";
        }

        await AssertTreeHoldsExactlyAsync(tree, expected, "resident");
        await DeactivateEveryLeafAsync(shard);
        await AssertTreeHoldsExactlyAsync(tree, expected, "after every leaf replayed");
    }

    [Test]
    public async Task BulkAppend_over_a_key_deleted_before_the_append_leaves_it_deleted()
    {
        const string treeId = "bulk-existing-tombstone";
        var (shard, tree) = await SingleShardTreeAsync(treeId);

        await shard.BulkAppendAsync("chunk-1", Entries(0, 8, "b"));
        await tree.SetAsync("k0013", Utf8("point"));
        await tree.DeleteAsync("k0013");

        // The tombstone is versioned by a real clock, so it beats the bulk entry
        // for the same key, as it does on every replica.
        await shard.BulkAppendAsync("chunk-2", Entries(8, 12, "b"));

        var expected = Expected(0, 20, "b");
        expected.Remove("k0013");

        await AssertTreeHoldsExactlyAsync(tree, expected, "resident");
        await DeactivateEveryLeafAsync(shard);
        await AssertTreeHoldsExactlyAsync(tree, expected, "after every leaf replayed");
    }

    private async Task<(IShardRootGrain Shard, ILattice Tree)> SingleShardTreeAsync(string treeId)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = SmallLeafClusterFixture.SmallMaxLeafKeys,
            ShardCount = 1,
        });

        return (
            _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/0"),
            _cluster.GrainFactory.GetGrain<ILattice>(treeId));
    }

    private static string Key(int i) => $"k{i:D4}";

    private static byte[] Utf8(string value) => Encoding.UTF8.GetBytes(value);

    private static List<KeyValuePair<string, byte[]>> Entries(int start, int count, string value) =>
        Enumerable.Range(start, count).Select(i => KeyValuePair.Create(Key(i), Utf8(value))).ToList();

    private static SortedDictionary<string, string> Expected(int start, int count, string value)
    {
        var expected = new SortedDictionary<string, string>(StringComparer.Ordinal);
        for (var i = start; i < start + count; i++)
        {
            expected[Key(i)] = value;
        }

        return expected;
    }

    /// <summary>
    /// Asserts point reads, the live count and an ordered scan all report exactly
    /// <paramref name="expected"/>: every key once, in order, with its value.
    /// </summary>
    private static async Task AssertTreeHoldsExactlyAsync(
        ILattice tree, SortedDictionary<string, string> expected, string when)
    {
        foreach (var (key, value) in expected)
        {
            var read = await tree.GetAsync(key);
            Assert.That(read, Is.Not.Null, $"{when}: point read of '{key}'");
            Assert.That(Encoding.UTF8.GetString(read!), Is.EqualTo(value), $"{when}: value of '{key}'");
        }

        var scanned = new List<KeyValuePair<string, string>>();
        await foreach (var (key, value) in tree.ScanEntriesAsync())
        {
            scanned.Add(KeyValuePair.Create(key, Encoding.UTF8.GetString(value)));
        }

        Assert.That(scanned, Is.EqualTo(expected.ToList()), $"{when}: the scan returns every key once, in order");
        Assert.That(await tree.CountAsync(), Is.EqualTo(expected.Count), $"{when}: the live count");
    }

    private async Task DeactivateEveryLeafAsync(IShardRootGrain shard)
    {
        var chain = new List<GrainId>();
        var leafId = await shard.GetLeftmostLeafIdAsync();
        while (leafId is { } id && chain.Count < 1_000)
        {
            chain.Add(id);
            leafId = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(id).GetNextSiblingAsync();
        }

        foreach (var id in chain)
        {
            await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(id).ForceDeactivateAsync();
        }

        // DeactivateOnIdle runs after the current turn, so it is not observable
        // synchronously.
        await Task.Delay(500);
    }
}
