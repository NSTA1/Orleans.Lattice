using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
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
/// last-writer-wins on its stamp. Bulk entries are stamped at the wall time of
/// the append (<c>HybridLogicalClock.Tick</c> from zero yields the current
/// time), so a chunk appended after a point write wins over it, and a row the
/// chunk does not carry keeps its value. That is the value every replica and
/// view already resolves. Nothing is refused: a chunk that succeeded before
/// still succeeds.
/// </para>
/// <para>
/// Every assertion runs twice: once on the resident leaves, and once after every
/// leaf has been taken cold and has replayed. The owner and a WAL reader must
/// resolve a contested key to the same value (<c>ContestedKeyAgrees</c> in
/// <c>WalLiveAdvance.tla</c>), and a replay is the owner's own WAL read.
/// </para>
/// <para>
/// The fixture also runs with the per-silo replay gate capped at one permit, so a
/// replay that holds its permit while it waits on a call needing one, or that
/// strands its permit, fails here every time instead of only under load
/// (issue #5775). Every cold read in it is an activation replay that resolves its
/// shard map from the registry.
/// </para>
/// </summary>
[TestFixture(0)]
[TestFixture(1)]
[Category("Integration")]
public sealed partial class BulkAppendExistingRowsIntegrationTests(int replayCeiling)
{
    private SmallLeafClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;
    private int _previousReplayCeilingCap;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        if (replayCeiling > 0)
        {
            _previousReplayCeilingCap = BPlusLeafGrain.ReplayCeilingCapForTest;
            BPlusLeafGrain.ReplayCeilingCapForTest = replayCeiling;
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }

        _fixture = new SmallLeafClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;

        // The relocation runs only while every silo serves the bulk-graft leaf
        // methods; otherwise the append takes the 10.2.0 path.
        await _cluster.WaitForAppendFenceGateAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
        if (replayCeiling > 0)
        {
            BPlusLeafGrain.ReplayCeilingCapForTest = _previousReplayCeilingCap;
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    public async Task BulkAppend_after_point_writes_above_the_maximum_keeps_every_key_readable_last_writer_wins()
    {
        const string treeId = "bulk-existing-interleaved-point";
        var (shard, tree) = await SingleShardTreeAsync(treeId);

        await shard.BulkAppendAsync("chunk-1", Entries(0, 8, "b"));

        // Acknowledged between the chunks, above the first chunk's maximum and
        // inside the second chunk's range. The second chunk carries k0014 as well,
        // stamped later, so its bulk value wins, as it does on every replica; it
        // does not carry k0015, which only the point row holds.
        await tree.SetAsync("k0014", Utf8("point"));
        await tree.SetAsync("k0015", Utf8("point"));

        var chunk2 = Entries(8, 12, "b");
        chunk2.RemoveAll(e => e.Key == "k0015");
        await shard.BulkAppendAsync("chunk-2", chunk2);

        var expected = Expected(0, 20, "b");
        expected["k0015"] = "point";

        await AssertTreeHoldsExactlyAsync(tree, expected, "resident");
        await DeactivateEveryLeafAsync(shard);
        await AssertTreeHoldsExactlyAsync(tree, expected, "after every leaf replayed");
    }

    [Test]
    public async Task BulkAppend_whose_below_edge_keys_are_all_held_with_other_values_applies_the_chunk()
    {
        // Review B2 (design #5604 change 4): every key of the chunk at or below the
        // right edge is held, so key presence alone would class it a re-delivery
        // and skip it. Its values differ, so it is a refresh: 10.2.0 applied it,
        // and every replica and view resolves the later-stamped bulk value.
        const string treeId = "bulk-existing-all-held";
        var (shard, tree) = await SingleShardTreeAsync(treeId);

        await shard.BulkAppendAsync("chunk-1", Entries(0, 8, "b"));
        for (var i = 8; i < 16; i++)
        {
            await tree.SetAsync(Key(i), Utf8("point"));
        }

        await shard.BulkAppendAsync("chunk-2", Entries(8, 12, "b"));

        var expected = Expected(0, 20, "b");

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
    public async Task BulkAppend_over_deleted_keys_resolves_each_by_its_stamp()
    {
        const string treeId = "bulk-existing-tombstone";
        var (shard, tree) = await SingleShardTreeAsync(treeId);

        await shard.BulkAppendAsync("chunk-1", Entries(0, 8, "b"));
        await tree.SetAsync("k0013", Utf8("point"));
        await tree.DeleteAsync("k0013");
        await tree.SetAsync("k0016", Utf8("point"));
        await tree.DeleteAsync("k0016");

        // The second chunk carries k0013, stamped after its tombstone, so the
        // bulk value wins as it does on every replica. It does not carry k0016,
        // whose tombstone, relocated with the other rows, keeps it deleted.
        var chunk2 = Entries(8, 12, "b");
        chunk2.RemoveAll(e => e.Key == "k0016");
        await shard.BulkAppendAsync("chunk-2", chunk2);

        var expected = Expected(0, 20, "b");
        expected.Remove("k0016");

        await AssertTreeHoldsExactlyAsync(tree, expected, "resident");
        await DeactivateEveryLeafAsync(shard);
        await AssertTreeHoldsExactlyAsync(tree, expected, "after every leaf replayed");
    }

    [Test]
    public async Task BulkAppend_appended_leaves_start_from_the_birth_hint_and_the_runs_switches_close_by_fence()
    {
        // Epic #5605, member 3: the appended leaves read from the hint captured before
        // they were born, so after a reactivation they replay only from there. Every
        // key of both chunks, and the point write between them, must survive that.
        const string treeId = "bulk-birth-hint-append";
        var (shard, tree) = await SingleShardTreeAsync(treeId);

        await shard.BulkAppendAsync("chunk-1", Entries(0, 8, "b"));
        // Between the chunks and inside the second one, so part of it lands below the
        // right edge and the rest is appended behind it.
        await tree.SetAsync("k0012x", Utf8("point"));
        await shard.BulkAppendAsync("chunk-2", Entries(8, 12, "b"));

        var expected = Expected(0, 20, "b");
        expected["k0012x"] = "point";

        await AssertSwitchesClosedAsync(treeId, "bulk:chunk-1", "bulk:chunk-2");
        await DeactivateEveryLeafAsync(shard);
        await AssertTreeHoldsExactlyAsync(tree, expected, "after every leaf replayed from its read position");
    }

    [Test]
    public async Task BulkLoad_leaves_start_from_the_birth_hint_and_every_key_survives_reactivation()
    {
        const string treeId = "bulk-birth-hint-load";
        var (shard, tree) = await SingleShardTreeAsync(treeId);

        await tree.BulkLoadAsync(Entries(0, 20, "b"));

        var expected = Expected(0, 20, "b");
        await AssertSwitchesClosedAsync(treeId);
        await DeactivateEveryLeafAsync(shard);
        await AssertTreeHoldsExactlyAsync(tree, expected, "after every leaf replayed from its read position");
    }

    /// <summary>
    /// Asserts the shard's marker holds no open hand-off and no stored birth hint, and
    /// has closed a bulk switch for each of <paramref name="tokens"/>, or at least one
    /// when none is named.
    /// </summary>
    private async Task AssertSwitchesClosedAsync(string treeId, params string[] tokens)
    {
        var storage = ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services
            .GetRequiredKeyedService<Orleans.Storage.IGrainStorage>(LatticeOptions.StorageProviderName);

        // A bulk append leaves its switch for the run's fence, which the shard closes
        // once no chunk has reached it for the idle interval (review C1 of #5702).
        var deadline = DateTime.UtcNow + ShardRootGrain.BulkSwitchIdleClose + TimeSpan.FromSeconds(20);
        var marker = (await HandOffMarkerRows.ReadShardAsync(storage, treeId, 0)).State;
        while (marker.Open.Count > 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(250);
            marker = (await HandOffMarkerRows.ReadShardAsync(storage, treeId, 0)).State;
        }

        var closed = marker.Closes.Select(c => c.Token).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(marker.Open, Is.Empty, "no bulk switch is left open");
            Assert.That(marker.BulkBirthHints, Is.Empty, "every birth hint is removed with its close");
            if (tokens.Length == 0)
                Assert.That(closed, Has.Some.StartsWith("bulk:"), "the load's switch closed");
            else
                Assert.That(closed, Is.SupersetOf(tokens), $"every append's switch closed; closed: {string.Join(", ", closed)}");
        });
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
