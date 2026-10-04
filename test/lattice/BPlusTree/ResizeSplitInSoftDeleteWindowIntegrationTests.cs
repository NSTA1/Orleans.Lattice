using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4478, end to end: once an online resize has completed, an adaptive
/// split of the resized copy runs while the copy the resize replaced still
/// mirrors into it. A saga bound to the replaced copy keeps preparing there and
/// delivering its terminals there; the mirror follows each refusal by the split
/// to the shard of the resized copy that owns the slot now, and the terminal
/// reaches every shard the split moved the replaced shard's slots to, so no
/// bucket is stranded and no acknowledged write is lost.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ResizeSplitInSoftDeleteWindowIntegrationTests
{
    private const int KeyCount = 48;

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

    private ILatticeRegistry Registry => _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    private async Task<(ILattice Tree, Dictionary<string, string> Expected)> CreateResizedTreeAsync(string treeId)
    {
        var tree = await _fixture.CreateTreeAsync(treeId);
        var expected = new Dictionary<string, string>();
        for (var i = 0; i < 200; i++)
        {
            var key = $"key-{i:D4}";
            await tree.SetAsync(key, Encoding.UTF8.GetBytes($"value-{i}"));
            expected[key] = $"value-{i}";
        }

        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
        await resize.ResizeAsync(64, 64);
        await resize.RunResizePassAsync();
        Assert.That(await resize.IsIdleAsync(), Is.True, "precondition: the resize completed");
        Assert.That(await Registry.ResolveAsync(treeId), Is.Not.EqualTo(treeId), "precondition: the tree is aliased to the resized copy");
        Assert.That(await resize.HoldsShardMigrationsAsync(), Is.True, "precondition: the replaced copy still mirrors");
        return (tree, expected);
    }

    private async Task SplitShardZeroAsync(string treeId)
    {
        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True, "the split of the resized copy must complete in the soft-delete window");
    }

    /// <summary>Keys the replaced copy's shard 0 owns, none of them written yet.</summary>
    private static List<string> KeysOwnedByShardZero(ShardMap replacedCopyMap, string prefix)
    {
        var keys = new List<string>(KeyCount);
        for (var i = 0; keys.Count < KeyCount; i++)
        {
            var key = $"{prefix}-{i:D5}";
            if (replacedCopyMap.Resolve(key) == 0) keys.Add(key);
        }

        return keys;
    }

    /// <summary>
    /// What a saga bound to the replaced copy does: prepares its batch on that
    /// copy's shard 0, which admits the bound saga through its fence and mirrors
    /// the prepares into the resized copy.
    /// </summary>
    private async Task PrepareOnReplacedCopyAsync(string replacedCopy, Guid txid, List<string> keys)
    {
        var entries = keys.Select(k => new KeyValuePair<string, byte[]>(k, Encoding.UTF8.GetBytes($"saga-{k}"))).ToList();
        using (LatticePreparedContext.BeginScope())
        using (LatticeAtomicBindingContext.With(replacedCopy))
        {
            LatticeTransactionContext.Set(txid);
            try
            {
                await _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{replacedCopy}/0").SetManyAsync(entries);
            }
            finally
            {
                LatticeTransactionContext.Set(Guid.Empty);
            }
        }
    }

    private async Task CommitOnReplacedCopyAsync(string replacedCopy, Guid txid)
    {
        using (LatticeAtomicBindingContext.With(replacedCopy))
        {
            await _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{replacedCopy}/0").AppendTxTerminalAsync(txid, committed: true);
        }
    }

    private async Task AssertMovedAndCommittedAsync(string treeId, ILattice tree, List<string> keys)
    {
        var map = (await Registry.GetEntryAsync(treeId))!.ShardMap!;
        Assert.That(keys.Any(k => map.Resolve(k) != 0), Is.True, "precondition: the split moved some of the batch's keys off shard 0");
        foreach (var key in keys)
        {
            var actual = await tree.GetAsync(key);
            Assert.That(actual is null ? null : Encoding.UTF8.GetString(actual), Is.EqualTo($"saga-{key}"),
                $"{key} (owned by shard {map.Resolve(key)} of the resized copy) must read the committed batch");
        }
    }

    [Test]
    public async Task A_split_of_the_resized_copy_runs_in_the_soft_delete_window_and_keeps_every_key()
    {
        var treeId = $"window-split-{Guid.NewGuid():N}";
        var (tree, expected) = await CreateResizedTreeAsync(treeId);

        await SplitShardZeroAsync(treeId);

        foreach (var (key, value) in expected)
        {
            var actual = await tree.GetAsync(key);
            Assert.That(actual is null ? null : Encoding.UTF8.GetString(actual), Is.EqualTo(value), key);
        }
    }

    [Test]
    public async Task A_bound_sagas_terminal_reaches_the_buckets_a_split_of_the_resized_copy_moved()
    {
        // The batch is prepared on the replaced copy before the split, so the
        // mirror lands it on the resized copy's shard 0 and the split's sweep
        // replays the moved keys' buckets onto the new shard. The terminal is
        // addressed to the replaced copy's shard 0 only; mirrored by index alone
        // it would resolve shard 0's buckets and strand the moved ones.
        var treeId = $"window-terminal-{Guid.NewGuid():N}";
        var (tree, _) = await CreateResizedTreeAsync(treeId);
        var replacedCopyMap = (await tree.GetRoutingAsync()).Map;
        var keys = KeysOwnedByShardZero(replacedCopyMap, "terminal");
        var txid = Guid.NewGuid();

        await PrepareOnReplacedCopyAsync(treeId, txid, keys);
        await SplitShardZeroAsync(treeId);
        await CommitOnReplacedCopyAsync(treeId, txid);

        await AssertMovedAndCommittedAsync(treeId, tree, keys);
    }

    [Test]
    public async Task A_bound_sagas_prepare_follows_a_split_of_the_resized_copy_to_the_slots_owner()
    {
        // The batch is prepared on the replaced copy after the split: the
        // resized copy's shard 0 refuses the moved keys' mirror, which must be
        // re-sent to the shard that owns them now rather than fail the prepare.
        var treeId = $"window-prepare-{Guid.NewGuid():N}";
        var (tree, _) = await CreateResizedTreeAsync(treeId);
        var replacedCopyMap = (await tree.GetRoutingAsync()).Map;
        var keys = KeysOwnedByShardZero(replacedCopyMap, "prepare");
        var txid = Guid.NewGuid();

        await SplitShardZeroAsync(treeId);
        await PrepareOnReplacedCopyAsync(treeId, txid, keys);
        await CommitOnReplacedCopyAsync(treeId, txid);

        await AssertMovedAndCommittedAsync(treeId, tree, keys);
    }

    private async Task AssertServesFromReplacedCopyAsync(string treeId, ILattice tree, Dictionary<string, string> expected)
    {
        Assert.That(await Registry.ResolveAsync(treeId), Is.EqualTo(treeId), "the undo must point the tree back at the replaced copy");
        foreach (var (key, value) in expected)
        {
            var actual = await tree.GetAsync(key);
            Assert.That(actual is null ? null : Encoding.UTF8.GetString(actual), Is.EqualTo(value), key);
        }

        await tree.SetAsync("after-undo", Encoding.UTF8.GetBytes("written"));
        Assert.That(Encoding.UTF8.GetString((await tree.GetAsync("after-undo"))!), Is.EqualTo("written"));
    }

    [Test]
    public async Task An_undo_after_a_split_of_the_resized_copy_committed_discards_the_copy_with_its_map()
    {
        var treeId = $"window-undo-after-{Guid.NewGuid():N}";
        var (tree, expected) = await CreateResizedTreeAsync(treeId);
        var replacedCopyMap = (await tree.GetRoutingAsync()).Map;
        await SplitShardZeroAsync(treeId);
        Assert.That((await Registry.GetEntryAsync(treeId))!.ShardMap!.Slots, Is.Not.EqualTo(replacedCopyMap.Slots),
            "precondition: the split moved slots on the resized copy");

        await tree.UndoResizeAsync();

        Assert.That((await tree.GetRoutingAsync(forceRefresh: true)).Map.Slots, Is.EqualTo(replacedCopyMap.Slots),
            "the undo restores the map that describes the replaced copy");
        await AssertServesFromReplacedCopyAsync(treeId, tree, expected);
    }

    [Test]
    public async Task An_undo_during_a_split_of_the_resized_copy_abandons_the_split()
    {
        // The split is bound to the resized copy. Once the undo points the tree
        // back at the replaced copy, the commit fence refuses the split's map and
        // the split abandons itself instead of committing a map that describes
        // the discarded copy (issue #4264).
        var treeId = $"window-undo-during-{Guid.NewGuid():N}";
        var (tree, expected) = await CreateResizedTreeAsync(treeId);
        var replacedCopyMap = (await tree.GetRoutingAsync()).Map;
        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0);

        await tree.UndoResizeAsync();
        await split.RunSplitPassAsync();

        Assert.That(await split.IsIdleAsync(), Is.True, "the split must not stall once the tree no longer resolves to its copy");
        Assert.That((await tree.GetRoutingAsync(forceRefresh: true)).Map.Slots, Is.EqualTo(replacedCopyMap.Slots),
            "the abandoned split must not move the replaced copy's slots");
        await AssertServesFromReplacedCopyAsync(treeId, tree, expected);
    }
}
