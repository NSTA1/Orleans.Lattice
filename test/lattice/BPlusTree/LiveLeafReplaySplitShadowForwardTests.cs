using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression for issue #909: a LIVE leaf's cold activation WAL replay must
/// keep a shadow-forwarded mutation whose key legitimately belongs to this
/// leaf after an adaptive shard split, even though the forwarded record still
/// carries the DONOR shard's source stamp.
/// <para>
/// A split drains a range of virtual slots from the donor shard to a new
/// target shard and swaps the routing map. A post-split write routed to the
/// donor for an already-moved slot is shadow-forwarded into the target's WAL
/// but keeps the donor's <c>ShardIndex</c> stamp. Before the fix the live
/// leaf's replay filter gated Set/Delete records on
/// <c>mutation.ShardIndex == leafShardIndex</c>, so on a cold reactivation
/// from a checkpoint taken before the forwarded write the donor-stamped record
/// was dropped - resurrecting a drained stale value or losing a tombstone.
/// This is the same flaw issue #907 fixed on the snapshot replay path; this
/// test pins the authoritative live path.
/// </para>
/// <para>
/// The repro injects an authoritative donor-stamped WAL record by calling the
/// donor leaf's <see cref="IBPlusLeafGrain.MergeEntriesAsync"/> for a moved
/// key (the leaf has no reject gate, so the record is stamped with the donor's
/// shard index and appended to the shared WAL), then cold-reactivates the
/// target leaf via <see cref="IBPlusLeafGrain.ForceDeactivateAsync"/> so it
/// replays the WAL suffix and the ownership filter decides the record's fate.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class LiveLeafReplaySplitShadowForwardTests
{
    // Large enough that each shard keeps a single leaf (so the leftmost leaf
    // owns every key the shard holds), small enough that the seed stays cheap.
    private const int LargeMaxLeafKeys = 256;
    private const int SeedKeyCount = 160;

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
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>
    /// Registers a fresh tree with a large per-leaf capacity so each shard is
    /// served by a single leaf, then returns a reference to it.
    /// </summary>
    private async Task<ILattice> CreateSingleLeafTreeAsync(string treeId)
    {
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = LargeMaxLeafKeys,
            ShardCount = FourShardClusterFixture.TestShardCount,
        });
        return _cluster.Client.GetGrain<ILattice>(treeId);
    }

    private async Task CommitSplitAsync(string treeId, int sourceShardIndex = 0)
    {
        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>(
            $"{treeId}/{sourceShardIndex}");
        await split.SplitAsync(sourceShardIndex);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True,
            $"Manual split of shard {sourceShardIndex} must complete.");
    }

    /// <summary>
    /// Finds the seeded keys whose owning physical shard changed across the
    /// split - i.e. the keys the donor shard now retains as orphans.
    /// </summary>
    private async Task<(List<string> Moved, ShardMap PostMap)> FindMovedKeysAsync(
        string treeId, IEnumerable<string> seededKeys)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var postMap = await registry.GetShardMapAsync(treeId);
        Assert.That(postMap, Is.Not.Null, "The split must publish a routing map.");
        var preMap = ShardMap.CreateDefault(
            LatticeConstants.DefaultVirtualShardCount, FourShardClusterFixture.TestShardCount);

        var moved = new List<string>();
        foreach (var key in seededKeys)
        {
            if (preMap.Resolve(key) != postMap!.Resolve(key))
                moved.Add(key);
        }
        return (moved, postMap!);
    }

    private async Task<IBPlusLeafGrain> GetLeftmostLeafAsync(string treeId, int shardIndex)
    {
        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{treeId}/{shardIndex}");
        var leafId = await shard.GetLeftmostLeafIdAsync();
        Assert.That(leafId, Is.Not.Null, $"Shard {shardIndex} of '{treeId}' must have a leaf.");
        return _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId!.Value.GetGuidKey());
    }

    /// <summary>
    /// Seeds the tree, splits shard 0, and returns the donor leaf, the target
    /// leaf, and the list of migrated keys. Each migrated key has a live
    /// orphan copy on the donor and a live authoritative copy on the target.
    /// </summary>
    private async Task<(IBPlusLeafGrain Donor, IBPlusLeafGrain Target, List<string> Moved)>
        SeedSplitAndLocateLeavesAsync(string treeId)
    {
        var tree = await CreateSingleLeafTreeAsync(treeId);
        for (var i = 0; i < SeedKeyCount; i++)
            await tree.SetAsync($"k-{i:D4}", Bytes($"v-{i}"));

        await CommitSplitAsync(treeId);

        var seeded = Enumerable.Range(0, SeedKeyCount).Select(i => $"k-{i:D4}").ToList();
        var (moved, postMap) = await FindMovedKeysAsync(treeId, seeded);
        Assert.That(moved, Has.Count.GreaterThanOrEqualTo(1),
            "The split must migrate at least one seeded key for this regression to be meaningful.");

        var targetShardIndex = postMap.Resolve(moved[0]);
        Assert.That(targetShardIndex, Is.Not.EqualTo(0),
            "A migrated key must now resolve to a shard other than the donor.");

        var donor = await GetLeftmostLeafAsync(treeId, 0);
        var target = await GetLeftmostLeafAsync(treeId, targetShardIndex);
        return (donor, target, moved);
    }

    [Test]
    public async Task Live_leaf_cold_replay_keeps_shadow_forwarded_delete_for_moved_key()
    {
        var treeId = $"live-replay-del-{Guid.NewGuid():N}";
        var (donor, target, moved) = await SeedSplitAndLocateLeavesAsync(treeId);
        var key = moved[0];

        // The target authoritatively serves the migrated key after the split.
        var before = await target.GetAsync(key);
        Assert.That(before, Is.Not.Null, "Sanity: target leaf must serve the migrated key.");

        // Build a donor-stamped tombstone at a strictly-higher HLC than the
        // migrated copy, then inject it through the DONOR leaf. The donor leaf
        // stamps the WAL record with its own (donor) shard index and appends
        // it to the shared WAL - exactly the shape a shadow-forwarded delete
        // takes. This models a delete routed to the donor for an already-moved
        // slot and forwarded into the target's WAL with the donor's stamp.
        var raw = await target.GetRawEntryAsync(key);
        Assert.That(raw, Is.Not.Null);
        var highClock = HybridLogicalClock.Tick(
            HybridLogicalClock.Tick(HybridLogicalClock.Tick(raw!.Value.Timestamp)));
        await donor.MergeEntriesAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            [key] = LwwValue<byte[]>.Tombstone(highClock),
        });

        // Cold-reactivate the target leaf so it replays the WAL suffix past its
        // checkpoint. The forwarded tombstone carries the donor's stamp; the
        // ownership filter must keep it (the key's slot routes here) instead of
        // dropping it on the stale stamp.
        await target.ForceDeactivateAsync();

        var after = await target.GetAsync(key);
        Assert.That(after, Is.Null,
            "A delete shadow-forwarded with the donor's stamp must survive the target leaf's cold "
            + "replay; before the fix the stamped-ShardIndex filter dropped it and resurrected the "
            + "drained value.");
    }

    [Test]
    public async Task Live_leaf_cold_replay_keeps_shadow_forwarded_update_for_moved_key()
    {
        var treeId = $"live-replay-upd-{Guid.NewGuid():N}";
        var (donor, target, moved) = await SeedSplitAndLocateLeavesAsync(treeId);
        var key = moved[0];

        var before = await target.GetAsync(key);
        Assert.That(before, Is.Not.Null, "Sanity: target leaf must serve the migrated key.");

        const string updatedValue = "UPDATED-VIA-DONOR-FORWARD";
        var raw = await target.GetRawEntryAsync(key);
        Assert.That(raw, Is.Not.Null);
        var highClock = HybridLogicalClock.Tick(
            HybridLogicalClock.Tick(HybridLogicalClock.Tick(raw!.Value.Timestamp)));
        await donor.MergeEntriesAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            [key] = LwwValue<byte[]>.Create(Bytes(updatedValue), highClock),
        });

        await target.ForceDeactivateAsync();

        var after = await target.GetAsync(key);
        Assert.That(after, Is.Not.Null,
            "An update shadow-forwarded with the donor's stamp must survive the target leaf's cold replay.");
        Assert.That(Encoding.UTF8.GetString(after!), Is.EqualTo(updatedValue),
            "After cold replay the target leaf must show the forwarded value, not the resurrected "
            + "drained pre-split copy that the stamped-ShardIndex filter would have left in place.");
    }
}
