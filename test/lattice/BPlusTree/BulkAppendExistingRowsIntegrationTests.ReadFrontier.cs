using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #5752: a leaf that takes over part of another leaf's range - an appended leaf
/// from the old rightmost leaf, a split sibling from its donor - must start reading
/// the WAL no higher than how far that leaf had replayed. A record of the range that
/// another author appended (another shard's range-delete walk, a duplicate activation
/// of the old leaf, a 10.2.0 silo during an upgrade) and that the old leaf had not
/// applied lies just above that frontier; started at the WAL head, the new leaf skips
/// it, and the old leaf, narrowed, skips it too.
/// <para>
/// Each test makes the old leaf replay every partition to its head, appends one such
/// record directly to the WAL, at the old leaf's frontier plus one, and then moves the
/// range. The record must be readable once every leaf has gone cold and replayed.
/// </para>
/// </summary>
public sealed partial class BulkAppendExistingRowsIntegrationTests
{
    [Test]
    public async Task BulkAppend_leaves_replay_a_record_of_their_range_the_old_leaf_never_applied()
    {
        const string treeId = "bulk-hint-foreign-record";
        const string foreignKey = "k0012x";
        var (shard, tree) = await SingleShardTreeAsync(treeId);
        var expected = await WriteOnEveryPartitionAsync(treeId, tree, "a");
        await ReplayRightmostLeafToHeadAsync(shard, tree, expected);

        await AppendForeignRecordAsync(treeId, foreignKey, "foreign");
        await shard.BulkAppendAsync("chunk", Entries(10, 6, "b"));

        foreach (var (key, value) in Expected(10, 6, "b"))
            expected[key] = value;
        expected[foreignKey] = "foreign";

        await AssertSwitchesClosedAsync(treeId, "bulk:chunk");
        await DeactivateEveryLeafAsync(shard);
        await AssertTreeHoldsExactlyAsync(tree, expected, "after every leaf replayed from its read position");
    }

    [Test]
    public async Task Split_sibling_replays_a_record_of_its_range_the_donor_never_applied()
    {
        const string treeId = "split-hint-foreign-record";
        const string foreignKey = "z0001";
        var (shard, tree) = await SingleShardTreeAsync(treeId);
        var expected = await WriteOnEveryPartitionAsync(treeId, tree, "a");
        await ReplayRightmostLeafToHeadAsync(shard, tree, expected);

        await AppendForeignRecordAsync(treeId, foreignKey, "foreign");

        // Writes into the rightmost leaf until it divides; the foreign key sorts above
        // every one of them, so it falls in the sibling's range.
        for (var i = 0; i < SmallLeafClusterFixture.SmallMaxLeafKeys + 1; i++)
        {
            var key = $"y{i:D4}";
            await tree.SetAsync(key, Utf8("point"));
            expected[key] = "point";
        }

        expected[foreignKey] = "foreign";
        await DeactivateEveryLeafAsync(shard);
        await AssertTreeHoldsExactlyAsync(tree, expected, "after every leaf replayed from its read position");
    }

    [Test]
    public async Task BulkLoad_leaves_replay_a_record_of_their_range_the_replaced_root_never_applied()
    {
        // Issue #5766: the bulk load's leaves start no higher than the replaced root's
        // frontier. The root leaf replays to its partition's head, is left empty, and
        // then another author (here, as a duplicate activation of that leaf would)
        // appends a key of the loaded range at the root's frontier plus one.
        const string treeId = "load-hint-foreign-record";
        const string foreignKey = "k0002x";
        var (shard, tree) = await SingleShardTreeAsync(treeId);
        var partitions = await SiloServices().GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(treeId);
        var partition = WalPartitionHash.Compute(foreignKey, partitions);
        var seed = Enumerable.Range(0, 10_000).Select(i => $"a{i:D4}")
            .First(k => WalPartitionHash.Compute(k, partitions) == partition);

        // A record on the foreign key's partition that the root leaf replays past,
        // after which the root holds nothing, so the load accepts the shard.
        await tree.SetAsync(seed, Utf8("seed"));
        await tree.DeleteAsync(seed);
        var root = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>((await shard.GetLeftmostLeafIdAsync())!.Value);
        await root.CompactTombstonesAsync(TimeSpan.Zero);
        await DeactivateEveryLeafAsync(shard);
        Assert.That(await tree.GetAsync(seed), Is.Null, "precondition: the root replayed and holds no row");
        Assert.That(await tree.CountAsync(), Is.Zero, "precondition: the shard is empty");

        await AppendForeignRecordAsync(treeId, foreignKey, "foreign");
        await tree.BulkLoadAsync(Entries(0, 5, "b"));

        var expected = Expected(0, 5, "b");
        expected[foreignKey] = "foreign";
        await AssertSwitchesClosedAsync(treeId);
        await DeactivateEveryLeafAsync(shard);
        await AssertTreeHoldsExactlyAsync(tree, expected, "after every leaf replayed from its read position");
    }
    /// <summary>
    /// Writes one key per WAL partition, all with <paramref name="prefix"/>, so every
    /// partition holds a record a replay can advance over.
    /// </summary>
    private async Task<SortedDictionary<string, string>> WriteOnEveryPartitionAsync(string treeId, ILattice tree, string prefix)
    {
        var partitions = await SiloServices().GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(treeId);
        var expected = new SortedDictionary<string, string>(StringComparer.Ordinal);
        var covered = new HashSet<int>();
        for (var i = 0; covered.Count < partitions && i < 10_000; i++)
        {
            var key = $"{prefix}{i:D4}";
            if (covered.Add(WalPartitionHash.Compute(key, partitions)))
            {
                await tree.SetAsync(key, Utf8("point"));
                expected[key] = "point";
            }
        }

        Assert.That(covered, Has.Count.EqualTo(partitions), "precondition: a record on every partition");
        return expected;
    }

    /// <summary>
    /// Takes every leaf cold, then reads the highest key, so the rightmost leaf
    /// activates and replays every partition to its head: its frontier is then the
    /// last offset of each, and the next record appended sits at the frontier plus one.
    /// </summary>
    private async Task ReplayRightmostLeafToHeadAsync(IShardRootGrain shard, ILattice tree, SortedDictionary<string, string> expected)
    {
        await DeactivateEveryLeafAsync(shard);
        Assert.That(await tree.GetAsync(expected.Keys.Last()), Is.Not.Null, "precondition: the rightmost leaf is active");
    }

    /// <summary>
    /// Appends a set of <paramref name="key"/> straight to the tree's WAL, as an author
    /// other than the leaf that declares the key would: no leaf applies it until one
    /// replays it.
    /// </summary>
    private async Task AppendForeignRecordAsync(string treeId, string key, string value)
    {
        await SiloServices().GetRequiredService<ICommitLogWriter>().AppendAsync(new WalRecord
        {
            TreeId = treeId,
            ShardIndex = 0,
            Op = MutationKind.Set,
            Key = key,
            Value = Utf8(value),
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        });
    }

    private IServiceProvider SiloServices() => ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services;
}