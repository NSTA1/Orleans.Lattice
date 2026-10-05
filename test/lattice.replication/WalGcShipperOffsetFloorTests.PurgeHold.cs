using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4534: a trim the retention ceiling forces past a shipper's durable
/// read position records the shipper's decision-purge hold before it trims; the
/// shipper releases the hold once its durable position covers the trimmed
/// offsets, and a shipper detached for a removed peer releases it and stops
/// holding the log. Runs the real shipper, the real WAL GC, and the real hold
/// and consumer-registry grains.
/// </summary>
public partial class WalGcShipperOffsetFloorTests
{
    private const string RetentionTree = "gc-shipper-retention-hold";
    private static readonly TimeSpan RetentionWindow = TimeSpan.FromMilliseconds(200);

    private TimeSpan _savedPurgeHoldCheckInterval;

    [Test]
    public async Task Retention_trim_past_a_shipper_records_its_purge_hold_before_the_trim()
    {
        var tree = RetentionTree;
        var client = _cluster.Client;
        await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
            tree,
            new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
        var partitions = await SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(tree);
        var (key, _, _, q) = PickKeys(partitions);
        var lattice = client.GetGrain<ILattice>(tree);

        // The peer never acknowledges, so the shipper's durable position stays 0.
        GatedRecordingTransport.Refuse(tree);
        var shipper = client.GetGrain<IReplicationShipperGrain>($"{tree}/{PeerClusterId}");
        await shipper.EnsureActiveAsync(CancellationToken.None);
        await lattice.SetAsync(key, [1]);
        await TestPoll.UntilAsync(
            async () => (await client.GetGrain<IWalOffsetConsumerRegistryGrain>(tree).GetConsumersAsync()).Count > 0,
            "the shipper to register as an offset consumer of the tree's log",
            TimeSpan.FromSeconds(30));

        await CheckpointLeavesPastAsync(tree, q, 0, lattice, key);
        await Task.Delay(RetentionWindow * 3);
        var trimmed = await RunGcOnEverySiloAsync(tree);

        var holds = await client.GetGrain<IWalPurgeHoldGrain>(tree).GetAsync();
        var consumerId = shipper.GetGrainId().ToString();
        Assert.Multiple(() =>
        {
            Assert.That(trimmed, Is.GreaterThan(0),
                "the retention ceiling must trim past the shipper, or the hold below proves nothing");
            Assert.That(holds.ContainsKey(consumerId), Is.True,
                "a trim past a shipper's unshipped position must record the shipper's decision-purge hold");
            Assert.That(holds.TryGetValue(consumerId, out var hold) && hold.TrimmedThrough[q] >= 0, Is.True,
                "the hold names the partition the trim passed the shipper in");
        });

        // The shipper lost the trimmed record, so it never releases the hold
        // itself; only a re-seed (or its peer's removal) does.
        await Task.Delay(ReplicationShipperGrain.PurgeHoldCheckInterval * 4);
        Assert.That((await client.GetGrain<IWalPurgeHoldGrain>(tree).GetAsync()).ContainsKey(consumerId), Is.True,
            "a shipper that lost records must keep its hold");
    }

    [Test]
    public async Task Shipper_releases_a_purge_hold_its_durable_position_covers()
    {
        var tree = "gc-shipper-hold-covered-" + Guid.NewGuid().ToString("N")[..8];
        var client = _cluster.Client;
        await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
            tree,
            new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
        var partitions = await SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(tree);
        var (key, _, _, q) = PickKeys(partitions);
        var lattice = client.GetGrain<ILattice>(tree);
        var shipper = client.GetGrain<IReplicationShipperGrain>($"{tree}/{PeerClusterId}");
        var consumerId = shipper.GetGrainId().ToString();
        var holdGrain = client.GetGrain<IWalPurgeHoldGrain>(tree);

        await shipper.EnsureActiveAsync(CancellationToken.None);
        await lattice.SetAsync(key, [1]);
        await TestPoll.UntilAsync(
            async () => (await shipper.AsReference<IWalOffsetConsumer>().GetDurableReadPositionsAsync(tree)) is { } p
                && p.Length > q && p[q] >= 1,
            "the shipper to durably ship the write",
            TimeSpan.FromSeconds(30));

        // A trim through offset 0 passed a stale read of the shipper's position,
        // as a GC pass that raced the in-flight batch's ack does.
        var covered = new long[partitions];
        Array.Fill(covered, -1L);
        covered[q] = 0;
        await holdGrain.AddAsync(consumerId, covered);
        await TestPoll.UntilAsync(
            async () => !(await holdGrain.GetAsync()).ContainsKey(consumerId),
            "the shipper to release a hold its durable position covers",
            TimeSpan.FromSeconds(30));

        // A hold past its durable position is a lost record: it stays.
        var uncovered = (long[])covered.Clone();
        uncovered[q] = 1_000;
        await holdGrain.AddAsync(consumerId, uncovered);
        await Task.Delay(ReplicationShipperGrain.PurgeHoldCheckInterval * 4);
        Assert.That((await holdGrain.GetAsync()).ContainsKey(consumerId), Is.True,
            "a hold the shipper's durable position does not cover must stay");
        await holdGrain.RemoveAsync(consumerId);
    }

    [Test]
    public async Task Detaching_a_removed_peers_shipper_releases_its_purge_hold_and_the_log()
    {
        var tree = "gc-shipper-detach-" + Guid.NewGuid().ToString("N")[..8];
        var client = _cluster.Client;
        await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
            tree,
            new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
        var partitions = await SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(tree);
        var (key, _, _, q) = PickKeys(partitions);
        var lattice = client.GetGrain<ILattice>(tree);
        var shipper = client.GetGrain<IReplicationShipperGrain>($"{tree}/{PeerClusterId}");
        var consumerId = shipper.GetGrainId().ToString();
        var holdGrain = client.GetGrain<IWalPurgeHoldGrain>(tree);
        var registry = client.GetGrain<IWalOffsetConsumerRegistryGrain>(tree);

        GatedRecordingTransport.Refuse(tree);
        await shipper.EnsureActiveAsync(CancellationToken.None);
        await lattice.SetAsync(key, [1]);
        await TestPoll.UntilAsync(
            async () => (await registry.GetConsumersAsync()).Contains(shipper.GetGrainId()),
            "the shipper to register as an offset consumer of the tree's log",
            TimeSpan.FromSeconds(30));
        var lost = new long[partitions];
        Array.Fill(lost, -1L);
        lost[q] = 1_000;
        await holdGrain.AddAsync(consumerId, lost);

        await shipper.DetachFromLogAsync(CancellationToken.None);

        var positions = await shipper.AsReference<IWalOffsetConsumer>().GetDurableReadPositionsAsync(tree);
        var holds = await holdGrain.GetAsync();
        var consumers = await registry.GetConsumersAsync();
        Assert.Multiple(() =>
        {
            Assert.That(holds.ContainsKey(consumerId), Is.False,
                "a removed peer's shipper must release its decision-purge hold");
            Assert.That(consumers, Does.Not.Contain(shipper.GetGrainId()),
                "a removed peer's shipper must withdraw from the log's offset consumers");
            Assert.That(positions, Is.Null, "a removed peer's shipper holds no position in the log");
        });

        // The GC no longer floors at a detached shipper, so a trim can pass a
        // prepare it has not read: the detach takes the peer off the log, and
        // its pushes ask for a re-seed while every saga record is withheld.
        await lattice.SetAsync(key + "-after-detach", [2]);
        await TestPoll.UntilAsync(
            () => GatedRecordingTransport.ReseedRequested(tree),
            "the detached shipper's pushes to ask the peer for a re-seed",
            TimeSpan.FromSeconds(30));

        Assert.That((await holdGrain.GetAsync()).Keys.Any(k => k.EndsWith("#replay", StringComparison.Ordinal)), Is.False,
            "a removed peer's shipper holds no replay purge hold either");

        // An export taken while the peer was away predates any hold, so it must
        // not satisfy the re-seed: re-attaching re-marks at the current epoch.
        var exportedWhileDetached = await client.GetGrain<IReplicationExportEpochGrain>(tree).AdvanceAsync();

        // Adding the peer back re-attaches the shipper, which registers again
        // before its next read.
        await shipper.EnsureActiveAsync(CancellationToken.None);
        Assert.That((await holdGrain.GetAsync()).Keys.Any(k => k.EndsWith("#replay", StringComparison.Ordinal)), Is.True,
            "re-attaching takes the replay purge hold again");
        await lattice.SetAsync(key + "-after-reattach", [3]);
        await TestPoll.UntilAsync(
            () => GatedRecordingTransport.ReseedEpoch(tree) >= exportedWhileDetached,
            "the re-attached shipper to ask for a re-seed past the export taken while it was detached",
            TimeSpan.FromSeconds(30));
        await TestPoll.UntilAsync(
            async () => (await registry.GetConsumersAsync()).Contains(shipper.GetGrainId()),
            "a re-attached shipper to register with the log again",
            TimeSpan.FromSeconds(30));
        Assert.That(await shipper.AsReference<IWalOffsetConsumer>().GetDurableReadPositionsAsync(tree), Is.Not.Null,
            "a re-attached shipper holds the log again");
    }
}
