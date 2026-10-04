using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4579: the WAL GC must not trim an entry the replication shipper has
/// not read, however that entry's HLC compares with the shipper's reported HLC
/// cursor. A WAL partition is not HLC-ordered in offset (a skewed silo clock, a
/// range delete's start stamp, a merge that preserves its source stamp), so an
/// unshipped entry can sit above the shipper's read position with a stamp below
/// the cursor it has already reported. Runs the real shipper, the real WAL shard
/// grains, the real leaf and the silo's own WAL GC.
/// </summary>
[TestFixture]
[Category("Integration")]
public class WalGcShipperOffsetFloorTests
{
    private const string LocalClusterId = "site-a";
    private const string PeerClusterId = "site-b";

    private TestCluster _cluster = null!;

    private IServiceProvider SiloServices
        => ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services;

    // The WAL GC runs a pass on every silo and the cursor registry is
    // process-local, so the scenario runs the pass on each silo: at least one of
    // them does not host the shipper.
    private IEnumerable<IServiceProvider> EverySilo
        => _cluster.GetActiveSilos().Cast<InProcessSiloHandle>().Select(s => s.SiloHost.Services);

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        GatedRecordingTransport.Reset();
        var builder = new TestClusterBuilder(initialSilosCount: 2);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }

        GatedRecordingTransport.Reset();
    }

    [Test]
    public async Task Gc_retains_an_unshipped_entry_stamped_below_the_shipper_cursor_after_its_leaf_checkpoints()
    {
        var tree = "gc-shipper-floor-" + Guid.NewGuid().ToString("N")[..8];
        var client = _cluster.Client;
        await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
            tree,
            new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
        var partitions = await SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(tree);
        Assert.That(partitions, Is.GreaterThan(1), "the scenario needs two WAL partitions");

        // Two keys in partition q, one key in another partition p.
        var (shippedInQ, unshippedInQ, inP, q) = PickKeys(partitions);
        var lattice = client.GetGrain<ILattice>(tree);
        await client.GetGrain<IReplicationShipperGrain>($"{tree}/{PeerClusterId}").EnsureActiveAsync(CancellationToken.None);

        // The shipper ships partition q's first entry, then a later entry from
        // partition p, and reports the later entry's HLC as its cursor.
        GatedRecordingTransport.Accepting = true;
        await lattice.SetAsync(shippedInQ, [1]);
        await lattice.SetAsync(inP, [2]);
        await TestPoll.UntilAsync(
            () => GatedRecordingTransport.Shipped(tree, shippedInQ) && GatedRecordingTransport.Shipped(tree, inP),
            "the shipper to deliver both committed writes",
            TimeSpan.FromSeconds(30));
        var shipperCursor = GatedRecordingTransport.ShippedStamp(tree, inP);
        await TestPoll.UntilAsync(
            async () =>
            {
                foreach (var silo in EverySilo)
                {
                    var snapshot = await silo.GetRequiredService<IWalCursorRegistry>().SnapshotAsync(tree);
                    if (snapshot.Any(s => s.ConsumerId == PeerClusterId && s.Cursor >= shipperCursor))
                    {
                        return true;
                    }
                }

                return false;
            },
            "the shipper to report the HLC of the last entry it shipped",
            TimeSpan.FromSeconds(30));

        // The peer goes quiet, so nothing more is acknowledged. A committed
        // write then reaches partition q stamped below the cursor the shipper
        // has already reported, as a write from a silo whose clock trails does.
        GatedRecordingTransport.Accepting = false;
        var lagging = new HybridLogicalClock
        {
            WallClockTicks = shipperCursor.WallClockTicks - TimeSpan.TicksPerMillisecond,
            Counter = 0,
        };
        var walShard = client.GetGrain<IWalShardGrain>($"{tree}/{q}");
        var unshippedOffset = await walShard.AppendAsync(
            new WalRecord
            {
                TreeId = tree,
                Op = MutationKind.Set,
                Key = unshippedInQ,
                Value = [3],
                Timestamp = lagging,
                OriginClusterId = LocalClusterId,
            },
            CancellationToken.None);
        Assert.That(unshippedOffset, Is.GreaterThan(0), "the unshipped entry sits above partition q's shipped entry");

        // The owning leaf applies the entry and checkpoints past it, so the
        // durable materialiser offset floor no longer holds it.
        await CheckpointLeavesPastAsync(tree, q, unshippedOffset, lattice, unshippedInQ);
        Assert.That(await lattice.GetAsync(unshippedInQ), Is.EqualTo(new byte[] { 3 }), "the leaf applied the entry");
        Assert.That(GatedRecordingTransport.Shipped(tree, unshippedInQ), Is.False, "the peer has not received the entry");

        var trimmed = await RunGcOnEverySiloAsync(tree);

        var retained = await walShard.ReadAsync(0, 8, CancellationToken.None);
        var shipper = client.GetGrain<IReplicationShipperGrain>($"{tree}/{PeerClusterId}").AsReference<IWalOffsetConsumer>();
        var positions = await shipper.GetDurableReadPositionsAsync(tree);
        var otherLog = await shipper.GetDurableReadPositionsAsync(tree + "-other");
        Assert.Multiple(() =>
        {
            Assert.That(positions, Is.Not.Null.And.Length.GreaterThan(q), "the shipper publishes a position for every partition it reads");
            Assert.That(positions![q], Is.EqualTo(unshippedOffset), "the shipper's durable position in q is the unshipped entry");
            Assert.That(otherLog, Is.Null, "the shipper holds no log it does not read");
            Assert.That(trimmed, Is.GreaterThan(0),
                "the pass must be able to trim partition q's shipped entry, or the retention below proves nothing");
            Assert.That(retained.Entries.Select(e => e.Sequence), Does.Not.Contain(0L),
                "partition q's shipped entry is trimmed");
            Assert.That(retained.Entries.Select(e => e.Sequence), Does.Contain(unshippedOffset),
                "an entry the shipper has not read was trimmed because its HLC is at or below the shipper's reported cursor");
        });

        // The peer comes back and receives the entry.
        GatedRecordingTransport.Accepting = true;
        await TestPoll.UntilAsync(
            () => GatedRecordingTransport.Shipped(tree, unshippedInQ),
            "the shipper to deliver the retained entry once the peer acknowledges again",
            TimeSpan.FromSeconds(60));
    }

    [Test]
    public async Task Gc_holds_every_entry_for_a_registered_shipper_that_has_acknowledged_nothing()
    {
        var tree = "gc-shipper-cold-" + Guid.NewGuid().ToString("N")[..8];
        var client = _cluster.Client;
        await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
            tree,
            new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
        var partitions = await SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(tree);
        var (key, _, _, q) = PickKeys(partitions);
        var lattice = client.GetGrain<ILattice>(tree);

        // The peer never acknowledges this tree, so the shipper registers and
        // reads but has no durable position: it would resume from offset 0.
        GatedRecordingTransport.Refuse(tree);
        var shipper = client.GetGrain<IReplicationShipperGrain>($"{tree}/{PeerClusterId}");
        await shipper.EnsureActiveAsync(CancellationToken.None);
        await lattice.SetAsync(key, [1]);
        await TestPoll.UntilAsync(
            async () => (await client.GetGrain<IWalOffsetConsumerRegistryGrain>(tree).GetConsumersAsync()).Count > 0,
            "the shipper to register as an offset consumer of the tree's log before it reads",
            TimeSpan.FromSeconds(30));

        await CheckpointLeavesPastAsync(tree, q, 0, lattice, key);
        var trimmed = await RunGcOnEverySiloAsync(tree);

        var retained = await client.GetGrain<IWalShardGrain>($"{tree}/{q}").ReadAsync(0, 8, CancellationToken.None);
        var positions = await shipper.AsReference<IWalOffsetConsumer>().GetDurableReadPositionsAsync(tree);
        Assert.Multiple(() =>
        {
            Assert.That(positions, Is.Not.Null.And.All.EqualTo(0L), "a shipper that has acknowledged nothing holds every partition from offset 0");
            Assert.That(trimmed, Is.Zero, "no pass may trim an entry a registered shipper has not acknowledged");
            Assert.That(retained.Entries.Select(e => e.Sequence), Does.Contain(0L), "the unacknowledged entry is retained");
        });
    }

    private async Task<long> RunGcOnEverySiloAsync(string tree)
    {
        long trimmed = 0;
        foreach (var silo in EverySilo)
        {
            trimmed += (await silo.GetRequiredService<ILatticeWalGc>().RunOnceAsync(tree)).EntriesTrimmed;
        }

        return trimmed;
    }

    private (string ShippedInQ, string UnshippedInQ, string InP, int Q) PickKeys(int partitions)
    {
        var shippedInQ = "k-0";
        var q = WalPartitionHash.Compute(shippedInQ, partitions);
        string? unshippedInQ = null;
        string? inP = null;
        for (var i = 1; unshippedInQ is null || inP is null; i++)
        {
            var key = $"k-{i}";
            var partition = WalPartitionHash.Compute(key, partitions);
            if (partition == q)
            {
                unshippedInQ ??= key;
            }
            else
            {
                inP ??= key;
            }
        }

        return (shippedInQ, unshippedInQ, inP, q);
    }

    /// <summary>
    /// Deactivates and re-reads the tree's leaves until one of them has a
    /// durable checkpoint in partition <paramref name="partition"/> at or past
    /// <paramref name="offset"/>. A leaf advances its durable pin on a cold
    /// activation's replay, so each attempt deactivates every data leaf the pin
    /// store names, lets the deactivation apply, and reads through the tree.
    /// </summary>
    private async Task CheckpointLeavesPastAsync(string tree, int partition, long offset, ILattice lattice, string key)
    {
        var client = _cluster.Client;
        var shards = WalMaterialiserPinRouting.ResolveShardCount(
            SiloServices.GetService<Microsoft.Extensions.Options.IOptionsMonitor<LatticeOptions>>());
        var pinKeys = WalMaterialiserPinRouting.EnumerateReadKeys(tree, shards);
        var suffix = "_" + partition;

        await TestPoll.UntilAsync(
            async () =>
            {
                var leaves = new HashSet<Guid>();
                var checkpointed = false;
                foreach (var pinKey in pinKeys)
                {
                    var pins = client.GetGrain<IWalMaterialiserPinGrain>(pinKey);
                    foreach (var (consumerId, pinOffset) in await pins.GetPinOffsetsAsync())
                    {
                        checkpointed |= consumerId.EndsWith(suffix, StringComparison.Ordinal) && pinOffset >= offset;
                        var start = consumerId.IndexOf("bplusleaf/", StringComparison.Ordinal);
                        var end = consumerId.LastIndexOf('_');
                        if (start >= 0 && end > start + 10
                            && Guid.TryParseExact(consumerId[(start + 10)..end], "N", out var leaf))
                        {
                            leaves.Add(leaf);
                        }
                    }
                }

                if (checkpointed)
                {
                    return true;
                }

                foreach (var leaf in leaves)
                {
                    await client.GetGrain<IBPlusLeafGrain>(leaf).ForceDeactivateAsync();
                }

                await Task.Delay(250);
                await lattice.GetAsync(key);
                return false;
            },
            $"a leaf to checkpoint partition {partition} through offset {offset}",
            TimeSpan.FromSeconds(60),
            TimeSpan.FromMilliseconds(100));
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            // The test drives the GC pass itself.
            siloBuilder.ConfigureLattice(o => o.WalGcInterval = TimeSpan.Zero);
            siloBuilder.AddLatticeReplication(o =>
            {
                o.ClusterId = LocalClusterId;
                o.ReplicationPeers = [PeerClusterId];
                o.ShipCursorWriteInterval = 1;
            });
            siloBuilder.Services.AddSingleton<IReplicationTransport, GatedRecordingTransport>();
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, LwwResolver>();
        }
    }

    private sealed class LwwResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

    /// <summary>
    /// Records every shipped entry by tree and key, and acknowledges only while
    /// <see cref="Accepting"/> is set. A refused ack leaves the shipper's
    /// cursors where they were, which is a peer that has stopped acknowledging.
    /// </summary>
    private sealed class GatedRecordingTransport(IWalRecordEncoder encoder) : IReplicationTransport
    {
        private static readonly ConcurrentDictionary<(string Tree, string Key), HybridLogicalClock> Delivered = new();
        private static readonly ConcurrentDictionary<string, bool> Refused = new(StringComparer.Ordinal);

        public static volatile bool Accepting = true;

        public static void Reset()
        {
            Delivered.Clear();
            Refused.Clear();
            Accepting = true;
        }

        /// <summary>Never acknowledges anything for <paramref name="tree"/>.</summary>
        public static void Refuse(string tree) => Refused[tree] = true;

        public static bool Shipped(string tree, string key) => Delivered.ContainsKey((tree, key));

        public static HybridLogicalClock ShippedStamp(string tree, string key)
            => Delivered.TryGetValue((tree, key), out var stamp) ? stamp : HybridLogicalClock.Zero;

        public Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
        {
            if (!Accepting || Refused.ContainsKey(batch.TreeName))
            {
                return Task.FromResult(new ReplicationAck { Accepted = false, HighestAppliedHlc = HybridLogicalClock.Zero });
            }

            if (batch.EncodedEnvelope is { } envelope)
            {
                foreach (var segment in envelope.EncodedEntries.Span)
                {
                    var record = encoder.Decode(segment.AsSpan(), batch.TreeName, envelope.Header.Mode);
                    Delivered[(batch.TreeName, record.Key)] = record.Timestamp;
                }
            }

            return Task.FromResult(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });
        }
    }
}
