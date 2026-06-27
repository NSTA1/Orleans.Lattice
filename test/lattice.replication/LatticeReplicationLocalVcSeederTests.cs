using Orleans.Lattice.BPlusTree.Grains;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of <see cref="LatticeReplicationLocalVcSeeder"/>:
/// the intra-cluster snapshot/restore vector-clock reconstruction
/// pass that walks restored values'' <see cref="LwwEntry.VectorClock"/>
/// slots and re-seeds the per-tree local vector clock.
/// </summary>
[TestFixture]
public partial class LatticeReplicationLocalVcSeederTests
{
    private const string Tree = "restored-tree";
    private const string OriginA = "site-a";
    private const string OriginB = "site-b";
    private const string OriginC = "site-c";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static LwwEntry Entry(string key, VersionVector? vc) => new()
    {
        Key = key,
        Value = [0xAA],
        Timestamp = Hlc(1),
        VectorClock = vc,
    };

    private static VersionVector Vector(params (string Origin, HybridLogicalClock Clock)[] entries)
    {
        var v = new VersionVector();
        foreach (var (origin, clock) in entries)
        {
            v.Entries[origin] = clock;
        }
        return v;
    }

    /// <summary>
    /// Builds a seeder with a substitute grain factory that returns
    /// the supplied shard count and per-shard leaf chains. Each
    /// outer list element is one shard; each inner list element is
    /// one leaf''s live entries (in order).
    /// </summary>
    private static (
        LatticeReplicationLocalVcSeeder Seeder,
        IGrainFactory Factory,
        ILatticeMergeModeResolver Resolver,
        IShardCountProvider ShardCounts,
        IReplicationHighWaterMarkGrain HwmGrain) CreateSeeder(
            LatticeMergeMode? mode = LatticeMergeMode.LwwRegister,
            IReadOnlyList<IReadOnlyList<IReadOnlyList<LwwEntry>>>? shards = null)
    {
        var factory = Substitute.For<IGrainFactory>();
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns(mode);
        var shardCounts = Substitute.For<IShardCountProvider>();
        shardCounts.GetShardCountAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(shards?.Count ?? 1));

        // HWM grain: pinned with the computed frontier. Returns an
        // empty vector by default.
        var hwmGrain = Substitute.For<IReplicationHighWaterMarkGrain>();
        hwmGrain.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwmGrain);

        if (shards is not null)
        {
            for (var s = 0; s < shards.Count; s++)
            {
                var shardKey = $"{Tree}/{s}";
                var shardGrain = Substitute.For<IShardRootGrain>();
                var shardLeaves = shards[s];

                if (shardLeaves.Count == 0)
                {
                    shardGrain.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));
                }
                else
                {
                    // Synthesize one GrainId per leaf and chain them
                    // via GetNextSiblingAsync. The actual GrainId
                    // value is irrelevant to the seeder - it only
                    // uses the value to resolve the next leaf grain
                    // through the substituted factory - so we use
                    // string-typed leaf ids derived from the shard
                    // and leaf indexes.
                    var leafIds = new GrainId[shardLeaves.Count];
                    for (var l = 0; l < shardLeaves.Count; l++)
                    {
                        leafIds[l] = GrainId.Create("test-leaf", $"{s}-{l}");
                    }

                    shardGrain.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(leafIds[0]));

                    for (var l = 0; l < shardLeaves.Count; l++)
                    {
                        var leaf = Substitute.For<IBPlusLeafGrain>();
                        leaf.GetLiveRawEntriesAsync().Returns(Task.FromResult(shardLeaves[l].ToList()));
                        leaf.GetNextSiblingAsync().Returns(
                            Task.FromResult<GrainId?>(l + 1 < leafIds.Length ? leafIds[l + 1] : null));
                        factory.GetGrain<IBPlusLeafGrain>(leafIds[l]).Returns(leaf);
                    }
                }

                factory.GetGrain<IShardRootGrain>(shardKey).Returns(shardGrain);
            }
        }

        var seeder = new LatticeReplicationLocalVcSeeder(factory, shardCounts, resolver);
        return (seeder, factory, resolver, shardCounts, hwmGrain);
    }

    // ------------------------------------------------------------------
    // Constructor null-arg guards
    // ------------------------------------------------------------------

    [Test]
    public void Constructor_throws_when_grain_factory_is_null()
    {
        Assert.That(
            () => new LatticeReplicationLocalVcSeeder(
                null!,
                Substitute.For<IShardCountProvider>(),
                Substitute.For<ILatticeMergeModeResolver>()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_shard_counts_is_null()
    {
        Assert.That(
            () => new LatticeReplicationLocalVcSeeder(
                Substitute.For<IGrainFactory>(),
                null!,
                Substitute.For<ILatticeMergeModeResolver>()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_mode_resolver_is_null()
    {
        Assert.That(
            () => new LatticeReplicationLocalVcSeeder(
                Substitute.For<IGrainFactory>(),
                Substitute.For<IShardCountProvider>(),
                null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    // ------------------------------------------------------------------
    // Argument validation on SeedFromTreeAsync
    // ------------------------------------------------------------------

    [Test]
    public void SeedFromTreeAsync_throws_when_tree_name_is_null()
    {
        var (seeder, _, _, _, _) = CreateSeeder();
        Assert.That(
            async () => await seeder.SeedFromTreeAsync(null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void SeedFromTreeAsync_throws_when_tree_name_is_empty()
    {
        var (seeder, _, _, _, _) = CreateSeeder();
        Assert.That(
            async () => await seeder.SeedFromTreeAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void SeedFromTreeAsync_observes_cancellation_before_dispatch()
    {
        var (seeder, _, _, _, _) = CreateSeeder();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await seeder.SeedFromTreeAsync(Tree, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    // ------------------------------------------------------------------
    // No-op gate
    // ------------------------------------------------------------------

    [Test]
    public async Task SeedFromTreeAsync_returns_no_op_report_when_tree_is_not_replicated()
    {
        var (seeder, factory, resolver, shardCounts, hwmGrain) = CreateSeeder(mode: null);

        var report = await seeder.SeedFromTreeAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeName, Is.EqualTo(Tree));
            Assert.That(report.SeedApplied, Is.False);
            Assert.That(report.Frontier, Is.Null);
            Assert.That(report.EntriesScanned, Is.Zero);
        });
        resolver.Received(1).Resolve(Tree);
        // No leaf walk, no shard count lookup, no HWM pin.
        await shardCounts.DidNotReceive().GetShardCountAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await hwmGrain.DidNotReceive().PinSnapshotAsync(
            Arg.Any<HybridLogicalClock>(), Arg.Any<VersionVector>(), Arg.Any<CancellationToken>());
    }

    // ------------------------------------------------------------------
    // Empty tree
    // ------------------------------------------------------------------

    [Test]
    public async Task SeedFromTreeAsync_handles_empty_tree_with_zero_entries()
    {
        // Single shard, zero leaves.
        var shards = new IReadOnlyList<IReadOnlyList<LwwEntry>>[]
        {
            Array.Empty<IReadOnlyList<LwwEntry>>(),
        };
        var (seeder, _, _, _, hwmGrain) = CreateSeeder(shards: shards);

        var report = await seeder.SeedFromTreeAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.SeedApplied, Is.True);
            Assert.That(report.EntriesScanned, Is.Zero);
            Assert.That(report.Frontier, Is.Not.Null);
            Assert.That(report.Frontier!.Entries, Is.Empty);
        });
        // Empty frontier still pinned so the receiver sees a fresh
        // (and consistent-with-empty-tree) HWM state.
        await hwmGrain.Received(1).PinSnapshotAsync(
            HybridLogicalClock.Zero,
            Arg.Is<VersionVector>(v => v.Entries.Count == 0),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SeedFromTreeAsync_handles_zero_shard_count_with_no_walk()
    {
        var shards = Array.Empty<IReadOnlyList<IReadOnlyList<LwwEntry>>>();
        var (seeder, _, _, _, hwmGrain) = CreateSeeder(shards: shards);

        var report = await seeder.SeedFromTreeAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.SeedApplied, Is.True);
            Assert.That(report.EntriesScanned, Is.Zero);
            Assert.That(report.Frontier!.Entries, Is.Empty);
        });
        await hwmGrain.Received(1).PinSnapshotAsync(
            HybridLogicalClock.Zero, Arg.Any<VersionVector>(), Arg.Any<CancellationToken>());
    }

    // ------------------------------------------------------------------
    // Pointwise-max accumulation across multiple origins
    // ------------------------------------------------------------------

    [Test]
    public async Task SeedFromTreeAsync_accumulates_pointwise_max_across_multiple_origins()
    {
        // Single shard, single leaf, three entries with different
        // origin-tagged VCs. Frontier is pointwise max across the
        // three.
        var shards = new IReadOnlyList<IReadOnlyList<LwwEntry>>[]
        {
            new IReadOnlyList<LwwEntry>[]
            {
                new[]
                {
                    Entry("k1", Vector((OriginA, Hlc(10)), (OriginB, Hlc(5)))),
                    Entry("k2", Vector((OriginA, Hlc(7)),  (OriginB, Hlc(20)))),
                    Entry("k3", Vector((OriginC, Hlc(3)))),
                },
            },
        };
        var (seeder, _, _, _, hwmGrain) = CreateSeeder(shards: shards);

        var report = await seeder.SeedFromTreeAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.SeedApplied, Is.True);
            Assert.That(report.EntriesScanned, Is.EqualTo(3));
            Assert.That(report.Frontier!.GetClock(OriginA), Is.EqualTo(Hlc(10)));
            Assert.That(report.Frontier!.GetClock(OriginB), Is.EqualTo(Hlc(20)));
            Assert.That(report.Frontier!.GetClock(OriginC), Is.EqualTo(Hlc(3)));
        });
        await hwmGrain.Received(1).PinSnapshotAsync(
            HybridLogicalClock.Zero,
            Arg.Is<VersionVector>(v =>
                v.GetClock(OriginA) == Hlc(10) &&
                v.GetClock(OriginB) == Hlc(20) &&
                v.GetClock(OriginC) == Hlc(3)),
            Arg.Any<CancellationToken>());
    }

    // ------------------------------------------------------------------
    // Walks every shard / every leaf in chain
    // ------------------------------------------------------------------

    [Test]
    public async Task SeedFromTreeAsync_walks_every_shard_and_every_leaf_in_the_chain()
    {
        // Two shards, each with two leaves carrying disjoint origins.
        var shards = new IReadOnlyList<IReadOnlyList<LwwEntry>>[]
        {
            new IReadOnlyList<LwwEntry>[]
            {
                new[] { Entry("k1", Vector((OriginA, Hlc(1)))) },
                new[] { Entry("k2", Vector((OriginA, Hlc(5)))) },
            },
            new IReadOnlyList<LwwEntry>[]
            {
                new[] { Entry("k3", Vector((OriginB, Hlc(2)))) },
                new[] { Entry("k4", Vector((OriginB, Hlc(8)))) },
            },
        };
        var (seeder, _, _, _, _) = CreateSeeder(shards: shards);

        var report = await seeder.SeedFromTreeAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesScanned, Is.EqualTo(4));
            Assert.That(report.Frontier!.GetClock(OriginA), Is.EqualTo(Hlc(5)));
            Assert.That(report.Frontier!.GetClock(OriginB), Is.EqualTo(Hlc(8)));
        });
    }

    // ------------------------------------------------------------------
    // Skip null VC slots (legacy persisted state, pre-causal+ entries)
    // ------------------------------------------------------------------

    [Test]
    public async Task SeedFromTreeAsync_skips_entries_whose_vector_clock_is_null()
    {
        var shards = new IReadOnlyList<IReadOnlyList<LwwEntry>>[]
        {
            new IReadOnlyList<LwwEntry>[]
            {
                new[]
                {
                    Entry("legacy", null),
                    Entry("modern", Vector((OriginA, Hlc(42)))),
                    Entry("legacy2", null),
                },
            },
        };
        var (seeder, _, _, _, _) = CreateSeeder(shards: shards);

        var report = await seeder.SeedFromTreeAsync(Tree);

        Assert.Multiple(() =>
        {
            // Legacy entries still count toward EntriesScanned -
            // the diagnostic counter reflects scan work, not VC-bearing rows.
            Assert.That(report.EntriesScanned, Is.EqualTo(3));
            Assert.That(report.Frontier!.Entries.Count, Is.EqualTo(1));
            Assert.That(report.Frontier!.GetClock(OriginA), Is.EqualTo(Hlc(42)));
        });
    }

    // ------------------------------------------------------------------
    // Pin contract
    // ------------------------------------------------------------------

    [Test]
    public async Task SeedFromTreeAsync_pins_with_zero_as_of_hlc()
    {
        // The intra-cluster snapshot/restore VC seeder is intra-cluster;
        // there is no cross-cluster snapshot HLC concept, so the durable
        // pin uses HybridLogicalClock.Zero as the asOfHlc placeholder.
        var shards = new IReadOnlyList<IReadOnlyList<LwwEntry>>[]
        {
            new IReadOnlyList<LwwEntry>[]
            {
                new[] { Entry("k1", Vector((OriginA, Hlc(1)))) },
            },
        };
        var (seeder, _, _, _, hwmGrain) = CreateSeeder(shards: shards);

        await seeder.SeedFromTreeAsync(Tree);

        await hwmGrain.Received(1).PinSnapshotAsync(
            HybridLogicalClock.Zero,
            Arg.Any<VersionVector>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SeedFromTreeAsync_pin_uses_per_tree_grain_keyed_by_tree_name()
    {
        var shards = new IReadOnlyList<IReadOnlyList<LwwEntry>>[]
        {
            new IReadOnlyList<LwwEntry>[]
            {
                new[] { Entry("k1", Vector((OriginA, Hlc(1)))) },
            },
        };
        var (seeder, factory, _, _, _) = CreateSeeder(shards: shards);

        await seeder.SeedFromTreeAsync(Tree);

        factory.Received().GetGrain<IReplicationHighWaterMarkGrain>(Tree);
    }

    // ------------------------------------------------------------------
    // Durable HWM-grain pin
    // ------------------------------------------------------------------

    [Test]
    public async Task SeedFromTreeAsync_pins_computed_frontier_on_hwm_grain()
    {
        var shards = new IReadOnlyList<IReadOnlyList<LwwEntry>>[]
        {
            new IReadOnlyList<LwwEntry>[]
            {
                new[]
                {
                    Entry("k1", Vector((OriginA, Hlc(11)), (OriginB, Hlc(22)))),
                },
            },
        };
        var (seeder, _, _, _, hwmGrain) = CreateSeeder(shards: shards);

        await seeder.SeedFromTreeAsync(Tree);

        // The durable seed is the PinSnapshotAsync call carrying the
        // pointwise-max frontier accumulated from the scanned values.
        await hwmGrain.Received(1).PinSnapshotAsync(
            HybridLogicalClock.Zero,
            Arg.Is<VersionVector>(v =>
                v.GetClock(OriginA) == Hlc(11) && v.GetClock(OriginB) == Hlc(22)),
            Arg.Any<CancellationToken>());
    }

    // ------------------------------------------------------------------
    // Frontier defensive copy
    // ------------------------------------------------------------------

    [Test]
    public async Task SeedFromTreeAsync_returns_defensive_copy_of_frontier()
    {
        var shards = new IReadOnlyList<IReadOnlyList<LwwEntry>>[]
        {
            new IReadOnlyList<LwwEntry>[]
            {
                new[] { Entry("k1", Vector((OriginA, Hlc(7)))) },
            },
        };
        var (seeder, _, _, _, hwmGrain) = CreateSeeder(shards: shards);

        VersionVector? pinned = null;
        hwmGrain
            .When(g => g.PinSnapshotAsync(
                Arg.Any<HybridLogicalClock>(),
                Arg.Any<VersionVector>(),
                Arg.Any<CancellationToken>()))
            .Do(ci => pinned = ci.Arg<VersionVector>());

        var first = await seeder.SeedFromTreeAsync(Tree);
        first.Frontier!.Entries[OriginC] = Hlc(999);

        // The vector pinned on the HWM grain must not pick up the
        // caller-side mutation of the returned report.Frontier.
        Assert.That(pinned, Is.Not.Null);
        Assert.That(pinned!.GetClock(OriginC), Is.EqualTo(HybridLogicalClock.Zero),
            "Mutating the returned report.Frontier must not leak into the pinned frontier.");
    }

    // ------------------------------------------------------------------
    // Partial restore (subset of original origins)
    // ------------------------------------------------------------------

    [Test]
    public async Task SeedFromTreeAsync_partial_restore_seeds_correctly_from_surviving_subset()
    {
        // Original tree had origins {A, B, C}. After a partial
        // restore, only entries tagged with {A, B} survive. The
        // seeder produces a frontier reflecting only the surviving
        // subset.
        var shards = new IReadOnlyList<IReadOnlyList<LwwEntry>>[]
        {
            new IReadOnlyList<LwwEntry>[]
            {
                new[]
                {
                    Entry("survivor1", Vector((OriginA, Hlc(13)))),
                    Entry("survivor2", Vector((OriginB, Hlc(17)))),
                },
            },
        };
        var (seeder, _, _, _, _) = CreateSeeder(shards: shards);

        var report = await seeder.SeedFromTreeAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.SeedApplied, Is.True);
            Assert.That(report.Frontier!.Entries.Keys, Is.EquivalentTo(new[] { OriginA, OriginB }),
                "Surviving subset frontier must not synthesize entries for absent origins.");
            Assert.That(report.Frontier!.GetClock(OriginA), Is.EqualTo(Hlc(13)));
            Assert.That(report.Frontier!.GetClock(OriginB), Is.EqualTo(Hlc(17)));
            Assert.That(report.Frontier!.GetClock(OriginC), Is.EqualTo(HybridLogicalClock.Zero));
        });
    }
}