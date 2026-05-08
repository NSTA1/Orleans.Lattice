using Orleans.Lattice.BPlusTree.Grains;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Additional regression coverage for
/// <see cref="LatticeReplicationLocalVcSeeder"/>: failure-propagation
/// paths, mid-walk cancellation, idempotency under repeated invocation,
/// cache-already-populated semantics, and uniformity across replication
/// modes / tree topologies.
/// </summary>
public partial class LatticeReplicationLocalVcSeederTests
{
    // ==================================================================
    // T-1 - Mid-walk cancellation
    // ==================================================================

    [Test]
    public void SeedFromTreeAsync_observes_cancellation_between_shards()
    {
        // Two shards. Shard 0 returns no leaves (empty chain) and uses
        // its return-side as the trigger to cancel the token. The
        // next iteration of the per-shard loop must observe the
        // cancellation BEFORE issuing GetLeftmostLeafIdAsync against
        // shard 1.
        var shards = new IReadOnlyList<IReadOnlyList<LwwEntry>>[]
        {
            Array.Empty<IReadOnlyList<LwwEntry>>(),
            Array.Empty<IReadOnlyList<LwwEntry>>(),
        };
        var (seeder, factory, _, _, cache, hwmGrain) = CreateSeeder(shards: shards);
        using var cts = new CancellationTokenSource();

        // Re-wire shard 0's leaf-id call to cancel the token as a
        // side effect of returning the empty chain. NSubstitute's
        // most-recent Returns wins.
        var firstShard = factory.GetGrain<IShardRootGrain>($"{Tree}/0");
        firstShard.GetLeftmostLeafIdAsync().Returns(_ =>
        {
            cts.Cancel();
            return Task.FromResult<GrainId?>(null);
        });

        Assert.That(
            async () => await seeder.SeedFromTreeAsync(Tree, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());

        // Shard 1's left-most leaf must never be queried.
        var secondShard = factory.GetGrain<IShardRootGrain>($"{Tree}/1");
        secondShard.DidNotReceive().GetLeftmostLeafIdAsync();
        // Durable pin must not have run.
        hwmGrain.DidNotReceive().PinSnapshotAsync(
            Arg.Any<HybridLogicalClock>(), Arg.Any<VersionVector>(), Arg.Any<CancellationToken>());
        // Cache must not have been primed: cold-start RPC returns
        // empty so a clean cache snapshot is empty.
        var snapshot = cache.GetSnapshotAsync(Tree).GetAwaiter().GetResult();
        Assert.That(snapshot.Entries, Is.Empty);
    }

    [Test]
    public void SeedFromTreeAsync_observes_cancellation_between_leaves()
    {
        // Single shard, two leaves. Configure leaf-0's GetNextSibling
        // to cancel the token and return leaf-1's id; the next
        // iteration's per-leaf ThrowIfCancellationRequested must fire
        // before GetLiveRawEntriesAsync runs against leaf-1.
        var factory = Substitute.For<IGrainFactory>();
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);
        var shardCounts = Substitute.For<IShardCountProvider>();
        shardCounts.GetShardCountAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(1));

        var hwmGrain = Substitute.For<IReplicationHighWaterMarkGrain>();
        hwmGrain.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwmGrain);

        var leaf0Id = GrainId.Create("test-leaf", "0-0");
        var leaf1Id = GrainId.Create("test-leaf", "0-1");

        var shard = Substitute.For<IShardRootGrain>();
        shard.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(leaf0Id));
        factory.GetGrain<IShardRootGrain>($"{Tree}/0").Returns(shard);

        using var cts = new CancellationTokenSource();

        var leaf0 = Substitute.For<IBPlusLeafGrain>();
        leaf0.GetLiveRawEntriesAsync().Returns(
            Task.FromResult(new List<LwwEntry> { Entry("k1", Vector((OriginA, Hlc(1)))) }));
        leaf0.GetNextSiblingAsync().Returns(_ =>
        {
            cts.Cancel();
            return Task.FromResult<GrainId?>(leaf1Id);
        });
        factory.GetGrain<IBPlusLeafGrain>(leaf0Id).Returns(leaf0);

        var leaf1 = Substitute.For<IBPlusLeafGrain>();
        leaf1.GetLiveRawEntriesAsync().Returns(
            Task.FromResult(new List<LwwEntry> { Entry("k2", Vector((OriginA, Hlc(2)))) }));
        factory.GetGrain<IBPlusLeafGrain>(leaf1Id).Returns(leaf1);

        var cache = new LocalVectorClockCache(factory);
        var seeder = new LatticeReplicationLocalVcSeeder(factory, shardCounts, resolver, cache);

        Assert.That(
            async () => await seeder.SeedFromTreeAsync(Tree, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());

        // Leaf-1 must never be read.
        leaf1.DidNotReceive().GetLiveRawEntriesAsync();
        // No durable pin, no cache prime.
        hwmGrain.DidNotReceive().PinSnapshotAsync(
            Arg.Any<HybridLogicalClock>(), Arg.Any<VersionVector>(), Arg.Any<CancellationToken>());
    }

    // ==================================================================
    // T-2 - PinSnapshotAsync failure propagation
    // ==================================================================

    [Test]
    public void SeedFromTreeAsync_propagates_pin_snapshot_failure_and_does_not_prime_cache()
    {
        var shards = new IReadOnlyList<IReadOnlyList<LwwEntry>>[]
        {
            new IReadOnlyList<LwwEntry>[]
            {
                new[] { Entry("k1", Vector((OriginA, Hlc(11)), (OriginB, Hlc(22)))) },
            },
        };
        var (seeder, _, _, _, cache, hwmGrain) = CreateSeeder(shards: shards);

        // Override the helper's default no-throw setup.
        hwmGrain.PinSnapshotAsync(
                Arg.Any<HybridLogicalClock>(), Arg.Any<VersionVector>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("pin failed"));

        Assert.That(
            async () => await seeder.SeedFromTreeAsync(Tree),
            Throws.InstanceOf<InvalidOperationException>().With.Message.EqualTo("pin failed"));

        // Cache must not have been primed: AdvanceForeign runs strictly
        // after a successful PinSnapshotAsync. The cold-start RPC
        // returns an empty vector so the post-failure snapshot is
        // empty when the seed never reached the cache-prime phase.
        var snapshot = cache.GetSnapshotAsync(Tree).GetAwaiter().GetResult();
        Assert.That(snapshot.Entries, Is.Empty,
            "Cache must not contain seeded origins when the durable pin failed.");
    }

    // ==================================================================
    // T-3 - GetLiveRawEntriesAsync failure propagation
    // ==================================================================

    [Test]
    public void SeedFromTreeAsync_propagates_leaf_read_failure_and_does_not_pin_or_prime()
    {
        // Manual setup so we can throw from the leaf's
        // GetLiveRawEntriesAsync without re-using the helper's
        // pre-canned shape.
        var factory = Substitute.For<IGrainFactory>();
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);
        var shardCounts = Substitute.For<IShardCountProvider>();
        shardCounts.GetShardCountAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(1));

        var hwmGrain = Substitute.For<IReplicationHighWaterMarkGrain>();
        hwmGrain.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwmGrain);

        var leafId = GrainId.Create("test-leaf", "0-0");
        var shard = Substitute.For<IShardRootGrain>();
        shard.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(leafId));
        factory.GetGrain<IShardRootGrain>($"{Tree}/0").Returns(shard);

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.GetLiveRawEntriesAsync().ThrowsAsync(new InvalidOperationException("leaf read failed"));
        factory.GetGrain<IBPlusLeafGrain>(leafId).Returns(leaf);

        var cache = new LocalVectorClockCache(factory);
        var seeder = new LatticeReplicationLocalVcSeeder(factory, shardCounts, resolver, cache);

        Assert.That(
            async () => await seeder.SeedFromTreeAsync(Tree),
            Throws.InstanceOf<InvalidOperationException>().With.Message.EqualTo("leaf read failed"));

        // No durable pin, no cache prime.
        hwmGrain.DidNotReceive().PinSnapshotAsync(
            Arg.Any<HybridLogicalClock>(), Arg.Any<VersionVector>(), Arg.Any<CancellationToken>());
        var snapshot = cache.GetSnapshotAsync(Tree).GetAwaiter().GetResult();
        Assert.That(snapshot.Entries, Is.Empty);
    }

    // ==================================================================
    // T-4 - Same-origin pointwise-max across multiple shards
    // ==================================================================

    [Test]
    public async Task SeedFromTreeAsync_takes_pointwise_max_for_shared_origin_across_shards()
    {
        // Two shards, both carrying entries from OriginA at different
        // HLCs. Frontier must be the higher of the two (Hlc(20)),
        // not the per-shard last-write or first-write.
        var shards = new IReadOnlyList<IReadOnlyList<LwwEntry>>[]
        {
            new IReadOnlyList<LwwEntry>[]
            {
                new[] { Entry("k1", Vector((OriginA, Hlc(5)))) },
            },
            new IReadOnlyList<LwwEntry>[]
            {
                new[] { Entry("k2", Vector((OriginA, Hlc(20)))) },
            },
        };
        var (seeder, _, _, _, _, _) = CreateSeeder(shards: shards);

        var report = await seeder.SeedFromTreeAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesScanned, Is.EqualTo(2));
            Assert.That(report.Frontier!.GetClock(OriginA), Is.EqualTo(Hlc(20)),
                "Cross-shard pointwise-max must surface the highest per-origin HLC.");
        });
    }

    // ==================================================================
    // T-5 - Idempotent under repeated invocation
    // ==================================================================

    [Test]
    public async Task SeedFromTreeAsync_is_idempotent_under_repeated_invocation()
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
        var (seeder, _, _, _, _, hwmGrain) = CreateSeeder(shards: shards);

        var first = await seeder.SeedFromTreeAsync(Tree);
        var second = await seeder.SeedFromTreeAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(first.SeedApplied, Is.True);
            Assert.That(second.SeedApplied, Is.True);
            Assert.That(second.EntriesScanned, Is.EqualTo(first.EntriesScanned));
            Assert.That(second.Frontier!.GetClock(OriginA), Is.EqualTo(first.Frontier!.GetClock(OriginA)));
            Assert.That(second.Frontier!.GetClock(OriginB), Is.EqualTo(first.Frontier!.GetClock(OriginB)));
        });
        // Both calls must reach the durable pin path - the seeder
        // does not short-circuit on a "second call" detection.
        await hwmGrain.Received(2).PinSnapshotAsync(
            HybridLogicalClock.Zero, Arg.Any<VersionVector>(), Arg.Any<CancellationToken>());
    }

    // ==================================================================
    // T-6 - Cache already populated (seed must not regress)
    // ==================================================================

    [Test]
    public async Task SeedFromTreeAsync_does_not_regress_already_populated_cache()
    {
        // Cache already holds OriginA = Hlc(50) (e.g. a producer was
        // emitting before the operator ran the seeder). The seeder's
        // walk produces a frontier with OriginA = Hlc(20). The cache
        // half is pointwise-max so the higher pre-existing value
        // must survive.
        var shards = new IReadOnlyList<IReadOnlyList<LwwEntry>>[]
        {
            new IReadOnlyList<LwwEntry>[]
            {
                new[] { Entry("k1", Vector((OriginA, Hlc(20)))) },
            },
        };
        var (seeder, _, _, _, cache, _) = CreateSeeder(shards: shards);

        // Pre-populate the cache. AdvanceForeign is the producer-side
        // pointwise-max accumulator.
        cache.AdvanceForeign(Tree, OriginA, Hlc(50));

        await seeder.SeedFromTreeAsync(Tree);

        var snapshot = await cache.GetSnapshotAsync(Tree);
        Assert.That(snapshot.GetClock(OriginA), Is.EqualTo(Hlc(50)),
            "The seed's pointwise-max advance must not regress an already-higher cache entry.");
    }

    // ==================================================================
    // T-7 - IShardCountProvider failure propagation
    // ==================================================================

    [Test]
    public void SeedFromTreeAsync_propagates_shard_count_provider_failure()
    {
        var (seeder, factory, _, shardCounts, _, hwmGrain) = CreateSeeder();
        shardCounts.GetShardCountAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("registry unreachable"));

        Assert.That(
            async () => await seeder.SeedFromTreeAsync(Tree),
            Throws.InstanceOf<InvalidOperationException>().With.Message.EqualTo("registry unreachable"));

        // Nothing downstream of the shard-count lookup must have run.
        factory.DidNotReceive().GetGrain<IShardRootGrain>(Arg.Any<string>());
        hwmGrain.DidNotReceive().PinSnapshotAsync(
            Arg.Any<HybridLogicalClock>(), Arg.Any<VersionVector>(), Arg.Any<CancellationToken>());
    }

    // ==================================================================
    // T-8 - Uniform behaviour across all replication modes
    // ==================================================================

    [TestCase(LatticeMergeMode.LwwRegister)]
    [TestCase(LatticeMergeMode.OrSet)]
    [TestCase(LatticeMergeMode.PnCounter)]
    [TestCase(LatticeMergeMode.VersionVector)]
    public async Task SeedFromTreeAsync_seeds_uniformly_across_all_replication_modes(LatticeMergeMode mode)
    {
        // The seeder reads raw VC slots only; mode is consulted only
        // by the no-op gate (any non-null mode advances past the
        // gate). Confirm every declared mode produces a successful
        // seed report with the same frontier shape.
        var shards = new IReadOnlyList<IReadOnlyList<LwwEntry>>[]
        {
            new IReadOnlyList<LwwEntry>[]
            {
                new[] { Entry("k1", Vector((OriginA, Hlc(13)))) },
            },
        };
        var (seeder, _, _, _, _, hwmGrain) = CreateSeeder(mode: mode, shards: shards);

        var report = await seeder.SeedFromTreeAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.SeedApplied, Is.True);
            Assert.That(report.EntriesScanned, Is.EqualTo(1));
            Assert.That(report.Frontier!.GetClock(OriginA), Is.EqualTo(Hlc(13)));
        });
        await hwmGrain.Received(1).PinSnapshotAsync(
            HybridLogicalClock.Zero, Arg.Any<VersionVector>(), Arg.Any<CancellationToken>());
    }

    // ==================================================================
    // T-9 - Single shard, deep leaf chain
    // ==================================================================

    [Test]
    public async Task SeedFromTreeAsync_walks_deep_leaf_chain_in_single_shard()
    {
        // One shard, five leaves chained via the sibling pointer.
        // Each leaf carries one entry tagged with OriginA at HLC
        // equal to its leaf index. The seeder must walk every leaf
        // and accumulate the pointwise-max (Hlc(4)).
        const int LeafCount = 5;
        var leafBatches = new IReadOnlyList<LwwEntry>[LeafCount];
        for (var i = 0; i < LeafCount; i++)
        {
            leafBatches[i] = new[] { Entry($"k{i}", Vector((OriginA, Hlc(i)))) };
        }
        var shards = new IReadOnlyList<IReadOnlyList<LwwEntry>>[] { leafBatches };
        var (seeder, _, _, _, _, _) = CreateSeeder(shards: shards);

        var report = await seeder.SeedFromTreeAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesScanned, Is.EqualTo(LeafCount),
                "Every leaf in the deep chain must contribute exactly its entries to the scan counter.");
            Assert.That(report.Frontier!.GetClock(OriginA), Is.EqualTo(Hlc(LeafCount - 1)),
                "Pointwise-max across a deep single-shard chain must surface the highest per-origin HLC.");
        });
    }
}
