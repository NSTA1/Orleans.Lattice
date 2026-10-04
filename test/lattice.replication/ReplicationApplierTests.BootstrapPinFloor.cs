using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression tests for issue #4463: the bootstrap pin must not install a
/// drop floor that discards writes the snapshot does not contain. Each test
/// pins the frontier the bootstrap coordinator would pin onto a REAL
/// <see cref="ReplicationHighWaterMarkGrain"/>, then delivers a write the
/// snapshot does not hold whose source HLC is at or below that frontier's
/// coordinate, and asserts it is applied. Both the single-entry and the batch
/// paths are covered, because both used to read the pinned floor.
/// </summary>
public partial class ReplicationApplierTests
{
    private const string ThirdOrigin = "site-d";

    private static (ReplicationApplier Applier, IReplicationApplyGrain Apply, ReplicationHighWaterMarkGrain Hwm)
        CreateApplierOverRealHwmGrain()
    {
        var factory = Substitute.For<IGrainFactory>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = new ReplicationHighWaterMarkGrain(new FakePersistentState<ReplicationHighWaterMarkState>());
        factory.GetGrain<IReplicationApplyGrain>(Tree).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Tree).Returns(hwm);
        var applier = new ReplicationApplier(factory, Monitor(), replicationContext: new AnyTreeLwwContext());
        return (applier, apply, hwm);
    }

    private static VersionVector Frontier(params (string Origin, HybridLogicalClock Clock)[] entries)
    {
        var v = new VersionVector();
        foreach (var (origin, clock) in entries)
        {
            v.Entries[origin] = clock;
        }
        return v;
    }

    // Case 1 of #4463 (TLC trace): b writes k1 at HLC 1; c bootstraps from b,
    // and the coordinator seals the source coordinate at the maximum HLC in the
    // snapshot (floor[b] = 1); b then writes k2 on a fresh leaf whose clock is
    // also at 1. k2 is not in the snapshot and must be applied when it arrives.
    [Test]
    public async Task ApplyAsync_applies_a_source_write_absent_from_the_snapshot_at_the_sealed_source_coordinate()
    {
        var (applier, apply, hwm) = CreateApplierOverRealHwmGrain();
        await hwm.PinSnapshotAsync(HybridLogicalClock.Zero, Frontier((RemoteCluster, Hlc(1))), CancellationToken.None);

        var result = await applier.ApplyAsync(SetEntry("k2", Hlc(1)));

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplySetAsync("k2", Arg.Any<byte[]>(), Hlc(1), RemoteCluster, null, Arg.Any<long>());
    }

    [Test]
    public async Task ApplyBatchAsync_applies_a_source_write_absent_from_the_snapshot_at_the_sealed_source_coordinate()
    {
        var (applier, apply, hwm) = CreateApplierOverRealHwmGrain();
        await hwm.PinSnapshotAsync(HybridLogicalClock.Zero, Frontier((RemoteCluster, Hlc(1))), CancellationToken.None);

        // Two entries so the batch takes the run path rather than the
        // single-entry fast path that defers to ApplyAsync.
        var result = await applier.ApplyBatchAsync(new[] { SetEntry("k2", Hlc(1)), SetEntry("k3", Hlc(10)) });

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items => items.Count == 2 && items[0].Key == "k2"));
    }

    // Case 2 of #4463: the source's frontier coordinate for a third origin is
    // its maximum applied HLC for that origin, which is non-monotonic in
    // delivery order (#1060). The source held the third origin's write at HLC 2
    // while its write at HLC 1 (another key) was still in flight, so the pinned
    // coordinate is 2. When the third origin delivers its HLC-1 write directly
    // to the bootstrapped cluster it must be applied.
    [Test]
    public async Task ApplyAsync_applies_a_third_origin_write_below_its_non_monotonic_frontier_coordinate()
    {
        var (applier, apply, hwm) = CreateApplierOverRealHwmGrain();
        await hwm.PinSnapshotAsync(
            HybridLogicalClock.Zero,
            Frontier((RemoteCluster, Hlc(3)), (ThirdOrigin, Hlc(2))),
            CancellationToken.None);

        var result = await applier.ApplyAsync(SetEntry("other-key", Hlc(1), origin: ThirdOrigin));

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplySetAsync("other-key", Arg.Any<byte[]>(), Hlc(1), ThirdOrigin, null, Arg.Any<long>());
    }

    [Test]
    public async Task ApplyBatchAsync_applies_a_third_origin_write_below_its_non_monotonic_frontier_coordinate()
    {
        var (applier, apply, hwm) = CreateApplierOverRealHwmGrain();
        await hwm.PinSnapshotAsync(
            HybridLogicalClock.Zero,
            Frontier((RemoteCluster, Hlc(3)), (ThirdOrigin, Hlc(2))),
            CancellationToken.None);

        var result = await applier.ApplyBatchAsync(new[]
        {
            SetEntry("other-key", Hlc(1), origin: ThirdOrigin),
            SetEntry("later-key", Hlc(10), origin: ThirdOrigin),
        });

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items => items.Count == 2 && items[0].Key == "other-key"));
    }

    [Test]
    public async Task PinSnapshotAsync_installs_the_vector_but_no_drop_floor()
    {
        var (_, _, hwm) = CreateApplierOverRealHwmGrain();

        await hwm.PinSnapshotAsync(HybridLogicalClock.Zero, Frontier((RemoteCluster, Hlc(5))), CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await hwm.GetAsync(RemoteCluster, CancellationToken.None), Is.EqualTo(Hlc(5)));
            Assert.That(await hwm.GetPinnedFloorAsync(RemoteCluster, CancellationToken.None), Is.EqualTo(HybridLogicalClock.Zero));
        });
    }
}
