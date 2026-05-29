using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for the receiver-side inbound peer-stats hook on
/// <see cref="ReplicationApplier"/>'s batch path. The applier
/// records one inbound success or error per per-origin run keyed on
/// <see cref="WalRecord.OriginClusterId"/>.
/// </summary>
public partial class ReplicationApplierTests
{
    private static (ReplicationApplier Applier, IReplicationApplyGrain Apply, ReplicationPeerStats Stats)
        CreateApplierWithStats()
    {
        var factory = Substitute.For<IGrainFactory>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationApplyGrain>(Tree).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Tree).Returns(hwm);
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>()).Returns(true);
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());
        var cache = new LocalVectorClockCache(factory);
        var stats = new ReplicationPeerStats();
        var applier = new ReplicationApplier(factory, Monitor(), cache, crdtShapes: null, logger: null, peerStats: stats);
        return (applier, apply, stats);
    }

    [Test]
    public async Task ApplyBatchAsync_records_inbound_success_per_origin_run()
    {
        var (applier, _, stats) = CreateApplierWithStats();
        var batch = new[]
        {
            SetEntry("a", Hlc(10)),
            SetEntry("b", Hlc(11)),
        };

        await applier.ApplyBatchAsync(batch);

        var inbound = stats.Snapshot()
            .SingleOrDefault(s => s.Direction == ReplicationContactDirection.Inbound
                && s.Peer == RemoteCluster && s.Tree == Tree);
        Assert.That(inbound, Is.Not.EqualTo(default(ReplicationPeerSnapshot)));
        Assert.That(inbound.LastContactSeconds, Is.Not.NaN);
        Assert.That(inbound.ConsecutiveErrors, Is.Zero);
    }

    [Test]
    public async Task ApplyBatchAsync_single_entry_path_also_records_inbound_success()
    {
        var (applier, _, stats) = CreateApplierWithStats();

        await applier.ApplyBatchAsync(new[] { SetEntry("a", Hlc(10)) });

        var inbound = stats.Snapshot()
            .SingleOrDefault(s => s.Direction == ReplicationContactDirection.Inbound);
        Assert.That(inbound, Is.Not.EqualTo(default(ReplicationPeerSnapshot)));
        Assert.That(inbound.LastContactSeconds, Is.Not.NaN);
    }

    [Test]
    public async Task ApplyBatchAsync_does_not_record_inbound_for_local_origin_entries()
    {
        var (applier, _, stats) = CreateApplierWithStats();

        await applier.ApplyBatchAsync(new[] { SetEntry("a", Hlc(10), origin: LocalCluster) });

        Assert.That(stats.Snapshot().Any(s => s.Direction == ReplicationContactDirection.Inbound), Is.False);
    }

    [Test]
    public async Task ApplyBatchAsync_multi_entry_run_records_inbound_error_when_apply_throws()
    {
        var (applier, apply, stats) = CreateApplierWithStats();
        apply.ApplyMergeManyAsync(Arg.Any<IReadOnlyList<ApplyMergeItem>>())
            .ThrowsAsync(new InvalidOperationException("simulated apply failure"));

        var batch = new[]
        {
            SetEntry("a", Hlc(10)),
            SetEntry("b", Hlc(11)),
        };

        Assert.That(async () => await applier.ApplyBatchAsync(batch),
            Throws.InstanceOf<InvalidOperationException>());

        var inbound = stats.Snapshot()
            .SingleOrDefault(s => s.Direction == ReplicationContactDirection.Inbound
                && s.Peer == RemoteCluster && s.Tree == Tree);
        Assert.That(inbound, Is.Not.EqualTo(default(ReplicationPeerSnapshot)));
        Assert.That(inbound.ConsecutiveErrors, Is.EqualTo(1),
            "a failed per-origin run records exactly one inbound error");
        Assert.That(inbound.LastContactSeconds, Is.NaN,
            "an errored run does not stamp the inbound last-contact timestamp");
    }
}
