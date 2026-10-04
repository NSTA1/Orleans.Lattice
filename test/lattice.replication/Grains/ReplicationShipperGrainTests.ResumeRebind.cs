using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// A coordinated restore pauses shipping, cuts the source alias over to the
/// restored copy, and resumes shipping at global completion. The alias-change
/// push to the shipper is best-effort, so a resumed shipper must re-resolve its
/// source identity before its first send; otherwise it keeps draining the
/// retired log until the backstop re-resolve, and a pre-restore write still
/// unshipped there re-advances the peer's restored cut (issue #4490).
/// </summary>
public partial class ReplicationShipperGrainTests
{
    private const string RestoredPhysical = "phys-restored";

    private static (
        ReplicationShipperGrain Grain,
        StubReplogShardGrain Retired,
        StubReplogShardGrain Restored,
        ILatticeRegistry Registry,
        AppliedStream Stream,
        AdvanceableClock Clock) CreateRestoreResumeShipper()
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shipper", $"{Tree}/{Peer}"));
        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(ITimerRegistry)).Returns(Substitute.For<ITimerRegistry>());
        ctx.ActivationServices.Returns(services);

        var options = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 1,
            ShipMaxInFlight = 1,
            WireVersionNegotiationEnabled = false,
            LivenessProbeInterval = System.Threading.Timeout.InfiniteTimeSpan,
            ShipSourceIdentityBackstopInterval = TimeSpan.FromSeconds(30),
        };

        var walEncoder = new StubWalRecordEncoder();
        var retired = new StubReplogShardGrain(walEncoder);
        var restored = new StubReplogShardGrain(walEncoder);
        var (factory, registry) = FactoryWithRegistry(Tree);
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(retired);
        factory.GetGrain<IWalShardGrain>($"{RestoredPhysical}/0").Returns(restored);

        var transport = Substitute.For<IReplicationTransport>();
        var stream = RecordAppliedStream(transport, walEncoder);
        var grain = new ReplicationShipperGrain(
            ctx, Substitute.For<IReminderRegistry>(), NullLogger<ReplicationShipperGrain>.Instance,
            Monitor(options), transport, new TestEncoder(), walEncoder, Substitute.For<IWalCursorRegistry>(),
            factory, new FakePersistentState<ReplicationShipperState>(),
            new ReplicationPeerStats(), Substitute.For<ILatticeMergeModeResolver>(),
            new WireVersionNegotiationState(), new NoOpReplicationDigestProbeTransport());
        grain.InitializeForTesting(Tree, Peer);
        var clock = new AdvanceableClock(DateTimeOffset.UnixEpoch);
        grain.SetCursorFlushClockForTesting(clock);
        return (grain, retired, restored, registry, stream, clock);
    }

    [Test]
    public async Task Resume_after_a_saga_pause_rebinds_to_the_restored_copy_before_its_first_send()
    {
        var (grain, retired, restored, registry, stream, clock) = CreateRestoreResumeShipper();
        await grain.PumpForTestingAsync(CancellationToken.None);

        // A write the shipper has not shipped yet, then the coordinated restore:
        // pause, cut the alias over to the restored copy (its push to this
        // shipper lost), and resume at global completion - well inside the
        // backstop interval.
        retired.Append(MakeEntry("pre-restore", ticks: 10));
        await grain.PauseShippingAsync("restore-saga", CancellationToken.None);
        registry.ResolveAsync(Arg.Any<string>()).Returns(RestoredPhysical);
        restored.Append(MakeEntry("restored-copy-write", ticks: 20));
        clock.Advance(TimeSpan.FromSeconds(10));
        await grain.ResumeShippingAsync("restore-saga", CancellationToken.None);

        await grain.PumpForTestingAsync(CancellationToken.None);

        var shipped = stream.Applied.SelectMany(b => b).Select(r => r.Key).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(shipped, Does.Not.Contain("pre-restore"),
                "a record of the retired log must not reach the peer's restored copy after the resume");
            Assert.That(shipped, Does.Contain("restored-copy-write"),
                "the resumed shipper drains the copy the alias now resolves to");
        });
    }

    [Test]
    public async Task Resume_without_an_alias_change_keeps_shipping_the_same_log()
    {
        var (grain, retired, _, _, stream, clock) = CreateRestoreResumeShipper();
        await grain.PumpForTestingAsync(CancellationToken.None);

        retired.Append(MakeEntry("during-pause", ticks: 10));
        await grain.PauseShippingAsync("aborted-saga", CancellationToken.None);
        clock.Advance(TimeSpan.FromSeconds(10));
        await grain.ResumeShippingAsync("aborted-saga", CancellationToken.None);

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(stream.Applied.SelectMany(b => b).Select(r => r.Key), Does.Contain("during-pause"),
            "an aborted saga leaves the alias where it was; the re-resolve rebinds nothing and the paused entry ships");
    }
}
