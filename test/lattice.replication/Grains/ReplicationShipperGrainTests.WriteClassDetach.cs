using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests.Grains;

public partial class ReplicationShipperGrainTests
{
    private static readonly string ShipperConsumerId = GrainId.Create("shipper", $"{Tree}/{Peer}").ToString();

    private static (IGrainFactory Factory, IWalPurgeHoldGrain Holds, IWalOffsetConsumerRegistryGrain Consumers)
        WriteClassDetachFactory()
    {
        var factory = Substitute.For<IGrainFactory>();
        var holds = Substitute.For<IWalPurgeHoldGrain>();
        factory.GetGrain<IWalPurgeHoldGrain>(Tree, Arg.Any<string?>()).Returns(holds);
        var consumers = Substitute.For<IWalOffsetConsumerRegistryGrain>();
        factory.GetGrain<IWalOffsetConsumerRegistryGrain>(Tree, Arg.Any<string?>()).Returns(consumers);
        var epoch = Substitute.For<IReplicationExportEpochGrain>();
        epoch.GetAsync().Returns(Task.FromResult(7L));
        factory.GetGrain<IReplicationExportEpochGrain>(Tree, Arg.Any<string?>()).Returns(epoch);
        var shard = Substitute.For<IWalShardGrain>();
        shard.ReadShippingAsync(Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(_ => new ValueTask<WalShardShippingPage>(new WalShardShippingPage { Entries = [], NextSequence = 5 }));
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(shard);
        return (factory, holds, consumers);
    }

    [Test]
    public async Task Detach_that_throws_taking_the_peer_off_the_log_is_retried_in_full()
    {
        var (factory, holds, consumers) = WriteClassDetachFactory();
        var calls = 0;
        holds.AddAsync(Arg.Any<string>(), Arg.Any<long[]>()).Returns(_ =>
            ++calls == 1 ? Task.FromException(new InvalidOperationException("hold unavailable")) : Task.CompletedTask);
        holds.ReleaseIfCoveredAsync(Arg.Any<string>(), Arg.Any<long[]?>()).Returns(Task.FromResult(true));
        var (grain, state, _, _, _, _, _) = Create(
            new LatticeReplicationOptions { ClusterId = LocalCluster }, grainFactory: factory);

        Assert.That(async () => await grain.DetachFromLogAsync(CancellationToken.None),
            Throws.TypeOf<InvalidOperationException>());
        Assert.Multiple(() =>
        {
            Assert.That(grain.DetachedFromLog, Is.False, "A failed detach must not look detached.");
            Assert.That(grain.SuspendedForWriteClass, Is.False);
            Assert.That(state.WriteCount, Is.Zero);
        });
        await consumers.DidNotReceive().UnregisterAsync(Arg.Any<GrainId>());

        await grain.DetachFromLogAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(grain.DetachedFromLog, Is.True);
            Assert.That(state.State.ReseedRequiredEpoch, Is.EqualTo(7L));
            Assert.That(state.State.ReplayHoldLog, Is.Null);
        });
        await consumers.Received(1).UnregisterAsync(Arg.Any<GrainId>());
        await holds.Received(1).ReleaseIfCoveredAsync(ShipperConsumerId, null);
        await holds.Received(1).RemoveAsync(ShipperConsumerId + "#replay");
    }

    [Test]
    public async Task Write_class_suspended_shipper_retries_a_failed_hold_release_while_still_disabled()
    {
        var (factory, holds, _) = WriteClassDetachFactory();
        var releases = 0;
        holds.ReleaseIfCoveredAsync(Arg.Any<string>(), Arg.Any<long[]?>()).Returns(_ =>
            ++releases == 1 ? Task.FromException<bool>(new InvalidOperationException("release failed")) : Task.FromResult(true));
        var removes = 0;
        holds.RemoveAsync(Arg.Any<string>()).Returns(_ =>
            ++removes == 1 ? Task.FromException(new InvalidOperationException("remove failed")) : Task.CompletedTask);
        var context = Substitute.For<ILatticeReplicationContext>();
        context.ResolveMergeMode(Tree).Returns((LatticeMergeMode?)null);
        var (grain, state, _, _, _, _, _) = Create(
            new LatticeReplicationOptions
            {
                ClusterId = LocalCluster,
                ReplicatedTrees = new Dictionary<string, LatticeMergeMode> { [Tree] = LatticeMergeMode.LwwRegister },
            },
            grainFactory: factory, replicationContext: context);
        var clock = new AdvanceableClock(DateTimeOffset.UnixEpoch);
        grain.SetCursorFlushClockForTesting(clock);

        await grain.PumpForTestingAsync();

        Assert.Multiple(() =>
        {
            Assert.That(grain.SuspendedForWriteClass, Is.True);
            Assert.That(state.State.ReplayHoldLog, Is.EqualTo(Tree), "The first replay-hold release failed.");
        });
        Assert.That(releases, Is.EqualTo(1));

        clock.Advance(ReplicationShipperGrain.PurgeHoldCheckInterval + TimeSpan.FromSeconds(1));
        await grain.PumpForTestingAsync();

        Assert.Multiple(() =>
        {
            Assert.That(grain.SuspendedForWriteClass, Is.True, "The tree is still disabled.");
            Assert.That(releases, Is.EqualTo(2), "The suspended shipper retries the hold release.");
            Assert.That(state.State.ReplayHoldLog, Is.Null, "The replay hold is released on the retry.");
        });
    }
}
