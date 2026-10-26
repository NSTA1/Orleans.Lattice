using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests;

public partial class CrossClusterAtomicVisibilityTests
{
    [Test]
    public async Task Reseed_rewind_uses_one_probe_per_prepare_and_failed_or_undecided_sagas_keep_clamping()
    {
        const string tree = "ccv-reseed-prepare-probe";
        var (feeds, encoder, live, ticks) = TrimmedSagaFeeds(tree);
        long? echo = null;
        var registry = ReplayRegistry(Guid.NewGuid(), Guid.NewGuid(), live);
        var pending = Guid.NewGuid();
        var decided = Guid.NewGuid();
        var absent = Guid.NewGuid();
        var failed = Guid.NewGuid();
        registry.HasUndecidedParticipantsAsync(pending).Returns(true);
        registry.HasUndecidedParticipantsAsync(failed)
            .Returns(Task.FromException<bool>(new InvalidOperationException("registry unavailable")));
        var state = new FakePersistentState<ReplicationShipperState>();
        var shipper = CreateShipper(tree, feeds, encoder, RecordingTransport(encoder, [], () => echo),
            state: state, configureFactory: factory =>
                factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry));

        await PumpAsync(shipper, ticks: 2);
        Assert.That(shipper.ReseedRequired, Is.True);
        Guid[] tracked = [pending, decided, absent, failed];
        foreach (var txid in tracked)
            state.State.Frontier.Prepares[txid] = new SourceFrontierPrepare { MinPrepare = Hlc(ticks, 1) };
        registry.ClearReceivedCalls();
        echo = 1;
        feeds[1].Append(LocalSet(tree, "wake", Hlc(ticks, 99)));
        await PumpAsync(shipper, ticks: 4);

        Assert.That(shipper.ReseedRequired, Is.False, "Exercise the real pump's rewind path.");
        Assert.That(state.State.Frontier.Prepares.Keys, Does.Contain(pending).And.Contain(failed));
        Assert.That(state.State.Frontier.Prepares.Keys, Does.Not.Contain(decided).And.Not.Contain(absent));
        foreach (var txid in tracked)
        {
            await registry.Received(1).HasUndecidedParticipantsAsync(txid);
            await registry.DidNotReceive().GetRecordedStatusAsync(txid);
            await registry.DidNotReceive().GetParticipantsAsync(txid);
        }
    }
}
