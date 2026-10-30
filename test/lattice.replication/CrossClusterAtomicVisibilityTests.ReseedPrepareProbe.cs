using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests;

public partial class CrossClusterAtomicVisibilityTests
{
    [TestCase(1, "complete", false)]
    [TestCase(128, "complete", false)]
    [TestCase(129, "complete", false)]
    [TestCase(257, "complete", false)]
    [TestCase(4, "missing", false)]
    [TestCase(4, "failure", false)]
    [TestCase(257, "complete", true)]
    public async Task Reseed_rewind_batches_prepare_probes_and_keeps_uncertain_windows(int count, string outcome, bool sharded)
    {
        const string tree = "ccv-reseed-prepare-probe";
        var (feeds, encoder, live, ticks) = TrimmedSagaFeeds(tree);
        long? echo = null;
        var registry = ReplayRegistry(Guid.NewGuid(), Guid.NewGuid(), live);
        Guid[] tracked = Enumerable.Range(0, count).Select(_ => Guid.NewGuid()).ToArray();
        if (sharded)
        {
            for (var i = 0; i < tracked.Length; i++)
            {
                var bytes = tracked[i].ToByteArray();
                bytes[7] = (byte)((bytes[7] & 0x0f) | 0x80);
                bytes[15] = (byte)(i % 2);
                tracked[i] = new Guid(bytes);
            }
        }
        var windows = new List<Guid[]>();
        registry.HasUndecidedParticipantsManyAsync(Arg.Any<IReadOnlyList<Guid>>()).Returns(call =>
        {
            var window = call.Arg<IReadOnlyList<Guid>>().ToArray();
            windows.Add(window);
            if (outcome == "failure")
                return Task.FromException<Dictionary<Guid, bool>>(new InvalidOperationException("registry unavailable"));
            var result = window.ToDictionary(txid => txid, txid => txid == tracked[0]);
            if (outcome == "missing") result.Remove(window[^1]);
            return Task.FromResult(result);
        });
        var state = new FakePersistentState<ReplicationShipperState>();
        var shipper = CreateShipper(tree, feeds, encoder, RecordingTransport(encoder, [], () => echo),
            state: state, configureFactory: factory =>
                factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry));

        await PumpAsync(shipper, ticks: 2);
        Assert.That(shipper.ReseedRequired, Is.True);
        foreach (var txid in tracked)
            state.State.Frontier.Prepares[txid] = new SourceFrontierPrepare { MinPrepare = Hlc(ticks, 1) };
        registry.ClearReceivedCalls();
        echo = 1;
        feeds[1].Append(LocalSet(tree, "wake", Hlc(ticks, 99)));
        await PumpAsync(shipper, ticks: 4);

        Assert.That(shipper.ReseedRequired, Is.False, "Exercise the real pump's rewind path.");
        Assert.That(state.State.Frontier.Prepares.Keys.Intersect(tracked),
            Is.EquivalentTo(outcome == "complete" ? tracked.Take(1) : tracked));
        Assert.That(windows.Count, Is.EqualTo(tracked.GroupBy(TxRegistryRouting.ShardOf)
            .Sum(group => (group.Count() + 127) / 128)));
        Assert.That(windows.All(window => window.Select(TxRegistryRouting.ShardOf).Distinct().Count() == 1), Is.True);
        Assert.That(windows.All(window => window.Length <= 128), Is.True);
        Assert.That(windows.SelectMany(window => window), Is.EquivalentTo(tracked));
        await registry.DidNotReceiveWithAnyArgs().GetRecordedStatusAsync(default);
        await registry.DidNotReceiveWithAnyArgs().GetParticipantsAsync(default);
    }
}
