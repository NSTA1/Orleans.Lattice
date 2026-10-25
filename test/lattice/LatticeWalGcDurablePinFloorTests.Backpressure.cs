using NSubstitute;

namespace Orleans.Lattice.Tests;

public sealed partial class LatticeWalGcDurablePinFloorTests
{
    [TestCase(WalSaturationState.Saturated)]
    [TestCase(WalSaturationState.Throttled)]
    public async Task Gc_trim_relief_runs_while_saturated_and_retains_the_durable_floor(WalSaturationState verdict)
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(verdict);
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        var pins = new Dictionary<string, HybridLogicalClock>
        {
            [LeafConsumer] = Hlc(10),
        };
        var gc = new LatticeWalGc(Services(provider, pins, signal), registry, Monitor());

        var report = await gc.RunOnceAsync(Tree).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.EqualTo(1));
            Assert.That(report.MinCursor, Is.EqualTo(Hlc(10)));
        });
        Assert.That(await SurvivingOffsetsAsync(provider), Is.EqualTo(new[] { 1L, 2L }));
        Assert.That(signal.ReceivedCalls(), Is.Empty, "GC relief cannot wait on saturation it is meant to relieve");
    }
}
