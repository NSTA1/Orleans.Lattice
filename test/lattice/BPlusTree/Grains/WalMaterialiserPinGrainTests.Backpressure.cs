using NSubstitute;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public sealed partial class WalMaterialiserPinGrainTests
{
    [TestCase(WalSaturationState.Saturated)]
    [TestCase(WalSaturationState.Throttled)]
    public async Task Durable_pin_relief_persists_while_the_signal_is_not_healthy(WalSaturationState verdict)
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(verdict);
        var (grain, state) = CreateGrain(saturationSignal: signal);

        await grain.ReportAsync(Consumer, Hlc(100)).WaitAsync(TimeSpan.FromSeconds(5));
        await grain.ReportAsync(Consumer, Hlc(200)).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(2));
            Assert.That(state.State.Pins[Consumer], Is.EqualTo(Hlc(200)));
        });
        Assert.That(signal.ReceivedCalls(), Is.Empty, "pin relief must not consult or wait on its own pressure signal");
    }
}
