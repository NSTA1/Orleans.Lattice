using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    [Test]
    public async Task ExecuteAsync_domain_timeout_is_faulted_not_an_undelivered_activation()
    {
        var time = new VirtualTimeProvider();
        var refusal = new LatticeTransactionOutcomeUnavailableException("callee reported an unknown outcome");
        var (scheduler, recorder) = BlockedTreeProbing(time, () => throw refusal);

        using (recorder)
        {
            try
            {
                await StartAndRunFirstPassAsync(scheduler, time);
                Assert.That(await AttemptsBeforeFirstAbandonmentAsync(time, recorder), Is.EqualTo(6));
                Assert.Multiple(() =>
                {
                    Assert.That(Outcomes(recorder, "faulted"), Is.EqualTo(6));
                    Assert.That(Outcomes(recorder, "undelivered"), Is.Zero);
                    Assert.That(Outcomes(recorder, "healed"), Is.Zero);
                });
            }
            finally
            {
                await scheduler.StopAsync(CancellationToken.None);
            }
        }
    }
}
