using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    [Test]
    public async Task ExecuteAsync_refunds_admission_refusals_after_a_prior_completed_drive()
    {
        const string Tree = "starvation-after-completed-drive";
        var calls = 0;
        var refuse = true;
        var time = new VirtualTimeProvider();
        var (scheduler, recorder) = BlockedTreeProbing(
            time,
            () => Interlocked.Increment(ref calls) > 1 && refuse
                ? AdmissionRefusedProbe()
                : Task.FromResult<string?>(Tree),
            treeId: Tree);

        using (recorder)
        {
            try
            {
                await StartAndRunFirstPassAsync(scheduler, time);
                await AdvanceAtLeastAsync(time, TimeSpan.FromHours(6));

                Assert.Multiple(() =>
                {
                    Assert.That(Outcomes(recorder, "completed"), Is.EqualTo(1),
                        "the refusal phase must follow a real completed drive, not an initially empty budget.");
                    Assert.That(Outcomes(recorder, "admission_refused"), Is.GreaterThan(6),
                        "refusals after a successful drive must outlive even the capped fault-refund budget.");
                    Assert.That(Outcomes(recorder, "attempted"), Is.EqualTo(1),
                        "only the completed drive is an attempt, regardless of later refusal count.");
                    Assert.That(Outcomes(recorder, "abandoned"), Is.Zero,
                        "a prior admission must not disable the unlimited admission-refusal refund.");
                });

                refuse = false;
                Assert.That(await AttemptsBeforeFirstAbandonmentAsync(time, recorder), Is.EqualTo(3),
                    "refunding later refusals must preserve, not erase, the first successful drive's budget charge.");
            }
            finally
            {
                await scheduler.StopAsync(CancellationToken.None);
            }
        }
    }
}
