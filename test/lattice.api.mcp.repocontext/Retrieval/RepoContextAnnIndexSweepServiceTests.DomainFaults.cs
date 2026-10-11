namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

public sealed partial class RepoContextAnnIndexSweepServiceTests
{
    [Test]
    public async Task Arming_preserves_domain_timeout_classification_instead_of_reporting_a_busy_coordinator()
    {
        var fault = new LatticeTransactionOutcomeUnavailableException("registry unavailable");
        var (factory, _) = TwoRepositories("alpha", fault);
        using var sweep = Sweep(Store(factory), Scheduler(factory));

        await sweep.StartAsync(Ct);
        try
        {
            Assert.That(await WaitForAsync(() =>
            {
                var snapshot = sweep.Reporter.Read();
                return snapshot.Armed + snapshot.Faulted >= 1;
            }, Ct), Is.True);
            Assert.Multiple(() =>
            {
                Assert.That(sweep.Reporter.Read().Faulted, Is.GreaterThanOrEqualTo(1));
                Assert.That(sweep.Reporter.Read().Arming.Deferred, Is.Zero);
                Assert.That(sweep.Reporter.Read().Arming.Armed, Is.GreaterThanOrEqualTo(1),
                    "A failed repository must not prevent the healthy repository behind it from arming.");
            });
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }
    }
}
