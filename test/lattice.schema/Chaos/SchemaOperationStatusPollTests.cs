using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Schema.Tests.Chaos;

[TestFixture]
public sealed class SchemaOperationStatusPollTests
{
    [Test]
    public async Task A_lost_status_response_is_retried_until_the_failed_record_is_observed()
    {
        var calls = 0;
        var failed = Record(LatticeOperationState.Failed);
        var observed = await SchemaOperationStatusPoll.WaitForFailedAsync(
            () => ++calls switch
            {
                1 => Task.FromException<LatticeOperationRecord?>(new TimeoutException("lost status response")),
                2 => Task.FromResult<LatticeOperationRecord?>(Record(LatticeOperationState.Running)),
                _ => Task.FromResult<LatticeOperationRecord?>(failed),
            },
            cadence: TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(3));
            Assert.That(observed, Is.SameAs(failed));
            Assert.That(observed.Phase, Is.EqualTo(SchemaOperationPhases.Build));
        });
    }

    [Test]
    public void A_non_timeout_status_fault_is_not_retried()
    {
        var fault = new InvalidOperationException("broken status read");
        var calls = 0;
        var observed = Assert.ThrowsAsync<InvalidOperationException>(() =>
            SchemaOperationStatusPoll.WaitForFailedAsync(() =>
            {
                calls++;
                return Task.FromException<LatticeOperationRecord?>(fault);
            }));

        Assert.Multiple(() =>
        {
            Assert.That(observed, Is.SameAs(fault));
            Assert.That(calls, Is.EqualTo(1));
        });
    }

    [TestCase(true)]
    [TestCase(false)]
    public void Timeouts_and_running_records_do_not_satisfy_the_bounded_failure_barrier(bool timeout)
    {
        var calls = 0;
        var observed = Assert.ThrowsAsync<AssertionException>(() =>
            SchemaOperationStatusPoll.WaitForFailedAsync(
                () =>
                {
                    calls++;
                    return timeout
                        ? Task.FromException<LatticeOperationRecord?>(new TimeoutException("still unavailable"))
                        : Task.FromResult<LatticeOperationRecord?>(Record(LatticeOperationState.Running));
                },
                timeout: TimeSpan.FromMilliseconds(20),
                cadence: TimeSpan.FromMilliseconds(5)));

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.GreaterThan(1));
            Assert.That(observed!.Message, Does.Contain("failed rather than running"));
        });
    }

    private static LatticeOperationRecord Record(LatticeOperationState state) => new()
    {
        OperationId = LatticeOperationKey.NewId(),
        TenantId = "default",
        Kind = SchemaOperationKinds.Remediation,
        State = state,
        Phase = SchemaOperationPhases.Build,
    };
}
