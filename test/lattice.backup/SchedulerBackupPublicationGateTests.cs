namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for the publication gate's retry
/// (<see cref="SchedulerBackupPublicationGate.RetryAsync{T}"/>): every grain call
/// it makes is idempotent, so a busy result and a grain response timeout are both
/// retried. A publish that completed in the grain after its caller's first attempt
/// timed out is then returned to the caller rather than reported as failed.
/// </summary>
[TestFixture]
public sealed class SchedulerBackupPublicationGateTests
{
    private static LatticeBackupOptions Fast(TimeSpan timeout) => new()
    {
        PublishHoldTimeout = timeout,
        PublishHoldRetryDelay = TimeSpan.FromMilliseconds(1),
    };

    [Test]
    public async Task A_call_that_timed_out_is_retried_and_its_result_returned()
    {
        var calls = 0;

        var result = await SchedulerBackupPublicationGate.RetryAsync(
            () => ++calls == 1 ? throw new TimeoutException("grain response timeout") : Task.FromResult("published"),
            _ => false,
            "id",
            Fast(TimeSpan.FromSeconds(30)),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo("published"));
            Assert.That(calls, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task A_busy_result_is_retried_until_the_call_is_not_busy()
    {
        var calls = 0;

        var result = await SchedulerBackupPublicationGate.RetryAsync(
            () => Task.FromResult(++calls),
            n => n < 3,
            "id",
            Fast(TimeSpan.FromSeconds(30)),
            CancellationToken.None);

        Assert.That(result, Is.EqualTo(3));
    }

    [Test]
    public void A_call_that_keeps_timing_out_fails_once_the_hold_timeout_passes()
    {
        Assert.That(
            async () => await SchedulerBackupPublicationGate.RetryAsync<string>(
                () => throw new TimeoutException("grain response timeout"),
                _ => false,
                "id",
                Fast(TimeSpan.FromMilliseconds(50)),
                CancellationToken.None),
            Throws.TypeOf<TimeoutException>().With.InnerException.TypeOf<TimeoutException>());
    }
}
