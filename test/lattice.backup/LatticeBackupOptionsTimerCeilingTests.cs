using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Tests that <see cref="LatticeBackupOptionsValidator"/> bounds the two backup timings
/// that are handed to timer-backed primitives:
/// <see cref="LatticeBackupOptions.CrossTreeFencePollInterval"/> is awaited with
/// <see cref="Task.Delay(TimeSpan, CancellationToken)"/> by the cross-tree fence drain,
/// and <see cref="LatticeBackupOptions.SinkSharingProbeTimeout"/> arms a
/// <see cref="CancellationTokenSource(TimeSpan)"/> in the silo-start sink guard and the
/// health sweep. Both primitives throw <see cref="ArgumentOutOfRangeException"/> above
/// <c>0xFFFFFFFE</c> milliseconds, so a longer value used to pass validation and then fail
/// every cross-tree capture that had to wait, and block silo start outright - the start
/// guard arms its timeout outside the fault handler that downgrades a failed probe to
/// Unverified. Each rejected value is checked against the primitive itself, so the tests
/// cannot pass by rejecting a value the timer would have accepted.
/// </summary>
[TestFixture]
public sealed class LatticeBackupOptionsTimerCeilingTests
{
    private static readonly TimeSpan TimerCeiling = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private static ValidateOptionsResult Validate(LatticeBackupOptions options) =>
        new LatticeBackupOptionsValidator().Validate(name: null, options);

    private static IEnumerable<TimeSpan> DurationsAboveTheTimerCeiling()
    {
        yield return TimerCeiling + TimeSpan.FromMilliseconds(1);
        yield return TimeSpan.FromDays(60);
        yield return TimeSpan.MaxValue;
    }

    [Test]
    public void The_validator_ceiling_is_the_longest_duration_a_timer_accepts()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeBackupOptionsValidator.MaxTimerDuration, Is.EqualTo(TimerCeiling));
            Assert.That(() => new CancellationTokenSource(TimerCeiling).Dispose(), Throws.Nothing);
            Assert.That(
                () => new CancellationTokenSource(TimerCeiling + TimeSpan.FromMilliseconds(1)),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [TestCaseSource(nameof(DurationsAboveTheTimerCeiling))]
    public void A_fence_poll_interval_a_timer_cannot_wait_is_rejected(TimeSpan interval)
    {
        var result = Validate(new LatticeBackupOptions { CrossTreeFencePollInterval = interval });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(
                result.FailureMessage,
                Does.Contain(nameof(LatticeBackupOptions.CrossTreeFencePollInterval) + " must be at most"));
            Assert.That(
                () => { _ = Task.Delay(interval, CancellationToken.None); },
                Throws.InstanceOf<ArgumentOutOfRangeException>(),
                "anti-vacuity: the rejected value is one Task.Delay itself refuses");
        });
    }

    [TestCaseSource(nameof(DurationsAboveTheTimerCeiling))]
    public void A_sink_sharing_probe_timeout_a_timer_cannot_wait_is_rejected(TimeSpan timeout)
    {
        var result = Validate(new LatticeBackupOptions { SinkSharingProbeTimeout = timeout });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(
                result.FailureMessage,
                Does.Contain(nameof(LatticeBackupOptions.SinkSharingProbeTimeout) + " must be at most"));
            Assert.That(
                () => new CancellationTokenSource(timeout),
                Throws.InstanceOf<ArgumentOutOfRangeException>(),
                "anti-vacuity: the rejected value is one CancellationTokenSource itself refuses");
        });
    }

    [Test]
    public void Timings_at_the_timer_ceiling_are_admitted()
    {
        var result = Validate(new LatticeBackupOptions
        {
            CrossTreeFencePollInterval = TimerCeiling,
            SinkSharingProbeTimeout = TimerCeiling,
        });

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void A_non_positive_sink_sharing_probe_timeout_is_still_rejected_as_non_positive()
    {
        var result = Validate(new LatticeBackupOptions { SinkSharingProbeTimeout = TimeSpan.Zero });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(
                result.FailureMessage,
                Does.Contain(nameof(LatticeBackupOptions.SinkSharingProbeTimeout) + " must be strictly positive"));
            Assert.That(result.Failures?.Count(), Is.EqualTo(1), "one broken rule reports one failure");
        });
    }
}
