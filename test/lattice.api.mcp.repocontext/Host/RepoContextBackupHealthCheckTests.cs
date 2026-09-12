using Microsoft.Extensions.Diagnostics.HealthChecks;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers <see cref="RepoContextBackupHealthCheck"/>, the health component that
/// issue #2640 records was missing entirely.
/// </summary>
/// <remarks>
/// <para>
/// The backup wiring kept a complete status object and printed it to the log, and no
/// health component read it, so a container on which every capture threw (issue
/// #2621, 14 out of 14) answered <c>/health/live</c> and <c>/health/ready</c> green.
/// The defect was not a flag reporting the wrong value; it was that no flag existed.
/// </para>
/// <para>
/// <b>The property under test is three-valued reporting.</b> A component that is
/// healthy until something fails cannot distinguish a container capturing hourly from
/// one that has never captured at all - which is the confusion that let an
/// unprotected deployment look fine. So "nothing captured yet" is asserted to be
/// Degraded, not Healthy, and that assertion is the one that must survive a future
/// tidy-up.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextBackupHealthCheckTests
{
    private const string Tree = RepoContextHostTrees.Memory;

    private static RepoContextBackupStatus Enabled() => new(enabled: true, scopedTreeId: Tree);

    private static void Capture(RepoContextBackupStatus status, int entryCount = 412)
        => status.RecordCapture(
            backupId: "b-1",
            capturedTreeId: Tree,
            entryCount: entryCount,
            isFull: true,
            requestedIncremental: false,
            capturedAtUtc: DateTimeOffset.UnixEpoch);

    private static HealthCheckResult Check(RepoContextBackupStatus status)
        => new RepoContextBackupHealthCheck(status)
            .CheckHealthAsync(new HealthCheckContext())
            .GetAwaiter()
            .GetResult();

    [Test]
    public void A_container_that_has_captured_nothing_does_not_report_healthy()
    {
        var result = Check(Enabled());

        // The whole issue in one assertion.
        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Degraded));
            Assert.That(result.Status, Is.Not.EqualTo(HealthStatus.Healthy));
            Assert.That(result.Description, Does.Contain("Nothing has been captured yet"));
        });
    }

    [Test]
    public void A_container_whose_captures_are_all_failing_reports_unhealthy()
    {
        var status = Enabled();
        status.RecordFailure("The value cannot be an empty string. (Parameter 'originId')");

        var result = Check(status);

        // The live state of issue #2621. Every surface reported healthy through it.
        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Unhealthy));
            Assert.That(result.Description, Does.Contain("FAILING"));
            Assert.That(
                result.Description,
                Does.Contain("originId"),
                "The failure text has to reach the probe, or an operator still has to go to the logs to "
                + "find out what broke.");
        });
    }

    [Test]
    public void A_failure_after_a_successful_capture_still_reports_unhealthy()
    {
        var status = Enabled();
        Capture(status);
        status.RecordFailure("sink unreachable");

        var result = Check(status);

        // Earlier output is recoverable, but the cadence is broken and the protection
        // is ageing. A backup that fails quietly is the thing being removed.
        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Unhealthy));
            Assert.That(result.Description, Does.Contain("still"));
        });
    }

    [Test]
    public void A_working_container_reports_healthy()
    {
        var status = Enabled();
        Capture(status);

        var result = Check(status);

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Healthy));
            Assert.That(result.Description, Does.Contain("412"));
        });
    }

    [Test]
    public void A_capture_that_described_zero_entries_is_degraded_rather_than_healthy()
    {
        var status = Enabled();
        Capture(status, entryCount: 0);

        var result = Check(status);

        // It succeeded, so nothing else reports a problem; it protects nothing.
        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Degraded));
            Assert.That(result.Description, Does.Contain("ZERO entries"));
        });
    }

    [Test]
    public void A_host_with_no_sink_configured_is_healthy_and_says_it_is_not_capturing()
    {
        var status = new RepoContextBackupStatus(enabled: false, scopedTreeId: Tree);

        var result = Check(status);

        // Failing this would make an optional durability feature mandatory: a host is
        // required to boot with no sink. The fact is reported in the message instead
        // of being implied by silence.
        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Healthy));
            Assert.That(result.Description, Does.Contain("DISABLED"));
            Assert.That(result.Description, Does.Contain(Tree));
        });
    }

    [Test]
    public void The_verdicts_distinguish_working_from_never_run_from_failing()
    {
        var working = Enabled();
        Capture(working);

        var neverRun = Enabled();

        var failing = Enabled();
        failing.RecordFailure("boom");

        HealthStatus[] verdicts = [Check(working).Status, Check(neverRun).Status, Check(failing).Status];

        // Three conditions, three verdicts. A component that collapsed any two of
        // these would keep the individual tests above green only by changing one of
        // them; this fails on the collapse itself.
        Assert.That(
            verdicts,
            Is.Unique,
            "Working, never-run, and failing must be distinguishable on the probe, or the probe cannot "
            + "report the condition it exists for.");
    }
}
