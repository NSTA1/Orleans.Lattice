using Orleans.Lattice.Explorer.Plugins.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// The notice a panel renders for a failed operation, and - the part that
/// matters - which failures it offers a retry for.
/// </summary>
/// <remarks>
/// A retry beside a query that will never succeed as sent invites a caller to
/// waste time; no retry beside a transient backend outage makes them abandon a
/// query that was fine. The seam keeps the three failure kinds apart precisely
/// so this decision can be right.
/// </remarks>
[TestFixture]
public sealed class TelemetryNoticeTests
{
    private static TelemetryOperationResult Failure(
        TelemetryQueryStatus status,
        ExplorerTelemetryBoundsViolation violation = ExplorerTelemetryBoundsViolation.None) =>
        TelemetryOperationResult.Failure(status, "the message the seam classified", violation);

    [Test]
    public void A_success_produces_no_notice_at_all() =>
        // A chart that rendered needs no banner saying so.
        Assert.That(TelemetryNotice.For(TelemetryOperationResult.Success("fine")), Is.Null);

    [Test]
    public void A_null_result_is_rejected() =>
        Assert.That(() => TelemetryNotice.For(null!), Throws.ArgumentNullException);

    [Test]
    public void Only_a_backend_outage_offers_a_retry()
    {
        var statuses = Enum.GetValues<TelemetryQueryStatus>()
            .Where(status => status != TelemetryQueryStatus.Succeeded);

        Assert.Multiple(() =>
        {
            foreach (var status in statuses)
            {
                var notice = TelemetryNotice.For(Failure(status));
                Assert.That(notice, Is.Not.Null, $"{status} must produce a notice");
                Assert.That(
                    notice!.IsRetryable,
                    Is.EqualTo(status == TelemetryQueryStatus.BackendUnavailable),
                    $"{status}");
            }
        });
    }

    [Test]
    public void Every_failure_carries_a_severity_and_the_seams_own_message()
    {
        var statuses = Enum.GetValues<TelemetryQueryStatus>()
            .Where(status => status != TelemetryQueryStatus.Succeeded);

        Assert.Multiple(() =>
        {
            foreach (var status in statuses)
            {
                var notice = TelemetryNotice.For(Failure(status))!;
                Assert.That(notice.Severity, Is.Not.Empty, $"{status}");
                Assert.That(notice.Message, Is.EqualTo("the message the seam classified"), $"{status}");
            }
        });
    }

    [Test]
    public void A_failure_with_no_message_still_says_something() =>
        Assert.That(
            TelemetryNotice.For(TelemetryOperationResult.Failure(TelemetryQueryStatus.Failed, string.Empty))!.Message,
            Is.Not.Empty);

    [Test]
    public void An_unauthenticated_connection_is_told_to_sign_in_because_that_is_recoverable() =>
        Assert.That(
            TelemetryNotice.For(Failure(TelemetryQueryStatus.AuthenticationRequired))!.Guidance,
            Does.Contain("Sign in"));

    [Test]
    public void An_absent_facade_is_muted_rather_than_styled_as_a_refusal() =>
        // There is nothing here, which is not the same as being told no.
        Assert.That(
            TelemetryNotice.For(Failure(TelemetryQueryStatus.Unavailable))!.Severity,
            Is.EqualTo(TelemetrySeverity.Muted));

    [Test]
    public void A_denial_is_styled_as_a_denial_rather_than_as_a_bad_request() =>
        Assert.That(
            TelemetryNotice.For(Failure(TelemetryQueryStatus.Denied))!.Severity,
            Is.EqualTo(TelemetrySeverity.Denied));

    [Test]
    public void Each_bounds_violation_names_the_control_the_caller_should_move()
    {
        var violations = new[]
        {
            (ExplorerTelemetryBoundsViolation.RangeTooLong, "shorter time range"),
            (ExplorerTelemetryBoundsViolation.LookbackTooOld, "more recent"),
            (ExplorerTelemetryBoundsViolation.TooManyPoints, "coarser step"),
            (ExplorerTelemetryBoundsViolation.StepBelowMinimum, "coarser step"),
            (ExplorerTelemetryBoundsViolation.StepAboveMaximum, "finer step"),
        };

        Assert.Multiple(() =>
        {
            foreach (var (violation, expected) in violations)
            {
                Assert.That(
                    TelemetryNotice.For(Failure(TelemetryQueryStatus.OutOfBounds, violation))!.Guidance,
                    Does.Contain(expected),
                    $"{violation}");
            }
        });
    }

    [Test]
    public void An_unclassified_bounds_violation_still_produces_a_notice_without_guidance() =>
        Assert.That(
            TelemetryNotice
                .For(Failure(TelemetryQueryStatus.OutOfBounds, ExplorerTelemetryBoundsViolation.Unspecified))!
                .Guidance,
            Is.Null);

    [Test]
    public void An_unknown_query_points_at_the_catalogue_rather_than_at_a_retry() =>
        Assert.Multiple(() =>
        {
            var notice = TelemetryNotice.For(Failure(TelemetryQueryStatus.UnknownQuery))!;
            Assert.That(notice.IsRetryable, Is.False);
            Assert.That(notice.Guidance, Does.Contain("catalogue"));
        });
}
