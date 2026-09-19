using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers <see cref="RepoContextBackupStatus.State"/>, the single derivation that
/// both the backup health check and the backup metric series read.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2640. The status object already held every fact needed to say whether the
/// agent-memory tree was protected, and no machine-readable surface read any of
/// them, so a container on which every capture threw (issue #2621, 14 out of 14)
/// answered every probe green. The derivation lives on the status precisely so the
/// two surfaces cannot drift into disagreeing; these tests pin the derivation itself.
/// </para>
/// <para>
/// <b>What they defend is discrimination, not presence.</b> A state value that
/// exists but collapses "never captured" into "healthy" would pass any test that
/// only asserted a value was reported. Every test here therefore names the exact
/// state expected and, where two conditions are near neighbours, asserts they differ
/// from each other rather than merely that each is non-null.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextBackupStateTests
{
    private const string Tree = RepoContextHostTrees.Memory;

    private static RepoContextBackupStatus Enabled() => new(enabled: true, scopedTreeId: Tree);

    private static void Capture(RepoContextBackupStatus status, int entryCount = 412, string id = "b-1")
        => status.RecordCapture(
            backupId: id,
            capturedTreeId: Tree,
            entryCount: entryCount,
            isFull: true,
            requestedIncremental: false,
            capturedAtUtc: DateTimeOffset.UnixEpoch);

    [Test]
    public void A_host_with_no_sink_configured_reports_disabled()
    {
        var status = new RepoContextBackupStatus(enabled: false, scopedTreeId: Tree);

        Assert.That(status.State, Is.EqualTo(RepoContextBackupState.Disabled));
    }

    [Test]
    public void An_enabled_host_that_has_captured_nothing_is_not_protected()
    {
        var status = Enabled();

        // The false green this whole issue is about: registration is not capture, so
        // the startup state must not be the same value as the working state.
        Assert.Multiple(() =>
        {
            Assert.That(status.State, Is.EqualTo(RepoContextBackupState.NeverCaptured));
            Assert.That(
                status.State,
                Is.Not.EqualTo(RepoContextBackupState.Protected),
                "A container that has captured nothing must not report the same state as one capturing "
                + "hourly, or the surface cannot tell an unprotected deployment from a protected one.");
        });
    }

    [Test]
    public void A_failure_before_any_capture_reports_that_nothing_is_recoverable()
    {
        var status = Enabled();
        status.RecordFailure("The value cannot be an empty string. (Parameter 'originId')");

        // This is the exact state issue #2621 sat in for 14 consecutive attempts.
        Assert.That(status.State, Is.EqualTo(RepoContextBackupState.FailingUnprotected));
    }

    [Test]
    public void A_failure_after_a_successful_capture_is_a_different_state_from_one_before_it()
    {
        var everCaptured = Enabled();
        Capture(everCaptured);
        everCaptured.RecordFailure("sink unreachable");

        var neverCaptured = Enabled();
        neverCaptured.RecordFailure("sink unreachable");

        Assert.Multiple(() =>
        {
            Assert.That(everCaptured.State, Is.EqualTo(RepoContextBackupState.FailingAfterCapture));
            Assert.That(neverCaptured.State, Is.EqualTo(RepoContextBackupState.FailingUnprotected));
            Assert.That(
                everCaptured.State,
                Is.Not.EqualTo(neverCaptured.State),
                "Whether anything this container produced is recoverable is the difference between an "
                + "ageing backup and no backup, so the two failures must not report the same value.");
        });
    }

    [Test]
    public void A_successful_capture_of_a_non_empty_selection_reports_protected()
    {
        var status = Enabled();
        Capture(status);

        Assert.That(status.State, Is.EqualTo(RepoContextBackupState.Protected));
    }

    [Test]
    public void A_successful_capture_describing_zero_entries_is_not_reported_as_protected()
    {
        var status = Enabled();
        Capture(status, entryCount: 0);

        // A capture over an empty or wrongly-scoped selection succeeds and reports
        // success everywhere else. It protects nothing, so it is not Protected.
        Assert.Multiple(() =>
        {
            Assert.That(status.State, Is.EqualTo(RepoContextBackupState.CapturedNothing));
            Assert.That(status.State, Is.Not.EqualTo(RepoContextBackupState.Protected));
        });
    }

    [Test]
    public void A_capture_after_a_failure_clears_the_failing_state()
    {
        var status = Enabled();
        status.RecordFailure("transient");
        Capture(status);

        // RecordCapture clears the last failure, so a recovered container must stop
        // reporting a fault; a state that latched would be as misleading as one that
        // never reported.
        Assert.That(status.State, Is.EqualTo(RepoContextBackupState.Protected));
    }

    [Test]
    public void Every_state_the_derivation_can_return_is_a_distinct_value()
    {
        var disabled = new RepoContextBackupStatus(enabled: false, scopedTreeId: Tree);

        var neverCaptured = Enabled();

        var failingUnprotected = Enabled();
        failingUnprotected.RecordFailure("boom");

        var capturedNothing = Enabled();
        Capture(capturedNothing, entryCount: 0);

        var failingAfterCapture = Enabled();
        Capture(failingAfterCapture);
        failingAfterCapture.RecordFailure("boom");

        var protectedStatus = Enabled();
        Capture(protectedStatus);

        RepoContextBackupState[] observed =
        [
            disabled.State,
            neverCaptured.State,
            failingUnprotected.State,
            capturedNothing.State,
            failingAfterCapture.State,
            protectedStatus.State,
        ];

        // The anti-collapse assertion. A future tidy-up that folds two of these
        // together would keep every individual test above passing only if it also
        // changed one of them; this fails outright on any collapse.
        Assert.That(
            observed,
            Is.Unique,
            "Six distinguishable conditions must report six distinct values, or a surface reading the "
            + "state cannot tell them apart.");
    }
}
