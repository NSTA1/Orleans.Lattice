namespace Orleans.Lattice.Tests.AdminAudit;

/// <summary>
/// Unit tests for <see cref="LatticeAdminAudit"/>, the single place the admin audit
/// protocol's ordering lives (#3638): a denial is recorded before the refusal is
/// rethrown, the intent precedes the commit and fails closed, and the finalize reports
/// only what the facade can prove.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticeAdminAuditTests
{
    private static readonly LatticeAdminAuditRequest Request = new(
        LatticeAdminAuditAction.RuleUpsert,
        LatticeAdminAuditRequest.ClusterScope,
        LatticeAdminAuditTargetKind.Rule,
        "tree/rule",
        new LatticeAdminAuditAnchor("sys-auth-policy", "k"));

    [Test]
    public void AuthorizeAsync_records_the_denial_before_the_refusal_is_rethrown()
    {
        var trail = new RecordingAdminAuditTrail();
        var refusal = new LatticeAuthorizationDeniedException("sys-auth-policy", LatticeOperation.Admin, "mallory", "no");

        var thrown = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(async () =>
            await LatticeAdminAudit.AuthorizeAsync(trail, Request, _ => ValueTask.FromException(refusal), CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.SameAs(refusal));
            Assert.That(trail.Calls, Is.EqualTo(new[] { "denied:" + LatticeAdminAuditReasonCodes.DeniedNotAuthorized }));
        });
    }

    [Test]
    public async Task AuthorizeAsync_records_nothing_when_authorization_passes()
    {
        var trail = new RecordingAdminAuditTrail();

        var record = await LatticeAdminAudit.AuthorizeAsync(trail, Request, _ => ValueTask.FromResult(42), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(record, Is.EqualTo(42));
            Assert.That(trail.Calls, Is.Empty);
        });
    }

    [Test]
    public void AuthorizeAsync_does_not_record_a_validation_error_as_a_denial()
    {
        var trail = new RecordingAdminAuditTrail();

        Assert.ThrowsAsync<ArgumentException>(async () =>
            await LatticeAdminAudit.AuthorizeAsync(
                trail, Request, _ => ValueTask.FromException(new ArgumentException("bad")), CancellationToken.None));

        Assert.That(trail.Calls, Is.Empty);
    }

    [Test]
    public async Task RunAsync_writes_the_intent_before_the_commit_and_finalizes_committed()
    {
        var trail = new RecordingAdminAuditTrail();

        await LatticeAdminAudit.RunAsync(trail, Request, _ =>
        {
            trail.Calls.Add("commit");
            return Task.CompletedTask;
        }, CancellationToken.None);

        Assert.That(trail.Calls, Is.EqualTo(new[] { "begin", "commit", "complete:Committed:" }));
    }

    [Test]
    public void RunAsync_refuses_the_call_before_the_commit_when_the_intent_cannot_be_written()
    {
        var trail = new RecordingAdminAuditTrail { FailBegin = true };
        var committed = false;

        Assert.ThrowsAsync<LatticeAdminAuditUnavailableException>(() =>
            LatticeAdminAudit.RunAsync(trail, Request, _ =>
            {
                committed = true;
                return Task.CompletedTask;
            }, CancellationToken.None));

        Assert.That(committed, Is.False, "an unauditable mutation must never reach its commit");
    }

    [Test]
    public async Task RunAsync_finalizes_a_reported_no_op_as_failed_no_change()
    {
        var trail = new RecordingAdminAuditTrail();

        var result = await LatticeAdminAudit.RunAsync(
            trail, Request, _ => Task.FromResult(false), static changed => changed, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.False);
            Assert.That(trail.Calls[^1], Is.EqualTo($"complete:Failed:{LatticeAdminAuditReasonCodes.FailedNoChange}"));
        });
    }

    [Test]
    public async Task RunAsync_leaves_the_intent_pending_when_the_result_is_undecided()
    {
        var trail = new RecordingAdminAuditTrail();

        await LatticeAdminAudit.RunAsync(
            trail, Request, _ => Task.FromResult(true), changed: null, CancellationToken.None, undecided: static _ => true);

        Assert.That(trail.Calls[^1], Is.EqualTo("complete:Pending:"));
    }

    [Test]
    public void RunAsync_leaves_the_intent_pending_on_a_failure_that_may_follow_the_commit()
    {
        var trail = new RecordingAdminAuditTrail();

        Assert.ThrowsAsync<TimeoutException>(() =>
            LatticeAdminAudit.RunAsync(trail, Request, _ => Task.FromException(new TimeoutException()), CancellationToken.None));

        Assert.That(trail.Calls, Is.EqualTo(new[] { "begin", "complete:Pending:" }));
    }

    [Test]
    public void RunAsync_finalizes_a_domain_refusal_as_failed_and_rethrows_it()
    {
        var trail = new RecordingAdminAuditTrail();

        Assert.ThrowsAsync<ArgumentException>(() =>
            LatticeAdminAudit.RunAsync(trail, Request, _ => Task.FromException(new ArgumentException("bad")), CancellationToken.None));

        Assert.That(trail.Calls[^1], Is.EqualTo($"complete:Failed:{LatticeAdminAuditReasonCodes.FailedValidation}"));
    }

    [Test]
    public async Task RunAsync_with_the_trail_disabled_runs_only_the_commit()
    {
        var trail = new RecordingAdminAuditTrail { IsEnabled = false, FailBegin = true };
        var committed = false;

        await LatticeAdminAudit.RunAsync(trail, Request, _ =>
        {
            committed = true;
            return Task.CompletedTask;
        }, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(committed, Is.True);
            Assert.That(trail.Calls, Is.Empty);
        });
    }

    [Test]
    public async Task RecordFailureAsync_records_a_denial_or_a_failure_and_skips_an_infrastructure_fault()
    {
        var trail = new RecordingAdminAuditTrail();

        await LatticeAdminAudit.RecordFailureAsync(trail, Request, new UnauthorizedAccessException());
        await LatticeAdminAudit.RecordFailureAsync(trail, Request, new ArgumentException("bad"));
        await LatticeAdminAudit.RecordFailureAsync(trail, Request, new TimeoutException());

        Assert.That(trail.Calls, Is.EqualTo(new[]
        {
            "denied:" + LatticeAdminAuditReasonCodes.DeniedNotAuthorized,
            "begin",
            $"complete:Failed:{LatticeAdminAuditReasonCodes.FailedValidation}",
        }));
    }

    [Test]
    public async Task RecordFailureAsync_swallows_an_unavailable_trail()
    {
        var trail = new RecordingAdminAuditTrail { FailBegin = true };

        await LatticeAdminAudit.RecordFailureAsync(trail, Request, new ArgumentException("bad"));

        Assert.That(trail.Calls, Is.Empty);
    }

    [Test]
    public void Classify_maps_each_failure_to_the_outcome_it_can_prove()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeAdminAudit.Classify(new UnauthorizedAccessException()).Outcome, Is.EqualTo(LatticeAdminAuditOutcome.Denied));
            Assert.That(LatticeAdminAudit.Classify(new KeyNotFoundException()).ReasonCode, Is.EqualTo(LatticeAdminAuditReasonCodes.FailedNotFound));
            Assert.That(LatticeAdminAudit.Classify(new ArgumentException()).ReasonCode, Is.EqualTo(LatticeAdminAuditReasonCodes.FailedValidation));
            Assert.That(LatticeAdminAudit.Classify(new OperationCanceledException()).Outcome, Is.EqualTo(LatticeAdminAuditOutcome.Pending));
            Assert.That(LatticeAdminAudit.Classify(new InvalidOperationException()).Outcome, Is.EqualTo(LatticeAdminAuditOutcome.Pending));
        });
    }
}
