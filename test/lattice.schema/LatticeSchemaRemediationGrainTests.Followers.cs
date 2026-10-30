using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Issue #5267: a tracked operation whose start joined an in-flight remediation
/// follows that remediation and no other. Two drivers run over one real
/// <see cref="LatticeSchemaRemediationGrain"/>: op2 joins R1, op1 drives R1 to its
/// end, and op3 is then accepted as R3. op2's driver must not slice, cancel or report
/// R3; with R1's outcome gone from the coordinator it ends superseded.
/// </summary>
public partial class LatticeSchemaRemediationGrainTests
{
    private static (string Key, string Value)[] FollowerEntries() =>
        [("k0", "{\"a\":0}"), ("k1", "{\"a\":1}")];

    /// <summary>op1 accepts R1, op2 joins it, op1 drives R1 to its end, and op3 is accepted as R3.</summary>
    private static async Task<(Harness H, LatticeSchemaRemediationReport Joined)> R1EndedThenR3AcceptedAsync()
    {
        var h = CreateGrain(FollowerEntries());
        var accepted1 = await h.Grain.AcceptAsync(LatticeValueTransform.Passthrough(), JsonPolicy(), "op1");
        var joined = await h.Grain.AcceptAsync(LatticeValueTransform.Passthrough(), JsonPolicy(), "op2");
        Assert.That(joined.OperationId, Is.EqualTo("op1"), "op2 joined R1");

        var ended = await SchemaRemediationDriver.DriveAsync(h.Grain, accepted1, progress: null, CancellationToken.None);
        Assert.That(ended.Succeeded, Is.True, ended.Reason);

        var accepted3 = await h.Grain.AcceptAsync(LatticeValueTransform.Passthrough(), MaxLenPolicy(1), "op3");
        Assert.That(accepted3.OperationId, Is.EqualTo("op3"), "R3 was accepted");
        return (h, joined);
    }

    private static void AssertSupersededAndR3Untouched(Harness h, LatticeSchemaRemediationReport followerReport)
    {
        var completion = SchemaOperationService.ToCompletion(followerReport);

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.InProgress, Is.True, "R3 is still in flight");
            Assert.That(h.State.State.OperationId, Is.EqualTo("op3"));
            Assert.That(h.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.DryRun), "op2 did not slice R3");
            Assert.That(h.State.State.ScannedCount, Is.Zero, "op2 did not slice R3");
            Assert.That(followerReport.OperationId, Is.EqualTo("op1"), "op2 does not report R3");
            Assert.That(completion.State, Is.EqualTo(LatticeOperationState.Failed));
            Assert.That(completion.Result?.GetValueOrDefault(SchemaOperationResultKeys.Outcome), Is.EqualTo("superseded"));
            Assert.That(SchemaOperationResultKeys.Superseded, Is.EqualTo("superseded"), "the recorded outcome value is wire-stable");
            Assert.That(
                completion.Result?.GetValueOrDefault(SchemaOperationResultKeys.RemediationOperationId),
                Is.EqualTo("op1"));
            Assert.That(completion.FailureReason, Does.Contain("'op1'").And.Contain("ended under another operation"));
            Assert.That(completion.FailureReason, Does.Contain("applied nothing further"));
        });
    }

    [Test]
    public async Task A_joined_driver_does_not_slice_or_report_a_later_remediation_after_the_followed_one_ended()
    {
        var (h, joined) = await R1EndedThenR3AcceptedAsync();

        var report = await SchemaRemediationDriver.DriveAsync(h.Grain, joined, progress: null, CancellationToken.None);

        AssertSupersededAndR3Untouched(h, report);
    }

    [Test]
    public async Task A_cancelled_joined_driver_does_not_cancel_slice_or_report_a_later_remediation()
    {
        var (h, joined) = await R1EndedThenR3AcceptedAsync();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var report = await SchemaRemediationDriver.DriveAsync(h.Grain, joined, progress: null, cts.Token);

        AssertSupersededAndR3Untouched(h, report);
    }

    [Test]
    public async Task A_joined_driver_reports_the_followed_remediation_outcome_while_the_coordinator_still_holds_it()
    {
        var h = CreateGrain(FollowerEntries());
        var accepted1 = await h.Grain.AcceptAsync(LatticeValueTransform.Passthrough(), JsonPolicy(), "op1");
        var joined = await h.Grain.AcceptAsync(LatticeValueTransform.Passthrough(), JsonPolicy(), "op2");
        await SchemaRemediationDriver.DriveAsync(h.Grain, accepted1, progress: null, CancellationToken.None);

        var report = await SchemaRemediationDriver.DriveAsync(h.Grain, joined, progress: null, CancellationToken.None);
        var completion = SchemaOperationService.ToCompletion(report);

        Assert.Multiple(() =>
        {
            Assert.That(report.Succeeded, Is.True, "R1's own terminal report");
            Assert.That(report.OperationId, Is.EqualTo("op1"));
            Assert.That(completion.State, Is.EqualTo(LatticeOperationState.Succeeded));
            Assert.That(
                completion.Result?.GetValueOrDefault(SchemaOperationResultKeys.Outcome),
                Is.EqualTo(SchemaOperationResultKeys.Completed));
        });
    }
}
