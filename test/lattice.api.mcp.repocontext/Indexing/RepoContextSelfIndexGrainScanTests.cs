using NSubstitute;
using Orleans.Runtime;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Unit tests for the self-heal scan loop <see cref="RepoContextSelfIndexGrain"/>
/// runs on every timer tick - the loop that makes "reach and stay fully indexed" a
/// guarantee the repository keeps on its own, with no client call.
/// <para>
/// A tick has to choose between four mutually exclusive outcomes, in a deliberate
/// order: re-drive a periodic content reconcile (so on-disk edits and deletions are
/// picked up), idle behind the jittered cooldown, re-drive a run that outright
/// failed (a failure before any structural record is written leaves nothing for the
/// file scan to detect), or walk one bounded page of the gap scan. These tests pin
/// each outcome, and the checkpointing that lets a paged scan resume, by driving the
/// grain's own captured timer callback through <see cref="SelfIndexGrainHarness"/>.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextSelfIndexGrainScanTests
{
    /// <summary>
    /// Arms a grain and moves it past the reconcile deadline that
    /// <c>EnsureRunningAsync</c> sets, so the next tick reaches the gap scan rather
    /// than being absorbed by the periodic reconcile that always runs first.
    /// </summary>
    private static async Task<RepoContextSelfIndexGrain> ArmedPastReconcileAsync(SelfIndexGrainHarness harness)
    {
        var grain = harness.CreateGrain();
        await grain.EnsureRunningAsync(SelfIndexGrainHarness.Request());

        // EnsureRunningAsync spaces the first reconcile one interval out. Leave that
        // deadline in the future so a tick falls through to the scan, and clear the
        // sweep cooldown so the scan is not gated behind it either.
        harness.State.State.NextReconcileAfterTicks = long.MaxValue;
        harness.State.State.NextSweepAfterTicks = 0;
        return grain;
    }

    [Test]
    public async Task EnsureRunningAsync_on_a_hub_arms_the_keepalive_the_timer_and_the_runner()
    {
        var harness = new SelfIndexGrainHarness();

        var progress = await harness.CreateGrain().EnsureRunningAsync(SelfIndexGrainHarness.Request());

        await harness.Reminders.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
        await harness.Runner.Received(1).StartIndexAsync(Arg.Any<RepoIndexJobRequest>());
        Assert.Multiple(() =>
        {
            Assert.That(harness.TimerCallback, Is.Not.Null, "A hub arms the scan timer as its onboarding backstop.");
            Assert.That(progress.Status, Is.EqualTo(RepoIndexStatus.Running),
                "The snapshot the runner accepted is returned verbatim.");
            Assert.That(harness.State.WriteCount, Is.EqualTo(1),
                "The first reconcile deadline is persisted so a restart mid-interval keeps it.");
            Assert.That(harness.State.State.NextReconcileAfterTicks, Is.GreaterThan(0),
                "Onboarding already reconciled the tree, so the next reconcile is spaced one interval out.");
        });
    }

    [Test]
    public async Task A_tick_past_the_reconcile_deadline_re_drives_a_periodic_reconcile()
    {
        var harness = new SelfIndexGrainHarness();
        var grain = harness.CreateGrain();
        await grain.EnsureRunningAsync(SelfIndexGrainHarness.Request());

        // Cross the reconcile deadline the onboarding pass just set.
        harness.State.State.NextReconcileAfterTicks = 0;
        harness.Job.ClearReceivedCalls();

        await harness.TickAsync();

        await harness.Job.Received(1).EnsureIndexedAsync();
        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.NextReconcileAfterTicks, Is.GreaterThan(0),
                "A completed reconcile schedules the next one, so reconciles are spaced rather than continuous.");
            Assert.That(harness.State.State.ResumeKey, Is.Null,
                "The reconcile ends the cycle, so no mid-scan resume point is left behind.");
        });
    }

    [Test]
    public async Task A_reconcile_tick_that_triggers_nothing_still_reschedules_and_ends_the_cycle()
    {
        var harness = new SelfIndexGrainHarness();
        // A run already in flight makes the idempotent re-drive a no-op.
        harness.Job.EnsureIndexedAsync().Returns(Task.FromResult(false));

        var grain = harness.CreateGrain();
        await grain.EnsureRunningAsync(SelfIndexGrainHarness.Request());
        harness.State.State.NextReconcileAfterTicks = 0;

        await harness.TickAsync();

        Assert.That(harness.State.State.NextReconcileAfterTicks, Is.GreaterThan(0),
            "A reconcile that triggered nothing still spaces the next one; it must not spin every tick.");
    }

    [Test]
    public async Task A_tick_inside_the_cooldown_is_a_cheap_no_op()
    {
        var harness = new SelfIndexGrainHarness();
        var grain = harness.CreateGrain();
        await grain.EnsureRunningAsync(SelfIndexGrainHarness.Request());

        harness.State.State.NextReconcileAfterTicks = long.MaxValue;
        harness.State.State.NextSweepAfterTicks = long.MaxValue;
        harness.Job.ClearReceivedCalls();
        var writesBefore = harness.State.WriteCount;

        await harness.TickAsync();

        await harness.Job.DidNotReceive().EnsureIndexedAsync(Arg.Any<bool>());
        await harness.Job.DidNotReceive().GetProgressAsync();
        Assert.That(harness.State.WriteCount, Is.EqualTo(writesBefore),
            "A cooling-down tick writes nothing: it is the cheap idle path between scans.");
    }

    [Test]
    public async Task A_fresh_cycle_re_drives_a_run_whose_last_pass_failed()
    {
        var harness = new SelfIndexGrainHarness();
        harness.Job.GetProgressAsync().Returns(Task.FromResult(new RepoIndexProgress
        {
            RepoId = SelfIndexGrainHarness.RepoId,
            Status = RepoIndexStatus.Failed,
            Phase = RepoIndexPhase.Pending,
        }));

        await ArmedPastReconcileAsync(harness);
        harness.Job.ClearReceivedCalls();

        await harness.TickAsync();

        await harness.Job.Received(1).EnsureIndexedAsync();
        Assert.That(harness.State.State.NextSweepAfterTicks, Is.GreaterThan(0),
            "The failed-run re-drive ends the cycle and spaces the next scan behind the cooldown.");
    }

    [Test]
    public async Task A_failed_run_re_drive_that_triggers_nothing_still_ends_the_cycle()
    {
        var harness = new SelfIndexGrainHarness();
        harness.Job.GetProgressAsync().Returns(Task.FromResult(new RepoIndexProgress
        {
            RepoId = SelfIndexGrainHarness.RepoId,
            Status = RepoIndexStatus.Failed,
            Phase = RepoIndexPhase.Pending,
        }));
        harness.Job.EnsureIndexedAsync().Returns(Task.FromResult(false));

        await ArmedPastReconcileAsync(harness);

        await harness.TickAsync();

        Assert.That(harness.State.State.NextSweepAfterTicks, Is.GreaterThan(0),
            "Whether or not a run was triggered, the failed-status branch ends the cycle.");
    }

    [Test]
    public async Task A_tick_over_a_clean_repository_finds_no_gap_and_re_drives_nothing()
    {
        var harness = new SelfIndexGrainHarness();
        harness.SeedEmbeddedFile("src/A.cs");
        harness.SeedEmbeddedFile("src/B.cs");

        await ArmedPastReconcileAsync(harness);
        harness.Job.ClearReceivedCalls();

        await harness.TickAsync();

        await harness.Job.DidNotReceive().EnsureIndexedAsync(Arg.Any<bool>());
        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.ResumeKey, Is.Null,
                "A fully walked clean range leaves no resume point.");
            Assert.That(harness.State.State.NextSweepAfterTicks, Is.GreaterThan(0),
                "A clean scan spaces the next one behind the jittered cooldown.");
        });
    }

    [Test]
    public async Task A_tick_that_finds_an_unembedded_file_re_drives_the_index_to_back_fill_it()
    {
        var harness = new SelfIndexGrainHarness();
        harness.SeedEmbeddedFile("src/A.cs");
        // B is structurally present but has no live embedding: the gap.
        harness.SeedFile("src/B.cs");

        await ArmedPastReconcileAsync(harness);
        harness.Job.ClearReceivedCalls();

        await harness.TickAsync();

        await harness.Job.Received(1).EnsureIndexedAsync(true);
        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.ResumeKey, Is.Null,
                "One trigger back-fills every gap, so the scan does not resume mid-repository.");
            Assert.That(harness.State.State.NextSweepAfterTicks, Is.GreaterThan(0),
                "A gap-found scan ends the cycle behind the cooldown rather than re-driving every tick.");
        });
    }

    [Test]
    public async Task A_gap_re_drive_that_triggers_nothing_still_ends_the_scan()
    {
        var harness = new SelfIndexGrainHarness();
        harness.SeedFile("src/only.cs");
        harness.Job.EnsureIndexedAsync(Arg.Any<bool>()).Returns(Task.FromResult(false));

        await ArmedPastReconcileAsync(harness);

        await harness.TickAsync();

        Assert.That(harness.State.State.NextSweepAfterTicks, Is.GreaterThan(0),
            "A back-fill already in flight is a no-op re-drive, but the scan cycle still ends.");
    }

    [Test]
    public async Task A_full_page_of_clean_files_checkpoints_a_resume_key_for_the_next_tick()
    {
        var harness = new SelfIndexGrainHarness();

        // Fill exactly one page so the scan reports more files remain and hands back
        // a resume point instead of ending the cycle.
        for (var i = 0; i < SelfIndexGrainHarness.PageSize; i++)
        {
            harness.SeedEmbeddedFile($"src/File{i:D4}.cs");
        }

        await ArmedPastReconcileAsync(harness);

        await harness.TickAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.ResumeKey, Is.Not.Null,
                "A page that filled hands back the successor of its last key so the walk continues.");
            Assert.That(harness.State.State.NextSweepAfterTicks, Is.Zero,
                "A mid-flight scan is not put behind the cooldown; the next tick continues it.");
        });
    }

    [Test]
    public async Task A_mid_flight_scan_continues_regardless_of_the_reconcile_and_cooldown_gates()
    {
        var harness = new SelfIndexGrainHarness();
        harness.SeedEmbeddedFile("src/A.cs");

        var grain = harness.CreateGrain();
        await grain.EnsureRunningAsync(SelfIndexGrainHarness.Request());

        // Both gates are wide open, but a non-null resume key means a scan is
        // mid-flight, so the tick must continue it rather than reconcile or idle.
        harness.State.State.ResumeKey = RepoContextKeys.File(SelfIndexGrainHarness.RepoId, "src/A.cs");
        harness.State.State.NextReconcileAfterTicks = 0;
        harness.State.State.NextSweepAfterTicks = long.MaxValue;
        harness.Job.ClearReceivedCalls();

        await harness.TickAsync();

        await harness.Job.DidNotReceive().EnsureIndexedAsync(Arg.Any<bool>());
        Assert.That(harness.State.State.ResumeKey, Is.Null,
            "The resumed page exhausted the range, so the completed scan clears its checkpoint.");
    }

    [Test]
    public async Task A_tick_stamps_the_run_credential_when_the_authority_resolves_one()
    {
        var harness = new SelfIndexGrainHarness();
        harness.SeedEmbeddedFile("src/A.cs");
        harness.RunAuthority.Resolve().Returns(new LatticeCredential("self-index", "test", "self-index"));

        await ArmedPastReconcileAsync(harness);

        await harness.TickAsync();

        harness.RunAuthority.Received().Resolve();
        Assert.That(harness.State.State.NextSweepAfterTicks, Is.GreaterThan(0),
            "The scan runs inside the credential scope exactly as it does without one.");
    }

    [Test]
    public async Task A_tick_whose_scan_faults_is_logged_and_absorbed_so_the_timer_survives()
    {
        var harness = new SelfIndexGrainHarness();
        harness.StructuralTree.KeysAsync().ReturnsForAnyArgs(
            _ => throw new InvalidOperationException("structural tree unavailable"));

        await ArmedPastReconcileAsync(harness);

        Assert.That(
            async () => await harness.TickAsync(),
            Throws.Nothing,
            "A scan fault is non-fatal: it is logged and retried on the next tick rather than killing the grain.");
    }

    [Test]
    public async Task A_tick_cancelled_by_host_shutdown_is_absorbed_without_logging_a_fault()
    {
        var harness = new SelfIndexGrainHarness();
        harness.StructuralTree.KeysAsync().ReturnsForAnyArgs(
            _ => throw new OperationCanceledException("host stopping"));

        await ArmedPastReconcileAsync(harness);

        Assert.That(
            async () => await harness.TickAsync(),
            Throws.Nothing,
            "Shutdown cancellation is expected: the keep-alive resumes the scan from the persisted checkpoint.");
    }

    [Test]
    public async Task Two_consecutive_scans_are_spaced_by_a_cooldown_above_the_base_interval()
    {
        var harness = new SelfIndexGrainHarness();
        harness.SeedEmbeddedFile("src/A.cs");

        await ArmedPastReconcileAsync(harness);
        await harness.TickAsync();
        var firstDeadline = harness.State.State.NextSweepAfterTicks;

        // A second scan from a later instant must be spaced from that later instant.
        harness.Time.Advance(TimeSpan.FromMinutes(10));
        harness.State.State.NextSweepAfterTicks = 0;
        await harness.TickAsync();

        Assert.Multiple(() =>
        {
            Assert.That(firstDeadline, Is.GreaterThan(0));
            Assert.That(harness.State.State.NextSweepAfterTicks, Is.GreaterThan(firstDeadline),
                "Each completed scan re-spaces the cooldown from the instant it finished.");
        });
    }
}
