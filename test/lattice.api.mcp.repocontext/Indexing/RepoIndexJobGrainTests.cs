using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Runtime;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Unit tests for <see cref="RepoIndexJobGrain"/>: the durable, reminder-anchored
/// coordinator for one repository's indexing job.
/// <para>
/// The contract under test is that the job is decoupled from the call that starts it
/// and survives a restart: a start records the request and hands off, a second start
/// while a run is live re-attaches instead of duplicating, a late progress report for
/// a settled job is ignored, and the resume reminder re-enqueues a genuinely running
/// job but retires itself once the job has settled. The two reminder-registry arms
/// are deliberately non-fatal - a reminder-service hiccup degrades the restart
/// backstop but must never fail the job - so they only run when the registry throws,
/// which is why they need a substituted registry to fault on demand.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoIndexJobGrainTests
{
    [Test]
    public async Task StartAsync_records_the_request_arms_the_reminder_and_hands_off_to_the_runner()
    {
        var harness = new RepoIndexJobGrainHarness();
        var request = RepoIndexJobGrainHarness.Request();

        var progress = await harness.CreateGrain().StartAsync(request);

        await harness.Reminders.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(),
            RepoIndexJobGrainHarness.ResumeReminderName,
            Arg.Any<TimeSpan>(),
            Arg.Any<TimeSpan>());
        harness.Runner.Received(1).Enqueue(request);
        Assert.Multiple(() =>
        {
            Assert.That(progress.Status, Is.EqualTo(RepoIndexStatus.Running));
            Assert.That(progress.RepoId, Is.EqualTo(RepoIndexJobGrainHarness.RepoId));
            Assert.That(progress.Attempt, Is.EqualTo(1), "The first start is attempt one.");
            Assert.That(harness.State.State.Request, Is.EqualTo(request),
                "The request is persisted so a resume needs no client call.");
            Assert.That(harness.State.WriteCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task StartAsync_while_a_run_is_live_re_attaches_instead_of_restarting_the_job()
    {
        var harness = new RepoIndexJobGrainHarness();
        var original = RepoIndexJobGrainHarness.Request();
        harness.State.State.Request = original;
        harness.State.State.Status = RepoIndexStatus.Running;
        harness.State.State.Attempt = 7;
        harness.State.State.FilesScanned = 42;

        var progress = await harness.CreateGrain().StartAsync(RepoIndexJobGrainHarness.Request(allowPrune: true));

        harness.Runner.Received(1).Enqueue(original);
        await harness.Reminders.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
        Assert.Multiple(() =>
        {
            Assert.That(progress.Attempt, Is.EqualTo(7),
                "Re-attaching must not consume a fresh attempt; the live run is unchanged.");
            Assert.That(progress.FilesScanned, Is.EqualTo(42),
                "Re-attaching reports the live run's current progress.");
            Assert.That(harness.State.WriteCount, Is.Zero,
                "A re-attach is a pure read of the live job.");
        });
    }

    [Test]
    public async Task StartAsync_re_attaching_a_running_job_with_no_persisted_request_falls_back_to_the_caller()
    {
        var harness = new RepoIndexJobGrainHarness();
        harness.State.State.Status = RepoIndexStatus.Running;
        var request = RepoIndexJobGrainHarness.Request();

        await harness.CreateGrain().StartAsync(request);

        harness.Runner.Received(1).Enqueue(request);
    }

    [Test]
    public void StartAsync_rejects_a_null_request()
    {
        var grain = new RepoIndexJobGrainHarness().CreateGrain();

        Assert.That(async () => await grain.StartAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task StartAsync_survives_a_reminder_registry_that_cannot_arm_the_resume_backstop()
    {
        var harness = new RepoIndexJobGrainHarness();
        harness.Reminders
            .RegisterOrUpdateReminder(Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .Throws(new InvalidOperationException("reminder service unavailable"));

        var progress = await harness.CreateGrain().StartAsync(RepoIndexJobGrainHarness.Request());

        harness.Runner.Received(1).Enqueue(Arg.Any<RepoIndexJobRequest>());
        Assert.That(progress.Status, Is.EqualTo(RepoIndexStatus.Running),
            "A reminder hiccup degrades only the restart backstop; the run still proceeds via the runner.");
    }

    [Test]
    public async Task StartAsync_clears_the_counters_and_error_of_a_previous_failed_attempt()
    {
        var harness = new RepoIndexJobGrainHarness();
        harness.State.State.Status = RepoIndexStatus.Failed;
        harness.State.State.Attempt = 3;
        harness.State.State.FilesScanned = 900;
        harness.State.State.FilesEmbedded = 900;
        harness.State.State.ChunksCommitted = 12;
        harness.State.State.Error = "boom";
        harness.State.State.CompletedAt = harness.Time.GetUtcNow();
        harness.State.State.ElapsedMilliseconds = 1234;

        var progress = await harness.CreateGrain().StartAsync(RepoIndexJobGrainHarness.Request());

        Assert.Multiple(() =>
        {
            Assert.That(progress.Attempt, Is.EqualTo(4), "A fresh start is the next attempt.");
            Assert.That(progress.FilesScanned, Is.Zero);
            Assert.That(progress.FilesEmbedded, Is.Zero);
            Assert.That(progress.ChunksCommitted, Is.Zero);
            Assert.That(progress.Error, Is.Null);
            Assert.That(progress.CompletedAt, Is.Null);
            Assert.That(progress.ElapsedMilliseconds, Is.Null);
        });
    }

    [Test]
    public async Task EnsureIndexedAsync_is_a_no_op_for_a_repository_that_was_never_bootstrapped()
    {
        var harness = new RepoIndexJobGrainHarness();

        var started = await harness.CreateGrain().EnsureIndexedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(started, Is.False,
                "With no persisted request there is nothing for the self-heal sweep to re-drive.");
            Assert.That(harness.State.WriteCount, Is.Zero);
        });
        harness.Runner.DidNotReceive().Enqueue(Arg.Any<RepoIndexJobRequest>());
    }

    [Test]
    public async Task EnsureIndexedAsync_is_a_no_op_while_a_run_is_already_in_flight()
    {
        var harness = new RepoIndexJobGrainHarness();
        harness.State.State.Request = RepoIndexJobGrainHarness.Request();
        harness.State.State.Status = RepoIndexStatus.Running;

        var started = await harness.CreateGrain().EnsureIndexedAsync();

        Assert.That(started, Is.False,
            "A live run's own back-fill closes any embedding gap, so a duplicate must not start.");
        Assert.That(harness.State.WriteCount, Is.Zero);
    }

    [Test]
    public async Task EnsureIndexedAsync_re_drives_a_settled_job_with_pruning_allowed()
    {
        var harness = new RepoIndexJobGrainHarness();
        harness.State.State.Request = RepoIndexJobGrainHarness.Request();
        harness.State.State.Status = RepoIndexStatus.Completed;

        var started = await harness.CreateGrain().EnsureIndexedAsync();

        Assert.That(started, Is.True);
        harness.Runner.Received(1).Enqueue(Arg.Is<RepoIndexJobRequest>(r => r.AllowPrune));
    }

    [Test]
    public async Task GetProgressAsync_and_GetRequestAsync_project_the_persisted_state()
    {
        var harness = new RepoIndexJobGrainHarness();
        var request = RepoIndexJobGrainHarness.Request();
        harness.State.State.Request = request;
        harness.State.State.Status = RepoIndexStatus.Running;
        harness.State.State.Phase = RepoIndexPhase.Vectorising;
        var grain = harness.CreateGrain();

        var progress = await grain.GetProgressAsync();
        var read = await grain.GetRequestAsync();

        Assert.Multiple(() =>
        {
            Assert.That(progress.Phase, Is.EqualTo(RepoIndexPhase.Vectorising));
            Assert.That(progress.RepoId, Is.EqualTo(RepoIndexJobGrainHarness.RepoId));
            Assert.That(read, Is.EqualTo(request));
        });
    }

    [Test]
    public async Task ReportProgressAsync_merges_only_the_fields_the_delta_carries()
    {
        var harness = new RepoIndexJobGrainHarness();
        harness.State.State.Status = RepoIndexStatus.Running;
        harness.State.State.FilesScanned = 10;
        harness.State.State.FilesAdded = 3;
        harness.Time.Advance(TimeSpan.FromMinutes(5));

        await harness.CreateGrain().ReportProgressAsync(new RepoIndexProgressUpdate
        {
            Phase = RepoIndexPhase.Applying,
            FilesUpdated = 4,
            ChunksTotal = 9,
            ChunksCommitted = 2,
            FilesEmbedded = 1,
            FilesContentProjected = 5,
            FilesRemoved = 6,
            FilesUnchanged = 7,
        });

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.Phase, Is.EqualTo(RepoIndexPhase.Applying));
            Assert.That(harness.State.State.FilesScanned, Is.EqualTo(10),
                "A field the delta omits is left untouched.");
            Assert.That(harness.State.State.FilesAdded, Is.EqualTo(3));
            Assert.That(harness.State.State.FilesUpdated, Is.EqualTo(4));
            Assert.That(harness.State.State.FilesRemoved, Is.EqualTo(6));
            Assert.That(harness.State.State.FilesUnchanged, Is.EqualTo(7));
            Assert.That(harness.State.State.ChunksTotal, Is.EqualTo(9));
            Assert.That(harness.State.State.ChunksCommitted, Is.EqualTo(2));
            Assert.That(harness.State.State.FilesEmbedded, Is.EqualTo(1));
            Assert.That(harness.State.State.FilesContentProjected, Is.EqualTo(5));
            Assert.That(harness.State.State.UpdatedAt, Is.EqualTo(harness.Time.GetUtcNow()));
            Assert.That(harness.State.WriteCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task ReportProgressAsync_ignores_a_straggling_report_for_a_settled_job()
    {
        var harness = new RepoIndexJobGrainHarness();
        harness.State.State.Status = RepoIndexStatus.Completed;
        harness.State.State.FilesScanned = 11;

        await harness.CreateGrain().ReportProgressAsync(new RepoIndexProgressUpdate { FilesScanned = 99 });

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.FilesScanned, Is.EqualTo(11),
                "A late runner callback must not revive a cleared or settled repository.");
            Assert.That(harness.State.WriteCount, Is.Zero);
        });
    }

    [Test]
    public async Task CompleteAsync_settles_the_job_and_retires_the_resume_reminder()
    {
        var harness = new RepoIndexJobGrainHarness();
        harness.State.State.Status = RepoIndexStatus.Running;
        harness.State.State.Error = "a fault from an earlier attempt";

        await harness.CreateGrain().CompleteAsync(
            new RepoIndexProgressUpdate { FilesScanned = 12, FilesAdded = 12 }, elapsedMilliseconds: 4321);

        await harness.Reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), harness.Reminder);
        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.Status, Is.EqualTo(RepoIndexStatus.Completed));
            Assert.That(harness.State.State.Phase, Is.EqualTo(RepoIndexPhase.Done));
            Assert.That(harness.State.State.FilesScanned, Is.EqualTo(12));
            Assert.That(harness.State.State.ElapsedMilliseconds, Is.EqualTo(4321));
            Assert.That(harness.State.State.CompletedAt, Is.EqualTo(harness.Time.GetUtcNow()));
            Assert.That(harness.State.State.Error, Is.Null, "Settling clears a stale error.");
        });
    }

    [Test]
    public async Task CompleteAsync_leaves_no_reminder_to_retire_when_the_registry_reports_none()
    {
        var harness = new RepoIndexJobGrainHarness();
        harness.Reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult<IGrainReminder?>(null));

        await harness.CreateGrain().CompleteAsync(default, elapsedMilliseconds: 1);

        await harness.Reminders.DidNotReceive().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
        Assert.That(harness.State.State.Status, Is.EqualTo(RepoIndexStatus.Completed));
    }

    [Test]
    public async Task CompleteAsync_survives_a_reminder_registry_that_cannot_retire_the_backstop()
    {
        var harness = new RepoIndexJobGrainHarness();
        harness.Reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Throws(new InvalidOperationException("reminder service unavailable"));

        await harness.CreateGrain().CompleteAsync(default, elapsedMilliseconds: 1);

        Assert.That(harness.State.State.Status, Is.EqualTo(RepoIndexStatus.Completed),
            "Failing to retire the reminder must not un-settle a job that genuinely completed.");
    }

    [Test]
    public async Task FailAsync_records_the_error_and_stops_the_job_retrying_forever()
    {
        var harness = new RepoIndexJobGrainHarness();
        harness.State.State.Status = RepoIndexStatus.Running;

        await harness.CreateGrain().FailAsync("IOException: the tree vanished");

        await harness.Reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), harness.Reminder);
        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.Status, Is.EqualTo(RepoIndexStatus.Failed));
            Assert.That(harness.State.State.Error, Is.EqualTo("IOException: the tree vanished"));
            Assert.That(harness.State.WriteCount, Is.EqualTo(1));
        });
    }

    [Test]
    public void FailAsync_rejects_a_null_error()
    {
        var grain = new RepoIndexJobGrainHarness().CreateGrain();

        Assert.That(async () => await grain.FailAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task CancelAndClearAsync_cancels_the_run_retires_the_reminder_and_wipes_the_state()
    {
        var harness = new RepoIndexJobGrainHarness();
        harness.State.State.Request = RepoIndexJobGrainHarness.Request();
        harness.State.State.Status = RepoIndexStatus.Running;
        harness.State.State.FilesScanned = 500;

        await harness.CreateGrain().CancelAndClearAsync();

        harness.Runner.Received(1).Cancel(RepoIndexJobGrainHarness.RepoId);
        await harness.Reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), harness.Reminder);
        Assert.Multiple(() =>
        {
            Assert.That(harness.State.ClearCount, Is.EqualTo(1));
            Assert.That(harness.State.State.Status, Is.EqualTo(RepoIndexStatus.None));
            Assert.That(harness.State.State.Request, Is.Null);
            Assert.That(harness.State.State.FilesScanned, Is.Zero);
        });
    }

    [Test]
    public async Task ReceiveReminder_ignores_a_reminder_that_is_not_the_resume_backstop()
    {
        var harness = new RepoIndexJobGrainHarness();
        harness.State.State.Request = RepoIndexJobGrainHarness.Request();
        harness.State.State.Status = RepoIndexStatus.Running;

        await harness.CreateGrain().ReceiveReminder("some-other-reminder", default);

        harness.Runner.DidNotReceive().Enqueue(Arg.Any<RepoIndexJobRequest>());
        await harness.Reminders.DidNotReceive().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public async Task ReceiveReminder_re_enqueues_the_persisted_request_for_a_job_still_running()
    {
        var harness = new RepoIndexJobGrainHarness();
        var request = RepoIndexJobGrainHarness.Request();
        harness.State.State.Request = request;
        harness.State.State.Status = RepoIndexStatus.Running;

        await harness.CreateGrain().ReceiveReminder(RepoIndexJobGrainHarness.ResumeReminderName, default);

        harness.Runner.Received(1).Enqueue(request);
        await harness.Reminders.DidNotReceive().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public async Task ReceiveReminder_retires_itself_once_the_job_has_settled()
    {
        var harness = new RepoIndexJobGrainHarness();
        harness.State.State.Request = RepoIndexJobGrainHarness.Request();
        harness.State.State.Status = RepoIndexStatus.Completed;

        await harness.CreateGrain().ReceiveReminder(RepoIndexJobGrainHarness.ResumeReminderName, default);

        await harness.Reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), harness.Reminder);
        harness.Runner.DidNotReceive().Enqueue(Arg.Any<RepoIndexJobRequest>());
    }

    [Test]
    public async Task ReceiveReminder_retires_itself_when_the_job_was_cleared_out_from_under_it()
    {
        var harness = new RepoIndexJobGrainHarness();
        harness.State.State.Status = RepoIndexStatus.Running;

        await harness.CreateGrain().ReceiveReminder(RepoIndexJobGrainHarness.ResumeReminderName, default);

        await harness.Reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), harness.Reminder);
        harness.Runner.DidNotReceive().Enqueue(Arg.Any<RepoIndexJobRequest>());
    }
}
