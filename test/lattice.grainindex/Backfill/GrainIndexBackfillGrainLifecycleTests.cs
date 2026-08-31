using NSubstitute;
using Orleans.Lattice.GrainIndex.Backfill;
using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex.Tests.Backfill;

/// <summary>
/// Covers <see cref="GrainIndexBackfillGrain"/>'s lifecycle: how a crawl starts,
/// how it is held and released, how a rebuild restarts it, and what the durable
/// heartbeat does with each state.
/// </summary>
[TestFixture]
public sealed class GrainIndexBackfillGrainLifecycleTests
{
    [Test]
    public async Task An_index_with_no_registry_record_reports_not_started()
    {
        var harness = new BackfillHarness().WithKeys("a");

        var status = await harness.CreateGrain().EnsureStartedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.State, Is.EqualTo(GrainIndexBackfillState.NotStarted),
                "An index that has not been reconciled has no declaration to crawl under.");
            Assert.That(harness.StoredCheckpoint(), Is.Null);
        });
    }

    [Test]
    public async Task An_index_that_owes_no_backfill_is_not_started()
    {
        var harness = new BackfillHarness().WithKeys("a").WithRegistryRecord(needsBackfill: false);

        var status = await harness.CreateGrain().EnsureStartedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.State, Is.EqualTo(GrainIndexBackfillState.NotStarted));
            Assert.That(harness.StoredCheckpoint(), Is.Null);
        });
    }

    [Test]
    public async Task A_first_start_runs_under_the_registrys_fingerprint_and_skips_indexed_grains()
    {
        var harness = new BackfillHarness().WithKeys("a").WithRegistryRecord();

        var status = await harness.CreateGrain().EnsureStartedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.State, Is.EqualTo(GrainIndexBackfillState.Running));
            Assert.That(status.RevisitsEnrolled, Is.False,
                "A first backfill has nothing to rewrite; skipping already-indexed grains is the point.");
            Assert.That(status.StartedUtc, Is.EqualTo(harness.Time.Now));
            Assert.That(harness.StoredCheckpoint()!.Fingerprint, Is.EqualTo(harness.Fingerprint));
        });
    }

    [Test]
    public async Task Starting_twice_resumes_rather_than_restarting()
    {
        var harness = new BackfillHarness().WithKeys("a", "b", "c").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();
        await grain.RunBatchAsync();

        var status = await harness.CreateGrain().EnsureStartedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.State, Is.EqualTo(GrainIndexBackfillState.Running));
            Assert.That(status.ResumeAfterKey, Is.EqualTo("b"),
                "Every silo calls this at start; one that reset the position would restart the "
                + "crawl on every deployment.");
            Assert.That(status.Visited, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task A_restarted_host_resumes_from_the_durable_checkpoint()
    {
        var harness = new BackfillHarness().WithKeys("a", "b", "c", "d").WithRegistryRecord();
        var first = harness.CreateGrain();
        await first.EnsureStartedAsync();
        await first.RunBatchAsync();

        // A fresh grain instance over the same durable store is exactly what a
        // silo restart leaves behind: no in-memory state, one checkpoint.
        var second = harness.CreateGrain();
        await second.EnsureStartedAsync();
        await second.RunBatchAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Activator.Activated, Is.EqualTo(new[] { "a", "b", "c", "d" }),
                "A resumed crawl must neither repeat nor skip.");
            Assert.That(harness.StoredCheckpoint()!.Visited, Is.EqualTo(4));
        });
    }

    [Test]
    public async Task A_completed_crawl_is_not_restarted_by_a_later_start()
    {
        var harness = new BackfillHarness().WithKeys("a").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();
        await grain.RunBatchAsync();

        var status = await harness.CreateGrain().EnsureStartedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.State, Is.EqualTo(GrainIndexBackfillState.Completed));
            Assert.That(harness.Activator.Activated, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task A_paused_crawl_is_not_resumed_by_a_later_start()
    {
        var harness = new BackfillHarness().WithKeys("a", "b", "c").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();
        await grain.PauseAsync();

        var status = await harness.CreateGrain().EnsureStartedAsync();

        Assert.That(status.State, Is.EqualTo(GrainIndexBackfillState.Paused),
            "Pausing is deliberate; the next silo start must not quietly undo it.");
    }

    [Test]
    public async Task Pausing_holds_the_crawl_without_losing_its_position()
    {
        var harness = new BackfillHarness().WithKeys("a", "b", "c", "d").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();
        await grain.RunBatchAsync();

        var paused = await grain.PauseAsync();
        var afterPause = await grain.RunBatchAsync();

        Assert.Multiple(() =>
        {
            Assert.That(paused.State, Is.EqualTo(GrainIndexBackfillState.Paused));
            Assert.That(paused.ResumeAfterKey, Is.EqualTo("b"));
            Assert.That(afterPause.Visited, Is.Zero, "A held crawl must do no work.");
            Assert.That(harness.Activator.Activated, Is.EqualTo(new[] { "a", "b" }));
        });
    }

    [Test]
    public async Task Resuming_continues_from_the_checkpoint()
    {
        var harness = new BackfillHarness().WithKeys("a", "b", "c", "d").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();
        await grain.RunBatchAsync();
        await grain.PauseAsync();

        var resumed = await grain.ResumeAsync();
        await grain.RunBatchAsync();

        Assert.Multiple(() =>
        {
            Assert.That(resumed.State, Is.EqualTo(GrainIndexBackfillState.Running));
            Assert.That(harness.Activator.Activated, Is.EqualTo(new[] { "a", "b", "c", "d" }));
        });
    }

    [Test]
    public async Task Pausing_or_resuming_a_completed_crawl_leaves_it_completed()
    {
        var harness = new BackfillHarness().WithKeys("a").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();
        await grain.RunBatchAsync();

        var paused = await grain.PauseAsync();
        var resumed = await grain.ResumeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(paused.State, Is.EqualTo(GrainIndexBackfillState.Completed));
            Assert.That(resumed.State, Is.EqualTo(GrainIndexBackfillState.Completed));
        });
    }

    [Test]
    public async Task Pausing_or_resuming_a_crawl_that_never_started_reports_not_started()
    {
        var harness = new BackfillHarness().WithKeys("a").WithRegistryRecord();
        var grain = harness.CreateGrain();

        var paused = await grain.PauseAsync();
        var resumed = await grain.ResumeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(paused.State, Is.EqualTo(GrainIndexBackfillState.NotStarted));
            Assert.That(resumed.State, Is.EqualTo(GrainIndexBackfillState.NotStarted));
            Assert.That(harness.StoredCheckpoint(), Is.Null);
        });
    }

    [Test]
    public async Task Restarting_crawls_the_whole_range_again_and_revisits_indexed_grains()
    {
        var harness = new BackfillHarness().WithKeys("a", "b").WithRegistryRecord().WithEnrolled("a");
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();
        await grain.RunBatchAsync();

        var restarted = await grain.RestartAsync();
        await grain.RunBatchAsync();

        Assert.Multiple(() =>
        {
            Assert.That(restarted.State, Is.EqualTo(GrainIndexBackfillState.Running));
            Assert.That(restarted.ResumeAfterKey, Is.Null);
            Assert.That(restarted.Visited, Is.Zero);
            Assert.That(restarted.RevisitsEnrolled, Is.True,
                "A restart that still skipped indexed grains would be a no-op on a completed crawl.");
            Assert.That(harness.Activator.Activated, Is.EqualTo(new[] { "b", "a", "b" }));
        });
    }

    [Test]
    public async Task A_restart_with_no_registry_record_still_runs_under_the_previous_fingerprint()
    {
        var harness = new BackfillHarness().WithKeys("a").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();

        var restarted = await harness.CreateGrain().RestartAsync();

        Assert.Multiple(() =>
        {
            Assert.That(restarted.State, Is.EqualTo(GrainIndexBackfillState.Running));
            Assert.That(harness.StoredCheckpoint()!.Fingerprint, Is.EqualTo(harness.Fingerprint));
        });
    }

    [Test]
    public async Task A_scheduled_rebuild_restarts_the_crawl_over_the_full_range()
    {
        var harness = new BackfillHarness().WithKeys("a", "b").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();
        await grain.RunBatchAsync();

        // The reconciler adopting a breaking change moves the fingerprint and
        // raises the flag again; only a breaking change moves it.
        harness.WithRegistryRecord(
            needsBackfill: true,
            fingerprint: new GrainIndexFingerprint("00000000000000000000000000000000"));

        var status = await harness.CreateGrain().EnsureStartedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.State, Is.EqualTo(GrainIndexBackfillState.Running));
            Assert.That(status.ResumeAfterKey, Is.Null);
            Assert.That(status.RevisitsEnrolled, Is.True,
                "The entries an indexed grain already owns describe the old declaration, so a "
                + "rebuild that skipped them would rebuild nothing.");
        });
    }

    [Test]
    public async Task The_status_is_answered_from_the_durable_checkpoint()
    {
        var harness = new BackfillHarness().WithKeys("a", "b", "c").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();
        await grain.RunBatchAsync();

        var status = await harness.CreateGrain().GetStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.IndexName, Is.EqualTo(BackfillHarness.IndexName));
            Assert.That(status.State, Is.EqualTo(GrainIndexBackfillState.Running));
            Assert.That(status.ResumeAfterKey, Is.EqualTo("b"));
            Assert.That(status.Passes, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task The_status_of_an_untouched_index_is_not_started()
    {
        var status = await new BackfillHarness().CreateGrain().GetStatusAsync();

        Assert.That(status.State, Is.EqualTo(GrainIndexBackfillState.NotStarted));
    }

    [Test]
    public async Task The_heartbeat_ignores_a_reminder_it_does_not_own()
    {
        var harness = new BackfillHarness().WithKeys("a").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();
        var writesBefore = harness.Checkpoints.WriteCount;

        await grain.ReceiveReminder("something-else", new TickStatus());

        Assert.That(harness.Checkpoints.WriteCount, Is.EqualTo(writesBefore));
    }

    [Test]
    public async Task The_heartbeat_resumes_a_failed_crawl_from_its_checkpoint()
    {
        var harness = new BackfillHarness().WithKeys("a", "b", "c").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();
        await grain.RunBatchAsync();

        var failed = harness.StoredCheckpoint()!
            .WithState(GrainIndexBackfillState.Failed, harness.Time.Now, "the registry was unavailable");
        harness.Checkpoints.Seed(BackfillHarness.IndexName, failed);

        var revived = harness.CreateGrain();
        await revived.ReceiveReminder(GrainIndexBackfillGrain.ReminderName, new TickStatus());
        var status = await revived.GetStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.State, Is.EqualTo(GrainIndexBackfillState.Running));
            Assert.That(status.ResumeAfterKey, Is.EqualTo("b"),
                "Recovering from a fault must not lose the position the crawl had reached.");
            Assert.That(status.FailureMessage, Is.EqualTo("the registry was unavailable"),
                "The reason a crawl stalled is worth keeping after it recovers.");
        });
    }

    [Test]
    public async Task The_heartbeat_unregisters_itself_once_the_crawl_has_completed()
    {
        var harness = new BackfillHarness().WithKeys("a").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();
        await grain.RunBatchAsync();

        await grain.ReceiveReminder(GrainIndexBackfillGrain.ReminderName, new TickStatus());

        await harness.Reminders
            .Received()
            .GetReminder(Arg.Any<GrainId>(), GrainIndexBackfillGrain.ReminderName);
    }

    [Test]
    public async Task The_heartbeat_does_nothing_for_an_index_with_no_checkpoint()
    {
        var harness = new BackfillHarness().WithKeys("a").WithRegistryRecord();

        await harness.CreateGrain().ReceiveReminder(GrainIndexBackfillGrain.ReminderName, new TickStatus());

        Assert.That(harness.Checkpoints.WriteCount, Is.Zero);
    }

    [Test]
    public async Task Deactivation_is_safe_when_no_pass_timer_was_ever_started()
    {
        var harness = new BackfillHarness().WithKeys("a").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();

        Assert.That(
            async () => await grain.OnDeactivateAsync(
                new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
                CancellationToken.None),
            Throws.Nothing);
    }

    [Test]
    public async Task No_reminder_is_registered_when_the_background_driver_is_off()
    {
        var harness = new BackfillHarness().WithKeys("a").WithRegistryRecord();

        await harness.CreateGrain().EnsureStartedAsync();

        await harness.Reminders
            .DidNotReceive()
            .RegisterOrUpdateReminder(
                Arg.Any<GrainId>(),
                Arg.Any<string>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task The_heartbeat_period_clears_the_orleans_minimum()
    {
        var harness = new BackfillHarness().WithKeys("a").WithRegistryRecord();
        harness.Options.BackfillEnabled = true;

        await harness.CreateGrain().EnsureStartedAsync();

        await harness.Reminders
            .Received(1)
            .RegisterOrUpdateReminder(
                Arg.Any<GrainId>(),
                GrainIndexBackfillGrain.ReminderName,
                GrainIndexBackfillGrain.ReminderPeriod,
                GrainIndexBackfillGrain.ReminderPeriod);

        Assert.That(GrainIndexBackfillGrain.ReminderPeriod, Is.GreaterThanOrEqualTo(TimeSpan.FromMinutes(1)),
            "Orleans refuses a shorter reminder period, so a shorter one would fail at run time.");
    }
}
