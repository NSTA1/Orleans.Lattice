using Orleans.Lattice.GrainIndex.Backfill;

namespace Orleans.Lattice.GrainIndex.Tests.Backfill;

/// <summary>
/// Covers <see cref="GrainIndexBackfillCheckpoint"/>: the transitions a crawl
/// makes between passes and the status it projects.
/// </summary>
[TestFixture]
public sealed class GrainIndexBackfillCheckpointTests
{
    private static readonly GrainIndexFingerprint Print = new("0123456789ABCDEF0123456789ABCDEF");
    private static readonly DateTimeOffset Origin = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Test]
    public void A_started_crawl_begins_at_the_head_of_the_range_with_no_totals()
    {
        var checkpoint = GrainIndexBackfillCheckpoint.Start(Print, revisitsEnrolled: false, Origin);

        Assert.Multiple(() =>
        {
            Assert.That(checkpoint.State, Is.EqualTo(GrainIndexBackfillState.Running));
            Assert.That(checkpoint.Fingerprint, Is.EqualTo(Print));
            Assert.That(checkpoint.ResumeAfterKey, Is.Null);
            Assert.That(checkpoint.Visited, Is.Zero);
            Assert.That(checkpoint.Enrolled, Is.Zero);
            Assert.That(checkpoint.Skipped, Is.Zero);
            Assert.That(checkpoint.Failed, Is.Zero);
            Assert.That(checkpoint.Passes, Is.Zero);
            Assert.That(checkpoint.RevisitsEnrolled, Is.False);
            Assert.That(checkpoint.StartedUtc, Is.EqualTo(Origin));
            Assert.That(checkpoint.UpdatedUtc, Is.EqualTo(Origin));
            Assert.That(checkpoint.CompletedUtc, Is.Null);
            Assert.That(checkpoint.FailureMessage, Is.Null);
        });
    }

    [Test]
    public void Advancing_accumulates_the_totals_and_moves_the_position()
    {
        var advanced = GrainIndexBackfillCheckpoint
            .Start(Print, revisitsEnrolled: true, Origin)
            .Advance("m", visited: 3, enrolled: 2, skipped: 1, failed: 0, Origin.AddMinutes(1))
            .Advance("z", visited: 2, enrolled: 1, skipped: 0, failed: 1, Origin.AddMinutes(2));

        Assert.Multiple(() =>
        {
            Assert.That(advanced.ResumeAfterKey, Is.EqualTo("z"));
            Assert.That(advanced.Visited, Is.EqualTo(5));
            Assert.That(advanced.Enrolled, Is.EqualTo(3));
            Assert.That(advanced.Skipped, Is.EqualTo(1));
            Assert.That(advanced.Failed, Is.EqualTo(1));
            Assert.That(advanced.Passes, Is.EqualTo(2));
            Assert.That(advanced.RevisitsEnrolled, Is.True, "A rebuild stays a rebuild across passes.");
            Assert.That(advanced.StartedUtc, Is.EqualTo(Origin));
            Assert.That(advanced.UpdatedUtc, Is.EqualTo(Origin.AddMinutes(2)));
        });
    }

    [Test]
    public void Advancing_without_a_key_keeps_the_position_it_already_had()
    {
        var advanced = GrainIndexBackfillCheckpoint
            .Start(Print, revisitsEnrolled: false, Origin)
            .Advance("m", visited: 1, enrolled: 1, skipped: 0, failed: 0, Origin)
            .Advance(null, visited: 0, enrolled: 0, skipped: 0, failed: 0, Origin);

        Assert.That(advanced.ResumeAfterKey, Is.EqualTo("m"),
            "A pass that took nothing must not rewind the crawl to the head of the range.");
    }

    [Test]
    public void Completing_stamps_the_completion_time()
    {
        var completed = GrainIndexBackfillCheckpoint
            .Start(Print, revisitsEnrolled: false, Origin)
            .WithState(GrainIndexBackfillState.Completed, Origin.AddHours(1));

        Assert.Multiple(() =>
        {
            Assert.That(completed.State, Is.EqualTo(GrainIndexBackfillState.Completed));
            Assert.That(completed.CompletedUtc, Is.EqualTo(Origin.AddHours(1)));
        });
    }

    [Test]
    public void A_transition_that_is_not_completion_leaves_the_completion_time_alone()
    {
        var paused = GrainIndexBackfillCheckpoint
            .Start(Print, revisitsEnrolled: false, Origin)
            .WithState(GrainIndexBackfillState.Paused, Origin.AddHours(1));

        Assert.Multiple(() =>
        {
            Assert.That(paused.State, Is.EqualTo(GrainIndexBackfillState.Paused));
            Assert.That(paused.CompletedUtc, Is.Null);
            Assert.That(paused.UpdatedUtc, Is.EqualTo(Origin.AddHours(1)));
        });
    }

    [Test]
    public void A_transition_preserves_the_position_and_the_totals()
    {
        var paused = GrainIndexBackfillCheckpoint
            .Start(Print, revisitsEnrolled: false, Origin)
            .Advance("k", visited: 4, enrolled: 4, skipped: 0, failed: 0, Origin)
            .WithState(GrainIndexBackfillState.Paused, Origin);

        Assert.Multiple(() =>
        {
            Assert.That(paused.ResumeAfterKey, Is.EqualTo("k"),
                "Pausing must cost the crawl nothing, or nobody would use it.");
            Assert.That(paused.Visited, Is.EqualTo(4));
            Assert.That(paused.Passes, Is.EqualTo(1));
        });
    }

    [Test]
    public void A_failure_message_is_recorded_and_then_carried_forward()
    {
        var failed = GrainIndexBackfillCheckpoint
            .Start(Print, revisitsEnrolled: false, Origin)
            .WithState(GrainIndexBackfillState.Failed, Origin, "the registry was unavailable");

        var resumed = failed.WithState(GrainIndexBackfillState.Running, Origin.AddMinutes(1));

        Assert.Multiple(() =>
        {
            Assert.That(failed.FailureMessage, Is.EqualTo("the registry was unavailable"));
            Assert.That(resumed.FailureMessage, Is.EqualTo("the registry was unavailable"),
                "Losing the reason on resume would erase the only record of why the crawl stalled.");
        });
    }

    [Test]
    public void The_projected_status_carries_every_field()
    {
        var checkpoint = GrainIndexBackfillCheckpoint
            .Start(Print, revisitsEnrolled: true, Origin)
            .Advance("q", visited: 7, enrolled: 5, skipped: 1, failed: 1, Origin.AddMinutes(3))
            .WithState(GrainIndexBackfillState.Paused, Origin.AddMinutes(4), "transient");

        var status = checkpoint.ToStatus("users");

        Assert.Multiple(() =>
        {
            Assert.That(status.IndexName, Is.EqualTo("users"));
            Assert.That(status.State, Is.EqualTo(GrainIndexBackfillState.Paused));
            Assert.That(status.ResumeAfterKey, Is.EqualTo("q"));
            Assert.That(status.Visited, Is.EqualTo(7));
            Assert.That(status.Enrolled, Is.EqualTo(5));
            Assert.That(status.Skipped, Is.EqualTo(1));
            Assert.That(status.Failed, Is.EqualTo(1));
            Assert.That(status.Passes, Is.EqualTo(1));
            Assert.That(status.RevisitsEnrolled, Is.True);
            Assert.That(status.StartedUtc, Is.EqualTo(Origin));
            Assert.That(status.UpdatedUtc, Is.EqualTo(Origin.AddMinutes(4)));
            Assert.That(status.CompletedUtc, Is.Null);
            Assert.That(status.FailureMessage, Is.EqualTo("transient"));
        });
    }

    [Test]
    public void Every_constructor_argument_is_carried_verbatim()
    {
        var checkpoint = new GrainIndexBackfillCheckpoint(
            GrainIndexBackfillState.Failed,
            Print,
            "cursor",
            visited: 9,
            enrolled: 8,
            skipped: 1,
            failed: 0,
            passes: 4,
            revisitsEnrolled: true,
            startedUtc: Origin,
            updatedUtc: Origin.AddDays(1),
            completedUtc: Origin.AddDays(2),
            failureMessage: "why");

        Assert.Multiple(() =>
        {
            Assert.That(checkpoint.State, Is.EqualTo(GrainIndexBackfillState.Failed));
            Assert.That(checkpoint.Fingerprint, Is.EqualTo(Print));
            Assert.That(checkpoint.ResumeAfterKey, Is.EqualTo("cursor"));
            Assert.That(checkpoint.Visited, Is.EqualTo(9));
            Assert.That(checkpoint.Enrolled, Is.EqualTo(8));
            Assert.That(checkpoint.Skipped, Is.EqualTo(1));
            Assert.That(checkpoint.Failed, Is.Zero);
            Assert.That(checkpoint.Passes, Is.EqualTo(4));
            Assert.That(checkpoint.RevisitsEnrolled, Is.True);
            Assert.That(checkpoint.StartedUtc, Is.EqualTo(Origin));
            Assert.That(checkpoint.UpdatedUtc, Is.EqualTo(Origin.AddDays(1)));
            Assert.That(checkpoint.CompletedUtc, Is.EqualTo(Origin.AddDays(2)));
            Assert.That(checkpoint.FailureMessage, Is.EqualTo("why"));
        });
    }

    [Test]
    public void The_projected_status_rejects_a_null_index_name()
    {
        var checkpoint = GrainIndexBackfillCheckpoint.Start(Print, revisitsEnrolled: false, Origin);

        Assert.That(() => checkpoint.ToStatus(null!), Throws.ArgumentNullException);
    }
}
