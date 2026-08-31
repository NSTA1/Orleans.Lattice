namespace Orleans.Lattice.GrainIndex.Tests.Backfill;

/// <summary>
/// Covers the two value types the backfill control surface answers with:
/// <see cref="GrainIndexBackfillStatus"/> and
/// <see cref="GrainIndexBackfillBatchResult"/>, plus the
/// <see cref="GrainIndexBackfillState"/> machine they report.
/// </summary>
[TestFixture]
public sealed class GrainIndexBackfillStatusTests
{
    private static readonly DateTimeOffset Origin = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Test]
    public void Every_constructor_argument_is_carried_verbatim()
    {
        var status = new GrainIndexBackfillStatus(
            "users",
            GrainIndexBackfillState.Running,
            "cursor",
            visited: 10,
            enrolled: 7,
            skipped: 2,
            failed: 1,
            passes: 5,
            revisitsEnrolled: true,
            startedUtc: Origin,
            updatedUtc: Origin.AddMinutes(1),
            completedUtc: Origin.AddMinutes(2),
            failureMessage: "why");

        Assert.Multiple(() =>
        {
            Assert.That(status.IndexName, Is.EqualTo("users"));
            Assert.That(status.State, Is.EqualTo(GrainIndexBackfillState.Running));
            Assert.That(status.ResumeAfterKey, Is.EqualTo("cursor"));
            Assert.That(status.Visited, Is.EqualTo(10));
            Assert.That(status.Enrolled, Is.EqualTo(7));
            Assert.That(status.Skipped, Is.EqualTo(2));
            Assert.That(status.Failed, Is.EqualTo(1));
            Assert.That(status.Passes, Is.EqualTo(5));
            Assert.That(status.RevisitsEnrolled, Is.True);
            Assert.That(status.StartedUtc, Is.EqualTo(Origin));
            Assert.That(status.UpdatedUtc, Is.EqualTo(Origin.AddMinutes(1)));
            Assert.That(status.CompletedUtc, Is.EqualTo(Origin.AddMinutes(2)));
            Assert.That(status.FailureMessage, Is.EqualTo("why"));
        });
    }

    [Test]
    public void A_null_index_name_is_rejected()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new GrainIndexBackfillStatus(
                    null!,
                    GrainIndexBackfillState.NotStarted,
                    null, 0, 0, 0, 0, 0, false, null, null, null, null),
                Throws.ArgumentNullException);

            Assert.That(() => GrainIndexBackfillStatus.NotStarted(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_not_started_status_is_empty_apart_from_the_index_name()
    {
        var status = GrainIndexBackfillStatus.NotStarted("users");

        Assert.Multiple(() =>
        {
            Assert.That(status.IndexName, Is.EqualTo("users"));
            Assert.That(status.State, Is.EqualTo(GrainIndexBackfillState.NotStarted));
            Assert.That(status.ResumeAfterKey, Is.Null);
            Assert.That(status.Visited, Is.Zero);
            Assert.That(status.Enrolled, Is.Zero);
            Assert.That(status.Skipped, Is.Zero);
            Assert.That(status.Failed, Is.Zero);
            Assert.That(status.Passes, Is.Zero);
            Assert.That(status.RevisitsEnrolled, Is.False);
            Assert.That(status.StartedUtc, Is.Null);
            Assert.That(status.UpdatedUtc, Is.Null);
            Assert.That(status.CompletedUtc, Is.Null);
            Assert.That(status.FailureMessage, Is.Null);
        });
    }

    [Test]
    public void Not_started_is_the_zero_state_so_an_absent_checkpoint_needs_no_special_case()
    {
        Assert.That((int)GrainIndexBackfillState.NotStarted, Is.Zero);
    }

    [Test]
    public void The_state_machine_names_every_stage_the_crawl_can_reach()
    {
        Assert.That(
            Enum.GetValues<GrainIndexBackfillState>(),
            Is.EquivalentTo(new[]
            {
                GrainIndexBackfillState.NotStarted,
                GrainIndexBackfillState.Running,
                GrainIndexBackfillState.Paused,
                GrainIndexBackfillState.Completed,
                GrainIndexBackfillState.Failed,
            }));
    }

    [Test]
    public void A_batch_result_carries_every_constructor_argument()
    {
        var result = new GrainIndexBackfillBatchResult(
            visited: 6,
            enrolled: 4,
            skipped: 1,
            failed: 1,
            GrainIndexBackfillState.Running,
            exhausted: false);

        Assert.Multiple(() =>
        {
            Assert.That(result.Visited, Is.EqualTo(6));
            Assert.That(result.Enrolled, Is.EqualTo(4));
            Assert.That(result.Skipped, Is.EqualTo(1));
            Assert.That(result.Failed, Is.EqualTo(1));
            Assert.That(result.State, Is.EqualTo(GrainIndexBackfillState.Running));
            Assert.That(result.Exhausted, Is.False);
        });
    }

    [Test]
    public void An_empty_batch_result_reports_the_state_and_no_work()
    {
        var result = GrainIndexBackfillBatchResult.None(GrainIndexBackfillState.Paused);

        Assert.Multiple(() =>
        {
            Assert.That(result.Visited, Is.Zero);
            Assert.That(result.Enrolled, Is.Zero);
            Assert.That(result.Skipped, Is.Zero);
            Assert.That(result.Failed, Is.Zero);
            Assert.That(result.State, Is.EqualTo(GrainIndexBackfillState.Paused));
            Assert.That(result.Exhausted, Is.False);
        });
    }

    [Test]
    public void Batch_results_with_the_same_figures_compare_equal()
    {
        var first = new GrainIndexBackfillBatchResult(1, 1, 0, 0, GrainIndexBackfillState.Running, false);
        var second = new GrainIndexBackfillBatchResult(1, 1, 0, 0, GrainIndexBackfillState.Running, false);
        var different = new GrainIndexBackfillBatchResult(2, 1, 0, 0, GrainIndexBackfillState.Running, false);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(second));
            Assert.That(first.GetHashCode(), Is.EqualTo(second.GetHashCode()));
            Assert.That(first, Is.Not.EqualTo(different));
        });
    }

    [Test]
    public void A_default_batch_result_reports_a_crawl_that_never_started()
    {
        var result = default(GrainIndexBackfillBatchResult);

        Assert.Multiple(() =>
        {
            Assert.That(result.State, Is.EqualTo(GrainIndexBackfillState.NotStarted));
            Assert.That(result.Visited, Is.Zero);
        });
    }
}
