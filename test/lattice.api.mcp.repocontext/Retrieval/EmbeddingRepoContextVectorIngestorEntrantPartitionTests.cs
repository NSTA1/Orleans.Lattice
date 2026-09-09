namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Pins the entrant partition the gap-shape history reports for issue #2292: whether a
/// file that entered this pass's gap set had been observed covered on the previous
/// measured pass, and therefore lost coverage, or was simply never observed covered.
/// </summary>
/// <remarks>
/// The distinction matters because the pre-existing <c>Entered</c> arm is computed
/// against the previous pass's <em>gap set</em>, and absence from a gap set has four
/// causes: observed covered, excluded as a changed file, not walked, or no measurement
/// at all. Only the first is a coverage regression. Under a static input the three
/// benign causes cannot arise, so <c>Entered</c> and the regressed arm agree - which is
/// exactly why the conflation went unnoticed and why these tests drive a non-static
/// input to separate them.
/// <para>
/// Pure in-process function test, standing up no silo and touching no store, so it
/// needs no slow category and runs in the fast dev loop.
/// </para>
/// </remarks>
[TestFixture]
public sealed class EmbeddingRepoContextVectorIngestorEntrantPartitionTests
{
    private const int WalkedFiles = 8146;

    [Test]
    public void An_entered_file_never_observed_covered_is_not_counted_as_regressed()
    {
        var history = new EmbeddingRepoContextVectorIngestor.FileGapHistory();

        // Pass 1 selects "a" and observes nothing covered, so "b" is absent from the
        // gap set for a benign reason: it was changed this pass and so was never
        // gap-eligible.
        history.Observe(Keys("a"), Keys(), Covered(), changedFileCount: 1, WalkedFiles);

        // Pass 2 changes nothing, so "b" becomes gap-eligible and enters.
        var stats = history.Observe(Keys("a", "b"), Keys(), Covered(), changedFileCount: 0, WalkedFiles);

        Assert.Multiple(() =>
        {
            Assert.That(stats.Entered, Is.EqualTo(1), "b was not a gap last pass, so it enters.");
            Assert.That(stats.Regressed, Is.Zero, "b was never observed covered, so it lost nothing.");
            Assert.That(stats.NoPriorCoverage, Is.EqualTo(1));
            Assert.That(stats.RegressionIsUnexplained, Is.False);
        });
    }

    [Test]
    public void An_entered_file_previously_observed_covered_is_counted_as_regressed()
    {
        var history = new EmbeddingRepoContextVectorIngestor.FileGapHistory();

        // Structurally identical to the preceding test but for the one difference
        // under examination: here "b" was observed covered, so its arrival in the gap
        // set is a loss of coverage rather than a first sighting.
        history.Observe(Keys("a"), Keys(), Covered("b"), changedFileCount: 0, WalkedFiles);

        var stats = history.Observe(Keys("a", "b"), Keys(), Covered(), changedFileCount: 0, WalkedFiles);

        Assert.Multiple(() =>
        {
            Assert.That(stats.Entered, Is.EqualTo(1), "Entered cannot tell these two cases apart.");
            Assert.That(stats.Regressed, Is.EqualTo(1));
            Assert.That(stats.NoPriorCoverage, Is.Zero);
            Assert.That(stats.RegressedSample, Is.EqualTo(new[] { "b" }));
            Assert.That(stats.RegressionIsUnexplained, Is.True);
        });
    }

    [Test]
    public void A_contentless_marked_file_counts_as_covered_for_the_regressed_arm()
    {
        var history = new EmbeddingRepoContextVectorIngestor.FileGapHistory();

        history.Observe(
            Keys("a"),
            Keys(),
            new RepoContextEmbeddingCoverage(SourceIds(), SourceIds("b")),
            changedFileCount: 0,
            WalkedFiles);

        var stats = history.Observe(Keys("a", "b"), Keys(), Covered(), changedFileCount: 0, WalkedFiles);

        Assert.That(stats.Regressed, Is.EqualTo(1), "Coverage is embedded or contentless, not embedded alone.");
    }

    [Test]
    public void Under_a_static_input_the_entered_and_regressed_arms_agree()
    {
        var history = new EmbeddingRepoContextVectorIngestor.FileGapHistory();

        // A static input: a fixed candidate set, nothing changed, everything walked.
        // Each candidate is therefore either a gap or covered, so the gap set's
        // complement is exactly the covered set.
        history.Observe(Keys("a", "b"), Keys(), Covered("c", "d", "e"), changedFileCount: 0, WalkedFiles);

        var stats = history.Observe(Keys("a", "b", "c"), Keys(), Covered("d", "e"), changedFileCount: 0, WalkedFiles);

        Assert.Multiple(() =>
        {
            Assert.That(stats.Entered, Is.EqualTo(1));
            Assert.That(stats.Regressed, Is.EqualTo(stats.Entered), "Under a static input the old arm is sound.");
            Assert.That(stats.NoPriorCoverage, Is.Zero);
        });
    }

    [Test]
    public void The_entrant_partition_sums_to_the_gap_count()
    {
        var history = new EmbeddingRepoContextVectorIngestor.FileGapHistory();

        history.Observe(Keys("a", "b"), Keys(), Covered("c"), changedFileCount: 0, WalkedFiles);

        var stats = history.Observe(Keys("a", "c", "d", "e"), Keys(), Covered(), changedFileCount: 0, WalkedFiles);

        Assert.Multiple(() =>
        {
            Assert.That(stats.Overlap + stats.Regressed + stats.NoPriorCoverage, Is.EqualTo(stats.CurrentCount));
            Assert.That(stats.Regressed + stats.NoPriorCoverage, Is.EqualTo(stats.Entered));
            Assert.That(stats.Regressed, Is.EqualTo(1), "Only c was observed covered.");
            Assert.That(stats.NoPriorCoverage, Is.EqualTo(2), "d and e were never observed at all.");
        });
    }

    [Test]
    public void An_unmeasured_pass_is_reported_and_suppresses_the_regression_warning()
    {
        var history = new EmbeddingRepoContextVectorIngestor.FileGapHistory();

        history.Observe(Keys("a"), Keys(), Covered("b"), changedFileCount: 0, WalkedFiles);

        // A converged pass, or one whose coverage probe failed, produces no gap shape
        // and so advances no history at all.
        history.NoteUnmeasuredPass();

        var stats = history.Observe(Keys("a", "b"), Keys(), Covered(), changedFileCount: 0, WalkedFiles);

        Assert.Multiple(() =>
        {
            Assert.That(stats.UnmeasuredPassesSincePrevious, Is.EqualTo(1));
            Assert.That(stats.Regressed, Is.EqualTo(1), "The regression is still counted.");
            Assert.That(
                stats.RegressionIsUnexplained,
                Is.False,
                "Anything could have happened in the interval, so it is not alarmable.");
        });
    }

    [Test]
    public void An_unmeasured_pass_count_resets_after_the_next_measured_pass()
    {
        var history = new EmbeddingRepoContextVectorIngestor.FileGapHistory();

        history.Observe(Keys("a"), Keys(), Covered("b"), changedFileCount: 0, WalkedFiles);
        history.NoteUnmeasuredPass();
        history.Observe(Keys("a", "b"), Keys(), Covered("c"), changedFileCount: 0, WalkedFiles);

        var stats = history.Observe(Keys("a", "b", "c"), Keys(), Covered(), changedFileCount: 0, WalkedFiles);

        Assert.Multiple(() =>
        {
            Assert.That(stats.UnmeasuredPassesSincePrevious, Is.Zero);
            Assert.That(stats.RegressionIsUnexplained, Is.True);
        });
    }

    [Test]
    public void A_regression_is_not_warned_when_either_pass_changed_a_file()
    {
        var changedNow = new EmbeddingRepoContextVectorIngestor.FileGapHistory();
        changedNow.Observe(Keys("a"), Keys(), Covered("b"), changedFileCount: 0, WalkedFiles);
        var nowStats = changedNow.Observe(Keys("a", "b"), Keys(), Covered(), changedFileCount: 3, WalkedFiles);

        var changedBefore = new EmbeddingRepoContextVectorIngestor.FileGapHistory();
        changedBefore.Observe(Keys("a"), Keys(), Covered("b"), changedFileCount: 2, WalkedFiles);
        var beforeStats = changedBefore.Observe(Keys("a", "b"), Keys(), Covered(), changedFileCount: 0, WalkedFiles);

        Assert.Multiple(() =>
        {
            Assert.That(nowStats.Regressed, Is.EqualTo(1));
            Assert.That(nowStats.ChangedFileCount, Is.EqualTo(3));
            Assert.That(nowStats.RegressionIsUnexplained, Is.False, "A contentless unmark is a changed file.");

            Assert.That(beforeStats.Regressed, Is.EqualTo(1));
            Assert.That(beforeStats.PreviousChangedFileCount, Is.EqualTo(2));
            Assert.That(beforeStats.RegressionIsUnexplained, Is.False);
        });
    }

    [Test]
    public void The_seed_pass_never_warns_because_it_has_nothing_to_compare_against()
    {
        var history = new EmbeddingRepoContextVectorIngestor.FileGapHistory();

        var stats = history.Observe(Keys("a", "b"), Keys(), Covered(), changedFileCount: 0, WalkedFiles);

        Assert.Multiple(() =>
        {
            Assert.That(stats.Passes, Is.EqualTo(1));
            Assert.That(stats.Regressed, Is.Zero, "Nothing can have lost coverage that was never observed.");
            Assert.That(
                stats.NoPriorCoverage,
                Is.EqualTo(stats.Entered),
                "The seed pass reports itself as entirely new, and the partition must account for all of it.");
            Assert.That(stats.RegressionIsUnexplained, Is.False);
        });
    }

    [Test]
    public void The_regressed_sample_is_bounded_and_deterministic()
    {
        var history = new EmbeddingRepoContextVectorIngestor.FileGapHistory();

        var all = Enumerable.Range(0, 40).Select(static i => $"file-{i:00}").ToArray();

        history.Observe(Keys("anchor"), Keys(), Covered(all), changedFileCount: 0, WalkedFiles);

        var stats = history.Observe(
            Keys(all.Append("anchor").ToArray()),
            Keys(),
            Covered(),
            changedFileCount: 0,
            WalkedFiles);

        Assert.Multiple(() =>
        {
            Assert.That(stats.Regressed, Is.EqualTo(40), "The count is the full regressed arm, not the sample.");
            Assert.That(
                stats.RegressedSample,
                Has.Count.EqualTo(EmbeddingRepoContextVectorIngestor.FileGapHistory.RegressedSampleSize));
            Assert.That(
                stats.RegressedSample,
                Is.EqualTo(all.Take(16).ToArray()),
                "Ordered before truncation, so the same regressed set always samples the same files.");
        });
    }

    private static HashSet<string> Keys(params string[] keys) =>
        new(keys, StringComparer.Ordinal);

    private static HashSet<string> SourceIds(params string[] keys) =>
        new(keys.Select(VectorCodec.SourceId), StringComparer.Ordinal);

    private static RepoContextEmbeddingCoverage Covered(params string[] keys) =>
        new(SourceIds(keys), new HashSet<string>(StringComparer.Ordinal));
}
