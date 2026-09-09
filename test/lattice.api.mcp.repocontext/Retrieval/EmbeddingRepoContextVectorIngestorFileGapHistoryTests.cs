namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for <see cref="EmbeddingRepoContextVectorIngestor.FileGapHistory"/>, the
/// per-repository cross-pass memory that turns the embedding back-fill's per-pass gap
/// selection into the shape the live never-converging defect (issue #2208) is read by:
/// the overlap of consecutive passes, the entered/left fringe, the rolling union of
/// gap-selected files, and - the sharpest of them - how many files the previous pass
/// both selected AND landed are being re-selected now. That last count is the file-arm
/// reading of the symbol arm's re-embed-loop signature (a write that reports success
/// yet is not observable on the next pass): a majority re-selected is the loop, near
/// zero is honest churn. These tests pin each field of the returned
/// <see cref="EmbeddingRepoContextVectorIngestor.FileGapStats"/> against hand-computed
/// set arithmetic so a wrong overlap, a mislabelled fringe, or a miscounted
/// landed-repeat cannot pass as a finding.
/// </summary>
/// <remarks>
/// Pure in-process function test: it drives <c>Observe</c> with plain string keys,
/// standing up no silo and touching no store, so it needs no slow category and runs in
/// the fast dev loop. The keys are opaque to the history, so arbitrary strings stand in
/// for canonical source keys without loss.
/// <para>
/// Every pass here observes no coverage, so the entrant partition added for issue #2292
/// classifies the whole entered fringe as <c>NoPriorCoverage</c> and these tests keep
/// pinning exactly what they always pinned. The partition itself is pinned separately by
/// <see cref="EmbeddingRepoContextVectorIngestorEntrantPartitionTests"/>.
/// </para>
/// </remarks>
[TestFixture]
public sealed class EmbeddingRepoContextVectorIngestorFileGapHistoryTests
{
    private const int WalkedFiles = 8095;

    private static HashSet<string> Set(params string[] keys)
        => new(keys, StringComparer.Ordinal);

    [Test]
    public void First_pass_has_no_previous_and_enters_its_whole_selection()
    {
        var history = new EmbeddingRepoContextVectorIngestor.FileGapHistory();

        var stats = history.Observe(Set("a", "b", "c"), Set("a", "b", "c"), RepoContextEmbeddingCoverage.Empty, changedFileCount: 0, WalkedFiles);

        Assert.Multiple(() =>
        {
            Assert.That(stats.CurrentCount, Is.EqualTo(3));
            Assert.That(stats.PreviousCount, Is.EqualTo(0));
            Assert.That(stats.Overlap, Is.EqualTo(0));
            Assert.That(stats.Entered, Is.EqualTo(3));
            Assert.That(stats.Left, Is.EqualTo(0));
            Assert.That(stats.PreviousLanded, Is.EqualTo(0));
            Assert.That(stats.LandedRepeats, Is.EqualTo(0));
            Assert.That(stats.UnionCount, Is.EqualTo(3));
            Assert.That(stats.Passes, Is.EqualTo(1));
            Assert.That(stats.WalkedFiles, Is.EqualTo(WalkedFiles));
            Assert.That(stats.UnionSaturated, Is.False);
        });
    }

    [Test]
    public void An_identical_second_pass_fully_overlaps_and_grows_no_union()
    {
        var history = new EmbeddingRepoContextVectorIngestor.FileGapHistory();
        history.Observe(Set("a", "b", "c"), Set("a", "b", "c"), RepoContextEmbeddingCoverage.Empty, changedFileCount: 0, WalkedFiles);

        var stats = history.Observe(Set("a", "b", "c"), Set("a", "b", "c"), RepoContextEmbeddingCoverage.Empty, changedFileCount: 0, WalkedFiles);

        // The pathological steady-state: the same files re-selected every quiet pass.
        Assert.Multiple(() =>
        {
            Assert.That(stats.PreviousCount, Is.EqualTo(3));
            Assert.That(stats.Overlap, Is.EqualTo(3));
            Assert.That(stats.Entered, Is.EqualTo(0));
            Assert.That(stats.Left, Is.EqualTo(0));
            Assert.That(stats.UnionCount, Is.EqualTo(3), "an identical pass adds nothing new to the union");
            Assert.That(stats.Passes, Is.EqualTo(2));
        });
    }

    [Test]
    public void A_rotating_fringe_is_reported_as_entered_and_left_against_a_high_overlap()
    {
        var history = new EmbeddingRepoContextVectorIngestor.FileGapHistory();
        history.Observe(Set("a", "b", "c"), Set("a", "b", "c"), RepoContextEmbeddingCoverage.Empty, changedFileCount: 0, WalkedFiles);

        // Same core {a,b}, drop c, add d: exactly the "high overlap with a small
        // rotating fringe" shape the PM flagged as a boundary condition in the
        // presence check rather than wholesale loss.
        var stats = history.Observe(Set("a", "b", "d"), Set("a", "b", "d"), RepoContextEmbeddingCoverage.Empty, changedFileCount: 0, WalkedFiles);

        Assert.Multiple(() =>
        {
            Assert.That(stats.Overlap, Is.EqualTo(2));
            Assert.That(stats.Entered, Is.EqualTo(1));
            Assert.That(stats.Left, Is.EqualTo(1));
            Assert.That(stats.UnionCount, Is.EqualTo(4), "d is new; a, b, c already counted");
        });
    }

    [Test]
    public void Landed_repeats_counts_only_previously_landed_files_that_are_re_selected()
    {
        var history = new EmbeddingRepoContextVectorIngestor.FileGapHistory();

        // Pass one selects {a,b,c} but only {a,b} land (c's batch failed, say).
        history.Observe(Set("a", "b", "c"), Set("a", "b"), RepoContextEmbeddingCoverage.Empty, changedFileCount: 0, WalkedFiles);

        // Pass two re-selects a (landed) and c (never landed) plus new d. Of the two
        // previously-landed files a and b, only a is re-selected, so the loop
        // signature is 1 - not 2 (b is not re-selected) and not 3 (c never landed, so
        // re-selecting it is honest, not a repeat of landed work).
        var stats = history.Observe(Set("a", "c", "d"), Set("a", "c", "d"), RepoContextEmbeddingCoverage.Empty, changedFileCount: 0, WalkedFiles);

        Assert.Multiple(() =>
        {
            Assert.That(stats.PreviousLanded, Is.EqualTo(2), "a and b landed last pass");
            Assert.That(stats.LandedRepeats, Is.EqualTo(1), "only a, of {a,b}, is re-selected");
        });
    }

    [Test]
    public void A_saturating_loss_grows_the_union_toward_the_corpus_pass_over_pass()
    {
        var history = new EmbeddingRepoContextVectorIngestor.FileGapHistory();

        // Every pass selects entirely fresh files: no overlap, and the union climbs
        // without bound - the "store is losing writes across the whole repository"
        // shape that points at durability loss rather than a confined presence bug.
        var first = history.Observe(Set("a", "b"), Set("a", "b"), RepoContextEmbeddingCoverage.Empty, changedFileCount: 0, WalkedFiles);
        var second = history.Observe(Set("c", "d"), Set("c", "d"), RepoContextEmbeddingCoverage.Empty, changedFileCount: 0, WalkedFiles);
        var third = history.Observe(Set("e", "f"), Set("e", "f"), RepoContextEmbeddingCoverage.Empty, changedFileCount: 0, WalkedFiles);

        Assert.Multiple(() =>
        {
            Assert.That(first.UnionCount, Is.EqualTo(2));
            Assert.That(second.Overlap, Is.EqualTo(0));
            Assert.That(second.UnionCount, Is.EqualTo(4));
            Assert.That(third.Overlap, Is.EqualTo(0));
            Assert.That(third.UnionCount, Is.EqualTo(6));
            Assert.That(third.Passes, Is.EqualTo(3));
        });
    }
}
