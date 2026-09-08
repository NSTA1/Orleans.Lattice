namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Tests for <see cref="RepoFileVectorIngestOutcome"/>, the seam record that lets a
/// vectorisation pass report whether it <i>proved</i> the repository's embedding
/// coverage complete rather than merely counting what it embedded.
/// <para>
/// The load-bearing property is that silence is not convergence: an outcome that
/// established no coverage must never read as converged, however few gaps it
/// happened to select. Getting that backwards would let one failed probe settle the
/// coordinator into skipping the gap scan forever.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoFileVectorIngestOutcomeTests
{
    [Test]
    public void A_pass_that_established_coverage_and_found_no_gap_is_converged()
    {
        var outcome = new RepoFileVectorIngestOutcome(FilesEmbedded: 4, GapsSelected: 0, CoverageEstablished: true);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Converged, Is.True);
            Assert.That(outcome.FilesEmbedded, Is.EqualTo(4), "changed files still embed on a converged pass");
        });
    }

    [Test]
    public void A_pass_that_found_a_gap_is_not_converged()
    {
        var outcome = new RepoFileVectorIngestOutcome(FilesEmbedded: 1, GapsSelected: 1, CoverageEstablished: true);

        Assert.That(outcome.Converged, Is.False, "a selected gap is an unhealed hole, whether or not it was filled");
    }

    [Test]
    public void A_pass_that_established_no_coverage_is_never_converged()
    {
        // The fail-closed direction: no provider, an unreachable provider, or a
        // failed probe all yield zero gaps for reasons that say nothing about the
        // repository, so zero must not be promoted to proof.
        var outcome = new RepoFileVectorIngestOutcome(FilesEmbedded: 0, GapsSelected: 0, CoverageEstablished: false);

        Assert.That(outcome.Converged, Is.False);
    }

    [Test]
    public void A_pass_that_skipped_its_gap_scan_is_never_converged()
    {
        // The same fail-closed direction one step further out. A pass that stood its
        // gap back-fill down under the file arm's backoff (issue #2208) reaches zero
        // selected gaps by never asking, which is indistinguishable from convergence
        // on every other field of this record. Excluding it here is what stops
        // "converged" quietly coming to mean "did not look", and it is the single
        // predicate the convergence fixtures rely on to stay honest once the arm can
        // skip at all.
        var outcome = new RepoFileVectorIngestOutcome(
            FilesEmbedded: 0,
            GapsSelected: 0,
            CoverageEstablished: true,
            Deferred: false,
            GapScanSkipped: true);

        Assert.Multiple(() =>
        {
            Assert.That(
                outcome.Converged,
                Is.False,
                "a skipped gap scan proves nothing, however clean every other field looks");

            // The control. Identical in every respect except the skip, so the flag is
            // demonstrably what flips the verdict rather than something else in the
            // record happening to be false.
            var scanned = outcome with { GapScanSkipped = false };
            Assert.That(
                scanned.Converged,
                Is.True,
                "and the same pass that DID scan is converged, so the exclusion above is attributable to the "
                + "skip alone");
        });
    }

    [Test]
    public void None_is_the_inert_outcome_and_claims_nothing()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RepoFileVectorIngestOutcome.None.FilesEmbedded, Is.Zero);
            Assert.That(RepoFileVectorIngestOutcome.None.GapsSelected, Is.Zero);
            Assert.That(RepoFileVectorIngestOutcome.None.CoverageEstablished, Is.False);
            Assert.That(
                RepoFileVectorIngestOutcome.None.Converged,
                Is.False,
                "a binding that never embeds must not be read as a converged repository");
        });
    }

    [Test]
    public void Two_outcomes_carrying_the_same_facts_are_equal()
    {
        // The coordinator stores the verdict in a cached snapshot and compares it to
        // decide whether the verdict flipped, so value equality is load-bearing.
        var a = new RepoFileVectorIngestOutcome(2, 1, true);
        var b = new RepoFileVectorIngestOutcome(2, 1, true);

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a with { GapsSelected = 0 }, Is.Not.EqualTo(b));
        });
    }
}
