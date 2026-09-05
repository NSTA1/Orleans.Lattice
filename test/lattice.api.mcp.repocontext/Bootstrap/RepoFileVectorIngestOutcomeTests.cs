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
