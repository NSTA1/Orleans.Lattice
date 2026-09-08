namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for the alternating coverage read-back's parity rule -
/// <see cref="EmbeddingRepoContextVectorIngestor.GapReadBackRunsOnPass"/> and
/// <see cref="EmbeddingRepoContextVectorIngestor.ClassifyGapReadBackArm"/>. The
/// read-back is the only store touch the back-fill instrumentation adds (issue #2208),
/// so it runs on alternate passes to turn the instrument into its own two-arm control:
/// a pass whose predecessor ran the read-back (arm A) against one whose predecessor did
/// not (arm B). If the two arms' gap-count distributions match on the live box the
/// read-back's perturbation is empirically dead; if they diverge it is quantified and
/// can be subtracted. That conclusion is only sound if the arm each pass falls in is
/// assigned correctly, which is what these tests pin: the parity is a pure function of
/// the 1-based pass ordinal, the first pass belongs to neither arm, and - the point
/// that is easy to get backwards - a pass's arm is decided by its PREDECESSOR's parity,
/// not its own.
/// </summary>
/// <remarks>
/// Pure in-process function test: it calls the two static classifiers with integer
/// ordinals, standing up no silo and touching no store, so it needs no slow category.
/// </remarks>
[TestFixture]
public sealed class EmbeddingRepoContextVectorIngestorGapReadBackArmTests
{
    [Test]
    public void Read_back_runs_on_even_pass_ordinals_only()
    {
        Assert.Multiple(() =>
        {
            Assert.That(EmbeddingRepoContextVectorIngestor.GapReadBackRunsOnPass(1), Is.False, "pass 1 is odd");
            Assert.That(EmbeddingRepoContextVectorIngestor.GapReadBackRunsOnPass(2), Is.True, "pass 2 is even");
            Assert.That(EmbeddingRepoContextVectorIngestor.GapReadBackRunsOnPass(3), Is.False);
            Assert.That(EmbeddingRepoContextVectorIngestor.GapReadBackRunsOnPass(4), Is.True);
            Assert.That(EmbeddingRepoContextVectorIngestor.GapReadBackRunsOnPass(179), Is.False);
            Assert.That(EmbeddingRepoContextVectorIngestor.GapReadBackRunsOnPass(180), Is.True);
        });
    }

    [Test]
    public void The_first_pass_belongs_to_neither_arm()
    {
        // Pass 1 has no predecessor, so its count cannot be attributed to a preceding
        // read-back either way; it must be excluded from the comparison, not silently
        // folded into an arm.
        Assert.That(
            EmbeddingRepoContextVectorIngestor.ClassifyGapReadBackArm(1),
            Is.EqualTo(EmbeddingRepoContextVectorIngestor.GapReadBackArm.Seed));
    }

    [Test]
    public void A_pass_arm_is_decided_by_its_predecessor_not_itself()
    {
        Assert.Multiple(() =>
        {
            // Pass 2's predecessor (pass 1, odd) did NOT run the read-back -> arm B.
            Assert.That(
                EmbeddingRepoContextVectorIngestor.ClassifyGapReadBackArm(2),
                Is.EqualTo(EmbeddingRepoContextVectorIngestor.GapReadBackArm.PriorReadBackSkipped));

            // Pass 3's predecessor (pass 2, even) DID run the read-back -> arm A.
            Assert.That(
                EmbeddingRepoContextVectorIngestor.ClassifyGapReadBackArm(3),
                Is.EqualTo(EmbeddingRepoContextVectorIngestor.GapReadBackArm.PriorReadBackRan));

            // Pass 4's predecessor (pass 3, odd) did NOT -> arm B.
            Assert.That(
                EmbeddingRepoContextVectorIngestor.ClassifyGapReadBackArm(4),
                Is.EqualTo(EmbeddingRepoContextVectorIngestor.GapReadBackArm.PriorReadBackSkipped));

            // Pass 5's predecessor (pass 4, even) DID -> arm A.
            Assert.That(
                EmbeddingRepoContextVectorIngestor.ClassifyGapReadBackArm(5),
                Is.EqualTo(EmbeddingRepoContextVectorIngestor.GapReadBackArm.PriorReadBackRan));
        });
    }

    [Test]
    public void Arms_alternate_and_the_read_back_pass_is_always_arm_B()
    {
        // A structural invariant worth pinning: because the read-back runs on even
        // passes and an even pass's predecessor is odd, every pass that itself runs the
        // read-back is classified arm B. So arm A is composed entirely of passes that
        // did NOT run their own read-back but followed one - exactly the passes whose
        // observability could have been warmed by the prior probe.
        for (var pass = 2; pass <= 200; pass++)
        {
            var arm = EmbeddingRepoContextVectorIngestor.ClassifyGapReadBackArm(pass);
            if (EmbeddingRepoContextVectorIngestor.GapReadBackRunsOnPass(pass))
            {
                Assert.That(
                    arm,
                    Is.EqualTo(EmbeddingRepoContextVectorIngestor.GapReadBackArm.PriorReadBackSkipped),
                    $"pass {pass} runs the read-back, so its predecessor was odd and it is arm B");
            }
            else
            {
                Assert.That(
                    arm,
                    Is.EqualTo(EmbeddingRepoContextVectorIngestor.GapReadBackArm.PriorReadBackRan),
                    $"pass {pass} skips the read-back, so its predecessor was even and it is arm A");
            }
        }
    }
}
