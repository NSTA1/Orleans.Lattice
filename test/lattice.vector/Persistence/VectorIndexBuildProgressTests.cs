using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Persistence;

[TestFixture]
public sealed class VectorIndexBuildProgressTests
{
    private static VectorIndexBuildProgress Progress(
        VectorIndexBuildPhase phase, int indexed, int expected) =>
        new(phase, Generation: 1, indexed, expected, PartitionsPersisted: 0, PartitionsTotal: 0,
            RestoredFromDurableState: false);

    [Test]
    public void Only_the_ready_phase_reports_ready()
    {
        foreach (var phase in Enum.GetValues<VectorIndexBuildPhase>())
        {
            var progress = Progress(phase, 1, 2);

            Assert.That(progress.IsReady, Is.EqualTo(phase == VectorIndexBuildPhase.Ready),
                $"Phase {phase} reported the wrong readiness.");
        }
    }

    [Test]
    public void The_ingested_fraction_is_the_share_of_the_source_held()
    {
        Assert.That(Progress(VectorIndexBuildPhase.Ingesting, 25, 100).IngestedFraction, Is.EqualTo(0.25d));
    }

    [Test]
    public void The_ingested_fraction_is_one_when_the_build_is_complete()
    {
        Assert.That(Progress(VectorIndexBuildPhase.Ready, 0, 100).IngestedFraction, Is.EqualTo(1d));
    }

    [Test]
    public void The_ingested_fraction_is_one_when_the_expected_count_is_unknown()
    {
        // Reporting a fraction of an unknown total would present a guess as a
        // fact, which is exactly what a readiness signal must not do.
        Assert.That(Progress(VectorIndexBuildPhase.Ingesting, 5, 0).IngestedFraction, Is.EqualTo(1d));
    }

    [Test]
    public void The_ingested_fraction_is_clamped_when_the_index_runs_ahead_of_the_count()
    {
        Assert.That(Progress(VectorIndexBuildPhase.Ingesting, 150, 100).IngestedFraction, Is.EqualTo(1d));
    }

    [Test]
    public void A_negative_expected_count_does_not_produce_a_negative_fraction()
    {
        Assert.That(Progress(VectorIndexBuildPhase.Ingesting, 5, -1).IngestedFraction, Is.EqualTo(1d));
    }

    [Test]
    public void Progress_carries_every_field_it_was_given()
    {
        var progress = new VectorIndexBuildProgress(
            VectorIndexBuildPhase.Persisting, 7, 8, 9, 10, 11, true);

        Assert.Multiple(() =>
        {
            Assert.That(progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Persisting));
            Assert.That(progress.Generation, Is.EqualTo(7));
            Assert.That(progress.VectorsIndexed, Is.EqualTo(8));
            Assert.That(progress.VectorsExpected, Is.EqualTo(9));
            Assert.That(progress.PartitionsPersisted, Is.EqualTo(10));
            Assert.That(progress.PartitionsTotal, Is.EqualTo(11));
            Assert.That(progress.RestoredFromDurableState, Is.True);
        });
    }

    [Test]
    public void The_phases_are_ordered_so_a_consumer_can_treat_them_as_a_watermark()
    {
        Assert.That(
            new[]
            {
                (int)VectorIndexBuildPhase.NotStarted,
                (int)VectorIndexBuildPhase.Ingesting,
                (int)VectorIndexBuildPhase.Training,
                (int)VectorIndexBuildPhase.Persisting,
                (int)VectorIndexBuildPhase.Ready,
            },
            Is.Ordered.Ascending);
    }
}
