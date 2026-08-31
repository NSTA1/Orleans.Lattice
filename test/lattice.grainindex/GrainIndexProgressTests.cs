namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexProgress"/>: the report an operator reads for
/// "how far along is this index".
/// </summary>
[TestFixture]
public sealed class GrainIndexProgressTests
{
    [Test]
    public void A_progress_report_keeps_every_figure_it_was_given()
    {
        var progress = new GrainIndexProgress(42, 100, 42d, "user-42", "boom");

        Assert.Multiple(() =>
        {
            Assert.That(progress.Processed, Is.EqualTo(42));
            Assert.That(progress.Total, Is.EqualTo(100));
            Assert.That(progress.PercentComplete, Is.EqualTo(42d));
            Assert.That(progress.LastProcessedKey, Is.EqualTo("user-42"));
            Assert.That(progress.LastError, Is.EqualTo("boom"));
        });
    }

    [Test]
    public void An_unbounded_population_leaves_total_and_percent_unknown()
    {
        var progress = new GrainIndexProgress(7, null, null, null, null);

        Assert.Multiple(() =>
        {
            Assert.That(progress.Processed, Is.EqualTo(7));
            Assert.That(progress.Total, Is.Null);
            Assert.That(progress.PercentComplete, Is.Null);
            Assert.That(progress.LastProcessedKey, Is.Null);
            Assert.That(progress.LastError, Is.Null);
        });
    }

    [Test]
    public void The_none_report_describes_a_crawl_that_has_never_run()
    {
        var none = GrainIndexProgress.None;

        Assert.Multiple(() =>
        {
            Assert.That(none.Processed, Is.Zero);
            Assert.That(none.Total, Is.Null);
            Assert.That(none.PercentComplete, Is.Null);
            Assert.That(none.LastProcessedKey, Is.Null);
            Assert.That(none.LastError, Is.Null);
        });
    }

    [Test]
    public void The_none_report_is_a_shared_singleton() =>
        Assert.That(GrainIndexProgress.None, Is.SameAs(GrainIndexProgress.None));
}
