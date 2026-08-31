namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexProjectionMode"/> and
/// <see cref="GrainIndexOutboxOptions"/>: the two knobs that decide when a
/// tracked write's entries become visible, and how quickly a deferred or failed
/// one catches up.
/// </summary>
[TestFixture]
public sealed class GrainIndexOutboxOptionsTests
{
    [Test]
    public void Synchronous_projection_is_the_zero_value_and_therefore_the_default()
    {
        Assert.Multiple(() =>
        {
            Assert.That((int)GrainIndexProjectionMode.Synchronous, Is.Zero);
            Assert.That(default(GrainIndexProjectionMode), Is.EqualTo(GrainIndexProjectionMode.Synchronous),
                "Surfacing failures has to be what a host gets without asking, or a silent index "
                + "is one forgotten setting away.");
            Assert.That(new GrainIndexOptions().ProjectionMode, Is.EqualTo(GrainIndexProjectionMode.Synchronous));
        });
    }

    [Test]
    public void The_eventual_mode_is_a_distinct_opt_in()
    {
        Assert.That((int)GrainIndexProjectionMode.Eventual, Is.EqualTo(1));
    }

    [Test]
    public void The_drain_runs_by_default_so_a_recorded_write_always_has_someone_to_apply_it()
    {
        var options = new GrainIndexOutboxOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.Enabled, Is.True);
            Assert.That(options.RetryInterval, Is.EqualTo(GrainIndexOutboxOptions.DefaultRetryInterval));
            Assert.That(options.MaxBatchSize, Is.EqualTo(GrainIndexOutboxOptions.DefaultMaxBatchSize));
        });
    }

    [Test]
    public void The_documented_defaults_are_the_ones_the_options_actually_use()
    {
        Assert.Multiple(() =>
        {
            Assert.That(GrainIndexOutboxOptions.DefaultRetryInterval, Is.EqualTo(TimeSpan.FromSeconds(5)));
            Assert.That(GrainIndexOutboxOptions.DefaultMaxBatchSize, Is.EqualTo(256));
        });
    }

    [Test]
    public void Every_setting_is_overridable()
    {
        var options = new GrainIndexOutboxOptions
        {
            Enabled = false,
            RetryInterval = TimeSpan.FromMinutes(2),
            MaxBatchSize = 8,
        };

        Assert.Multiple(() =>
        {
            Assert.That(options.Enabled, Is.False);
            Assert.That(options.RetryInterval, Is.EqualTo(TimeSpan.FromMinutes(2)));
            Assert.That(options.MaxBatchSize, Is.EqualTo(8));
        });
    }
}
