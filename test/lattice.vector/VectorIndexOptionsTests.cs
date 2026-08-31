namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Unit tests for the index configuration: the validated setters, the derived
/// sizing rules, and the defensive copy.
/// </summary>
[TestFixture]
public sealed class VectorIndexOptionsTests
{
    [Test]
    public void Defaults_are_the_documented_values()
    {
        var options = new VectorIndexOptions();

        Assert.That(options.Dimensions, Is.EqualTo(0));
        Assert.That(options.Metric, Is.EqualTo(VectorDistanceMetric.Cosine));
        Assert.That(options.PartitionCount, Is.EqualTo(0));
        Assert.That(options.Probes, Is.EqualTo(0));
        Assert.That(options.Seed, Is.EqualTo(0x9E3779B97F4A7C15UL));
        Assert.That(options.TrainingSampleSize, Is.EqualTo(32_768));
        Assert.That(options.MaxTrainingIterations, Is.EqualTo(10));
        Assert.That(options.MinimumTrainingCount, Is.EqualTo(1_024));
    }

    [Test]
    public void Dimensions_rejects_a_non_positive_value()
    {
        var options = new VectorIndexOptions();

        Assert.Throws<ArgumentOutOfRangeException>(() => options.Dimensions = 0);
        Assert.Throws<ArgumentOutOfRangeException>(() => options.Dimensions = -1);
    }

    [Test]
    public void PartitionCount_and_Probes_accept_zero_but_reject_a_negative_value()
    {
        var options = new VectorIndexOptions { PartitionCount = 0, Probes = 0 };

        Assert.That(options.PartitionCount, Is.EqualTo(0));
        Assert.That(options.Probes, Is.EqualTo(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => options.PartitionCount = -1);
        Assert.Throws<ArgumentOutOfRangeException>(() => options.Probes = -1);
    }

    [Test]
    public void Training_knobs_reject_a_non_positive_value()
    {
        var options = new VectorIndexOptions();

        Assert.Throws<ArgumentOutOfRangeException>(() => options.TrainingSampleSize = 0);
        Assert.Throws<ArgumentOutOfRangeException>(() => options.MaxTrainingIterations = 0);
        Assert.Throws<ArgumentOutOfRangeException>(() => options.MinimumTrainingCount = 0);
    }

    [Test]
    public void Validate_rejects_options_whose_dimensions_were_never_set()
    {
        var thrown = Assert.Throws<ArgumentException>(() => new VectorIndexOptions().Validate());

        Assert.That(thrown!.Message, Does.Contain("Dimensions"));
    }

    [Test]
    public void Validate_rejects_a_metric_outside_the_defined_members()
    {
        var options = new VectorIndexOptions { Dimensions = 4, Metric = (VectorDistanceMetric)99 };

        var thrown = Assert.Throws<ArgumentException>(options.Validate);

        Assert.That(thrown!.Message, Does.Contain("99"));
    }

    [Test]
    public void Validate_accepts_a_fully_configured_instance()
    {
        Assert.DoesNotThrow(() => new VectorIndexOptions { Dimensions = 8 }.Validate());
        Assert.DoesNotThrow(() =>
            new VectorIndexOptions { Dimensions = 8, Metric = VectorDistanceMetric.DotProduct }.Validate());
    }

    [Test]
    public void AutoPartitionCount_is_the_square_root_of_the_corpus()
    {
        Assert.That(VectorIndexOptions.AutoPartitionCount(0), Is.EqualTo(0));
        Assert.That(VectorIndexOptions.AutoPartitionCount(1), Is.EqualTo(1));
        Assert.That(VectorIndexOptions.AutoPartitionCount(10_000), Is.EqualTo(100));
        Assert.That(VectorIndexOptions.AutoPartitionCount(73_537), Is.EqualTo(271));
    }

    [Test]
    public void AutoPartitionCount_is_clamped_to_the_maximum()
    {
        Assert.That(
            VectorIndexOptions.AutoPartitionCount(int.MaxValue),
            Is.EqualTo(VectorIndexOptions.MaximumPartitionCount));
    }

    [Test]
    public void AutoPartitionCount_rejects_a_negative_corpus_size()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => VectorIndexOptions.AutoPartitionCount(-1));
    }

    [Test]
    public void AutoProbes_grows_with_the_square_root_of_the_partition_count()
    {
        Assert.That(VectorIndexOptions.AutoProbes(0), Is.EqualTo(0));
        Assert.That(VectorIndexOptions.AutoProbes(4), Is.EqualTo(4));
        Assert.That(VectorIndexOptions.AutoProbes(16), Is.EqualTo(8));
        Assert.That(VectorIndexOptions.AutoProbes(141), Is.EqualTo(24));
        Assert.That(VectorIndexOptions.AutoProbes(271), Is.EqualTo(34));
        Assert.That(VectorIndexOptions.AutoProbes(1_000), Is.EqualTo(64));
    }

    [Test]
    public void AutoProbes_scans_a_shrinking_fraction_of_a_growing_corpus()
    {
        // This is the property that keeps query cost sub-linear: a fixed fraction
        // of the partitions would put the corpus back in the exponent.
        var small = VectorIndexOptions.AutoPartitionCount(10_000);
        var large = VectorIndexOptions.AutoPartitionCount(1_000_000);

        var smallFraction = (double)VectorIndexOptions.AutoProbes(small) / small;
        var largeFraction = (double)VectorIndexOptions.AutoProbes(large) / large;

        Assert.That(largeFraction, Is.LessThan(smallFraction));
    }

    [Test]
    public void AutoProbes_rejects_a_negative_partition_count()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => VectorIndexOptions.AutoProbes(-1));
    }

    [Test]
    public void Clone_copies_every_knob()
    {
        var options = new VectorIndexOptions
        {
            Dimensions = 12,
            Metric = VectorDistanceMetric.DotProduct,
            PartitionCount = 7,
            Probes = 3,
            Seed = 42,
            TrainingSampleSize = 64,
            MaxTrainingIterations = 2,
            MinimumTrainingCount = 5,
        };

        var clone = options.Clone();

        Assert.That(clone.Dimensions, Is.EqualTo(12));
        Assert.That(clone.Metric, Is.EqualTo(VectorDistanceMetric.DotProduct));
        Assert.That(clone.PartitionCount, Is.EqualTo(7));
        Assert.That(clone.Probes, Is.EqualTo(3));
        Assert.That(clone.Seed, Is.EqualTo(42UL));
        Assert.That(clone.TrainingSampleSize, Is.EqualTo(64));
        Assert.That(clone.MaxTrainingIterations, Is.EqualTo(2));
        Assert.That(clone.MinimumTrainingCount, Is.EqualTo(5));
    }

    [Test]
    public void An_index_is_unaffected_by_mutation_of_the_options_it_was_built_from()
    {
        var options = new VectorIndexOptions { Dimensions = 4 };
        var index = new VectorIndex(options);

        options.Dimensions = 99;

        Assert.That(index.Dimensions, Is.EqualTo(4));
    }
}
