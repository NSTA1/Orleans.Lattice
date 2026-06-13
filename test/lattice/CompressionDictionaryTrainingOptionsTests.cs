namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit coverage for <see cref="CompressionDictionaryTrainingOptions"/>: the
/// documented defaults and the <see cref="CompressionDictionaryTrainingOptions.Validate"/>
/// range checks.
/// </summary>
[TestFixture]
public sealed class CompressionDictionaryTrainingOptionsTests
{
    [Test]
    public void Defaults_are_disabled_and_documented()
    {
        var options = new CompressionDictionaryTrainingOptions();
        Assert.Multiple(() =>
        {
            Assert.That(options.Enabled, Is.False);
            Assert.That(options.MaxSampleCount, Is.EqualTo(1024));
            Assert.That(options.MaxReservoirBytes, Is.EqualTo(8L * 1024 * 1024));
            Assert.That(options.MaxSampleBytes, Is.EqualTo(64 * 1024));
            Assert.That(options.SamplingRate, Is.EqualTo(1.0));
            Assert.That(options.DictionaryCapacityBytes, Is.EqualTo(112 * 1024));
            Assert.That(options.MinSamplesToTrain, Is.EqualTo(100));
            Assert.That(options.MinTrainingInterval, Is.EqualTo(TimeSpan.FromMinutes(5)));
            Assert.That(options.RetainedVersionCount, Is.EqualTo(4));
            Assert.That(options.FirstDictionaryId, Is.EqualTo(1u));
        });
    }

    [Test]
    public void Validate_passes_for_defaults()
    {
        var options = new CompressionDictionaryTrainingOptions();
        Assert.That(() => options.Validate(), Throws.Nothing);
    }

    [Test]
    public void Validate_passes_for_zero_min_interval()
    {
        var options = new CompressionDictionaryTrainingOptions { MinTrainingInterval = TimeSpan.Zero };
        Assert.That(() => options.Validate(), Throws.Nothing);
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_rejects_non_positive_max_sample_count(int value)
    {
        var options = new CompressionDictionaryTrainingOptions { MaxSampleCount = value };
        Assert.That(() => options.Validate(), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [TestCase(0L)]
    [TestCase(-1L)]
    public void Validate_rejects_non_positive_max_reservoir_bytes(long value)
    {
        var options = new CompressionDictionaryTrainingOptions { MaxReservoirBytes = value };
        Assert.That(() => options.Validate(), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_rejects_non_positive_max_sample_bytes(int value)
    {
        var options = new CompressionDictionaryTrainingOptions { MaxSampleBytes = value };
        Assert.That(() => options.Validate(), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Validate_rejects_reservoir_smaller_than_one_sample()
    {
        var options = new CompressionDictionaryTrainingOptions
        {
            MaxSampleBytes = 4096,
            MaxReservoirBytes = 2048,
        };
        Assert.That(() => options.Validate(), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [TestCase(-0.1)]
    [TestCase(1.1)]
    [TestCase(double.NaN)]
    public void Validate_rejects_sampling_rate_out_of_range(double value)
    {
        var options = new CompressionDictionaryTrainingOptions { SamplingRate = value };
        Assert.That(() => options.Validate(), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_rejects_non_positive_dictionary_capacity(int value)
    {
        var options = new CompressionDictionaryTrainingOptions { DictionaryCapacityBytes = value };
        Assert.That(() => options.Validate(), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_rejects_non_positive_min_samples(int value)
    {
        var options = new CompressionDictionaryTrainingOptions { MinSamplesToTrain = value };
        Assert.That(() => options.Validate(), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Validate_rejects_negative_min_interval()
    {
        var options = new CompressionDictionaryTrainingOptions { MinTrainingInterval = TimeSpan.FromSeconds(-1) };
        Assert.That(() => options.Validate(), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_rejects_non_positive_retained_version_count(int value)
    {
        var options = new CompressionDictionaryTrainingOptions { RetainedVersionCount = value };
        Assert.That(() => options.Validate(), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Validate_rejects_reserved_first_dictionary_id()
    {
        var options = new CompressionDictionaryTrainingOptions { FirstDictionaryId = 0u };
        Assert.That(() => options.Validate(), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }
}
