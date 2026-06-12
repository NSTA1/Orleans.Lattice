namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit coverage for <see cref="CompressionDictionaryTrainingReservoir"/>: the
/// count and byte bounds, the per-sample cap, and the deterministic
/// sampling-rate behaviour driven by an injected sampling-decision source.
/// </summary>
[TestFixture]
public sealed class CompressionDictionaryTrainingReservoirTests
{
    private static Func<double> SamplerOf(params double[] values)
    {
        var i = 0;
        return () => values[i++ % values.Length];
    }

    private static byte[] Bytes(int length, byte fill)
    {
        var buffer = new byte[length];
        Array.Fill(buffer, fill);
        return buffer;
    }

    [Test]
    public void TryObserve_rejects_empty_payload()
    {
        var reservoir = new CompressionDictionaryTrainingReservoir(10, 1024, 64, 1.0);
        Assert.Multiple(() =>
        {
            Assert.That(reservoir.TryObserve(ReadOnlySpan<byte>.Empty), Is.False);
            Assert.That(reservoir.SampleCount, Is.EqualTo(0));
            Assert.That(reservoir.TotalBytes, Is.EqualTo(0));
        });
    }

    [Test]
    public void TryObserve_rejects_oversized_sample()
    {
        var reservoir = new CompressionDictionaryTrainingReservoir(10, 1024, maxSampleBytes: 8, samplingRate: 1.0);
        Assert.Multiple(() =>
        {
            Assert.That(reservoir.TryObserve(Bytes(9, 1)), Is.False);
            Assert.That(reservoir.SampleCount, Is.EqualTo(0));
        });
    }

    [Test]
    public void TryObserve_admits_sample_at_per_sample_cap()
    {
        var reservoir = new CompressionDictionaryTrainingReservoir(10, 1024, maxSampleBytes: 8, samplingRate: 1.0);
        Assert.That(reservoir.TryObserve(Bytes(8, 1)), Is.True);
        Assert.That(reservoir.SampleCount, Is.EqualTo(1));
    }

    [Test]
    public void TryObserve_enforces_count_bound_by_evicting_oldest()
    {
        var reservoir = new CompressionDictionaryTrainingReservoir(maxSampleCount: 3, maxReservoirBytes: 1_000_000, maxSampleBytes: 64, samplingRate: 1.0);
        for (byte b = 1; b <= 5; b++)
        {
            reservoir.TryObserve(Bytes(4, b));
        }

        var snapshot = reservoir.Snapshot();
        Assert.Multiple(() =>
        {
            Assert.That(reservoir.SampleCount, Is.EqualTo(3));
            Assert.That(snapshot.Length, Is.EqualTo(3));
            // Oldest two (fill 1, 2) evicted; 3, 4, 5 remain in order.
            Assert.That(snapshot[0][0], Is.EqualTo((byte)3));
            Assert.That(snapshot[2][0], Is.EqualTo((byte)5));
        });
    }

    [Test]
    public void TryObserve_enforces_byte_bound()
    {
        // 4-byte samples, byte cap 10 => at most 2 samples (8 bytes) retained.
        var reservoir = new CompressionDictionaryTrainingReservoir(maxSampleCount: 100, maxReservoirBytes: 10, maxSampleBytes: 8, samplingRate: 1.0);
        for (byte b = 1; b <= 5; b++)
        {
            reservoir.TryObserve(Bytes(4, b));
        }

        Assert.Multiple(() =>
        {
            Assert.That(reservoir.TotalBytes, Is.LessThanOrEqualTo(10));
            Assert.That(reservoir.SampleCount, Is.EqualTo(2));
        });
    }

    [Test]
    public void TotalBytes_tracks_retained_sample_sizes()
    {
        var reservoir = new CompressionDictionaryTrainingReservoir(10, 1_000_000, 64, 1.0);
        reservoir.TryObserve(Bytes(5, 1));
        reservoir.TryObserve(Bytes(7, 2));
        Assert.That(reservoir.TotalBytes, Is.EqualTo(12));
    }

    [Test]
    public void TryObserve_zero_sampling_rate_admits_nothing()
    {
        var reservoir = new CompressionDictionaryTrainingReservoir(10, 1024, 64, samplingRate: 0.0, sampler: SamplerOf(0.0));
        Assert.Multiple(() =>
        {
            Assert.That(reservoir.TryObserve(Bytes(4, 1)), Is.False);
            Assert.That(reservoir.SampleCount, Is.EqualTo(0));
        });
    }

    [Test]
    public void TryObserve_sampling_rate_admits_only_winning_draws()
    {
        // rate 0.5: admit when the draw is < 0.5.
        var reservoir = new CompressionDictionaryTrainingReservoir(
            maxSampleCount: 100, maxReservoirBytes: 1_000_000, maxSampleBytes: 64,
            samplingRate: 0.5, sampler: SamplerOf(0.1, 0.9, 0.4, 0.99));

        var admitted = 0;
        for (var i = 0; i < 4; i++)
        {
            if (reservoir.TryObserve(Bytes(4, 1)))
            {
                admitted++;
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.EqualTo(2));
            Assert.That(reservoir.SampleCount, Is.EqualTo(2));
        });
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Constructor_rejects_non_positive_count(int value)
    {
        Assert.That(
            () => new CompressionDictionaryTrainingReservoir(value, 1024, 64, 1.0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [TestCase(double.NaN)]
    [TestCase(-0.1)]
    [TestCase(1.1)]
    public void Constructor_rejects_bad_sampling_rate(double value)
    {
        Assert.That(
            () => new CompressionDictionaryTrainingReservoir(10, 1024, 64, value),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }
}
