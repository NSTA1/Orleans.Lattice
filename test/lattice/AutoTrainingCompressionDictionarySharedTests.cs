using System.Text;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit coverage for the self-distributing shared-dictionary surface of
/// <see cref="AutoTrainingCompressionDictionaryProvider"/>: the active-id seam
/// (<see cref="ILatticeActiveCompressionDictionary"/>), the training sampler
/// seam (<see cref="ILatticeCompressionDictionarySampler"/>), the pulled-bytes
/// install sink (<see cref="ILatticeCompressionDictionarySink"/>), and the
/// <see cref="AutoTrainingCompressionDictionaryProvider.MinTrainingInterval"/>
/// driver hint.
/// </summary>
[TestFixture]
public sealed class AutoTrainingCompressionDictionarySharedTests
{
    private sealed class FakeClock(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _now = start;
        public override DateTimeOffset GetUtcNow() => _now;
        public void Advance(TimeSpan delta) => _now += delta;
    }

    private static CompressionDictionaryTrainingOptions EnabledOptions(
        Action<CompressionDictionaryTrainingOptions>? tweak = null)
    {
        var options = new CompressionDictionaryTrainingOptions
        {
            Enabled = true,
            MinSamplesToTrain = 8,
            MinTrainingInterval = TimeSpan.Zero,
            DictionaryCapacityBytes = 4096,
            MaxSampleCount = 4096,
            MaxReservoirBytes = 16L * 1024 * 1024,
            RetainedVersionCount = 4,
        };
        tweak?.Invoke(options);
        return options;
    }

    private static void FeedCorpus(AutoTrainingCompressionDictionaryProvider provider, int count)
    {
        for (var i = 0; i < count; i++)
        {
            provider.Observe(Encoding.UTF8.GetBytes(
                $"user:{i % 50}|order:{i}|status:shipped|region:eu-west-1|sku:widget-{i % 13}|qty:{i % 7}|"));
        }
    }

    [Test]
    public void MinTrainingInterval_reflects_the_configured_option()
    {
        var interval = TimeSpan.FromSeconds(42);
        using var provider = new AutoTrainingCompressionDictionaryProvider(
            EnabledOptions(o => o.MinTrainingInterval = interval));

        Assert.That(provider.MinTrainingInterval, Is.EqualTo(interval));
    }

    [Test]
    public void ActiveDictionaryId_is_zero_before_first_train()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(EnabledOptions());

        Assert.That(((ILatticeActiveCompressionDictionary)provider).ActiveDictionaryId, Is.EqualTo(0u));
    }

    [Test]
    public void ActiveDictionaryId_tracks_the_current_trained_id()
    {
        var clock = new FakeClock(DateTimeOffset.UnixEpoch);
        using var provider = new AutoTrainingCompressionDictionaryProvider(EnabledOptions(), clock);

        FeedCorpus(provider, 64);
        Assert.That(provider.TryTrain(), Is.True);

        Assert.That(
            ((ILatticeActiveCompressionDictionary)provider).ActiveDictionaryId,
            Is.EqualTo(provider.CurrentDictionaryId));
        Assert.That(provider.CurrentDictionaryId, Is.Not.EqualTo(0u));
    }

    [Test]
    public void Sampler_seam_feeds_the_training_reservoir()
    {
        var clock = new FakeClock(DateTimeOffset.UnixEpoch);
        using var provider = new AutoTrainingCompressionDictionaryProvider(EnabledOptions(), clock);
        var sampler = (ILatticeCompressionDictionarySampler)provider;

        for (var i = 0; i < 64; i++)
        {
            sampler.Observe(Encoding.UTF8.GetBytes(
                $"user:{i % 50}|order:{i}|status:shipped|region:eu-west-1|sku:widget-{i % 13}|qty:{i % 7}|"));
        }

        Assert.That(provider.TryTrain(), Is.True);
        Assert.That(provider.CurrentDictionaryId, Is.Not.EqualTo(0u));
    }

    [Test]
    public void TryInstall_publishes_pulled_bytes_under_the_requested_id()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(EnabledOptions());
        var bytes = new byte[] { 1, 2, 3, 4, 5 };

        Assert.That(provider.TryInstall(7u, bytes), Is.True);
        Assert.That(provider.TryGetDictionary(7u, out var stored), Is.True);
        Assert.That(stored.ToArray(), Is.EqualTo(bytes));
        Assert.That(provider.AvailableDictionaryIds, Does.Contain(7u));
    }

    [Test]
    public void TryInstall_does_not_change_the_active_id()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(EnabledOptions());

        provider.TryInstall(9u, new byte[] { 1, 2, 3 });

        Assert.That(provider.CurrentDictionaryId, Is.EqualTo(0u));
        Assert.That(((ILatticeActiveCompressionDictionary)provider).ActiveDictionaryId, Is.EqualTo(0u));
    }

    [Test]
    public void TryInstall_is_idempotent_for_byte_identical_content()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(EnabledOptions());
        var bytes = new byte[] { 4, 5, 6 };

        Assert.That(provider.TryInstall(3u, bytes), Is.True);
        Assert.That(provider.TryInstall(3u, bytes.ToArray()), Is.True);
    }

    [Test]
    public void TryInstall_rejects_a_colliding_payload_under_a_live_id()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(EnabledOptions());

        Assert.That(provider.TryInstall(5u, new byte[] { 1, 1, 1 }), Is.True);
        Assert.That(provider.TryInstall(5u, new byte[] { 2, 2, 2 }), Is.False);
        Assert.That(provider.TryGetDictionary(5u, out var stored), Is.True);
        Assert.That(stored.ToArray(), Is.EqualTo(new byte[] { 1, 1, 1 }));
    }

    [Test]
    public void TryInstall_rejects_reserved_id_and_empty_bytes()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(EnabledOptions());

        Assert.Multiple(() =>
        {
            Assert.That(provider.TryInstall(0u, new byte[] { 1 }), Is.False);
            Assert.That(provider.TryInstall(1u, ReadOnlyMemory<byte>.Empty), Is.False);
        });
    }

    [Test]
    public void TryInstall_copies_the_bytes_so_later_mutation_does_not_corrupt_the_dictionary()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(EnabledOptions());
        var bytes = new byte[] { 9, 9, 9 };

        provider.TryInstall(2u, bytes);
        bytes[0] = 0;

        Assert.That(provider.TryGetDictionary(2u, out var stored), Is.True);
        Assert.That(stored.ToArray(), Is.EqualTo(new byte[] { 9, 9, 9 }));
    }

    [Test]
    public void TryInstall_throws_after_dispose()
    {
        var provider = new AutoTrainingCompressionDictionaryProvider(EnabledOptions());
        provider.Dispose();

        Assert.That(
            () => provider.TryInstall(1u, new byte[] { 1 }),
            Throws.InstanceOf<ObjectDisposedException>());
    }
}
