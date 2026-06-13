using System.Diagnostics.Metrics;
using System.Text;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit coverage for <see cref="AutoTrainingCompressionDictionaryProvider"/>:
/// default-off no-op behaviour, cadence and minimum-sample gating, versioned
/// roll-over with a stable content hash, the bounded retained-version ring,
/// the round-trip improvement over the no-dictionary baseline through
/// <see cref="ZstdDictionaryLatticeCompressor"/>, disposal, and telemetry.
/// </summary>
[TestFixture]
public sealed class AutoTrainingCompressionDictionaryProviderTests
{
    private sealed class FakeClock(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _now = start;
        public override DateTimeOffset GetUtcNow() => _now;
        public void Advance(TimeSpan delta) => _now += delta;
    }

    private static byte[] Record(int i)
        => Encoding.UTF8.GetBytes(
            $"user:{i % 50}|order:{i}|status:shipped|region:eu-west-1|sku:widget-{i % 13}|qty:{i % 7}|");

    private static void FeedCorpus(AutoTrainingCompressionDictionaryProvider provider, int count, string salt = "")
    {
        for (var i = 0; i < count; i++)
        {
            var record = salt.Length == 0
                ? Record(i)
                : Encoding.UTF8.GetBytes($"{salt}|user:{i % 50}|order:{i}|status:shipped|region:eu-west-1|sku:widget-{i % 13}|");
            provider.Observe(record);
        }
    }

    private static CompressionDictionaryTrainingOptions EnabledOptions(Action<CompressionDictionaryTrainingOptions>? tweak = null)
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

    [Test]
    public void Constructor_throws_on_null_options()
    {
        Assert.That(
            () => new AutoTrainingCompressionDictionaryProvider(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_validates_options()
    {
        var bad = new CompressionDictionaryTrainingOptions { MinSamplesToTrain = 0 };
        Assert.That(
            () => new AutoTrainingCompressionDictionaryProvider(bad),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Disabled_provider_is_a_no_op()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(
            new CompressionDictionaryTrainingOptions { Enabled = false });

        FeedCorpus(provider, 500);
        var trained = provider.TryTrain();

        Assert.Multiple(() =>
        {
            Assert.That(provider.Enabled, Is.False);
            Assert.That(trained, Is.False);
            Assert.That(provider.CurrentDictionaryId, Is.EqualTo(0u));
            Assert.That(provider.TryGetDictionary(1u, out _), Is.False);
        });
    }

    [Test]
    public void Disabled_provider_emits_no_training_runs()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(
            new CompressionDictionaryTrainingOptions { Enabled = false });

        var captured = CaptureTrainingRuns(() =>
        {
            FeedCorpus(provider, 500);
            provider.TryTrain();
        });

        Assert.That(captured, Is.Empty);
    }

    [Test]
    public void Disabled_provider_reports_no_active_version_gauge()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(
            new CompressionDictionaryTrainingOptions { Enabled = false });

        Assert.That(ReadActiveVersion(), Is.Null);
    }

    [Test]
    public void TryTrain_below_min_samples_is_skipped()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(EnabledOptions(o => o.MinSamplesToTrain = 1000));
        FeedCorpus(provider, 10);

        var captured = CaptureTrainingRuns(() => provider.TryTrain());

        Assert.Multiple(() =>
        {
            Assert.That(provider.CurrentDictionaryId, Is.EqualTo(0u));
            Assert.That(captured, Has.Count.EqualTo(1));
            Assert.That(captured[0].Outcome, Is.EqualTo("skipped_insufficient_samples"));
        });
    }

    [Test]
    public void TryTrain_produces_a_versioned_dictionary()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(EnabledOptions());
        FeedCorpus(provider, 1000);

        var trained = provider.TryTrain();

        Assert.Multiple(() =>
        {
            Assert.That(trained, Is.True);
            Assert.That(provider.CurrentDictionaryId, Is.EqualTo(1u));
            Assert.That(provider.TryGetDictionary(1u, out var dict), Is.True);
            Assert.That(dict.Length, Is.GreaterThan(0));
        });
    }

    [Test]
    public void TryTrain_is_idempotent_for_an_identical_corpus()
    {
        var clock = new FakeClock(DateTimeOffset.UnixEpoch);
        using var provider = new AutoTrainingCompressionDictionaryProvider(
            EnabledOptions(o => o.MinTrainingInterval = TimeSpan.FromMinutes(5)), clock);

        FeedCorpus(provider, 1000);
        Assert.That(provider.TryTrain(), Is.True);
        var firstId = provider.CurrentDictionaryId;

        // Same corpus, cadence elapsed: trains the byte-identical dictionary,
        // so no version bump.
        clock.Advance(TimeSpan.FromMinutes(10));
        var trainedAgain = provider.TryTrain();

        Assert.Multiple(() =>
        {
            Assert.That(trainedAgain, Is.False);
            Assert.That(provider.CurrentDictionaryId, Is.EqualTo(firstId));
        });
    }

    [Test]
    public void TryTrain_bumps_version_for_a_changed_corpus()
    {
        var clock = new FakeClock(DateTimeOffset.UnixEpoch);
        using var provider = new AutoTrainingCompressionDictionaryProvider(
            EnabledOptions(o => o.MinTrainingInterval = TimeSpan.FromMinutes(5)), clock);

        FeedCorpus(provider, 1000, salt: "alpha");
        Assert.That(provider.TryTrain(), Is.True);
        Assert.That(provider.CurrentDictionaryId, Is.EqualTo(1u));

        clock.Advance(TimeSpan.FromMinutes(10));
        FeedCorpus(provider, 1000, salt: "bravo-distinct-prefix-shapes");
        Assert.That(provider.TryTrain(), Is.True);
        Assert.That(provider.CurrentDictionaryId, Is.EqualTo(2u));
    }

    [Test]
    public void TryTrain_respects_the_cadence_window()
    {
        var clock = new FakeClock(DateTimeOffset.UnixEpoch);
        using var provider = new AutoTrainingCompressionDictionaryProvider(
            EnabledOptions(o => o.MinTrainingInterval = TimeSpan.FromMinutes(5)), clock);

        FeedCorpus(provider, 1000, salt: "alpha");
        Assert.That(provider.TryTrain(), Is.True);

        // Inside the cadence window: skipped.
        FeedCorpus(provider, 1000, salt: "bravo-distinct");
        var captured = CaptureTrainingRuns(() => provider.TryTrain());

        Assert.Multiple(() =>
        {
            Assert.That(provider.CurrentDictionaryId, Is.EqualTo(1u));
            Assert.That(captured, Has.Count.EqualTo(1));
            Assert.That(captured[0].Outcome, Is.EqualTo("skipped_cadence"));
        });

        // After the window: trains again.
        clock.Advance(TimeSpan.FromMinutes(6));
        Assert.That(provider.TryTrain(), Is.True);
        Assert.That(provider.CurrentDictionaryId, Is.EqualTo(2u));
    }

    [Test]
    public void TryGetDictionary_returns_false_for_zero_and_unknown_ids()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(EnabledOptions());
        FeedCorpus(provider, 1000);
        provider.TryTrain();

        Assert.Multiple(() =>
        {
            Assert.That(provider.TryGetDictionary(0u, out var zero), Is.False);
            Assert.That(zero.IsEmpty, Is.True);
            Assert.That(provider.TryGetDictionary(9999u, out _), Is.False);
        });
    }

    [Test]
    public void Retained_version_ring_evicts_beyond_the_cap()
    {
        var clock = new FakeClock(DateTimeOffset.UnixEpoch);
        using var provider = new AutoTrainingCompressionDictionaryProvider(
            EnabledOptions(o =>
            {
                o.RetainedVersionCount = 2;
                o.MinTrainingInterval = TimeSpan.Zero;
            }),
            clock);

        for (var v = 1; v <= 3; v++)
        {
            FeedCorpus(provider, 1000, salt: $"corpus-variant-{v}-distinct-prefix");
            Assert.That(provider.TryTrain(), Is.True, $"training pass {v} should publish");
        }

        Assert.Multiple(() =>
        {
            Assert.That(provider.CurrentDictionaryId, Is.EqualTo(3u));
            Assert.That(provider.TryGetDictionary(1u, out _), Is.False, "oldest version evicted");
            Assert.That(provider.TryGetDictionary(2u, out _), Is.True);
            Assert.That(provider.TryGetDictionary(3u, out _), Is.True);
        });
    }

    [Test]
    public void Trained_dictionary_round_trips_and_beats_the_no_dictionary_baseline()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(EnabledOptions());
        FeedCorpus(provider, 1000);
        Assert.That(provider.TryTrain(), Is.True);

        var id = provider.CurrentDictionaryId;
        using var compressor = new ZstdDictionaryLatticeCompressor(3, provider);

        var payload = Record(123_456);

        var dictDst = new byte[compressor.GetMaxCompressedLength(payload.Length, id)];
        var dictLen = compressor.Compress(payload, dictDst, id);

        var roundTrip = new byte[payload.Length];
        compressor.Decompress(dictDst.AsSpan(0, dictLen), roundTrip, payload.Length, id);

        var baseDst = new byte[compressor.GetMaxCompressedLength(payload.Length, 0u)];
        var baseLen = compressor.Compress(payload, baseDst, 0u);

        Assert.Multiple(() =>
        {
            Assert.That(roundTrip, Is.EqualTo(payload), "dictionary round-trip must be byte-equal");
            Assert.That(dictLen, Is.LessThan(baseLen), "trained dictionary should beat the no-dictionary baseline");
        });
    }

    [Test]
    public void Enabled_provider_reports_gauges_after_training()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(EnabledOptions());
        FeedCorpus(provider, 1000);
        provider.TryTrain();

        Assert.Multiple(() =>
        {
            Assert.That(ReadActiveVersion(), Is.EqualTo(1L));
            Assert.That(ReadReservoirFill("samples"), Is.EqualTo(1000L));
            Assert.That(ReadReservoirFill("bytes"), Is.GreaterThan(0L));
        });
    }

    [Test]
    public void AvailableDictionaryIds_is_empty_while_disabled()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(
            new CompressionDictionaryTrainingOptions { Enabled = false });

        FeedCorpus(provider, 500);
        provider.TryTrain();

        Assert.That(provider.AvailableDictionaryIds, Is.Empty);
    }

    [Test]
    public void AvailableDictionaryIds_is_empty_before_first_train()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(EnabledOptions());

        Assert.That(provider.AvailableDictionaryIds, Is.Empty);
    }

    [Test]
    public void AvailableDictionaryIds_advertises_the_trained_id()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(EnabledOptions());
        FeedCorpus(provider, 1000);
        Assert.That(provider.TryTrain(), Is.True);

        Assert.Multiple(() =>
        {
            Assert.That(provider.AvailableDictionaryIds, Is.EqualTo(new[] { 1u }));
            Assert.That(provider.AvailableDictionaryIds, Does.Not.Contain(0u));
        });
    }

    [Test]
    public void AvailableDictionaryIds_tracks_the_retained_ring_in_ascending_order()
    {
        var clock = new FakeClock(DateTimeOffset.UnixEpoch);
        using var provider = new AutoTrainingCompressionDictionaryProvider(
            EnabledOptions(o =>
            {
                o.RetainedVersionCount = 2;
                o.MinTrainingInterval = TimeSpan.Zero;
            }),
            clock);

        for (var v = 1; v <= 3; v++)
        {
            FeedCorpus(provider, 1000, salt: $"corpus-variant-{v}-distinct-prefix");
            Assert.That(provider.TryTrain(), Is.True, $"training pass {v} should publish");
        }

        // Ring cap is 2, so id 1 is evicted and only the two most recent
        // versions remain, ordered ascending.
        Assert.That(provider.AvailableDictionaryIds, Is.EqualTo(new[] { 2u, 3u }));
    }

    [Test]
    public void AvailableDictionaryIds_is_exposed_through_the_catalog_interface()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(EnabledOptions());
        FeedCorpus(provider, 1000);
        Assert.That(provider.TryTrain(), Is.True);

        ILatticeCompressionDictionaryCatalog catalog = provider;

        Assert.That(catalog.AvailableDictionaryIds, Is.EqualTo(new[] { 1u }));
    }

    [Test]
    public void Dispose_makes_members_throw()
    {
        var provider = new AutoTrainingCompressionDictionaryProvider(EnabledOptions());
        provider.Dispose();

        Assert.Multiple(() =>
        {
            byte[] sample = [1, 2, 3];
            Assert.That(() => provider.Observe(sample), Throws.InstanceOf<ObjectDisposedException>());
            Assert.That(() => provider.TryTrain(), Throws.InstanceOf<ObjectDisposedException>());
            Assert.That(() => provider.TryGetDictionary(1u, out _), Throws.InstanceOf<ObjectDisposedException>());
        });
    }

    private static List<(long Value, string? Outcome)> CaptureTrainingRuns(Action action)
    {
        var captured = new List<(long, string?)>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (inst, l) =>
            {
                if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter)
                    && inst.Name == LatticeMetrics.CompressionDictionaryTrainingRunsName)
                {
                    l.EnableMeasurementEvents(inst);
                }
            },
        };
        listener.SetMeasurementEventCallback<long>((inst, value, tags, _) =>
        {
            string? outcome = null;
            foreach (var t in tags)
            {
                if (t.Key == LatticeMetrics.TagOutcome)
                {
                    outcome = (string?)t.Value;
                }
            }
            captured.Add((value, outcome));
        });
        listener.Start();
        action();
        return captured;
    }

    private static long? ReadActiveVersion()
    {
        long? found = null;
        using var listener = new MeterListener
        {
            InstrumentPublished = (inst, l) =>
            {
                if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter)
                    && inst.Name == LatticeMetrics.CompressionDictionaryActiveVersionName)
                {
                    l.EnableMeasurementEvents(inst);
                }
            },
        };
        listener.SetMeasurementEventCallback<long>((inst, value, tags, _) => found = value);
        listener.Start();
        listener.RecordObservableInstruments();
        return found;
    }

    private static long? ReadReservoirFill(string kind)
    {
        long? found = null;
        using var listener = new MeterListener
        {
            InstrumentPublished = (inst, l) =>
            {
                if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter)
                    && inst.Name == LatticeMetrics.CompressionDictionaryReservoirFillName)
                {
                    l.EnableMeasurementEvents(inst);
                }
            },
        };
        listener.SetMeasurementEventCallback<long>((inst, value, tags, _) =>
        {
            foreach (var t in tags)
            {
                if (t.Key == LatticeMetrics.TagKind && (string?)t.Value == kind)
                {
                    found = value;
                }
            }
        });
        listener.Start();
        listener.RecordObservableInstruments();
        return found;
    }
}
