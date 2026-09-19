using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// Positive controls for the two WAL compaction instruments added by issue
/// #3107.
/// <para>
/// A declared, documented and paneled instrument whose recording site is never
/// reached is frozen at zero forever, and on a scrape - and on every enrolment,
/// ordering and doc-coverage gate - that is indistinguishable from an
/// instrument that is correct and merely quiet. So each of the three
/// <c>trigger</c> arms is driven into its own condition here and observed
/// advancing, and the two arms it is not is observed staying still: the arms
/// form an identity matrix, so every off-diagonal zero is proven observable by
/// the diagonal entry in the same row.
/// </para>
/// <para>
/// The listener is started through
/// <see cref="MeterListening.StartForInstrument(Instrument, Action{MeterListener})"/>
/// rather than by hand, so the owning type initialiser has necessarily
/// completed before the listener exists and the re-entrant publication hazard
/// is not expressible.
/// </para>
/// </summary>
[TestFixture]
public sealed class FileWalCompactionTelemetryTests
{
    private const string TreeId = "compaction-telemetry-tree";
    private const int PayloadBytes = 4096;

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private string _root = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "lattice-wal-compaction-telemetry", Guid.NewGuid().ToString("N"));
        System.IO.Directory.CreateDirectory(_root);
    }

    [TearDown]
    public void TearDown()
    {
        try
        {
            if (System.IO.Directory.Exists(_root))
            {
                System.IO.Directory.Delete(_root, recursive: true);
            }
        }
        catch (IOException)
        {
            // A best-effort cleanup failure must never redden a green run.
        }
    }

    [Test]
    public async Task Compaction_records_the_ceiling_trigger_and_no_other_arm()
    {
        var counts = await DriveAndCountAsync(async sut =>
        {
            await AppendAsync(sut, count: 50);
            await sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 4, CancellationToken.None);
        },
        compactionMinimumDeadBytes: 1024,
        compactionMaximumDeadBytes: 4 * PayloadBytes);

        Assert.Multiple(() =>
        {
            Assert.That(counts.Ceiling, Is.EqualTo(1), "The absolute ceiling is the only threshold met at a dead fraction of 0.1.");
            Assert.That(counts.Ratio, Is.Zero, "The ratio was nowhere near met, so charging it would misattribute the cause.");
            Assert.That(counts.Reconcile, Is.Zero, "No activation-time reconcile ran in this scenario.");
        });
    }

    [Test]
    public async Task Compaction_records_the_ratio_trigger_and_no_other_arm()
    {
        var counts = await DriveAndCountAsync(async sut =>
        {
            await AppendAsync(sut, count: 20);
            await sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 14, CancellationToken.None);
        },
        compactionMinimumDeadBytes: 1024,
        compactionMaximumDeadBytes: FileWalStorageOptions.DefaultCompactionMaximumDeadBytes);

        Assert.Multiple(() =>
        {
            Assert.That(counts.Ratio, Is.EqualTo(1), "A dead fraction of 0.75 clears the 0.5 ratio.");
            Assert.That(counts.Ceiling, Is.Zero, "The ceiling is disabled by default and must not be charged.");
            Assert.That(counts.Reconcile, Is.Zero);
        });
    }

    [Test]
    public async Task Compaction_records_the_reconcile_trigger_and_no_other_arm()
    {
        // Write and trim below both thresholds so nothing compacts in-line,
        // leaving dead bytes for the activation-time reconcile to reclaim.
        using (var writer = CreateProvider(compactionMinimumDeadBytes: 1024 * 1024))
        {
            await AppendAsync(writer, count: 20);
            await writer.TrimAsync(TreeId, 0, throughOffsetInclusive: 4, CancellationToken.None);
        }

        var counts = await DriveAndCountAsync(
            sut => sut.ReconcileAsync(TreeId, 0, CancellationToken.None),
            compactionMinimumDeadBytes: 1024 * 1024,
            compactionMaximumDeadBytes: FileWalStorageOptions.DefaultCompactionMaximumDeadBytes);

        Assert.Multiple(() =>
        {
            Assert.That(counts.Reconcile, Is.EqualTo(1), "Reconcile compacts unconditionally whenever dead bytes are held.");
            Assert.That(counts.Ratio, Is.Zero);
            Assert.That(counts.Ceiling, Is.Zero);
        });
    }

    [Test]
    public async Task Reclaimed_bytes_counter_advances_by_the_dead_bytes_the_rewrite_released()
    {
        long reclaimed = 0;
        long shrinkage;

        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalCompactionReclaimedBytes,
            l => l.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
            {
                if (MatchesTree(tags))
                {
                    Interlocked.Add(ref reclaimed, measurement);
                }
            }));

        using (var sut = CreateProvider(
            compactionMinimumDeadBytes: 1024,
            compactionMaximumDeadBytes: 4 * PayloadBytes))
        {
            await AppendAsync(sut, count: 50);
            var before = await sut.GetPhysicalByteSizeAsync(TreeId, 0, CancellationToken.None);
            await sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 4, CancellationToken.None);
            var after = await sut.GetPhysicalByteSizeAsync(TreeId, 0, CancellationToken.None);
            shrinkage = before - after;
        }

        listener.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(reclaimed, Is.GreaterThan(0), "The counter must actually move, or its zeros elsewhere are unearned.");
            // The rewrite drops five dead data entries and the trim marker
            // appended alongside them, so the file shrinks by at least the
            // reclaimed payload. Asserting the relation rather than an exact
            // equality keeps the framing overhead out of the expectation while
            // still deriving it independently of the counter.
            Assert.That(
                shrinkage,
                Is.GreaterThanOrEqualTo(reclaimed),
                "Bytes reported reclaimed must be backed by a real fall in physical occupancy.");
        });
    }

    [Test]
    public async Task Loading_a_shard_primes_every_trigger_arm_at_zero()
    {
        var armed = new HashSet<string>(StringComparer.Ordinal);

        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalCompactions,
            l => l.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
            {
                if (measurement == 0 && MatchesTree(tags) && TriggerOf(tags) is { } trigger)
                {
                    lock (armed)
                    {
                        armed.Add(trigger);
                    }
                }
            }));

        using (var sut = CreateProvider())
        {
            // Any operation that loads the shard is enough; nothing here
            // compacts.
            await AppendAsync(sut, count: 1);
        }

        listener.Dispose();

        Assert.That(
            armed,
            Is.EquivalentTo(new[] { "ratio", "ceiling", "reconcile" }),
            "An unprimed arm makes 'this WAL never compacted' indistinguishable from 'this provider is not deployed'.");
    }

    /// <summary>
    /// The priming guarantee is per shard, not per tree (issue #3206).
    /// <para>
    /// Compaction is decided per shard, so the arm set has to be primed once
    /// for every shard that loads. Priming only the tree would leave "this
    /// shard has never compacted" indistinguishable from "this shard is not
    /// reporting", which is the exact ambiguity the shard tag exists to
    /// remove: a dashboard that groups by shard would simply show no series
    /// for the stranded shard, and an absent series reads as an
    /// un-deployed provider rather than as a fault.
    /// </para>
    /// <para>
    /// Two shards are loaded and the full cross product of shard and trigger
    /// arm is required, so a regression that primes once per tree (or once per
    /// process) fails on the missing second shard rather than passing on the
    /// first.
    /// </para>
    /// </summary>
    [Test]
    public async Task Loading_two_shards_primes_every_trigger_arm_at_zero_once_per_shard()
    {
        var armed = new HashSet<(int Shard, string Trigger)>();

        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalCompactions,
            l => l.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
            {
                if (measurement == 0
                    && MatchesTree(tags)
                    && TriggerOf(tags) is { } trigger
                    && ShardOf(tags) is { } shard)
                {
                    lock (armed)
                    {
                        armed.Add((shard, trigger));
                    }
                }
            }));

        using (var sut = CreateProvider())
        {
            await AppendAsync(sut, count: 1, shard: 0);
            await AppendAsync(sut, count: 1, shard: 1);
        }

        listener.Dispose();

        Assert.That(
            armed,
            Is.EquivalentTo(new[]
            {
                (0, "ratio"), (0, "ceiling"), (0, "reconcile"),
                (1, "ratio"), (1, "ceiling"), (1, "reconcile"),
            }),
            "Every loaded shard must publish a measured zero on every arm, or a stranded shard is invisible rather than flat.");
    }

    /// <summary>
    /// Both compaction instruments must carry the storage shard the rewrite
    /// actually ran on (issue #3206), and it must be
    /// <see cref="LatticeMetrics.TagShard"/> rather than
    /// <see cref="LatticeMetrics.TagPartition"/>: the latter names the
    /// producer-side writer partition and is reserved for the writer-layer
    /// instruments, so overloading it here would make the two unjoinable.
    /// </summary>
    [Test]
    public async Task Compaction_reports_the_shard_the_rewrite_ran_on()
    {
        var compactionShards = new HashSet<int>();
        var reclaimedShards = new HashSet<int>();
        var partitionTagSeen = false;

        void Observe(HashSet<int> sink, ReadOnlySpan<KeyValuePair<string, object?>> tags, long measurement)
        {
            if (measurement == 0 || !MatchesTree(tags))
            {
                return;
            }

            foreach (var tag in tags)
            {
                if (string.Equals(tag.Key, LatticeMetrics.TagPartition, StringComparison.Ordinal))
                {
                    partitionTagSeen = true;
                }
            }

            if (ShardOf(tags) is { } shard)
            {
                lock (sink)
                {
                    sink.Add(shard);
                }
            }
        }

        using var compactions = MeterListening.StartForInstrument(
            LatticeMetrics.WalCompactions,
            l => l.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
                Observe(compactionShards, tags, measurement)));

        using var reclaimed = MeterListening.StartForInstrument(
            LatticeMetrics.WalCompactionReclaimedBytes,
            l => l.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
                Observe(reclaimedShards, tags, measurement)));

        using (var sut = CreateProvider(
            compactionMinimumDeadBytes: 1024,
            compactionMaximumDeadBytes: 4 * PayloadBytes))
        {
            await AppendAsync(sut, count: 50, shard: 3);
            await sut.TrimAsync(TreeId, 3, throughOffsetInclusive: 4, CancellationToken.None);
        }

        compactions.Dispose();
        reclaimed.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(compactionShards, Is.EquivalentTo(new[] { 3 }), "The compaction counter must name the shard it rewrote.");
            Assert.That(reclaimedShards, Is.EquivalentTo(new[] { 3 }), "The reclaimed-bytes counter must name the shard it rewrote.");
            Assert.That(partitionTagSeen, Is.False, "The storage shard is tagged 'shard'; 'partition' names the writer partition and must not be overloaded.");
        });
    }

    // --- helpers ------------------------------------------------------------

    private readonly record struct TriggerCounts(long Ratio, long Ceiling, long Reconcile);

    private async Task<TriggerCounts> DriveAndCountAsync(
        Func<FileWalStorageProvider, Task> drive,
        int compactionMinimumDeadBytes,
        long compactionMaximumDeadBytes)
    {
        long ratio = 0, ceiling = 0, reconcile = 0;

        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalCompactions,
            l => l.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
            {
                if (measurement == 0 || !MatchesTree(tags))
                {
                    // Priming writes zeros on every arm; they are asserted by
                    // their own fixture and would mask a missing increment
                    // here.
                    return;
                }

                switch (TriggerOf(tags))
                {
                    case "ratio": Interlocked.Add(ref ratio, measurement); break;
                    case "ceiling": Interlocked.Add(ref ceiling, measurement); break;
                    case "reconcile": Interlocked.Add(ref reconcile, measurement); break;
                }
            }));

        using (var sut = CreateProvider(compactionMinimumDeadBytes, compactionMaximumDeadBytes))
        {
            await drive(sut);
        }

        listener.Dispose();
        return new TriggerCounts(ratio, ceiling, reconcile);
    }

    /// <summary>
    /// The gate's inputs are published for a shard that is evaluated and
    /// <b>declines</b>, not only for one that rewrites (issue #3206).
    /// <para>
    /// This is the distinction the outcome counters cannot draw. A shard whose
    /// <c>wal.compactions</c> series is flat may be evaluated every sweep and
    /// correctly declining, or may never be reaching the evaluation at all,
    /// and those have opposite remedies. The minimum-dead floor here is set
    /// far above anything the trim can produce, so nothing compacts and the
    /// only evidence the evaluation ran is the sample itself.
    /// </para>
    /// <para>
    /// The dead figures are also asserted against each other. The entry counts
    /// are exact, and the byte totals are required to divide into a plausible
    /// mean payload - which is precisely what the pair promises and what the
    /// per-record framing correction needs, since a physical-minus-retained
    /// subtraction otherwise folds that framing invisibly into a derived dead
    /// ratio.
    /// </para>
    /// </summary>
    [Test]
    public async Task Declining_to_compact_still_samples_the_gate_inputs_for_that_shard()
    {
        var samples = new Dictionary<string, List<(int Shard, long Value)>>(StringComparer.Ordinal);

        void Capture(Instrument instrument, long measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            if (!MatchesTree(tags) || ShardOf(tags) is not { } shard)
            {
                return;
            }

            lock (samples)
            {
                if (!samples.TryGetValue(instrument.Name, out var list))
                {
                    list = [];
                    samples[instrument.Name] = list;
                }

                list.Add((shard, measurement));
            }
        }

        using var retainedBytes = MeterListening.StartForInstrument(
            LatticeMetrics.WalCompactionEvalRetainedBytes,
            l => l.SetMeasurementEventCallback<long>((i, m, t, _) => Capture(i, m, t)));
        using var deadBytes = MeterListening.StartForInstrument(
            LatticeMetrics.WalCompactionEvalDeadBytes,
            l => l.SetMeasurementEventCallback<long>((i, m, t, _) => Capture(i, m, t)));
        using var retainedEntries = MeterListening.StartForInstrument(
            LatticeMetrics.WalCompactionEvalRetainedEntries,
            l => l.SetMeasurementEventCallback<long>((i, m, t, _) => Capture(i, m, t)));
        using var deadEntries = MeterListening.StartForInstrument(
            LatticeMetrics.WalCompactionEvalDeadEntries,
            l => l.SetMeasurementEventCallback<long>((i, m, t, _) => Capture(i, m, t)));

        const int Appended = 20;
        const int TrimThrough = 4;
        const int Trimmed = TrimThrough + 1;

        using (var sut = CreateProvider(compactionMinimumDeadBytes: int.MaxValue))
        {
            await AppendAsync(sut, count: Appended, shard: 2);
            await sut.TrimAsync(TreeId, 2, throughOffsetInclusive: TrimThrough, CancellationToken.None);

            // Nothing may have compacted, or the sample would be evidence of a
            // rewrite rather than of a declined evaluation.
            Assert.That(
                await sut.GetPhysicalByteSizeAsync(TreeId, 2, CancellationToken.None),
                Is.GreaterThan(0));
        }

        retainedBytes.Dispose();
        deadBytes.Dispose();
        retainedEntries.Dispose();
        deadEntries.Dispose();

        // The last sample of each series is the state at the evaluation that
        // followed the trim; earlier ones are the arming sample taken at load.
        long Last(string name)
        {
            Assert.That(samples, Does.ContainKey(name), $"'{name}' reported no measurement at all.");
            var list = samples[name];
            Assert.That(list.Select(s => s.Shard), Is.All.EqualTo(2), $"'{name}' reported the wrong shard.");
            return list[^1].Value;
        }

        Assert.Multiple(() =>
        {
            var deadEntryCount = Last(LatticeMetrics.WalCompactionEvalDeadEntriesName);
            var deadByteCount = Last(LatticeMetrics.WalCompactionEvalDeadBytesName);
            var retainedEntryCount = Last(LatticeMetrics.WalCompactionEvalRetainedEntriesName);
            var retainedByteCount = Last(LatticeMetrics.WalCompactionEvalRetainedBytesName);

            Assert.That(
                deadEntryCount,
                Is.EqualTo(Trimmed),
                "A declined evaluation must still report the dead backlog it declined on.");
            Assert.That(
                retainedEntryCount,
                Is.EqualTo(Appended - Trimmed),
                "A declined evaluation must still report the live payload it weighed the backlog against.");

            // The recorded bytes are serialized-record payloads, so they exceed
            // the raw value by a mutation envelope of a few hundred bytes and
            // vary slightly with key length. Bounding the derived mean rather
            // than asserting an exact product is what the pair actually
            // promises: that dividing bytes by entries yields a real mean
            // payload, which is the quantity the framing correction needs.
            Assert.That(
                (double)deadByteCount / deadEntryCount,
                Is.InRange(PayloadBytes, PayloadBytes + 1024),
                "Mean dead payload must be recoverable from the pair.");
            Assert.That(
                (double)retainedByteCount / retainedEntryCount,
                Is.InRange(PayloadBytes, PayloadBytes + 1024),
                "Mean live payload must be recoverable from the pair.");
        });
    }

    /// <summary>
    /// Every loaded shard arms all four gate-input samples, once each, from its
    /// own post-recovery state (issue #3206).
    /// <para>
    /// This is the counterpart to the trigger-arm priming assertion above and
    /// carries the same weight: an unsampled shard leaves "this shard holds no
    /// dead bytes" indistinguishable from "this shard is not reporting", and a
    /// dashboard grouped by shard renders both as an absent series.
    /// </para>
    /// </summary>
    [Test]
    public async Task Loading_two_shards_arms_every_gate_input_once_per_shard()
    {
        var armed = new HashSet<(string Instrument, int Shard)>();

        void Capture(Instrument instrument, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            if (MatchesTree(tags) && ShardOf(tags) is { } shard)
            {
                lock (armed)
                {
                    armed.Add((instrument.Name, shard));
                }
            }
        }

        using var retainedBytes = MeterListening.StartForInstrument(
            LatticeMetrics.WalCompactionEvalRetainedBytes,
            l => l.SetMeasurementEventCallback<long>((i, _, t, _) => Capture(i, t)));
        using var deadBytes = MeterListening.StartForInstrument(
            LatticeMetrics.WalCompactionEvalDeadBytes,
            l => l.SetMeasurementEventCallback<long>((i, _, t, _) => Capture(i, t)));
        using var retainedEntries = MeterListening.StartForInstrument(
            LatticeMetrics.WalCompactionEvalRetainedEntries,
            l => l.SetMeasurementEventCallback<long>((i, _, t, _) => Capture(i, t)));
        using var deadEntries = MeterListening.StartForInstrument(
            LatticeMetrics.WalCompactionEvalDeadEntries,
            l => l.SetMeasurementEventCallback<long>((i, _, t, _) => Capture(i, t)));

        using (var sut = CreateProvider())
        {
            await AppendAsync(sut, count: 1, shard: 0);
            await AppendAsync(sut, count: 1, shard: 1);
        }

        retainedBytes.Dispose();
        deadBytes.Dispose();
        retainedEntries.Dispose();
        deadEntries.Dispose();

        var expected =
            from name in new[]
            {
                LatticeMetrics.WalCompactionEvalRetainedBytesName,
                LatticeMetrics.WalCompactionEvalDeadBytesName,
                LatticeMetrics.WalCompactionEvalRetainedEntriesName,
                LatticeMetrics.WalCompactionEvalDeadEntriesName,
            }
            from shard in new[] { 0, 1 }
            select (name, shard);

        Assert.That(
            armed,
            Is.EquivalentTo(expected),
            "Every gate input must be armed for every shard that loads, or an absent series is ambiguous.");
    }

    private static bool MatchesTree(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        foreach (var tag in tags)
        {
            if (string.Equals(tag.Key, LatticeMetrics.TagTree, StringComparison.Ordinal))
            {
                return string.Equals(tag.Value as string, TreeId, StringComparison.Ordinal);
            }
        }

        return false;
    }

    private static string? TriggerOf(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        foreach (var tag in tags)
        {
            if (string.Equals(tag.Key, LatticeMetrics.TagTrigger, StringComparison.Ordinal))
            {
                return tag.Value as string;
            }
        }

        return null;
    }

    private static int? ShardOf(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        foreach (var tag in tags)
        {
            if (string.Equals(tag.Key, LatticeMetrics.TagShard, StringComparison.Ordinal))
            {
                return tag.Value as int?;
            }
        }

        return null;
    }

    private static async Task AppendAsync(FileWalStorageProvider sut, int count)
        => await AppendAsync(sut, count, shard: 0);

    private static async Task AppendAsync(FileWalStorageProvider sut, int count, int shard)
    {
        for (var i = 0; i < count; i++)
        {
            await sut.AppendBatchAsync(TreeId, shard, new[] { Entry(i) }, CancellationToken.None);
        }
    }

    private static WalEntry Entry(long offset)
    {
        var value = new byte[PayloadBytes];
        value.AsSpan().Fill((byte)(offset & 0xFF));
        return new WalEntry
        {
            Offset = offset,
            Mutation = new LatticeMutation
            {
                TreeId = TreeId,
                Kind = MutationKind.Set,
                Key = "k" + offset.ToString(System.Globalization.CultureInfo.InvariantCulture),
                Value = value,
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                OriginClusterId = "site-a",
            },
        };
    }

    private FileWalStorageProvider CreateProvider(
        int compactionMinimumDeadBytes = FileWalStorageOptions.DefaultCompactionMinimumDeadBytes,
        long compactionMaximumDeadBytes = FileWalStorageOptions.DefaultCompactionMaximumDeadBytes)
    {
        var options = Options.Create(new FileWalStorageOptions
        {
            RootDirectory = _root,
            FlushToDisk = false,
            CompactionMinimumDeadBytes = compactionMinimumDeadBytes,
            CompactionMaximumDeadBytes = compactionMaximumDeadBytes,
        });
        return new FileWalStorageProvider(options, _serializer);
    }
}
