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

    private static async Task AppendAsync(FileWalStorageProvider sut, int count)
    {
        for (var i = 0; i < count; i++)
        {
            await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(i) }, CancellationToken.None);
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
