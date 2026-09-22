using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// Positive controls for the two WAL recovery instruments added by issue
/// #3366.
/// <para>
/// Activation-time recovery truncates every data record that no commit record
/// sealed. The truncation is correct, but before #3366 it was also silent, so
/// a shard that discarded a tail was indistinguishable on every surface from
/// one that had nothing to discard. That mattered on a live estate: agent
/// memory written before a restart was found missing afterwards, and because
/// recovery destroys the bytes it discards, no scrape, log or snapshot taken
/// after the load could say whether recovery was responsible.
/// </para>
/// <para>
/// A counter that is only ever observed at zero cannot be trusted to report a
/// non-zero, so each arm is driven into a real non-zero here and the arm it is
/// not is observed staying still. The two arms separate two distinct causes
/// that the byte figure alone conflates, so the discriminating case - bytes
/// without records - is asserted explicitly rather than assumed.
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
public sealed class FileWalRecoveryTornTailTelemetryTests
{
    private const int PayloadBytes = 512;

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private string _root = null!;
    private string _treeId = null!;

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
        // A per-test tree id keeps the tag filter below exact even when
        // fixtures share the process-wide meter.
        _treeId = "recovery-torn-tail-" + Guid.NewGuid().ToString("N");
        _root = Path.Combine(Path.GetTempPath(), "lattice-wal-recovery-telemetry", Guid.NewGuid().ToString("N"));
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

    /// <summary>
    /// The clean case must report an explicit zero, not an absence.
    /// <para>
    /// This is the reading the whole instrument exists to make possible. If a
    /// cleanly recovered shard published nothing, then "this shard discarded
    /// nothing" and "this shard is not reporting" would render identically on
    /// a scrape, and a non-zero elsewhere could never be trusted to mean the
    /// tail was really lost rather than merely observed for the first time.
    /// </para>
    /// </summary>
    [Test]
    public async Task Recovering_a_cleanly_closed_shard_primes_both_arms_at_zero_and_charges_neither()
    {
        using (var writer = CreateProvider())
        {
            await AppendAsync(writer, count: 4);
        }

        var observed = await ObserveReloadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(observed.BytesPrimed, Is.True, "An unprimed byte arm makes a clean recovery indistinguishable from an absent provider.");
            Assert.That(observed.RecordsPrimed, Is.True, "An unprimed record arm has the same ambiguity as the byte arm.");
            Assert.That(observed.Bytes, Is.Zero, "Every batch was sealed by a commit, so recovery had nothing to truncate.");
            Assert.That(observed.Records, Is.Zero, "Every batch was sealed by a commit, so no complete record went unsealed.");
        });
    }

    /// <summary>
    /// Complete records that no commit sealed are charged to both arms, and
    /// the byte figure is derived independently of the counter.
    /// <para>
    /// Removing the final byte tears the last batch's commit record, which
    /// leaves the data record ahead of it complete but unsealed. That is the
    /// serious shape: the batch reached the file intact and the commit that
    /// would have made it durable never followed, which after a drain that
    /// reported success means an acknowledged write did not survive.
    /// </para>
    /// </summary>
    [Test]
    public async Task Recovering_a_shard_whose_last_commit_is_torn_charges_the_unsealed_record_to_both_arms()
    {
        long lengthBeforeFinalBatch;
        long lengthWithFinalBatch;

        using (var writer = CreateProvider())
        {
            await AppendAsync(writer, count: 3);
            lengthBeforeFinalBatch = new FileInfo(LogPath()).Length;
            await AppendOneAsync(writer, offset: 3);
            lengthWithFinalBatch = new FileInfo(LogPath()).Length;
        }

        // Drop the final byte so the commit record that sealed the last batch
        // can no longer be read, leaving its data record complete but
        // unsealed.
        var truncatedLength = lengthWithFinalBatch - 1;
        using (var stream = new FileStream(LogPath(), FileMode.Open, FileAccess.Write))
        {
            stream.SetLength(truncatedLength);
        }

        var expectedBytes = truncatedLength - lengthBeforeFinalBatch;
        var observed = await ObserveReloadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(observed.Records, Is.EqualTo(1), "Exactly one complete data record was left unsealed by the torn commit.");
            Assert.That(
                observed.Bytes,
                Is.EqualTo(expectedBytes),
                "The byte charge must equal the distance from the last durable boundary to the end of the file, measured independently of the counter.");
        });
    }

    /// <summary>
    /// Trailing bytes that form no complete record charge the byte arm alone.
    /// <para>
    /// This is the case that justifies carrying two instruments rather than
    /// one. A process killed part-way through a single append leaves bytes
    /// that are not a record, so the record arm stays at zero while the byte
    /// arm moves. Were only bytes reported, that ordinary and benign shape
    /// would be indistinguishable from a run of complete records that were
    /// never committed, which is neither ordinary nor benign.
    /// </para>
    /// </summary>
    [Test]
    public async Task Recovering_a_shard_with_trailing_partial_bytes_charges_bytes_but_no_record()
    {
        using (var writer = CreateProvider())
        {
            await AppendAsync(writer, count: 2);
        }

        const int TrailingBytes = 64;
        using (var stream = new FileStream(LogPath(), FileMode.Open, FileAccess.Write))
        {
            stream.Seek(0, SeekOrigin.End);
            stream.Write(new byte[TrailingBytes]);
        }

        var observed = await ObserveReloadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(observed.Bytes, Is.EqualTo(TrailingBytes), "The trailing bytes form no readable record, so all of them are torn.");
            Assert.That(observed.Records, Is.Zero, "No complete data record was left unsealed, so charging the record arm would misattribute the cause.");
        });
    }

    /// <summary>
    /// Loads the shard under a listener and returns what recovery reported.
    /// The listener is started before the provider exists, so the priming
    /// writes that happen during the load are observed rather than missed.
    /// </summary>
    private async Task<RecoveryObservation> ObserveReloadAsync()
    {
        long bytes = 0;
        long records = 0;
        var bytesPrimed = false;
        var recordsPrimed = false;

        using var byteListener = MeterListening.StartForInstrument(
            LatticeMetrics.WalRecoveryTornTailBytes,
            l => l.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                if (!MatchesTree(tags))
                {
                    return;
                }

                if (measurement == 0)
                {
                    Volatile.Write(ref bytesPrimed, true);
                }
                else
                {
                    Interlocked.Add(ref bytes, measurement);
                }
            }));

        using var recordListener = MeterListening.StartForInstrument(
            LatticeMetrics.WalRecoveryTornTailRecords,
            l => l.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                if (!MatchesTree(tags))
                {
                    return;
                }

                if (measurement == 0)
                {
                    Volatile.Write(ref recordsPrimed, true);
                }
                else
                {
                    Interlocked.Add(ref records, measurement);
                }
            }));

        using (var sut = CreateProvider())
        {
            // Any operation that forces the load is enough; reconcile is used
            // because it never appends and so cannot perturb the file.
            await sut.ReconcileAsync(_treeId, 0, CancellationToken.None);
        }

        byteListener.Dispose();
        recordListener.Dispose();

        return new RecoveryObservation(
            Volatile.Read(ref bytes),
            Volatile.Read(ref records),
            Volatile.Read(ref bytesPrimed),
            Volatile.Read(ref recordsPrimed));
    }

    private string LogPath()
    {
        var files = System.IO.Directory.GetFiles(_root, "wal.log", SearchOption.AllDirectories);
        Assert.That(files, Has.Length.EqualTo(1), "The fixture writes exactly one shard, so a different count means the layout moved.");
        return files[0];
    }

    private bool MatchesTree(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        foreach (var tag in tags)
        {
            if (string.Equals(tag.Key, LatticeMetrics.TagTree, StringComparison.Ordinal))
            {
                return string.Equals(tag.Value as string, _treeId, StringComparison.Ordinal);
            }
        }

        return false;
    }

    private async Task AppendAsync(FileWalStorageProvider sut, int count)
    {
        for (var i = 0; i < count; i++)
        {
            await AppendOneAsync(sut, i);
        }
    }

    private async Task AppendOneAsync(FileWalStorageProvider sut, long offset)
        => await sut.AppendBatchAsync(_treeId, 0, new[] { Entry(offset) }, CancellationToken.None);

    private WalEntry Entry(long offset)
    {
        var value = new byte[PayloadBytes];
        value.AsSpan().Fill((byte)(offset & 0xFF));
        return new WalEntry
        {
            Offset = offset,
            Mutation = new LatticeMutation
            {
                TreeId = _treeId,
                Kind = MutationKind.Set,
                Key = "k" + offset.ToString(System.Globalization.CultureInfo.InvariantCulture),
                Value = value,
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                OriginClusterId = "site-a",
            },
        };
    }

    private FileWalStorageProvider CreateProvider()
    {
        var options = Options.Create(new FileWalStorageOptions
        {
            RootDirectory = _root,
            FlushToDisk = false,
        });
        return new FileWalStorageProvider(options, _serializer);
    }

    private readonly record struct RecoveryObservation(
        long Bytes,
        long Records,
        bool BytesPrimed,
        bool RecordsPrimed);
}
