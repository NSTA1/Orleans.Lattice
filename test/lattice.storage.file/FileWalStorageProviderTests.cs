using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Buffers.Binary;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// Unit tests proving the durable, observable guarantees of
/// <see cref="FileWalStorageProvider"/> match the WAL storage contract:
/// atomic all-or-nothing batch append (including under a simulated
/// mid-write crash), a monotonic durable tail, verbatim/dense offsets with
/// overlap rejection, out-of-order concurrent append with an honest gap on
/// a failed flush, crash-recovery roll-forward via
/// <see cref="FileWalStorageProvider.ReconcileAsync"/>, and trim /
/// retained-byte accounting. All tests are deterministic - no timing,
/// ordering, <c>Task.Delay</c>, wall-clock, or GC dependence.
/// </summary>
[TestFixture]
public sealed class FileWalStorageProviderTests
{
    private const string TreeId = "tree-alpha";

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private OrleansBinaryWalRecordEncoder _encoder = null!;
    private string _root = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
        _encoder = new OrleansBinaryWalRecordEncoder(_serializer);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "lattice-file-wal-tests", Guid.NewGuid().ToString("N"));
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
            // Best-effort cleanup; a leaked temp directory does not fail the test.
        }
    }

    private FileWalStorageProvider CreateProvider(bool flushToDisk = true)
    {
        var options = Options.Create(new FileWalStorageOptions
        {
            RootDirectory = _root,
            FlushToDisk = flushToDisk,
        });
        return new FileWalStorageProvider(options, _serializer);
    }

    private static WalEntry Entry(long offset, string key = "k", byte tag = 1) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.Set,
            Key = key,
            Value = new byte[] { tag },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        },
    };

    private static async Task<List<WalEntry>> ReadAllAsync(
        FileWalStorageProvider sut,
        string tree,
        int shard,
        long fromOffsetExclusive = -1L,
        int maxEntries = 1024)
    {
        var collected = new List<WalEntry>();
        await foreach (var entry in sut.ReadAsync(tree, shard, fromOffsetExclusive, maxEntries, CancellationToken.None))
        {
            collected.Add(entry);
        }

        return collected;
    }

    // --- append / read round-trip ----------------------------------------

    [Test]
    public async Task AppendBatchAsync_round_trips_every_mutation_field()
    {
        using var sut = CreateProvider();
        var hlc = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var mutation = new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.Set,
            Key = "users/42",
            Value = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF },
            Timestamp = hlc,
            IsTombstone = false,
            ExpiresAtTicks = 1_700_000_000_000L,
            OriginClusterId = "site-b",
        };

        await sut.AppendBatchAsync(TreeId, 0, new[] { new WalEntry { Offset = 0L, Mutation = mutation } }, CancellationToken.None);
        var read = await ReadAllAsync(sut, TreeId, 0);

        Assert.That(read, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(read[0].Offset, Is.EqualTo(0L));
            Assert.That(read[0].Mutation.TreeId, Is.EqualTo(TreeId));
            Assert.That(read[0].Mutation.Kind, Is.EqualTo(MutationKind.Set));
            Assert.That(read[0].Mutation.Key, Is.EqualTo("users/42"));
            Assert.That(read[0].Mutation.Value, Is.EqualTo(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }));
            Assert.That(read[0].Mutation.Timestamp, Is.EqualTo(hlc));
            Assert.That(read[0].Mutation.ExpiresAtTicks, Is.EqualTo(1_700_000_000_000L));
            Assert.That(read[0].Mutation.OriginClusterId, Is.EqualTo("site-b"));
        });
    }

    [Test]
    public async Task ReadAsync_honours_from_offset_exclusive_and_max_entries()
    {
        using var sut = CreateProvider();
        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2), Entry(3) }, CancellationToken.None);

        var afterOne = await ReadAllAsync(sut, TreeId, 0, fromOffsetExclusive: 1L);
        Assert.That(afterOne.Select(e => e.Offset), Is.EqualTo(new[] { 2L, 3L }));

        var limited = await ReadAllAsync(sut, TreeId, 0, fromOffsetExclusive: -1L, maxEntries: 2);
        Assert.That(limited.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L }));
    }

    [Test]
    public async Task ReadAsync_returns_empty_for_an_unknown_shard()
    {
        using var sut = CreateProvider();
        var read = await ReadAllAsync(sut, TreeId, 7);
        Assert.That(read, Is.Empty);
    }

    // --- monotonic tail / highest / lowest -------------------------------

    [Test]
    public async Task GetHighestOffsetAsync_returns_minus_one_for_empty_shard()
    {
        using var sut = CreateProvider();
        var head = await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.That(head, Is.EqualTo(-1L));
    }

    [Test]
    public async Task GetLowestOffsetAsync_returns_minus_one_for_empty_shard()
    {
        using var sut = CreateProvider();
        var low = await sut.GetLowestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.That(low, Is.EqualTo(-1L));
    }

    [Test]
    public async Task AppendBatchAsync_advances_the_head_monotonically()
    {
        using var sut = CreateProvider();
        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);
        Assert.That(await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(1L));

        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(2), Entry(3) }, CancellationToken.None);
        Assert.That(await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(3L));
        Assert.That(await sut.GetLowestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(0L));
    }

    [Test]
    public async Task GetHighestOffsetAsync_recovers_persisted_head_on_a_fresh_provider()
    {
        using (var first = CreateProvider())
        {
            await first.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2) }, CancellationToken.None);
        }

        using var second = CreateProvider();
        var head = await second.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        var read = await ReadAllAsync(second, TreeId, 0);

        Assert.That(head, Is.EqualTo(2L));
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L }));
    }

    // --- dense-within-batch / overlap rejection --------------------------

    [Test]
    public void AppendBatchAsync_rejects_a_gap_within_the_batch()
    {
        using var sut = CreateProvider();
        var batch = new[] { Entry(0), Entry(2) };

        Assert.That(
            async () => await sut.AppendBatchAsync(TreeId, 0, batch, CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task AppendBatchAsync_rejects_a_batch_overlapping_persisted_offsets()
    {
        using var sut = CreateProvider();
        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2) }, CancellationToken.None);

        Assert.That(
            async () => await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(2), Entry(3) }, CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());

        // The rejected batch left no partial state - head is unchanged.
        Assert.That(await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(2L));
        var read = await ReadAllAsync(sut, TreeId, 0);
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L }));
    }

    // --- out-of-order concurrent append with honest gap ------------------

    [Test]
    public async Task AppendBatchAsync_accepts_out_of_order_batches_and_reports_the_sorted_head()
    {
        using var sut = CreateProvider();

        // A higher batch arrives before a lower one (WalMaxPendingBatches > 1).
        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(2), Entry(3) }, CancellationToken.None);
        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);

        var read = await ReadAllAsync(sut, TreeId, 0);
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L, 3L }));
        Assert.That(await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(3L));
    }

    [Test]
    public async Task A_missing_middle_batch_surfaces_as_an_honest_gap()
    {
        using var sut = CreateProvider();

        // Batches [0,1] and [4,5] persist; the middle batch [2,3] "failed
        // its flush" (never appended). The gap is surfaced honestly rather
        // than silently reordered or back-filled.
        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);
        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(4), Entry(5) }, CancellationToken.None);

        var read = await ReadAllAsync(sut, TreeId, 0);
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 4L, 5L }));
        Assert.That(await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(5L));
    }

    // --- atomic all-or-nothing under a simulated mid-write crash ---------

    [Test]
    public async Task A_torn_commit_trailer_rolls_the_whole_batch_back_on_recovery()
    {
        long logLengthAfterFirst;
        using (var first = CreateProvider())
        {
            await first.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);
            logLengthAfterFirst = new FileInfo(LogPath(TreeId, 0)).Length;

            // Append a second batch, then simulate a crash mid-write by
            // stripping the commit trailer so the data records for the
            // second batch are orphaned.
            await first.AppendBatchAsync(TreeId, 0, new[] { Entry(2), Entry(3) }, CancellationToken.None);
        }

        // Truncate away the commit trailer of the second batch (its data
        // records remain on disk but are uncommitted).
        TruncateLog(TreeId, 0, byNBytes: FileWalRecordFormat.CommitRecordLength);

        using var recovered = CreateProvider();
        await recovered.ReconcileAsync(TreeId, 0, CancellationToken.None);

        var read = await ReadAllAsync(recovered, TreeId, 0);
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L }),
            "the uncommitted second batch must roll back entirely - no partial entry survives");
        Assert.That(await recovered.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(1L));

        // The torn tail was truncated back to the first batch's durable end.
        Assert.That(new FileInfo(LogPath(TreeId, 0)).Length, Is.EqualTo(logLengthAfterFirst));
    }

    [Test]
    public async Task A_corrupt_oversized_length_field_in_the_tail_is_rolled_back_without_throwing()
    {
        // Regression: a torn tail whose 32-bit body-length field decodes to a
        // near-int.MaxValue garbage value must be discarded as a torn tail. A
        // naive FramingOverhead + bodyLen sum overflows to a negative record
        // length, slips past the bounds check, and then throws inside
        // ArrayPool.Rent - hard-failing recovery instead of rolling the tail
        // back to the last durable boundary.
        long logLengthAfterFirst;
        using (var first = CreateProvider())
        {
            await first.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);
            logLengthAfterFirst = new FileInfo(LogPath(TreeId, 0)).Length;
        }

        // Append a raw torn record: a data type byte, an int32 body-length of
        // int.MaxValue, then a few filler bytes far short of that length.
        var tail = new byte[FileWalRecordFormat.FramingOverhead];
        tail[0] = FileWalRecordFormat.RecordTypeData;
        BinaryPrimitives.WriteInt32LittleEndian(tail.AsSpan(1, 4), int.MaxValue);
        AppendRaw(TreeId, 0, tail);

        using var recovered = CreateProvider();
        Assert.DoesNotThrowAsync(() => recovered.ReconcileAsync(TreeId, 0, CancellationToken.None),
            "an oversized garbage length in the tail must be treated as a torn tail, not throw");

        var read = await ReadAllAsync(recovered, TreeId, 0);
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L }),
            "the committed batch must survive the corrupt tail");
        Assert.That(new FileInfo(LogPath(TreeId, 0)).Length, Is.EqualTo(logLengthAfterFirst),
            "the corrupt tail must be truncated back to the last durable boundary");
    }

    [Test]
    public async Task A_batch_whose_data_records_are_partially_written_rolls_back()
    {
        using (var first = CreateProvider())
        {
            await first.AppendBatchAsync(TreeId, 0, new[] { Entry(0) }, CancellationToken.None);
            await first.AppendBatchAsync(TreeId, 0, new[] { Entry(1), Entry(2) }, CancellationToken.None);
        }

        // Strip the commit trailer plus part of the trailing data record of
        // the second batch: the recovery scan sees a torn record and rolls
        // the whole batch back.
        TruncateLog(TreeId, 0, byNBytes: FileWalRecordFormat.CommitRecordLength + 4);

        using var recovered = CreateProvider();
        var read = await ReadAllAsync(recovered, TreeId, 0);
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L }));
    }

    [Test]
    public async Task Recovery_reads_committed_batches_without_an_explicit_reconcile()
    {
        using (var first = CreateProvider())
        {
            await first.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);
        }

        // A fresh provider must roll forward committed batches lazily on the
        // first read, even without an explicit ReconcileAsync call.
        using var second = CreateProvider();
        var read = await ReadAllAsync(second, TreeId, 0);
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L }));
    }

    // --- crash-recovery roll-forward preserves fidelity ------------------

    [Test]
    public async Task ReconcileAsync_rolls_committed_batches_forward_with_full_fidelity()
    {
        using (var first = CreateProvider())
        {
            await first.AppendBatchAsync(TreeId, 0, new[]
            {
                Entry(0, "a", 10),
                Entry(1, "b", 20),
            }, CancellationToken.None);
            await first.AppendBatchAsync(TreeId, 0, new[]
            {
                Entry(2, "c", 30),
            }, CancellationToken.None);
        }

        using var recovered = CreateProvider();
        await recovered.ReconcileAsync(TreeId, 0, CancellationToken.None);
        var read = await ReadAllAsync(recovered, TreeId, 0);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L }));
        Assert.That(read.Select(e => e.Mutation.Key), Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(read[0].Mutation.Value, Is.EqualTo(new byte[] { 10 }));
        Assert.That(read[2].Mutation.Value, Is.EqualTo(new byte[] { 30 }));
    }

    // --- trim / retained-byte accounting ---------------------------------

    [Test]
    public async Task TrimAsync_removes_entries_at_or_below_the_watermark()
    {
        using var sut = CreateProvider();
        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2), Entry(3) }, CancellationToken.None);

        await sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 1L, CancellationToken.None);

        var read = await ReadAllAsync(sut, TreeId, 0);
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 2L, 3L }));
        Assert.That(await sut.GetLowestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(2L));
        Assert.That(await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(3L));
    }

    [Test]
    public async Task GetRetainedByteSizeAsync_tracks_live_payload_bytes_across_append_and_trim()
    {
        using var sut = CreateProvider();
        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2) }, CancellationToken.None);

        var afterAppend = await sut.GetRetainedByteSizeAsync(TreeId, 0, CancellationToken.None);
        Assert.That(afterAppend, Is.GreaterThan(0L));

        await sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 0L, CancellationToken.None);
        var afterTrim = await sut.GetRetainedByteSizeAsync(TreeId, 0, CancellationToken.None);

        Assert.That(afterTrim, Is.LessThan(afterAppend));
        Assert.That(afterTrim, Is.GreaterThan(0L));
    }

    [Test]
    public async Task TrimAsync_is_durable_across_a_provider_restart()
    {
        using (var first = CreateProvider())
        {
            await first.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2) }, CancellationToken.None);
            await first.TrimAsync(TreeId, 0, throughOffsetInclusive: 1L, CancellationToken.None);
        }

        using var second = CreateProvider();
        var read = await ReadAllAsync(second, TreeId, 0);
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 2L }));
        Assert.That(await second.GetLowestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(2L));
    }

    [Test]
    public async Task TrimAsync_with_a_negative_watermark_is_a_no_op()
    {
        using var sut = CreateProvider();
        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);

        await sut.TrimAsync(TreeId, 0, throughOffsetInclusive: -1L, CancellationToken.None);

        var read = await ReadAllAsync(sut, TreeId, 0);
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L }));
    }

    // --- compaction -------------------------------------------------------

    [Test]
    public async Task TrimAsync_compacts_reclaimable_space_and_survivors_stay_readable()
    {
        // A low compaction floor and threshold force trim-triggered
        // compaction deterministically (no timing dependence): after the
        // trim, more than half the on-disk payload is dead.
        var options = Options.Create(new FileWalStorageOptions
        {
            RootDirectory = _root,
            CompactionMinimumDeadBytes = 1,
            CompactionThreshold = 0.5,
        });
        using var sut = new FileWalStorageProvider(options, _serializer);

        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2), Entry(3) }, CancellationToken.None);
        var lengthBeforeTrim = new FileInfo(LogPath(TreeId, 0)).Length;

        await sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 2L, CancellationToken.None);

        // The segment file physically shrank (compaction rewrote it) and
        // the surviving entry is still readable and durable.
        var lengthAfterTrim = new FileInfo(LogPath(TreeId, 0)).Length;
        Assert.That(lengthAfterTrim, Is.LessThan(lengthBeforeTrim));

        var read = await ReadAllAsync(sut, TreeId, 0);
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 3L }));
    }

    [Test]
    public async Task Compaction_survives_a_provider_restart()
    {
        var options = Options.Create(new FileWalStorageOptions
        {
            RootDirectory = _root,
            CompactionMinimumDeadBytes = 1,
            CompactionThreshold = 0.5,
        });
        using (var first = new FileWalStorageProvider(options, _serializer))
        {
            await first.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2), Entry(3) }, CancellationToken.None);
            await first.TrimAsync(TreeId, 0, throughOffsetInclusive: 2L, CancellationToken.None);
        }

        using var second = new FileWalStorageProvider(options, _serializer);
        await second.ReconcileAsync(TreeId, 0, CancellationToken.None);
        var read = await ReadAllAsync(second, TreeId, 0);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 3L }));
        Assert.That(await second.GetLowestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(3L));
    }

    [Test]
    public async Task Appending_after_compaction_continues_the_offset_sequence()
    {
        var options = Options.Create(new FileWalStorageOptions
        {
            RootDirectory = _root,
            CompactionMinimumDeadBytes = 1,
            CompactionThreshold = 0.5,
        });
        using var sut = new FileWalStorageProvider(options, _serializer);

        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2), Entry(3) }, CancellationToken.None);
        await sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 2L, CancellationToken.None);

        // A subsequent append after the segment file was rewritten must
        // land at the correct file position and read back cleanly.
        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(4), Entry(5) }, CancellationToken.None);
        var read = await ReadAllAsync(sut, TreeId, 0);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 3L, 4L, 5L }));
        Assert.That(read[1].Mutation.Key, Is.EqualTo("k"));
    }

    // --- helpers ----------------------------------------------------------

    private string LogPath(string treeId, int shard) => Path.Combine(
        _root,
        FileWalStorageProvider.EncodePathSegment(treeId),
        "shard-" + shard.ToString(System.Globalization.CultureInfo.InvariantCulture),
        "wal.log");

    private void TruncateLog(string treeId, int shard, int byNBytes)
    {
        var path = LogPath(treeId, shard);
        using var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        fs.SetLength(fs.Length - byNBytes);
    }

    private void AppendRaw(string treeId, int shard, byte[] bytes)
    {
        var path = LogPath(treeId, shard);
        using var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        fs.Seek(0, SeekOrigin.End);
        fs.Write(bytes, 0, bytes.Length);
    }
}
