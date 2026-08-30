using System.Buffers.Binary;
using System.IO.Hashing;

namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// Unit tests for the crash-recovery scanner in <see cref="FileWalShard"/> and
/// the framing invariants it enforces on a torn or corrupt segment file. Each
/// test hand-writes a segment file at the byte level, so a specific corruption
/// shape - a negative or overflowing body length, a CRC that does not match, a
/// commit trailer that seals the wrong number of data records, an unknown record
/// tag, a data body too short to hold its own offset prefix, or a fragment
/// shorter than a record header - is exercised deterministically rather than
/// hoped for.
/// <para>
/// The contract under test is uniform: every one of these shapes must be treated
/// as a crash tail and discarded, the file truncated back to the last durable
/// boundary, and the records committed before that boundary preserved verbatim.
/// Recovery must never throw and must never surface a partially-written batch.
/// </para>
/// </summary>
[TestFixture]
public sealed class FileWalShardRecoveryTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(
            Path.GetTempPath(),
            "lattice-file-wal-recovery-tests",
            Guid.NewGuid().ToString("N"));
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

    private string ShardDirectory => Path.Combine(_root, "shard");

    private string LogPath => Path.Combine(ShardDirectory, "wal.log");

    private FileWalShard CreateShard(
        double compactionThreshold = FileWalStorageOptions.DefaultCompactionThreshold,
        int compactionMinimumDeadBytes = FileWalStorageOptions.DefaultCompactionMinimumDeadBytes)
    {
        var options = new FileWalStorageOptions
        {
            RootDirectory = _root,
            FlushToDisk = false,
            CompactionThreshold = compactionThreshold,
            CompactionMinimumDeadBytes = compactionMinimumDeadBytes,
        };
        return new FileWalShard(ShardDirectory, options);
    }

    private static PreparedWalRecord Record(long offset, byte[] payload) =>
        new(offset, payload);

    /// <summary>
    /// Appends one committed batch through the production write path, then hands
    /// back the byte length of the resulting file - the last durable boundary
    /// that recovery must truncate a corrupt tail back to.
    /// </summary>
    private async Task<long> WriteOneCommittedBatchAsync(params byte[][] payloads)
    {
        using var shard = CreateShard();
        var records = new PreparedWalRecord[payloads.Length];
        for (var i = 0; i < payloads.Length; i++)
        {
            records[i] = Record(i, payloads[i]);
        }

        await shard.AppendAsync(records, CancellationToken.None);
        return new FileInfo(LogPath).Length;
    }

    private static void AppendRawBytes(string path, ReadOnlySpan<byte> bytes)
    {
        using var stream = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.None);
        stream.Write(bytes);
    }

    /// <summary>
    /// Frames a record exactly as <see cref="FileWalRecordFormat"/> does, but
    /// with a caller-chosen body-length field, so a test can write a length that
    /// the production writer would never emit.
    /// </summary>
    private static byte[] FrameWithBodyLength(byte type, int declaredBodyLength, ReadOnlySpan<byte> body)
    {
        var buffer = new byte[5 + body.Length + 4];
        buffer[0] = type;
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(1, 4), declaredBodyLength);
        body.CopyTo(buffer.AsSpan(5, body.Length));
        var crc = Crc32.HashToUInt32(buffer.AsSpan(0, 5 + body.Length));
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(5 + body.Length, 4), crc);
        return buffer;
    }

    private static byte[] Payload(int length, byte fill)
    {
        var payload = new byte[length];
        Array.Fill(payload, fill);
        return payload;
    }

    private async Task<long[]> RecoveredOffsetsAsync()
    {
        using var shard = CreateShard();
        var (offsets, _) = await shard.SnapshotAsync(-1L, int.MaxValue, CancellationToken.None);
        return offsets;
    }

    [Test]
    public async Task AppendAsync_with_an_empty_batch_does_not_create_a_segment_file()
    {
        using var shard = CreateShard();

        await shard.AppendAsync(Array.Empty<PreparedWalRecord>(), CancellationToken.None);

        Assert.That(
            System.IO.File.Exists(LogPath),
            Is.False,
            "An empty batch must short-circuit before the shard is even loaded.");
    }

    [Test]
    public async Task A_negative_body_length_in_the_tail_is_discarded_without_throwing()
    {
        var durableLength = await WriteOneCommittedBatchAsync(Payload(8, 0xAA));
        AppendRawBytes(LogPath, FrameWithBodyLength(FileWalRecordFormat.RecordTypeData, -1, Payload(8, 0xBB)));

        var offsets = await RecoveredOffsetsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Is.EqualTo(new[] { 0L }), "The committed batch must survive.");
            Assert.That(
                new FileInfo(LogPath).Length,
                Is.EqualTo(durableLength),
                "A negative body length is a torn tail and must be truncated away.");
        });
    }

    [Test]
    public async Task A_body_length_that_would_overflow_the_framing_arithmetic_is_discarded()
    {
        // int.MaxValue - FramingOverhead + 1 is the smallest length whose
        // FramingOverhead + bodyLen would wrap negative in 32-bit arithmetic.
        // It must be rejected by the 64-bit guard rather than used to size a
        // buffer.
        var durableLength = await WriteOneCommittedBatchAsync(Payload(8, 0xAA));
        AppendRawBytes(
            LogPath,
            FrameWithBodyLength(
                FileWalRecordFormat.RecordTypeData,
                int.MaxValue - FileWalRecordFormat.FramingOverhead + 1,
                Payload(8, 0xBB)));

        var offsets = await RecoveredOffsetsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Is.EqualTo(new[] { 0L }));
            Assert.That(new FileInfo(LogPath).Length, Is.EqualTo(durableLength));
        });
    }

    [Test]
    public async Task A_body_length_past_the_end_of_the_file_is_discarded()
    {
        var durableLength = await WriteOneCommittedBatchAsync(Payload(8, 0xAA));
        AppendRawBytes(
            LogPath,
            FrameWithBodyLength(FileWalRecordFormat.RecordTypeData, 4096, Payload(8, 0xBB)));

        var offsets = await RecoveredOffsetsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Is.EqualTo(new[] { 0L }));
            Assert.That(new FileInfo(LogPath).Length, Is.EqualTo(durableLength));
        });
    }

    [Test]
    public async Task A_record_whose_crc_does_not_match_its_body_is_discarded()
    {
        var durableLength = await WriteOneCommittedBatchAsync(Payload(8, 0xAA));

        // A structurally valid trim record whose trailing CRC has been flipped.
        var corrupt = new byte[FileWalRecordFormat.TrimRecordLength];
        FileWalRecordFormat.WriteTrimRecord(corrupt, 0L);
        corrupt[^1] ^= 0xFF;
        AppendRawBytes(LogPath, corrupt);

        var offsets = await RecoveredOffsetsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                offsets,
                Is.EqualTo(new[] { 0L }),
                "A trim record that fails its CRC must not be applied.");
            Assert.That(new FileInfo(LogPath).Length, Is.EqualTo(durableLength));
        });
    }

    [Test]
    public async Task A_commit_that_seals_the_wrong_number_of_records_rolls_the_run_back()
    {
        var durableLength = await WriteOneCommittedBatchAsync(Payload(8, 0xAA));

        // One further data record, then a commit claiming to seal three. The
        // count does not match the pending run, so everything from the start of
        // the run is treated as a crash tail.
        var dataBody = new byte[FileWalRecordFormat.DataBodyPrefix + 4];
        BinaryPrimitives.WriteInt64LittleEndian(dataBody.AsSpan(0, 8), 1L);
        AppendRawBytes(
            LogPath,
            FrameWithBodyLength(FileWalRecordFormat.RecordTypeData, dataBody.Length, dataBody));

        var commit = new byte[FileWalRecordFormat.CommitRecordLength];
        FileWalRecordFormat.WriteCommitRecord(commit, 3);
        AppendRawBytes(LogPath, commit);

        var offsets = await RecoveredOffsetsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                offsets,
                Is.EqualTo(new[] { 0L }),
                "A mismatched commit count must not seal the pending run.");
            Assert.That(new FileInfo(LogPath).Length, Is.EqualTo(durableLength));
        });
    }

    [Test]
    public async Task An_unknown_record_type_stops_recovery_at_the_last_durable_boundary()
    {
        var durableLength = await WriteOneCommittedBatchAsync(Payload(8, 0xAA));

        // Type tag 9 is not one of Data/Commit/Trim. It is well-framed and its
        // CRC is valid, so the scanner accepts the frame but the recovery switch
        // must refuse to interpret it.
        AppendRawBytes(LogPath, FrameWithBodyLength(9, 4, new byte[] { 1, 2, 3, 4 }));

        var offsets = await RecoveredOffsetsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Is.EqualTo(new[] { 0L }));
            Assert.That(
                new FileInfo(LogPath).Length,
                Is.EqualTo(durableLength),
                "An unrecognised record tag must be treated as a crash tail.");
        });
    }

    [Test]
    public async Task A_data_body_too_short_to_hold_its_offset_prefix_is_discarded()
    {
        var durableLength = await WriteOneCommittedBatchAsync(Payload(8, 0xAA));

        // A Data body of 4 bytes cannot hold the 8-byte offset prefix, so the
        // derived payload length is negative and the record is corrupt.
        AppendRawBytes(
            LogPath,
            FrameWithBodyLength(FileWalRecordFormat.RecordTypeData, 4, new byte[] { 1, 2, 3, 4 }));

        var offsets = await RecoveredOffsetsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Is.EqualTo(new[] { 0L }));
            Assert.That(new FileInfo(LogPath).Length, Is.EqualTo(durableLength));
        });
    }

    [Test]
    public async Task A_trailing_fragment_shorter_than_a_record_header_is_discarded()
    {
        var durableLength = await WriteOneCommittedBatchAsync(Payload(8, 0xAA));

        // Fewer than the 5 bytes needed for a type tag plus body-length prefix.
        AppendRawBytes(LogPath, new byte[] { 1, 0, 0 });

        var offsets = await RecoveredOffsetsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Is.EqualTo(new[] { 0L }));
            Assert.That(new FileInfo(LogPath).Length, Is.EqualTo(durableLength));
        });
    }

    [Test]
    public async Task A_valid_trim_record_in_the_tail_is_applied_on_recovery()
    {
        // The positive control for the corruption tests above: an intact,
        // self-committing trim marker written after a committed batch must be
        // honoured, proving the scanner accepts what it should.
        await WriteOneCommittedBatchAsync(Payload(8, 0xAA), Payload(8, 0xBB));

        var trim = new byte[FileWalRecordFormat.TrimRecordLength];
        FileWalRecordFormat.WriteTrimRecord(trim, 0L);
        AppendRawBytes(LogPath, trim);

        var offsets = await RecoveredOffsetsAsync();

        Assert.That(
            offsets,
            Is.EqualTo(new[] { 1L }),
            "Offset 0 was trimmed by the durable marker and must not be replayed.");
    }

    [Test]
    public async Task Recovery_of_an_empty_segment_file_yields_no_entries()
    {
        System.IO.Directory.CreateDirectory(ShardDirectory);
        await System.IO.File.WriteAllBytesAsync(LogPath, Array.Empty<byte>());

        using var shard = CreateShard();

        Assert.Multiple(async () =>
        {
            Assert.That(await shard.GetHighestOffsetAsync(CancellationToken.None), Is.EqualTo(-1L));
            Assert.That(await shard.GetLowestOffsetAsync(CancellationToken.None), Is.EqualTo(-1L));
        });
    }

    [Test]
    public async Task ReconcileAsync_compacts_the_dead_bytes_a_trim_left_behind()
    {
        // A trim below the compaction minimum leaves dead bytes on disk that the
        // trim path deliberately does not reclaim. Activation-time reconcile is
        // the seam that must reclaim them.
        using (var writer = CreateShard())
        {
            await writer.AppendAsync(
                new[] { Record(0, Payload(4096, 0xAA)), Record(1, Payload(4096, 0xBB)) },
                CancellationToken.None);
            await writer.TrimAsync(0L, CancellationToken.None);
        }

        var beforeLength = new FileInfo(LogPath).Length;

        using var shard = CreateShard();
        await shard.ReconcileAsync(CancellationToken.None);
        var afterLength = new FileInfo(LogPath).Length;

        var (offsets, payloads) = await shard.SnapshotAsync(-1L, int.MaxValue, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(afterLength, Is.LessThan(beforeLength), "Reconcile must reclaim the trimmed bytes.");
            Assert.That(offsets, Is.EqualTo(new[] { 1L }));
            Assert.That(payloads[0], Is.EqualTo(Payload(4096, 0xBB)), "The survivor must round-trip verbatim.");
        });
    }

    [Test]
    public async Task ReconcileAsync_leaves_a_shard_with_no_dead_bytes_untouched()
    {
        using (var writer = CreateShard())
        {
            await writer.AppendAsync(new[] { Record(0, Payload(64, 0xAA)) }, CancellationToken.None);
        }

        var beforeLength = new FileInfo(LogPath).Length;

        using var shard = CreateShard();
        await shard.ReconcileAsync(CancellationToken.None);

        Assert.That(new FileInfo(LogPath).Length, Is.EqualTo(beforeLength));
    }

    [Test]
    public async Task Compaction_rewrites_a_payload_larger_than_the_default_scratch_buffer()
    {
        // The compaction scratch buffer is rented at 64 KiB. A payload larger
        // than that forces the grow-and-re-rent path, which must still produce a
        // byte-exact survivor.
        var large = Payload((64 * 1024) + 4096, 0xCD);

        using (var writer = CreateShard())
        {
            await writer.AppendAsync(
                new[] { Record(0, Payload(128 * 1024, 0xAA)), Record(1, large) },
                CancellationToken.None);
            await writer.TrimAsync(0L, CancellationToken.None);
        }

        using var shard = CreateShard();
        var (offsets, payloads) = await shard.SnapshotAsync(-1L, int.MaxValue, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Is.EqualTo(new[] { 1L }));
            Assert.That(payloads[0], Is.EqualTo(large));
        });
    }

    [Test]
    public async Task Trim_below_the_minimum_dead_bytes_does_not_compact()
    {
        using var shard = CreateShard(compactionMinimumDeadBytes: 1024 * 1024);
        await shard.AppendAsync(
            new[] { Record(0, Payload(4096, 0xAA)), Record(1, Payload(4096, 0xBB)) },
            CancellationToken.None);
        var beforeLength = new FileInfo(LogPath).Length;

        await shard.TrimAsync(0L, CancellationToken.None);

        Assert.That(
            new FileInfo(LogPath).Length,
            Is.GreaterThan(beforeLength),
            "Below the dead-byte minimum the file only grows by the trim marker.");
    }

    [Test]
    public async Task Trim_below_the_dead_ratio_threshold_does_not_compact()
    {
        // Dead bytes clear the minimum, but the dead fraction (4 KiB of 132 KiB)
        // stays under the configured ratio, so compaction must not run.
        using var shard = CreateShard(compactionThreshold: 0.9d, compactionMinimumDeadBytes: 1024);
        await shard.AppendAsync(
            new[] { Record(0, Payload(4096, 0xAA)), Record(1, Payload(128 * 1024, 0xBB)) },
            CancellationToken.None);
        var beforeLength = new FileInfo(LogPath).Length;

        await shard.TrimAsync(0L, CancellationToken.None);

        Assert.That(new FileInfo(LogPath).Length, Is.GreaterThan(beforeLength));
    }

    [Test]
    public async Task Trim_on_an_empty_shard_records_the_watermark_without_compacting()
    {
        // Exercises the zero-payload guard: dead bytes clear the configured
        // minimum of zero, but there is no payload at all to compact.
        using var shard = CreateShard(compactionMinimumDeadBytes: 0);

        await shard.TrimAsync(5L, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await shard.GetHighestOffsetAsync(CancellationToken.None), Is.EqualTo(-1L));
            Assert.That(await shard.GetRetainedByteSizeAsync(CancellationToken.None), Is.Zero);
        });
    }

    [Test]
    public async Task Dispose_is_idempotent()
    {
        var shard = CreateShard();
        await shard.AppendAsync(new[] { Record(0, Payload(8, 0xAA)) }, CancellationToken.None);

        shard.Dispose();

        Assert.DoesNotThrow(shard.Dispose, "A second dispose must short-circuit.");
    }
}
