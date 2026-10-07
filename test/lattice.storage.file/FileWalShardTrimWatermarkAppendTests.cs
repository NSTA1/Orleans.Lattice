namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// Regression tests for issue #4073: an append that reused an offset at or below
/// the durable trim watermark was acknowledged and readable for the rest of the
/// process, then discarded by the next recovery, which treats every entry at or
/// below the watermark as already trimmed.
/// <para>
/// The overlap check consulted only the live entry list, so it passed both when
/// the trim had emptied the shard and when the reused offset sat below the
/// surviving entries. Each test reopens the shard on the same directory to model
/// a restart, because the loss is invisible until the next recovery.
/// </para>
/// </summary>
[TestFixture]
public sealed class FileWalShardTrimWatermarkAppendTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(
            Path.GetTempPath(),
            "lattice-file-wal-trim-watermark-append-tests",
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

    private FileWalShard CreateShard() => new(
        ShardDirectory,
        new FileWalStorageOptions { RootDirectory = _root, FlushToDisk = false });

    private static PreparedWalRecord Record(long offset, byte fill) => new(offset, new[] { fill });

    private static async Task<long[]> LiveOffsetsAsync(FileWalShard shard)
    {
        var (offsets, _) = await shard.SnapshotAsync(-1L, int.MaxValue, long.MaxValue, CancellationToken.None);
        return offsets.ToArray();
    }

    [Test]
    public async Task A_crash_after_the_trim_marker_leaves_the_covered_records_on_disk_and_never_readable()
    {
        // Issue #4621: the trim marker is durable before the in-memory delete, so
        // a process lost in between leaves the trimmed records' bytes on disk
        // below the marker. Recovery must report the marker and drop them.
        // Compaction is held off so the trimmed bytes stay where a crash leaves them.
        FileWalShard Uncompacted() => new(
            ShardDirectory,
            new FileWalStorageOptions { RootDirectory = _root, FlushToDisk = false, CompactionMinimumDeadBytes = int.MaxValue });

        using (var shard = Uncompacted())
        {
            await shard.AppendAsync(
                new[] { Record(0, 0xA0), Record(1, 0xA1), Record(2, 0xA2) },
                CancellationToken.None);
            await shard.TrimAsync(1, CancellationToken.None);
        }

        using var reopened = Uncompacted();
        var live = await LiveOffsetsAsync(reopened);
        Assert.Multiple(async () =>
        {
            Assert.That(reopened.DeadEntries, Is.EqualTo(2), "the trimmed records are still on disk");
            Assert.That(live, Is.EqualTo(new long[] { 2 }), "and never read back");
            Assert.That(await reopened.GetTrimWatermarkAsync(CancellationToken.None), Is.EqualTo(1L));
            Assert.That(await reopened.GetLowestOffsetAsync(CancellationToken.None), Is.EqualTo(2L));
        });
    }
    [Test]
    public async Task AppendAsync_rejects_an_offset_at_or_below_the_watermark_of_an_emptied_shard()
    {
        using (var shard = CreateShard())
        {
            await shard.AppendAsync(
                new[] { Record(0, 0xA0), Record(1, 0xA1), Record(2, 0xA2) },
                CancellationToken.None);
            await shard.TrimAsync(2, CancellationToken.None);

            Assert.That(
                async () => await shard.AppendAsync(new[] { Record(1, 0xB1) }, CancellationToken.None),
                Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("trim watermark"),
                "Offset 1 was persisted and trimmed. Accepting it acknowledges a write the next "
                + "recovery discards as already trimmed.");
            Assert.That(await LiveOffsetsAsync(shard), Is.Empty, "A rejected batch must write nothing.");
        }

        using var reopened = CreateShard();
        var survivors = await LiveOffsetsAsync(reopened);
        var highest = await reopened.GetHighestOffsetAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(survivors, Is.Empty);
            Assert.That(highest, Is.EqualTo(2L));
        });
    }

    [Test]
    public async Task AppendAsync_rejects_an_offset_below_the_watermark_beneath_surviving_entries()
    {
        using (var shard = CreateShard())
        {
            await shard.AppendAsync(
                Enumerable.Range(0, 8).Select(i => Record(i, (byte)i)).ToArray(),
                CancellationToken.None);
            await shard.TrimAsync(4, CancellationToken.None);

            // The live list holds 5..7, so offset 2 does not collide with any live
            // entry - the case the live-list-only overlap check waved through.
            Assert.That(
                async () => await shard.AppendAsync(new[] { Record(2, 0xC2) }, CancellationToken.None),
                Throws.InstanceOf<InvalidOperationException>());
        }

        using var reopened = CreateShard();
        Assert.That(await LiveOffsetsAsync(reopened), Is.EqualTo(new[] { 5L, 6L, 7L }));
    }

    [Test]
    public async Task AppendAsync_rejects_a_batch_that_straddles_the_watermark()
    {
        using var shard = CreateShard();
        await shard.AppendAsync(
            new[] { Record(0, 0xD0), Record(1, 0xD1), Record(2, 0xD2) },
            CancellationToken.None);
        await shard.TrimAsync(2, CancellationToken.None);

        Assert.That(
            async () => await shard.AppendAsync(
                new[] { Record(2, 0xE2), Record(3, 0xE3) },
                CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>(),
            "The batch is all-or-nothing, so one trimmed offset rejects the whole batch.");
        Assert.That(await LiveOffsetsAsync(shard), Is.Empty);
    }

    [Test]
    public async Task AppendAsync_accepts_the_first_offset_above_the_watermark_and_it_survives_recovery()
    {
        using (var shard = CreateShard())
        {
            await shard.AppendAsync(
                new[] { Record(0, 0xF0), Record(1, 0xF1), Record(2, 0xF2) },
                CancellationToken.None);
            await shard.TrimAsync(2, CancellationToken.None);
            await shard.AppendAsync(new[] { Record(3, 0xF3) }, CancellationToken.None);
        }

        using var reopened = CreateShard();
        Assert.That(
            await LiveOffsetsAsync(reopened),
            Is.EqualTo(new[] { 3L }),
            "The guard must not reject the next offset above the watermark.");
    }
}
