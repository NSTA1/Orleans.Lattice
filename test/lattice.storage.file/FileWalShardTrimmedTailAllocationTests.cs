namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// Regression tests for issue #3366: entries appended to a fully-trimmed shard
/// were silently destroyed by the next recovery.
/// <para>
/// <see cref="FileWalShard.GetHighestOffsetAsync"/> answered from the live
/// entry list, which <c>RecoverFromDisk</c> populates with only the entries
/// above the durable trim watermark. A shard trimmed through offset <c>N</c>
/// therefore reported <c>-1</c>, the WAL grain set
/// <c>_nextOffset = -1 + 1 = 0</c>, and every subsequent append landed at an
/// offset at or below <c>N</c>. Those appends committed, acknowledged, and read
/// back normally for the life of the process; the next recovery then classified
/// them as already-trimmed and discarded them, incrementing no counter. The
/// watermark is durable across both recovery and compaction, so the loss
/// repeated on every restart.
/// </para>
/// <para>
/// The fixture drives the real file-backed write, trim, and recovery paths and
/// reopens the shard on the same directory to model a process restart, so it
/// reproduces the destruction rather than asserting the getter in isolation.
/// </para>
/// </summary>
[TestFixture]
public sealed class FileWalShardTrimmedTailAllocationTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(
            Path.GetTempPath(),
            "lattice-file-wal-trimmed-tail-tests",
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

    private FileWalShard CreateShard()
    {
        var options = new FileWalStorageOptions
        {
            RootDirectory = _root,
            FlushToDisk = false,
        };
        return new FileWalShard(ShardDirectory, options);
    }

    private static PreparedWalRecord Record(long offset, byte fill) =>
        new(offset, new[] { fill });

    /// <summary>
    /// Models the WAL grain's activation arithmetic
    /// (<c>_nextOffset = GetHighestOffsetAsync() + 1</c>).
    /// </summary>
    private static async Task<long> NextOffsetAsync(FileWalShard shard) =>
        await shard.GetHighestOffsetAsync(CancellationToken.None) + 1;

    [Test]
    public async Task GetHighestOffsetAsync_reports_the_trim_watermark_when_every_entry_is_trimmed()
    {
        using var shard = CreateShard();
        await shard.AppendAsync(
            new[] { Record(0, 0xA0), Record(1, 0xA1), Record(2, 0xA2) },
            CancellationToken.None);

        await shard.TrimAsync(2, CancellationToken.None);

        var highest = await shard.GetHighestOffsetAsync(CancellationToken.None);

        Assert.That(
            highest,
            Is.EqualTo(2L),
            "A fully-trimmed shard must still report its watermark so the next "
            + "allocated offset is 3. Reporting -1 restarts allocation at 0, "
            + "beneath the durable trim floor.");
    }

    [Test]
    public async Task GetHighestOffsetAsync_reports_the_watermark_after_a_restart()
    {
        using (var shard = CreateShard())
        {
            await shard.AppendAsync(
                new[] { Record(0, 0xB0), Record(1, 0xB1) },
                CancellationToken.None);
            await shard.TrimAsync(1, CancellationToken.None);
        }

        using var reopened = CreateShard();

        Assert.That(
            await reopened.GetHighestOffsetAsync(CancellationToken.None),
            Is.EqualTo(1L),
            "The trim watermark is restored by RecoverFromDisk, so the reopened "
            + "shard must answer from it rather than from the empty live list.");
    }

    [Test]
    public async Task An_entry_appended_after_a_full_trim_survives_a_later_recovery()
    {
        // 1. Write and fully trim, so the live list is empty and the durable
        //    watermark sits at offset 2.
        using (var shard = CreateShard())
        {
            await shard.AppendAsync(
                new[] { Record(0, 0xC0), Record(1, 0xC1), Record(2, 0xC2) },
                CancellationToken.None);
            await shard.TrimAsync(2, CancellationToken.None);
        }

        // 2. Restart and append exactly as the WAL grain would, allocating from
        //    the recovered tail.
        long appendedAt;
        using (var reopened = CreateShard())
        {
            appendedAt = await NextOffsetAsync(reopened);
            await reopened.AppendAsync(
                new[] { Record(appendedAt, 0xD0) },
                CancellationToken.None);

            var (liveOffsets, _) = await reopened.SnapshotAsync(
                -1L, int.MaxValue, long.MaxValue, CancellationToken.None);
            Assert.That(
                liveOffsets,
                Is.EqualTo(new[] { appendedAt }),
                "The append must be readable in the writing process - the defect "
                + "is invisible until the NEXT recovery, which is what made it "
                + "present as unexplained memory loss.");
        }

        // 3. Restart again. Before the fix the entry was born at offset 0,
        //    satisfied Offset <= watermark, and was discarded here silently.
        using var afterRecovery = CreateShard();
        var (survivingOffsets, survivingPayloads) = await afterRecovery.SnapshotAsync(
            -1L, int.MaxValue, long.MaxValue, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                appendedAt,
                Is.EqualTo(3L),
                "Allocation must resume strictly above the trim watermark.");
            Assert.That(
                survivingOffsets,
                Is.EqualTo(new[] { 3L }),
                "The acknowledged append must survive recovery. An empty result "
                + "is issue #3366: the entry was classified as already-trimmed "
                + "and discarded without incrementing any counter.");
            Assert.That(
                survivingPayloads[0],
                Is.EqualTo(new byte[] { 0xD0 }),
                "The surviving entry must be the one that was written.");
        });
    }

    [Test]
    public async Task GetHighestOffsetAsync_prefers_the_live_tail_when_it_exceeds_the_watermark()
    {
        using var shard = CreateShard();
        await shard.AppendAsync(
            new[] { Record(0, 0xE0), Record(1, 0xE1), Record(2, 0xE2) },
            CancellationToken.None);

        await shard.TrimAsync(0, CancellationToken.None);

        Assert.That(
            await shard.GetHighestOffsetAsync(CancellationToken.None),
            Is.EqualTo(2L),
            "A partial trim must not drag the reported tail down to the "
            + "watermark; the live tail is still the high-water mark.");
    }

    [Test]
    public async Task GetHighestOffsetAsync_reports_minus_one_for_a_shard_that_never_accepted_an_entry()
    {
        using var shard = CreateShard();

        Assert.That(
            await shard.GetHighestOffsetAsync(CancellationToken.None),
            Is.EqualTo(-1L),
            "The empty-shard sentinel is unchanged: -1 means 'never written', "
            + "so the first allocated offset is still 0.");
    }
}
