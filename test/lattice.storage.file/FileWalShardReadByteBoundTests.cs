using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// Regression tests for issue #2689: the read path bounded a page by entry
/// count and never by bytes, while the write path that produced those
/// entries bounded both. A page of large records was therefore unbounded in
/// memory, and it was held twice - once materialised into <c>byte[][]</c> by
/// <c>FileWalShard.SnapshotAsync</c> and again as the deserializer
/// re-allocated each payload.
/// <para>
/// The fixture pins three distinct properties, because they fail
/// independently and have different remedies:
/// </para>
/// <list type="number">
/// <item><description><b>The bound exists and bites.</b> A page whose
/// payloads exceed <see cref="FileWalStorageOptions.MaxReadBatchBytes"/> is
/// truncated below the requested entry count, on both read
/// overloads.</description></item>
/// <item><description><b>The bound can never stall a reader.</b> A single
/// entry larger than the whole budget is still returned. Every reader on
/// this path treats an empty page as end-of-stream, so a page that returned
/// nothing here would wedge replay at that offset permanently - the exact
/// failure the bound exists to end.</description></item>
/// <item><description><b>Truncation is a resumption, not a skip.</b>
/// Draining the log the way <c>WalCommitLogReader</c> drains it - resuming
/// from the last offset actually returned - yields every entry exactly once
/// and in order, however aggressively pages are truncated.</description></item>
/// </list>
/// <para>
/// All tests are deterministic: no timing, wall-clock, or GC dependence.
/// </para>
/// </summary>
[TestFixture]
public sealed class FileWalShardReadByteBoundTests
{
    private const string TreeId = "tree-byte-bound";

    /// <summary>Payload size that makes the arithmetic legible: 4 entries per 4 KiB budget.</summary>
    private const int PayloadBytes = 1024;

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
        _root = Path.Combine(Path.GetTempPath(), "lattice-file-wal-bytebound", Guid.NewGuid().ToString("N"));
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

    private FileWalStorageProvider CreateProvider(long maxReadBatchBytes)
    {
        var options = Options.Create(new FileWalStorageOptions
        {
            RootDirectory = _root,
            FlushToDisk = false,
            MaxReadBatchBytes = maxReadBatchBytes,
        });
        return new FileWalStorageProvider(options, _serializer);
    }

    private static WalEntry Entry(long offset, int valueBytes)
    {
        // The value dominates the serialised payload, so a payload-byte
        // budget is expressed directly in terms of it.
        var value = new byte[valueBytes];
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

    private static async Task AppendAsync(FileWalStorageProvider sut, int count, int valueBytes)
    {
        for (var i = 0; i < count; i++)
        {
            await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(i, valueBytes) }, CancellationToken.None);
        }
    }

    private static async Task<List<WalEntry>> PageAsync(
        FileWalStorageProvider sut,
        long fromOffsetExclusive,
        int maxEntries)
    {
        var page = new List<WalEntry>();
        await foreach (var entry in sut.ReadAsync(TreeId, 0, fromOffsetExclusive, maxEntries, CancellationToken.None))
        {
            page.Add(entry);
        }

        return page;
    }

    // --- 1. the bound exists and bites ------------------------------------

    [Test]
    public async Task ReadAsync_truncates_a_page_that_exceeds_the_byte_budget()
    {
        // 32 entries of ~1 KiB against a 4 KiB budget: the count bound (32)
        // is deliberately satisfiable, so any truncation is attributable to
        // the byte bound alone.
        using var sut = CreateProvider(maxReadBatchBytes: 4 * PayloadBytes);
        await AppendAsync(sut, count: 32, valueBytes: PayloadBytes);

        var page = await PageAsync(sut, fromOffsetExclusive: -1L, maxEntries: 32);

        Assert.Multiple(() =>
        {
            Assert.That(page, Is.Not.Empty, "the byte bound must never empty a page");
            Assert.That(
                page,
                Has.Count.LessThan(32),
                "a 32 KiB run of entries must not materialise in one page under a 4 KiB budget");
            Assert.That(
                page.Select(e => e.Offset),
                Is.EqualTo(Enumerable.Range(0, page.Count).Select(i => (long)i)),
                "a truncated page must still be the ascending prefix of the requested range");
        });
    }

    [Test]
    public async Task ReadEncodedAsync_truncates_a_page_that_exceeds_the_byte_budget()
    {
        // ReadEncodedAsync is a separate materialisation path over the same
        // SnapshotAsync call. It is the zero-copy shipping/replication read,
        // so a bound applied only to ReadAsync would leave it unbounded.
        using var sut = CreateProvider(maxReadBatchBytes: 4 * PayloadBytes);
        await AppendAsync(sut, count: 32, valueBytes: PayloadBytes);

        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);
        var page = await sut.ReadEncodedAsync(TreeId, 0, -1L, 32, encoder, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Offsets.Length, Is.Not.Zero);
            Assert.That(page.Offsets.Length, Is.LessThan(32));
            Assert.That(
                page.EncodedEntries.Length,
                Is.EqualTo(page.Offsets.Length),
                "segments and offsets must stay parallel through truncation");
            Assert.That(
                page.HighestOffsetInclusive,
                Is.EqualTo(page.Offsets.Span[^1]),
                "the page's reported high-water mark must reflect what was actually returned, "
                + "or a resuming caller would skip the truncated tail");
        });
    }

    [Test]
    public async Task ReadAsync_returns_the_full_requested_count_when_the_budget_is_ample()
    {
        // Negative control. Without it, a bound that truncated every page to
        // one entry would pass the two tests above while destroying read
        // throughput, and nothing would say so.
        using var sut = CreateProvider(maxReadBatchBytes: FileWalStorageOptions.DefaultMaxReadBatchBytes);
        await AppendAsync(sut, count: 32, valueBytes: PayloadBytes);

        var page = await PageAsync(sut, fromOffsetExclusive: -1L, maxEntries: 32);

        Assert.That(
            page,
            Has.Count.EqualTo(32),
            "the byte bound must not truncate an ordinary page that fits comfortably inside the default budget");
    }

    // --- 2. the bound can never stall a reader ----------------------------

    [Test]
    public async Task ReadAsync_still_returns_one_entry_larger_than_the_entire_budget()
    {
        // The anti-stall floor, and the single most load-bearing assertion in
        // this fixture. Every reader on this path treats an empty page as
        // end-of-stream: WalShardGrain.ReadAsync reports
        // NextSequence == fromSequence and WalCommitLogReader breaks its loop.
        // So a page that returned nothing because its first entry exceeded the
        // budget would not merely be slow - it would wedge replay at that
        // offset forever, reproducing issue #2689 by a new route while wearing
        // the fix's clothes.
        using var sut = CreateProvider(maxReadBatchBytes: 8L);
        await AppendAsync(sut, count: 3, valueBytes: 64 * PayloadBytes);

        var page = await PageAsync(sut, fromOffsetExclusive: -1L, maxEntries: 16);

        Assert.Multiple(() =>
        {
            Assert.That(
                page,
                Has.Count.EqualTo(1),
                "an oversized entry must be returned alone: neither dropped (which stalls replay) "
                + "nor accompanied (which is what the budget forbids)");
            Assert.That(page[0].Offset, Is.EqualTo(0L));
            Assert.That(page[0].Mutation.Value, Has.Length.EqualTo(64 * PayloadBytes));
        });
    }

    [Test]
    public async Task ReadAsync_drains_an_oversized_log_one_entry_at_a_time_without_stalling()
    {
        // The anti-stall floor must compose into forward progress, not just
        // survive a single call: this is the property that makes an
        // already-wedged deployment recover unassisted.
        using var sut = CreateProvider(maxReadBatchBytes: 8L);
        await AppendAsync(sut, count: 5, valueBytes: 16 * PayloadBytes);

        var drained = await DrainLikeCommitLogReaderAsync(sut, maxEntriesPerPage: 16, expectedTotal: 5);

        Assert.That(
            drained,
            Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L }),
            "every entry must still be reachable when each one individually exceeds the budget");
    }

    // --- 3. truncation is a resumption, not a skip ------------------------

    [Test]
    public async Task Repeated_byte_truncated_pages_drain_every_entry_exactly_once_and_in_order()
    {
        // This is the caller-semantics test. FileWalShard is reached through
        // WalShardGrain -> WalCommitLogReader -> LeafReplayCoordinatorGrain,
        // and WalCommitLogReader resumes from page.NextSequence, which
        // WalShardGrain derives from the last entry actually RETURNED
        // (collected[^1].Sequence + 1) rather than from what was requested.
        // That is precisely what makes a short page a resumption instead of a
        // skip. This test drives the provider under that same discipline, so a
        // truncation that lost or duplicated an entry would fail here.
        using var sut = CreateProvider(maxReadBatchBytes: 3 * PayloadBytes);
        await AppendAsync(sut, count: 50, valueBytes: PayloadBytes);

        var drained = await DrainLikeCommitLogReaderAsync(sut, maxEntriesPerPage: 256, expectedTotal: 50);

        Assert.That(
            drained,
            Is.EqualTo(Enumerable.Range(0, 50).Select(i => (long)i).ToArray()),
            "a byte-truncated drain must be lossless, duplicate-free, and ordered");
    }

    /// <summary>
    /// Drains the whole log exactly as <c>WalCommitLogReader</c> does:
    /// resume from the last offset actually returned, and stop only on an
    /// empty page. <paramref name="expectedTotal"/> bounds the loop so a
    /// non-advancing cursor fails as an assertion rather than hanging the
    /// test host.
    /// </summary>
    private static async Task<List<long>> DrainLikeCommitLogReaderAsync(
        FileWalStorageProvider sut,
        int maxEntriesPerPage,
        int expectedTotal)
    {
        var drained = new List<long>();
        var from = -1L;
        var pages = 0;

        while (true)
        {
            // A page can legally hold one entry, so the log needs at most
            // expectedTotal pages; one spare absorbs the terminating empty
            // page. Anything beyond that is a cursor that failed to advance.
            Assert.That(
                ++pages,
                Is.LessThanOrEqualTo(expectedTotal + 1),
                "the read cursor stopped advancing: the byte bound emptied a page that had entries left");

            var page = await PageAsync(sut, from, maxEntriesPerPage);
            if (page.Count == 0)
            {
                return drained;
            }

            drained.AddRange(page.Select(e => e.Offset));
            from = page[^1].Offset;
        }
    }

    // --- configuration ----------------------------------------------------

    [Test]
    public void Default_read_byte_budget_leaves_room_for_a_full_default_write_batch()
    {
        // The write path already bounds a batch by bytes. A read budget below
        // that envelope would truncate every page carrying a full batch, so
        // the two defaults are coupled and this pins the relationship rather
        // than the literal.
        Assert.Multiple(() =>
        {
            Assert.That(new FileWalStorageOptions().MaxReadBatchBytes,
                Is.EqualTo(FileWalStorageOptions.DefaultMaxReadBatchBytes));
            Assert.That(
                FileWalStorageOptions.DefaultMaxReadBatchBytes,
                Is.GreaterThanOrEqualTo(LatticeOptions.DefaultWalMaxBatchBytes),
                "a read page must be able to hold at least one full default-sized write batch");
        });
    }

    [TestCase(0L)]
    [TestCase(-1L)]
    public void Constructing_the_provider_with_a_non_positive_read_budget_is_rejected(long budget)
    {
        // Checked in the constructor as well as in the options validator: a
        // host or test that builds the provider from Options.Create never runs
        // the registration-time validator, and a budget that reached the shard
        // would fail per-read, far from the misconfiguration.
        var options = Options.Create(new FileWalStorageOptions
        {
            RootDirectory = _root,
            MaxReadBatchBytes = budget,
        });

        var ex = Assert.Throws<ArgumentException>(() => new FileWalStorageProvider(options, _serializer));
        Assert.That(ex!.Message, Does.Contain(nameof(FileWalStorageOptions.MaxReadBatchBytes)));
    }

    [TestCase(0L, false)]
    [TestCase(-1L, false)]
    [TestCase(1L, true)]
    public void Validator_rejects_a_non_positive_read_byte_budget(long budget, bool expectedValid)
    {
        var validator = new FileWalStorageOptionsValidator();

        var result = validator.Validate(
            null,
            new FileWalStorageOptions { RootDirectory = _root, MaxReadBatchBytes = budget });

        Assert.That(result.Succeeded, Is.EqualTo(expectedValid));
        if (!expectedValid)
        {
            Assert.That(
                result.FailureMessage,
                Does.Contain(nameof(FileWalStorageOptions.MaxReadBatchBytes)));
        }
    }
}
