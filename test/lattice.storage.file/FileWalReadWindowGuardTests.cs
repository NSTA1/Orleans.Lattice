using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization;
using Orleans.Serialization.Session;

namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// The read window's argument guards and degenerate-window early returns, on
/// every read overload the file provider exposes.
/// <para>
/// Each of the three shard-level read overloads
/// (<c>SnapshotAsync</c>, <c>SnapshotDecodedAsync</c>,
/// <c>SnapshotFilteredAsync</c>) carries its own private copy of the
/// <c>maxEntries</c> and <c>maxBytes</c> guards, and the provider's
/// <c>ReadFilteredAsync</c> carries a fourth copy of the entry guard. They are
/// separate claims: a guard removed from one overload leaves the other three
/// passing, so each is asserted where it lives rather than once by proxy.
/// </para>
/// <para>
/// The same reasoning applies to the resumption guards. A reader resumes from
/// the last offset it was handed, so <c>fromOffsetExclusive</c> can legitimately
/// arrive as <see cref="long.MaxValue"/>; the <c>+ 1</c> the window arithmetic
/// performs would overflow to <see cref="long.MinValue"/> and silently re-read
/// the whole log from its head. That is a wrong-data failure rather than a
/// crash, which is exactly the kind that survives an untested guard.
/// </para>
/// </summary>
[TestFixture]
public sealed class FileWalReadWindowGuardTests
{
    private const string TreeId = "tree-read-window";

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private OrleansBinaryWalRecordEncoder _encoder = null!;
    private WalRecordRoutingReader _routing = null!;
    private string _root = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
        _encoder = new OrleansBinaryWalRecordEncoder(_serializer);
        _routing = new WalRecordRoutingReader(_services.GetRequiredService<SerializerSessionPool>());
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "lattice-file-wal-window", Guid.NewGuid().ToString("N"));
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

    private FileWalStorageOptions Options() => new()
    {
        RootDirectory = _root,
        FlushToDisk = false,
    };

    private FileWalShard CreateShard(string name = "shard") =>
        new(Path.Combine(_root, name), Options());

    private FileWalShard CreateShard(IWalReadPressureGovernor governor, string name = "shard") =>
        new(Path.Combine(_root, name), Options(), TreeId, 0, governor);

    /// <summary>Appends one encoded record per key, at ascending offsets from zero.</summary>
    private async Task AppendAsync(FileWalShard shard, params string[] keys)
    {
        var records = new PreparedWalRecord[keys.Length];
        for (var i = 0; i < keys.Length; i++)
        {
            var record = new WalRecord
            {
                TreeId = TreeId,
                Op = MutationKind.Set,
                Key = keys[i],
                Value = new byte[32],
                OriginClusterId = "site-a",
            };
            var writer = new ArrayBufferWriter<byte>();
            _encoder.Encode(in record, writer);
            records[i] = new PreparedWalRecord(i, writer.WrittenMemory.ToArray());
        }

        await shard.AppendAsync(records, CancellationToken.None);
    }

    private static void ReturnPage(long[] offsets, WalRecord[] records)
    {
        if (offsets.Length > 0)
        {
            ArrayPool<long>.Shared.Return(offsets);
            ArrayPool<WalRecord>.Shared.Return(records, clearArray: true);
        }
    }

    // ---------------------------------------------------------------------
    // maxEntries / maxBytes guards, one claim per overload.
    // ---------------------------------------------------------------------

    [TestCase(0)]
    [TestCase(-1)]
    public void SnapshotAsync_rejects_a_non_positive_max_entries(int maxEntries)
    {
        using var shard = CreateShard();

        var ex = Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await shard.SnapshotAsync(-1L, maxEntries, 1024L, CancellationToken.None));

        Assert.That(ex!.ParamName, Is.EqualTo("maxEntries"));
    }

    [TestCase(0L)]
    [TestCase(-1L)]
    public void SnapshotAsync_rejects_a_non_positive_max_bytes(long maxBytes)
    {
        using var shard = CreateShard();

        var ex = Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await shard.SnapshotAsync(-1L, 8, maxBytes, CancellationToken.None));

        Assert.That(ex!.ParamName, Is.EqualTo("maxBytes"));
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void SnapshotDecodedAsync_rejects_a_non_positive_max_entries(int maxEntries)
    {
        using var shard = CreateShard();

        var ex = Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await shard.SnapshotDecodedAsync(
                -1L, maxEntries, 1024L, _ => 0, CancellationToken.None));

        Assert.That(ex!.ParamName, Is.EqualTo("maxEntries"));
    }

    [TestCase(0L)]
    [TestCase(-1L)]
    public void SnapshotDecodedAsync_rejects_a_non_positive_max_bytes(long maxBytes)
    {
        using var shard = CreateShard();

        var ex = Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await shard.SnapshotDecodedAsync(
                -1L, 8, maxBytes, _ => 0, CancellationToken.None));

        Assert.That(ex!.ParamName, Is.EqualTo("maxBytes"));
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void SnapshotFilteredAsync_rejects_a_non_positive_max_entries(int maxEntries)
    {
        using var shard = CreateShard();

        var ex = Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await shard.SnapshotFilteredAsync(
                -1L,
                long.MaxValue,
                maxEntries,
                1024L,
                new WalKeyFilter(null, null),
                routing: null,
                _serializer.Deserialize,
                CancellationToken.None));

        Assert.That(ex!.ParamName, Is.EqualTo("maxEntries"));
    }

    [TestCase(0L)]
    [TestCase(-1L)]
    public void SnapshotFilteredAsync_rejects_a_non_positive_max_bytes(long maxBytes)
    {
        using var shard = CreateShard();

        var ex = Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await shard.SnapshotFilteredAsync(
                -1L,
                long.MaxValue,
                8,
                maxBytes,
                new WalKeyFilter(null, null),
                routing: null,
                _serializer.Deserialize,
                CancellationToken.None));

        Assert.That(ex!.ParamName, Is.EqualTo("maxBytes"));
    }

    /// <summary>
    /// The provider's filtered read is an iterator, so its guard runs on the
    /// first <c>MoveNextAsync</c> rather than at the call. Enumerating is
    /// therefore part of the assertion, not incidental to it.
    /// </summary>
    [TestCase(0)]
    [TestCase(-1)]
    public void ReadFilteredAsync_rejects_a_non_positive_max_entries(int maxEntries)
    {
        using var provider = new FileWalStorageProvider(
            Microsoft.Extensions.Options.Options.Create(Options()), _serializer);

        var ex = Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
        {
            await foreach (var _ in provider.ReadFilteredAsync(
                TreeId, 0, -1L, long.MaxValue, maxEntries, new WalKeyFilter(null, null), CancellationToken.None))
            {
                // The guard throws before the first element is produced.
            }
        });

        Assert.That(ex!.ParamName, Is.EqualTo("maxEntries"));
    }

    // ---------------------------------------------------------------------
    // Degenerate windows: an exhausted resumption cursor must not wrap.
    // ---------------------------------------------------------------------

    [Test]
    public async Task SnapshotAsync_returns_an_empty_page_when_resuming_past_the_last_representable_offset()
    {
        using var shard = CreateShard();
        await AppendAsync(shard, "a", "b", "c");

        var (offsets, payloads) = await shard.SnapshotAsync(
            long.MaxValue, 8, 64L * 1024, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Is.Empty, "long.MaxValue + 1 would wrap and re-read the log from its head");
            Assert.That(payloads, Is.Empty);
        });
    }

    [Test]
    public async Task SnapshotDecodedAsync_returns_an_empty_page_when_resuming_past_the_last_representable_offset()
    {
        using var shard = CreateShard();
        await AppendAsync(shard, "a", "b", "c");
        var decodes = 0;

        var (offsets, values) = await shard.SnapshotDecodedAsync(
            long.MaxValue,
            8,
            64L * 1024,
            _ =>
            {
                decodes++;
                return 0;
            },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Is.Empty);
            Assert.That(values, Is.Empty);
            Assert.That(decodes, Is.Zero, "nothing may be decoded for a window that cannot contain an entry");
        });
    }

    [Test]
    public async Task SnapshotFilteredAsync_returns_an_empty_page_when_resuming_past_the_last_representable_offset()
    {
        using var shard = CreateShard();
        await AppendAsync(shard, "a", "b", "c");

        var (offsets, records, count) = await shard.SnapshotFilteredAsync(
            long.MaxValue,
            long.MaxValue,
            8,
            64L * 1024,
            new WalKeyFilter(null, null),
            routing: null,
            _serializer.Deserialize,
            CancellationToken.None);

        try
        {
            Assert.That(count, Is.Zero);
            Assert.That(offsets, Is.Empty);
        }
        finally
        {
            ReturnPage(offsets, records);
        }
    }

    /// <summary>
    /// An inverted or empty window - one whose inclusive upper bound is at or
    /// below its exclusive lower bound - contains no offset by construction, and
    /// must yield nothing rather than a wrapped or inverted range. That contract
    /// is what is asserted here. Note the early-out clause that names this case
    /// is defensive rather than load-bearing: the downstream empty-range check
    /// reaches the same verdict for every inverted window, so removing the clause
    /// changes no result. The contract is still worth pinning, because it is the
    /// caller-visible property and it would survive a rewrite of either check.
    /// </summary>
    [TestCase(5L, 5L)]
    [TestCase(5L, 2L)]
    public async Task SnapshotFilteredAsync_returns_an_empty_page_for_an_inverted_window(
        long fromExclusive, long toInclusive)
    {
        using var shard = CreateShard();
        await AppendAsync(shard, "a", "b", "c", "d", "e", "f", "g", "h");

        var (offsets, records, count) = await shard.SnapshotFilteredAsync(
            fromExclusive,
            toInclusive,
            8,
            64L * 1024,
            new WalKeyFilter(null, null),
            routing: null,
            _serializer.Deserialize,
            CancellationToken.None);

        try
        {
            Assert.That(count, Is.Zero);
        }
        finally
        {
            ReturnPage(offsets, records);
        }
    }

    // ---------------------------------------------------------------------
    // The shard's own defence against a governor that narrows below one byte.
    // ---------------------------------------------------------------------

    /// <summary>
    /// <c>IWalReadPressureGovernor</c> is an injectable seam, so the shard cannot
    /// assume its narrowing stays positive. The clamp is observable because
    /// <c>Narrow</c> admits a further entry only while the running payload total
    /// does not EXCEED the budget: a zero-byte entry followed by a one-byte entry
    /// totals exactly one byte, which fits a clamped budget of 1 and does not fit
    /// the raw 0 the governor returned. Without the clamp the second entry is
    /// dropped, so this pins the clamp itself rather than merely executing it.
    /// </summary>
    [TestCase(0L)]
    [TestCase(-1L)]
    public async Task A_governor_narrowing_below_one_byte_is_clamped_to_a_one_byte_budget(long narrowedTo)
    {
        var governor = new FixedBudgetGovernor(narrowedTo);
        using var shard = CreateShard(governor);
        await shard.AppendAsync(
            new[]
            {
                new PreparedWalRecord(0, Array.Empty<byte>()),
                new PreparedWalRecord(1, new byte[1]),
            },
            CancellationToken.None);

        var (offsets, payloads) = await shard.SnapshotAsync(
            -1L, 8, 64L * 1024, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Is.EqualTo(new[] { 0L, 1L }));
            Assert.That(payloads.Length, Is.EqualTo(2));
        });
    }

    /// <summary>
    /// The companion invariant, and the load-bearing one: however far the budget
    /// is narrowed, a page is never empty. Every reader on this path treats an
    /// empty page as end-of-stream, so a starved page would wedge replay at that
    /// offset permanently. This is pinned by the always-take-one floor in
    /// <c>Narrow</c> rather than by the budget clamp above, which is why it is a
    /// separate test: the two fail independently and have different remedies.
    /// </summary>
    [TestCase(0L)]
    [TestCase(-1L)]
    public async Task A_governor_narrowing_below_one_byte_still_yields_a_non_empty_page(long narrowedTo)
    {
        var governor = new FixedBudgetGovernor(narrowedTo);
        using var shard = CreateShard(governor);
        await AppendAsync(shard, "a", "b", "c");

        var (offsets, payloads) = await shard.SnapshotAsync(
            -1L, 8, 64L * 1024, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(offsets.Length, Is.EqualTo(1), "a read may be narrowed but never starved");
            Assert.That(offsets[0], Is.Zero);
            Assert.That(payloads.Length, Is.EqualTo(1));
        });
    }

    // ---------------------------------------------------------------------
    // Dead-byte accounting, the quantity the compaction gate tests.
    // ---------------------------------------------------------------------

    /// <summary>
    /// The shard publishes both halves of its dead-record accounting so the
    /// compaction gate's inputs are observable rather than inferred from file
    /// size. Counting bytes without counting records leaves the dead records'
    /// mean payload uncomputable, which is what makes the true dead ratio
    /// bounded rather than exact (issue #3206), so both are asserted together.
    /// </summary>
    [Test]
    public async Task Trimming_publishes_both_the_dead_byte_and_dead_record_counts()
    {
        var options = Options();

        // Keep compaction out of the way: it is the thing that resets these two,
        // and this test is about what they read before it runs.
        options.CompactionMinimumDeadBytes = int.MaxValue;
        using var shard = new FileWalShard(Path.Combine(_root, "dead"), options);

        var payloads = new[]
        {
            new PreparedWalRecord(0, new byte[16]),
            new PreparedWalRecord(1, new byte[32]),
            new PreparedWalRecord(2, new byte[64]),
        };
        await shard.AppendAsync(payloads, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(shard.DeadBytes, Is.Zero, "nothing is dead before a trim");
            Assert.That(shard.DeadEntries, Is.Zero);
        });

        await shard.TrimAsync(1L, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(shard.DeadEntries, Is.EqualTo(2), "offsets 0 and 1 are now trimmed");
            Assert.That(shard.DeadBytes, Is.EqualTo(16 + 32));
        });
    }

    // ---------------------------------------------------------------------
    // The filtered path's own unaffordable-read verdict.
    // ---------------------------------------------------------------------

    /// <summary>
    /// The filtered read carries its own copy of the narrow-and-retry loop, and
    /// so its own terminal arm: once the window is down to a single entry there
    /// is nothing left to give up, and an allocation failure is a resource
    /// verdict rather than corruption. Asserting it here is a separate claim from
    /// the decoded path's copy, which a sibling fixture pins.
    /// </summary>
    [Test]
    public async Task An_unaffordable_single_entry_on_the_filtered_path_is_refused_as_a_resource_verdict()
    {
        using var shard = CreateShard();
        await AppendAsync(shard, "a");

        var ex = Assert.ThrowsAsync<WalReadUnderPressureException>(
            async () => await shard.SnapshotFilteredAsync(
                -1L,
                long.MaxValue,
                1,
                64L * 1024,
                new WalKeyFilter(null, null),
                routing: null,
                _ => throw new OutOfMemoryException("scripted: never affordable."),
                CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Offset, Is.Zero);
            Assert.That(ex.InnerException, Is.TypeOf<OutOfMemoryException>());
        });
    }

    /// <summary>
    /// A zero-length payload is a legal record, not a gap: it has no routing
    /// prefix to classify, so it reaches the caller through the full decode and
    /// is delivered like any other. The prefix read for such an entry is a
    /// zero-byte one, which the shard skips; that skip is an I/O optimisation
    /// with no observable behaviour of its own, so what is asserted here is the
    /// delivery, which is the property a caller depends on.
    /// </summary>
    [Test]
    public async Task A_zero_length_payload_is_delivered_without_a_routing_prefix_read()
    {
        using var shard = CreateShard();
        await shard.AppendAsync(
            new[] { new PreparedWalRecord(0, Array.Empty<byte>()) }, CancellationToken.None);

        var decodes = 0;
        var (offsets, records, count) = await shard.SnapshotFilteredAsync(
            -1L,
            long.MaxValue,
            8,
            64L * 1024,
            new WalKeyFilter(null, null),
            _routing,
            sequence =>
            {
                decodes++;
                Assert.That(sequence.Length, Is.Zero);
                return new WalRecord { TreeId = TreeId, Op = MutationKind.Set, Key = "empty" };
            },
            CancellationToken.None);

        try
        {
            Assert.Multiple(() =>
            {
                Assert.That(count, Is.EqualTo(1));
                Assert.That(offsets[0], Is.Zero);
                Assert.That(records[0].Key, Is.EqualTo("empty"));
                Assert.That(decodes, Is.EqualTo(1));
            });
        }
        finally
        {
            ReturnPage(offsets, records);
        }
    }

    /// <summary>
    /// A governor whose narrowing is fixed, including at values the interface
    /// permits but the production governor never produces.
    /// </summary>
    private sealed class FixedBudgetGovernor(long budget) : IWalReadPressureGovernor
    {
        public long NarrowBudget(long configuredMaxBytes) => budget;

        public byte[] Allocate(int byteCount) => new byte[byteCount];
    }
}
