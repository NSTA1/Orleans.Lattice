using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// Regression tests for issue #3107: WAL byte accounting could neither report
/// nor bound physical disk usage.
/// <para>
/// Two independent defects are pinned here, because they fail separately and
/// have separate remedies:
/// </para>
/// <list type="number">
/// <item><description><b>Occupancy was unobservable.</b>
/// <c>GetRetainedByteSizeAsync</c> counts live payload only. It excludes
/// per-record framing and, far more importantly, excludes <i>dead</i> bytes -
/// payload already trimmed but not yet reclaimed by compaction. Since the file
/// provider only reclaims space by rewriting the shard, dead bytes are a
/// designed-in component of occupancy that can approach the size of the live
/// payload, so the retained figure could understate real disk usage by a factor
/// approaching two with nothing exposing the gap.</description></item>
/// <item><description><b>Waste was bounded only as a ratio.</b>
/// <c>CompactionThreshold</c> bounds dead bytes relative to live data and
/// <c>CompactionMinimumDeadBytes</c> is only a floor, so absolute waste grew
/// without limit as a shard grew. There was no way to express "never waste more
/// than N bytes".</description></item>
/// </list>
/// <para>
/// The tests are deliberately written against observable size rather than
/// internal counters wherever possible: a test that only asserted on
/// <c>DeadBytes</c> would pass even if compaction never touched the file.
/// </para>
/// </summary>
[TestFixture]
public sealed class FileWalPhysicalByteAccountingTests
{
    private const string TreeId = "tree-physical-bytes";
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
        _root = Path.Combine(Path.GetTempPath(), "lattice-file-wal-physical", Guid.NewGuid().ToString("N"));
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

    // --- physical accounting ------------------------------------------------

    [Test]
    public async Task GetPhysicalByteSizeAsync_matches_the_actual_file_length()
    {
        using var sut = CreateProvider();
        await AppendAsync(sut, count: 20);

        var physical = await sut.GetPhysicalByteSizeAsync(TreeId, 0, CancellationToken.None);

        Assert.That(physical, Is.EqualTo(LogLength()),
            "The physical figure must be the real file length, not an estimate of it.");
    }

    [Test]
    public async Task GetPhysicalByteSizeAsync_exceeds_retained_while_dead_bytes_are_held()
    {
        // Threshold above 1.0 disables trim-triggered compaction, so the dead
        // bytes stay on disk and the gap is observable. This is precisely the
        // state the live incident was in: trimming healthily, reclaiming
        // nothing.
        using var sut = CreateProvider(compactionThreshold: 2.0);
        await AppendAsync(sut, count: 20);
        await sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 9, CancellationToken.None);

        var retained = await sut.GetRetainedByteSizeAsync(TreeId, 0, CancellationToken.None);
        var physical = await sut.GetPhysicalByteSizeAsync(TreeId, 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(physical, Is.EqualTo(LogLength()));
            Assert.That(physical, Is.GreaterThan(retained),
                "Half the payload is dead but still on disk, so physical must exceed retained. "
                + "Equality here would mean the seam still cannot see dead bytes.");
        });
    }

    [Test]
    public async Task GetRetainedByteSizeAsync_does_not_fall_when_compaction_reclaims_space()
    {
        // Pins the two figures as genuinely independent quantities rather than
        // one derived from the other: compaction moves physical and leaves
        // retained untouched, because no live entry was removed.
        using var sut = CreateProvider(compactionThreshold: 2.0);
        await AppendAsync(sut, count: 20);
        await sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 9, CancellationToken.None);

        var retainedBefore = await sut.GetRetainedByteSizeAsync(TreeId, 0, CancellationToken.None);
        var physicalBefore = await sut.GetPhysicalByteSizeAsync(TreeId, 0, CancellationToken.None);

        await sut.ReconcileAsync(TreeId, 0, CancellationToken.None);

        var retainedAfter = await sut.GetRetainedByteSizeAsync(TreeId, 0, CancellationToken.None);
        var physicalAfter = await sut.GetPhysicalByteSizeAsync(TreeId, 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(retainedAfter, Is.EqualTo(retainedBefore), "Compaction removes no live entry.");
            Assert.That(physicalAfter, Is.LessThan(physicalBefore), "Compaction must return bytes to the filesystem.");
            Assert.That(physicalAfter, Is.EqualTo(LogLength()));
        });
    }

    [Test]
    public async Task GetPhysicalByteSizeAsync_survives_reload_and_still_matches_the_file()
    {
        using (var writer = CreateProvider(compactionThreshold: 2.0))
        {
            await AppendAsync(writer, count: 20);
            await writer.TrimAsync(TreeId, 0, throughOffsetInclusive: 9, CancellationToken.None);
        }

        var expected = LogLength();

        using var reloaded = CreateProvider(compactionThreshold: 2.0);
        var physical = await reloaded.GetPhysicalByteSizeAsync(TreeId, 0, CancellationToken.None);

        Assert.That(physical, Is.EqualTo(expected),
            "Recovery rebuilds the write position from the file, so the figure must survive a restart.");
    }

    [Test]
    public async Task GetPhysicalByteSizeAsync_returns_zero_for_an_untouched_shard()
    {
        using var sut = CreateProvider();

        var physical = await sut.GetPhysicalByteSizeAsync(TreeId, 0, CancellationToken.None);

        Assert.That(physical, Is.Zero, "An empty shard occupies nothing and must report 0, not the -1 sentinel.");
    }

    [Test]
    public void GetPhysicalByteSizeAsync_default_implementation_reports_unsupported()
    {
        // The fallback that keeps the in-memory and Azure Table providers
        // working unchanged. If this ever returned 0 instead of -1, every
        // consumer would silently read "this tree uses no disk".
        IWalStorageProvider bare = new UnsupportedProvider();

        Assert.That(
            bare.GetPhysicalByteSizeAsync("t", 0, CancellationToken.None).GetAwaiter().GetResult(),
            Is.EqualTo(-1L));
    }

    // --- the absolute dead-byte ceiling -------------------------------------

    [Test]
    public async Task Absolute_ceiling_compacts_well_below_the_ratio_threshold()
    {
        // The whole point of the option. The ratio is left at its default 0.5
        // and never reached: only a tenth of the payload is trimmed, so without
        // the ceiling nothing would be reclaimed.
        using var sut = CreateProvider(
            compactionMinimumDeadBytes: 1024,
            compactionMaximumDeadBytes: 4 * PayloadBytes);
        await AppendAsync(sut, count: 50);

        var before = LogLength();
        await sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 4, CancellationToken.None);

        var physical = await sut.GetPhysicalByteSizeAsync(TreeId, 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(physical, Is.LessThan(before), "The ceiling must have forced a compaction.");
            Assert.That(physical, Is.EqualTo(LogLength()));
        });
    }

    [Test]
    public async Task Absolute_ceiling_disabled_leaves_dead_bytes_in_place_below_the_ratio()
    {
        // The negative control for the test above, and the guard against the
        // option silently defaulting to on: identical workload, ceiling off,
        // and the file must not shrink.
        using var sut = CreateProvider(compactionMinimumDeadBytes: 1024);
        await AppendAsync(sut, count: 50);

        var before = LogLength();
        await sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 4, CancellationToken.None);

        var physical = await sut.GetPhysicalByteSizeAsync(TreeId, 0, CancellationToken.None);
        var retained = await sut.GetRetainedByteSizeAsync(TreeId, 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            // The file grew by one trim marker rather than shrinking: nothing
            // was rewritten, which is the whole assertion.
            Assert.That(physical, Is.GreaterThanOrEqualTo(before),
                "With the ceiling disabled the ratio governs, and at a dead fraction of 0.1 it is not met.");
            Assert.That(
                retained,
                Is.LessThan(physical),
                "Dead bytes are being held, which is exactly the condition the ceiling exists to bound.");
        });
    }

    [Test]
    public async Task Absolute_ceiling_does_not_override_the_minimum_dead_byte_floor()
    {
        // Scope this precisely, because the obvious generalisation of it is
        // false. What is pinned here is only that a shard holding LESS dead
        // space than the floor is not compacted, whatever the ceiling says:
        // 4 KB of dead payload against a 1 MB floor. It does NOT establish
        // that a sub-floor ceiling is harmless in general, and it cannot,
        // because it never reaches the ceiling comparison at all. The case
        // where the shard HAS cleared the floor is the opposite result and is
        // pinned by the test below.
        using var sut = CreateProvider(
            compactionMinimumDeadBytes: 1024 * 1024,
            compactionMaximumDeadBytes: 1);
        await AppendAsync(sut, count: 10);

        var before = LogLength();
        await sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 0, CancellationToken.None);

        Assert.That(
            await sut.GetPhysicalByteSizeAsync(TreeId, 0, CancellationToken.None),
            Is.GreaterThanOrEqualTo(before),
            "The minimum-dead-bytes floor still suppresses churn, whatever the ceiling says, "
            + "so the file must not have been rewritten.");
    }

    [Test]
    public async Task A_ceiling_below_the_floor_compacts_once_the_floor_is_cleared()
    {
        // The behaviour the option's documentation described as "no effect",
        // and the reason a non-zero ceiling below the floor is now rejected by
        // the validator. The floor is evaluated first and returns early, so
        // every shard that reaches the ceiling comparison already holds
        // dead >= floor; with ceiling < floor the comparison dead >= ceiling
        // is then unconditionally true. The ceiling is not ignored, it is
        // relocated to the floor.
        //
        // The dead fraction here is 3/20 = 0.15, far below the 0.5 ratio
        // default, so the ratio arm provably declines and a rewrite can only
        // have come from the ceiling. That isolation is the point: it is what
        // separates "the clamp fired" from "the ratio happened to fire".
        const int Floor = 2 * PayloadBytes;
        using var sut = CreateProvider(
            compactionMinimumDeadBytes: Floor,
            compactionMaximumDeadBytes: 1);
        await AppendAsync(sut, count: 20);

        var before = LogLength();
        await sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 2, CancellationToken.None);
        var physical = await sut.GetPhysicalByteSizeAsync(TreeId, 0, CancellationToken.None);

        Assert.That(
            physical,
            Is.LessThan(before),
            "Three dead payloads clear the two-payload floor, so the ceiling comparison is "
            + "reached and 1 <= dead is trivially true. The shard is rewritten at a dead "
            + "fraction of 0.15, which the ratio trigger would have declined: a sub-floor "
            + "ceiling is the most aggressive setting available, not an inert one.");
    }

    [Test]
    public async Task Ratio_threshold_still_compacts_when_no_ceiling_is_configured()
    {
        // Guards the default path against regression by the new branch.
        using var sut = CreateProvider(compactionMinimumDeadBytes: 1024);
        await AppendAsync(sut, count: 20);

        var before = LogLength();
        await sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 14, CancellationToken.None);

        Assert.That(
            await sut.GetPhysicalByteSizeAsync(TreeId, 0, CancellationToken.None),
            Is.LessThan(before),
            "Three quarters of the payload is dead, comfortably past the 0.5 default.");
    }

    [Test]
    public async Task Compaction_preserves_every_live_entry_when_the_ceiling_fires()
    {
        // Compaction rewrites the file. A bound that reclaimed space by losing
        // data would satisfy every size assertion above, so the payload is
        // verified independently.
        using var sut = CreateProvider(
            compactionMinimumDeadBytes: 1024,
            compactionMaximumDeadBytes: 4 * PayloadBytes);
        await AppendAsync(sut, count: 30);
        await sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 9, CancellationToken.None);

        var entries = new List<WalEntry>();
        await foreach (var entry in sut.ReadAsync(TreeId, 0, fromOffsetExclusive: -1, maxEntries: 100, CancellationToken.None))
        {
            entries.Add(entry);
        }

        Assert.Multiple(() =>
        {
            Assert.That(entries.Select(e => e.Offset), Is.EqualTo(Enumerable.Range(10, 20).Select(i => (long)i)));
            Assert.That(entries.All(e => e.Mutation.Value!.Length == PayloadBytes), Is.True);
        });
    }

    // --- options validation -------------------------------------------------

    [Test]
    public void Validator_rejects_a_negative_ceiling()
    {
        var result = new FileWalStorageOptionsValidator().Validate(null, new FileWalStorageOptions
        {
            RootDirectory = "/tmp",
            CompactionMaximumDeadBytes = -1,
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(FileWalStorageOptions.CompactionMaximumDeadBytes)));
        });
    }

    [Test]
    public void Validator_accepts_zero_the_disabled_default()
    {
        var result = new FileWalStorageOptionsValidator().Validate(null, new FileWalStorageOptions
        {
            RootDirectory = "/tmp",
        });

        Assert.Multiple(() =>
        {
            Assert.That(FileWalStorageOptions.DefaultCompactionMaximumDeadBytes, Is.Zero,
                "The ceiling must ship disabled: enabling it by default would trade write amplification "
                + "for bounded disk on every existing deployment without asking.");
            Assert.That(result.Succeeded, Is.True);
        });
    }

    [Test]
    public void Validator_rejects_a_ceiling_below_the_floor()
    {
        // The two bounds are individually valid and jointly unsatisfiable.
        // Accepting this pair does not give the operator a 1-byte ceiling; it
        // gives them a 65536-byte one, because the floor is evaluated first.
        // Rejecting is the fail-closed reading: the provider cannot honour
        // what was configured, and what it would substitute is the worst
        // write-amplification setting the option can express.
        var result = new FileWalStorageOptionsValidator().Validate(null, new FileWalStorageOptions
        {
            RootDirectory = "/tmp",
            CompactionMaximumDeadBytes = 1,
        });

        Assert.Multiple(() =>
        {
            Assert.That(
                FileWalStorageOptions.DefaultCompactionMinimumDeadBytes,
                Is.GreaterThan(1),
                "The case only exists while the default floor is above the configured ceiling.");
            Assert.That(result.Failed, Is.True,
                "A non-zero ceiling beneath the floor must not start the host.");
            Assert.That(
                result.FailureMessage,
                Does.Contain(nameof(FileWalStorageOptions.CompactionMaximumDeadBytes))
                    .And.Contain(nameof(FileWalStorageOptions.CompactionMinimumDeadBytes)),
                "The message must name both bounds: the fault is the relationship between them, "
                + "so naming either one alone sends the operator to the wrong knob.");
        });
    }

    [Test]
    public void Validator_accepts_a_ceiling_equal_to_the_floor()
    {
        // The boundary, and the supported way to express "compact as soon as
        // there is enough dead space to be worth it". The rule is strictly
        // below, so equality must pass: it is the one sub-ratio setting whose
        // effective bound is the value the operator actually wrote.
        var result = new FileWalStorageOptionsValidator().Validate(null, new FileWalStorageOptions
        {
            RootDirectory = "/tmp",
            CompactionMinimumDeadBytes = 4096,
            CompactionMaximumDeadBytes = 4096,
        });

        Assert.That(result.Succeeded, Is.True);
    }

    // --- helpers ------------------------------------------------------------

    private long LogLength() => new FileInfo(Path.Combine(_root, TreeId, "shard-0", "wal.log")).Length;

    private FileWalStorageProvider CreateProvider(
        double compactionThreshold = FileWalStorageOptions.DefaultCompactionThreshold,
        int compactionMinimumDeadBytes = FileWalStorageOptions.DefaultCompactionMinimumDeadBytes,
        long compactionMaximumDeadBytes = FileWalStorageOptions.DefaultCompactionMaximumDeadBytes)
    {
        var options = Options.Create(new FileWalStorageOptions
        {
            RootDirectory = _root,
            FlushToDisk = false,
            CompactionThreshold = compactionThreshold,
            CompactionMinimumDeadBytes = compactionMinimumDeadBytes,
            CompactionMaximumDeadBytes = compactionMaximumDeadBytes,
        });
        return new FileWalStorageProvider(options, _serializer);
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

    /// <summary>
    /// A provider that overrides nothing, so it exercises the interface's own
    /// default implementations.
    /// </summary>
    private sealed class UnsupportedProvider : IWalStorageProvider
    {
        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => AsyncEnumerable.Empty<WalEntry>();

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Task.FromResult(-1L);

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Task.FromResult(-1L);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }
}
