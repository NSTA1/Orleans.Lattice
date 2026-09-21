using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// End-to-end regression tests for issue #3207, driving the real
/// <see cref="LatticeWalGc"/> over a real <see cref="FileWalStorageProvider"/>
/// so that the reclamation is proven in physical bytes on disk rather than in
/// a call record.
/// <para>
/// The defect is a reachability one and its shape matters. Every compaction
/// threshold is read in exactly one place, at the end of the provider's trim,
/// and it is read there unconditionally - a trim that releases no entry still
/// evaluates. The gate on the ratio and the ceiling was therefore never "did
/// we trim" but "was the trim called at all", and the collector returns before
/// calling it whenever the scan found no eligible entry. A shard that has
/// already accumulated dead bytes and only later comes to rest on the floor
/// produces exactly that, on every sweep, for as long as the floor is held.
/// The result is not slow reclamation but stranded reclamation: dead bytes sit
/// <i>above</i> the threshold with no path to an evaluation, so no value of
/// any compaction option can reach them. That is the population the repair
/// restores, and it is the one the fixtures below drive.
/// </para>
/// <para>
/// <b>It is not the only population, and the repair does not serve the other
/// one (issue #3207).</b> A shard whose <b>first</b> entry has always sat above
/// the floor has never called the trim at all, and dead bytes rise only as a
/// direct or replayed consequence of that call - no write path marks anything
/// dead. Its dead-byte count is therefore pinned at zero in perpetuity, which
/// is the operand every threshold reads, so reaching the evaluation makes such
/// a shard <i>asked</i> without making it <i>reclaimable</i>. That is not a
/// shortfall in the repair: those retained bytes are live, not dead, so there
/// is nothing there for a compaction to return. The remedy for that population
/// is to make it nameable rather than to rewrite it, and
/// <c>A_shard_that_has_never_trimmed_is_asked_and_correctly_reclaims_nothing</c>
/// below pins that boundary so the distinction cannot be lost again.
/// </para>
/// <para>
/// The fixtures below therefore put the floor <b>below the whole retained
/// range</b>. A floor sitting mid-log would release the prefix beneath it,
/// which calls the trim, which evaluates - re-proving the one path that was
/// never broken. That case is present too, as a control, precisely so it
/// cannot be mistaken for the regression.
/// </para>
/// </summary>
[TestFixture]
public sealed class FileWalCompactionReachabilityTests
{
    private const string TreeId = "compaction-reachability-tree";
    private const string LeafConsumer = "_lattice_materialiser_" + TreeId + "_leaf-1";
    private const int PayloadBytes = 4096;

    /// <summary>
    /// Appended offsets 0..19, of which 0..14 are trimmed to dead before the
    /// collector ever runs. The surviving range is 15..19, so a floor below 15
    /// strands the scan on its first entry, and the dead fraction of 0.75
    /// clears the 0.5 ratio.
    /// </summary>
    private const int Appended = 20;

    private const int TrimThrough = 14;
    private const long FirstRetainedOffset = TrimThrough + 1;

    /// <summary>
    /// The base offset for the never-trimmed population. It is far enough above
    /// zero that the collector can be given a floor beneath the whole range,
    /// which is the only way to reach a shard that has never called the trim.
    /// </summary>
    private const long UntrimmedFirstOffset = 100;

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
        _root = Path.Combine(Path.GetTempPath(), "lattice-wal-compaction-reachability", Guid.NewGuid().ToString("N"));
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
    /// The decisive test. The shard holds a dead backlog well clear of the
    /// ratio, and the collector's scan stops on its very first entry, so no
    /// trim is issued and the only evaluation site on the trim path is never
    /// reached. The rewrite must happen regardless, and must be visible as a
    /// fall in physical occupancy rather than only as a counter.
    /// </summary>
    [Test]
    public async Task A_shard_whose_first_entry_is_above_the_offset_floor_still_compacts()
    {
        await SeedDeadBacklogAsync();

        using var sut = CreateProvider(compactionMinimumDeadBytes: 1024);
        var before = await sut.GetPhysicalByteSizeAsync(TreeId, 0, CancellationToken.None);

        var (report, compactions, stops) = await RunCollectorAsync(sut, checkpointOffset: FirstRetainedOffset - 5);

        var after = await sut.GetPhysicalByteSizeAsync(TreeId, 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.Zero,
                "The floor is below the whole retained range, so the scan stops on its first entry.");
            Assert.That(stops, Is.EqualTo(new[] { "offset_floor" }),
                "Control on the fixture: the pass must be held by the offset floor, which is the production "
                + "signature this test exists to reproduce.");
            Assert.That(compactions, Is.EqualTo(new[] { "ratio" }),
                "A dead fraction of 0.75 clears the 0.5 ratio, so the newly reachable evaluation must rewrite "
                + "the segment. Before this change the evaluation was never reached at all, so the shard held "
                + "its dead bytes at any ratio for as long as the floor stood.");
            Assert.That(after, Is.LessThan(before),
                "Reclamation is a physical property. A compaction counter that advances without the file "
                + "shrinking would leave the founding defect - unbounded WAL growth - entirely in place.");
        });
    }

    /// <summary>
    /// The rewrite is a physical reorganisation and must leave the shard's
    /// logical contents bit-identical: same offsets, same order, same
    /// payloads, and in particular the same lowest and highest offset. A
    /// reclamation that moved a watermark would be a data-loss bug wearing a
    /// space-saving costume, and it would do so on a path that now runs
    /// unattended on every sweep.
    /// </summary>
    [Test]
    public async Task Compacting_through_the_stranded_path_moves_no_offset_and_drops_no_entry()
    {
        await SeedDeadBacklogAsync();

        using var sut = CreateProvider(compactionMinimumDeadBytes: 1024);
        var lowestBefore = await sut.GetLowestOffsetAsync(TreeId, 0, CancellationToken.None);
        var highestBefore = await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        var entriesBefore = await ReadAllAsync(sut);

        await RunCollectorAsync(sut, checkpointOffset: FirstRetainedOffset - 5);

        var lowestAfter = await sut.GetLowestOffsetAsync(TreeId, 0, CancellationToken.None);
        var highestAfter = await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        var entriesAfter = await ReadAllAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(lowestAfter, Is.EqualTo(lowestBefore), "Compaction must not advance the retention floor.");
            Assert.That(highestAfter, Is.EqualTo(highestBefore), "Compaction must not disturb the append head.");
            Assert.That(
                entriesAfter.Select(static e => e.Offset),
                Is.EqualTo(entriesBefore.Select(static e => e.Offset)),
                "Every retained entry must survive the rewrite at its own offset.");
            Assert.That(
                entriesAfter.Select(static e => e.Mutation.Key),
                Is.EqualTo(entriesBefore.Select(static e => e.Mutation.Key)),
                "The payloads must survive too - an offset-preserving rewrite that lost values would pass a "
                + "watermark assertion and still be a loss.");
        });
    }

    /// <summary>
    /// Reachability is not the same as compaction, and this test holds the
    /// difference open.
    /// <para>
    /// The dead backlog here sits below the configured minimum, so the newly
    /// reached evaluation correctly declines. What must still happen is the
    /// evaluation itself: the gate inputs are sampled, so a shard that is
    /// being consulted and refusing is now distinguishable from one that is
    /// never consulted at all. Those two states are identical on the outcome
    /// counters and have opposite remedies - one is a threshold to tune, the
    /// other is a call site that does not exist - and the entire cost of this
    /// defect was that they could not be told apart.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_stranded_shard_below_the_threshold_is_evaluated_and_declines_rather_than_going_unasked()
    {
        await SeedDeadBacklogAsync();

        using var sut = CreateProvider(compactionMinimumDeadBytes: int.MaxValue);

        // Load the shard before listening, so the arming samples taken at load
        // are not mistaken for evidence that the collector reached an
        // evaluation.
        var before = await sut.GetPhysicalByteSizeAsync(TreeId, 0, CancellationToken.None);

        var evaluations = 0;
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalCompactionEvalDeadBytes,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                if (MatchesTree(tags))
                {
                    Interlocked.Increment(ref evaluations);
                }
            }));

        var (report, compactions, _) = await RunCollectorAsync(sut, checkpointOffset: FirstRetainedOffset - 5);

        listener.Dispose();

        var after = await sut.GetPhysicalByteSizeAsync(TreeId, 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.Zero);
            Assert.That(evaluations, Is.EqualTo(1),
                "The stranded shard must be asked exactly once per pass. Declining is a legitimate answer; "
                + "not being asked is what made the backlog unreachable.");
            Assert.That(compactions, Is.Empty,
                "Below the minimum there is nothing worth rewriting, and reachability must not be bought by "
                + "compacting unconditionally - that would trade unbounded growth for unbounded write "
                + "amplification on a shard the collector revisits every sweep.");
            Assert.That(after, Is.EqualTo(before), "A declined evaluation must not touch the file.");
        });
    }

    /// <summary>
    /// Control, and the reason the remedy is keyed to what the scan
    /// <em>released</em> rather than to why it stopped.
    /// <para>
    /// A floor sitting <em>within</em> the retained range releases the prefix
    /// beneath it and then stops on the first entry above it - reporting the
    /// very same <c>offset_floor</c> arm as the stranded pass, while having
    /// trimmed. So the stop reason does not distinguish the broken case from
    /// the healthy one, and a remedy conditioned on it would either fire
    /// redundantly on shards that had just evaluated inside their own trim, or
    /// - had it been keyed to some other arm - miss the stranded case
    /// entirely. The quantity that separates them is whether anything was
    /// released, which is what the call site actually tests.
    /// </para>
    /// <para>
    /// It also guards the pre-existing path: a future change must not obtain
    /// reachability by giving up trimming. And it is the shape the decisive
    /// test must not be confused with, since a fixture built this way passes
    /// against the unfixed build and proves nothing.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_floor_inside_the_retained_range_still_trims_and_compacts_on_the_pre_existing_path()
    {
        await SeedDeadBacklogAsync();

        using var sut = CreateProvider(compactionMinimumDeadBytes: 1024);

        var (report, compactions, stops) = await RunCollectorAsync(sut, checkpointOffset: FirstRetainedOffset + 2);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.GreaterThan(0),
                "Offsets at or below a mid-range floor are eligible, so the scan releases them.");
            Assert.That(stops, Is.EqualTo(new[] { "offset_floor" }),
                "The healthy pass stops on the same arm as the stranded one, which is precisely why the "
                + "remedy must not read the stop reason: this arm cannot tell the two apart.");
            Assert.That(compactions, Is.EqualTo(new[] { "ratio" }),
                "The trim's own evaluation must still fire - the remedy adds a second entry point, it does "
                + "not move the first - and it must fire exactly once, not once per entry point.");
        });
    }

    /// <summary>
    /// The boundary of what the repair can do, pinned so it cannot be lost
    /// again (issue #3207). This shard's first entry has always sat above the
    /// floor, so it has never called the provider's trim, and dead bytes rise
    /// only as a direct or replayed consequence of that call. Its dead-byte
    /// count is pinned at zero, which is the operand every compaction arm
    /// reads.
    /// <para>
    /// The assertions reproduce, in a fixture, the exact signature such a shard
    /// publishes in production: the evaluation is <b>recorded</b> - retained
    /// bytes are non-zero, so the shard is demonstrably being asked - and its
    /// dead-byte answer is zero on every one of those same evaluations. That
    /// pairing is the proof that the shard is not being skipped but is
    /// declining on a quantity which cannot move, and it is why no
    /// threshold-based remedy can ever observe this population: there is no
    /// value of the ratio, the minimum, or the ceiling that a zero clears.
    /// </para>
    /// <para>
    /// It is equally the proof that nothing is being lost. A compaction returns
    /// exactly the dead bytes it finds, and these retained bytes are live, so a
    /// rewrite here would move several megabytes to reclaim none of them. The
    /// remedy for this population is therefore to make it <i>nameable</i> -
    /// covered by
    /// <c>LatticeWalGcTrimStopReasonTests.RunOnceAsync_separates_a_shard_that_releases_nothing_from_a_healthy_shard_that_also_stops</c>,
    /// which joins the shard-attributed stop arm against that shard's flat
    /// entries-trimmed - and not to widen any threshold.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_shard_that_has_never_trimmed_is_asked_and_correctly_reclaims_nothing()
    {
        await SeedUntrimmedBacklogAsync();

        using var sut = CreateProvider(compactionMinimumDeadBytes: 1024);

        var deadSamples = new List<long>();
        var retainedSamples = new List<long>();

        using var deadListener = MeterListening.StartForInstrument(
            LatticeMetrics.WalCompactionEvalDeadBytes,
            l => l.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                if (MatchesTree(tags))
                {
                    lock (deadSamples)
                    {
                        deadSamples.Add(measurement);
                    }
                }
            }));

        using var retainedListener = MeterListening.StartForInstrument(
            LatticeMetrics.WalCompactionEvalRetainedBytes,
            l => l.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                if (MatchesTree(tags))
                {
                    lock (retainedSamples)
                    {
                        retainedSamples.Add(measurement);
                    }
                }
            }));

        var (report, compactions, stops) = await RunCollectorAsync(sut, checkpointOffset: UntrimmedFirstOffset - 10);

        deadListener.Dispose();
        retainedListener.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.Zero,
                "Every entry sits above the floor, so the scan releases nothing - which is correct, and is "
                + "the data-loss guard working rather than a fault to be relaxed.");
            Assert.That(stops, Is.EqualTo(new[] { "offset_floor" }),
                "The shard stops on the same arm a healthy shard stops on once it has released all it may, "
                + "which is exactly why the arm needs the shard dimension to be actionable.");

            Assert.That(retainedSamples, Is.Not.Empty,
                "Retained bytes being recorded at all is the evidence that the evaluation is reached: the "
                + "shard is being asked.");
            Assert.That(retainedSamples, Has.Count.GreaterThanOrEqualTo(2),
                "Opening the shard evaluates once on its own, so a single sample would prove only that the "
                + "file was loaded. A second evaluation in the same pass is the repair firing after a scan "
                + "that released nothing - the shard declines visibly, having been asked.");
            Assert.That(retainedSamples, Is.All.GreaterThan(0),
                "The shard is holding real payload - this is the several-megabytes-retained reading an "
                + "operator sees, and it is live data, not reclaimable slack.");
            Assert.That(deadSamples, Has.Count.EqualTo(retainedSamples.Count),
                "Dead and retained are recorded together on every evaluation, so they can be compared "
                + "sample for sample rather than across differently-sized series.");
            Assert.That(deadSamples, Is.All.Zero,
                "The answer is pinned at zero on every evaluation. No write path marks bytes dead, so a "
                + "shard that has never trimmed has no dead-byte accounting for any threshold to read.");

            Assert.That(compactions, Is.Empty,
                "Declining is correct: a compaction returns exactly the dead bytes it finds, so rewriting "
                + "this shard would be unbounded write amplification for no reclaimed byte.");
        });
    }

    // --- helpers ------------------------------------------------------------

    /// <summary>
    /// Appends a segment whose every offset sits above <see cref="UntrimmedFirstOffset"/>
    /// and never trims it, so the shard reaches the collector with a dead-byte
    /// count that has never been raised. The provider is disposed so the
    /// collector under test opens the shard from disk, exactly as a restarted
    /// host would - which also exercises the recovery path that replays trim
    /// markers, of which there are none to replay.
    /// </summary>
    private async Task SeedUntrimmedBacklogAsync()
    {
        using var writer = CreateProvider(compactionMinimumDeadBytes: int.MaxValue);
        for (var i = 0; i < Appended; i++)
        {
            await writer.AppendBatchAsync(
                TreeId, 0, new[] { Entry(UntrimmedFirstOffset + i) }, CancellationToken.None);
        }

        Assert.That(
            await writer.GetLowestOffsetAsync(TreeId, 0, CancellationToken.None),
            Is.EqualTo(UntrimmedFirstOffset),
            "The seed must leave the whole range above the floor the collector will be given, or the shard "
            + "would release a prefix and stop being the never-trimmed population under test.");
    }

    /// <summary>
    /// Appends a full segment and trims its prefix to dead without compacting,
    /// by holding the minimum-dead floor above anything the trim can produce.
    /// The provider is disposed so the collector under test opens the shard
    /// from disk, exactly as a restarted host would.
    /// </summary>
    private async Task SeedDeadBacklogAsync()
    {
        using var writer = CreateProvider(compactionMinimumDeadBytes: int.MaxValue);
        for (var i = 0; i < Appended; i++)
        {
            await writer.AppendBatchAsync(TreeId, 0, new[] { Entry(i) }, CancellationToken.None);
        }

        await writer.TrimAsync(TreeId, 0, TrimThrough, CancellationToken.None);

        Assert.That(
            await writer.GetLowestOffsetAsync(TreeId, 0, CancellationToken.None),
            Is.EqualTo(FirstRetainedOffset),
            "The seed must leave the retained range starting above zero, or the floor cannot be placed beneath it.");
    }

    /// <summary>
    /// Runs one collector pass over <paramref name="sut"/> with the durable
    /// leaf checkpoint pinned at <paramref name="checkpointOffset"/>, returning
    /// the report alongside the compaction trigger arms and trim stop arms that
    /// advanced during it.
    /// </summary>
    private static async Task<(LatticeWalGcReport Report, List<string> Compactions, List<string> Stops)>
        RunCollectorAsync(FileWalStorageProvider sut, long checkpointOffset)
    {
        var compactions = new List<string>();
        var stops = new List<string>();

        using var compactionListener = MeterListening.StartForInstrument(
            LatticeMetrics.WalCompactions,
            l => l.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                // Loading a shard primes every arm with a measured zero; only a
                // real increment is evidence of a rewrite.
                if (measurement > 0 && MatchesTree(tags) && TagOf(tags, LatticeMetrics.TagTrigger) is { } trigger)
                {
                    lock (compactions)
                    {
                        compactions.Add(trigger);
                    }
                }
            }));

        using var stopListener = MeterListening.StartForInstrument(
            LatticeMetrics.WalGcTrimStops,
            l => l.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                if (measurement > 0 && MatchesTree(tags) && TagOf(tags, LatticeMetrics.TagReason) is { } reason)
                {
                    lock (stops)
                    {
                        stops.Add(reason);
                    }
                }
            }));

        var report = await CollectorFor(sut, checkpointOffset).RunOnceAsync(TreeId);

        compactionListener.Dispose();
        stopListener.Dispose();
        return (report, compactions, stops);
    }

    private static LatticeWalGc CollectorFor(FileWalStorageProvider provider, long checkpointOffset)
    {
        var registry = new InMemoryWalCursorRegistry();

        // The consumer cursors sit far above every seeded entry, so HLC
        // eligibility never stops the scan and the offset floor is the only
        // thing that can.
        registry.ReportCursorAsync(TreeId, "shipper", Hlc(1_000_000)).GetAwaiter().GetResult();
        registry.ReportCursorAsync(TreeId, LeafConsumer, Hlc(1_000_000)).GetAwaiter().GetResult();

        var pinGrain = Substitute.For<IWalMaterialiserPinGrain>();
        pinGrain
            .GetPinsAsync()
            .Returns(Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
                new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
                {
                    [LeafConsumer] = Hlc(1_000_000),
                }));
        pinGrain
            .GetPinOffsetsAsync()
            .Returns(Task.FromResult<IReadOnlyDictionary<string, long>>(
                new Dictionary<string, long>(StringComparer.Ordinal) { [LeafConsumer] = checkpointOffset }));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);

        var services = new ServiceCollection();
        services.AddSingleton<IWalStorageProvider>(provider);
        services.AddSingleton(factory);

        var options = new LatticeOptions { WalPartitions = 1 };
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);

        return new LatticeWalGc(services.BuildServiceProvider(), registry, monitor);
    }

    private static async Task<List<WalEntry>> ReadAllAsync(FileWalStorageProvider sut)
    {
        var entries = new List<WalEntry>();
        await foreach (var entry in sut.ReadAsync(TreeId, 0, -1, int.MaxValue, CancellationToken.None))
        {
            entries.Add(entry);
        }

        return entries;
    }

    private static bool MatchesTree(ReadOnlySpan<KeyValuePair<string, object?>> tags)
        => string.Equals(TagOf(tags, LatticeMetrics.TagTree), TreeId, StringComparison.Ordinal);

    private static string? TagOf(ReadOnlySpan<KeyValuePair<string, object?>> tags, string key)
    {
        foreach (var tag in tags)
        {
            if (string.Equals(tag.Key, key, StringComparison.Ordinal))
            {
                return tag.Value as string;
            }
        }

        return null;
    }

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

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
                Timestamp = Hlc(10 + offset),
                OriginClusterId = "site-a",
            },
        };
    }

    private FileWalStorageProvider CreateProvider(int compactionMinimumDeadBytes)
    {
        var options = Options.Create(new FileWalStorageOptions
        {
            RootDirectory = _root,
            FlushToDisk = false,
            CompactionMinimumDeadBytes = compactionMinimumDeadBytes,
        });
        return new FileWalStorageProvider(options, _serializer);
    }
}
