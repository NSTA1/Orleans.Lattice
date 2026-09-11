using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Archive;

/// <summary>
/// A restore drill: exports a realistic memory tree, restores it into a separate
/// store, and verifies the result by CONTENT rather than by count.
/// <para>
/// <b>Why by content.</b> Counts match trivially, and every check in this area that
/// only counted has passed on an archive nobody had read. Memory values are Orleans
/// records base64-encoded inside a multi-value register JSON envelope, so a plaintext
/// scan of a snapshot finds every key and, necessarily, no body ever - a check with
/// exactly one reachable outcome, which can therefore never distinguish a good archive
/// from a shredded one.
/// </para>
/// <para>
/// <b>Why it seeds its own snapshot.</b> An earlier form of this drill read the live
/// container's archive from a host path. A fixture that depends on a host artefact
/// silently no-ops wherever the artefact is absent, which is the same family of defect
/// it exists to catch. Seeding makes it run identically everywhere.
/// </para>
/// <para>
/// <b>Why it seeds through the production accessor.</b> That is the path production
/// writes through. A raw store write leaves a record un-enveloped - a shape production
/// never emits - so a control built on one cannot produce the input the real case
/// produces and cannot exclude anything about it. See issue #2641.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextMemoryRestoreDrillTests
{
    private const string RepoId = "lattice";

    private string scratch = string.Empty;

    private static CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    /// <summary>The body of the longest seeded entry, whose tail is the truncation discriminator.</summary>
    private static string LongBody => new string('x', 4000) + "-END-OF-LONGEST-ENTRY";

    [SetUp]
    public void SetUp()
    {
        scratch = Path.Combine(Path.GetTempPath(), $"lattice-restore-drill-{Guid.NewGuid():N}");
        Directory.CreateDirectory(scratch);
    }

    [TearDown]
    public void TearDown()
    {
        try
        {
            if (Directory.Exists(scratch))
            {
                Directory.Delete(scratch, recursive: true);
            }
        }
        catch (IOException)
        {
            // A leftover scratch directory is not a test failure.
        }
    }

    private static RepoContextMemoryArchiveOptions Options(
        string directory,
        RepoContextMemoryArchiveRestoreMode mode = RepoContextMemoryArchiveRestoreMode.Auto)
        => new() { Directory = directory, RestoreMode = mode };

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks };

    /// <summary>
    /// Writes one memory record through the production capture path, so the store holds
    /// the enveloped shape a real store holds.
    /// </summary>
    private static async Task WriteAsync(
        ILattice tree,
        Serializer serializer,
        string topic,
        string id,
        string body,
        string replicaId = "local",
        long clock = 1_000,
        IReadOnlyList<string>? tags = null,
        IReadOnlyList<string>? links = null)
    {
        var tagSet = new OrSet();
        foreach (var tag in tags ?? [])
        {
            tagSet.Add(Encoding.UTF8.GetBytes(tag), Guid.NewGuid().ToString("N"), 0L);
        }

        var linkMap = new OrMap<string, OrSet>();
        if (links is { Count: > 0 })
        {
            var targets = new OrSet();
            foreach (var link in links)
            {
                targets.Add(Encoding.UTF8.GetBytes(link), Guid.NewGuid().ToString("N"), 0L);
            }

            linkMap.Set("related", Guid.NewGuid().ToString("N"), targets);
        }

        var record = new MemoryRecord
        {
            RepoId = RepoId,
            Topic = topic,
            Id = id,
            Kind = MemoryKind.Note,
            Title = RepoContextValues.Lww($"title-{id}", Clock(clock)),
            Body = RepoContextValues.Lww(body, Clock(clock)),
            Author = RepoContextValues.Lww("drill", Clock(clock)),
            Tags = tagSet,
            Links = linkMap,
        };

        await RepoContextMemoryCodec
            .Accessor(tree, RepoContextKeys.Memory(RepoId, topic, id))
            .SetAsync(replicaId, serializer.SerializeToArray(record), Ct);
    }

    /// <summary>
    /// Seeds a store that resembles a real memory tree: several topics, one entry far
    /// longer than the rest (so a truncation has a tail to lose), one entry written
    /// concurrently by two replicas (so the register carries concurrent values and the
    /// decode path has to merge rather than take the first), and knowledge-link edges.
    /// </summary>
    private static async Task<int> SeedAsync(ILattice tree, Serializer serializer)
    {
        for (var i = 0; i < 40; i++)
        {
            await WriteAsync(
                tree, serializer, "gotchas", $"gotcha-{i:D3}", $"body of gotcha {i}",
                tags: ["epic-2368", "drill"]);
        }

        for (var i = 0; i < 12; i++)
        {
            await WriteAsync(
                tree, serializer, "conventions", $"convention-{i:D3}", $"body of convention {i}",
                links: [$"repo/{RepoId}/mem/gotchas/gotcha-000"]);
        }

        await WriteAsync(tree, serializer, "decisions", "longest", LongBody);

        // The in-place CRDT merge. Two replicas write the same key, so the register
        // holds two concurrent values and the fold has to reduce them through the
        // record's own merge. A reader that takes the first value passes every
        // count-based check and silently drops the later edit.
        await WriteAsync(tree, serializer, "decisions", "merged", "first writer body", "alpha", 1_000);
        await WriteAsync(tree, serializer, "decisions", "merged", "second writer body", "beta", 2_000);

        return 40 + 12 + 1 + 1;
    }

    /// <summary>
    /// Reads every memory record back out of the tree through the supported decoder.
    /// </summary>
    private static async Task<(Dictionary<string, MemoryRecord> Records, List<string> Undecodable)>
        ReadAllAsync(ILattice tree, Serializer serializer)
    {
        var records = new Dictionary<string, MemoryRecord>(StringComparer.Ordinal);
        var undecodable = new List<string>();
        string? continuation = null;

        do
        {
            var page = await RepoContextPortability.EnumerateAsync(
                tree, RepoContextKeys.AllReposPrefix(), continuation, 500, null, Ct);

            foreach (var record in page.Records)
            {
                var stored = await tree.GetAsync(record.Key, Ct);
                try
                {
                    var folded = RepoContextMemoryCodec.Fold(stored, serializer);
                    if (folded is null)
                    {
                        undecodable.Add($"{record.Key} [folded to null]");
                        continue;
                    }

                    records[record.Key] = folded;
                }
                catch (Exception ex)
                {
                    undecodable.Add($"{record.Key} {ex.GetType().Name}");
                }
            }

            continuation = page.ContinuationToken;
        }
        while (!string.IsNullOrEmpty(continuation));

        return (records, undecodable);
    }

    private async Task<(string Directory, int Seeded, string TreeName)> ExportSeededArchiveAsync(
        RepoContextMcpHarness harness, Serializer serializer, string directory)
    {
        Directory.CreateDirectory(directory);
        var sourceName = $"repocontext-drill-src-{Guid.NewGuid():N}";
        var source = harness.GrainFactory.GetGrain<ILattice>(sourceName);
        var seeded = await SeedAsync(source, serializer);
        var export = await new RepoContextMemoryArchive(Options(directory))
            .ExportAsync(source, serializer, Ct);

        Assert.That(
            export.Outcome, Is.EqualTo(RepoContextMemoryArchiveExportOutcome.Written),
            "The drill cannot proceed without an exported snapshot.");
        Assert.That(export.RecordCount, Is.EqualTo(seeded), "Export must carry every seeded record.");
        return (directory, seeded, sourceName);
    }

    private async Task<string> TruncateAsync(string directory, double fraction)
    {
        // Cut well past the header, so the file still looks like a valid snapshot and
        // still carries many complete records. A cut that removed the header would be
        // caught by anything at all and would prove nothing.
        var damaged = Path.Combine(scratch, $"damaged-{Guid.NewGuid():N}");
        Directory.CreateDirectory(damaged);
        var full = await File.ReadAllBytesAsync(
            Path.Combine(directory, RepoContextMemoryArchive.SnapshotFileName), Ct);
        await File.WriteAllBytesAsync(
            Path.Combine(damaged, RepoContextMemoryArchive.SnapshotFileName),
            full[..(int)(full.Length * fraction)],
            Ct);
        return damaged;
    }

    private static ILattice Target(RepoContextMcpHarness harness)
        => harness.GrainFactory.GetGrain<ILattice>($"repocontext-drill-dst-{Guid.NewGuid():N}");

    [Test]
    public async Task A_restored_archive_reproduces_its_contents_not_merely_its_count()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var (directory, seeded, _) = await ExportSeededArchiveAsync(harness, serializer, scratch);

        var target = Target(harness);
        var restore = await new RepoContextMemoryArchive(Options(directory))
            .RestoreAsync(target, serializer, Ct);

        var (records, undecodable) = await ReadAllAsync(target, serializer);
        TestContext.WriteLine(
            $"RESTORED   outcome={restore.Outcome} read={restore.RecordsRead} "
            + $"inStore={restore.RecordsInStore} decoded={records.Count} undecoded={undecodable.Count}");

        Assert.Multiple(() =>
        {
            Assert.That(restore.Outcome, Is.EqualTo(RepoContextMemoryRestoreOutcome.Restored));
            Assert.That(restore.RecordsRead, Is.EqualTo(seeded));
            Assert.That(undecodable, Is.Empty, "Every restored record must decode.");
            Assert.That(records, Has.Count.EqualTo(seeded));

            // Content, not count. Every body must be non-empty: a restore that wrote
            // the right number of empty shells passes a count check.
            Assert.That(
                records.Values.Where(r => string.IsNullOrEmpty(RepoContextValues.ReadString(r.Body))),
                Is.Empty,
                "Every restored record must carry a non-empty body.");

            // The tail of the longest entry. A truncation loses the end of the largest
            // record first, so the tail is the part a length-based check cannot cover.
            var longest = records.Values
                .OrderByDescending(r => RepoContextValues.ReadString(r.Body)?.Length ?? 0)
                .First();
            Assert.That(longest.Id, Is.EqualTo("longest"));
            Assert.That(RepoContextValues.ReadString(longest.Body), Is.EqualTo(LongBody));
            Assert.That(
                RepoContextValues.ReadString(longest.Body), Does.EndWith("-END-OF-LONGEST-ENTRY"));

            // The entry two replicas wrote concurrently. Its register carries two
            // values, so this asserts the decode path merged rather than taking the
            // first - which no count-based check can see.
            var merged = records[RepoContextKeys.Memory(RepoId, "decisions", "merged")];
            Assert.That(
                RepoContextValues.ReadString(merged.Body), Is.EqualTo("second writer body"),
                "The later concurrent write must win the merge, so the fold reduced both values.");

            // Knowledge-link edges survive.
            var linked = records[RepoContextKeys.Memory(RepoId, "conventions", "convention-000")];
            Assert.That(linked.Links.Get("related"), Is.Not.Null, "Link edges must survive the round trip.");

            // Tags survive.
            var tagged = records[RepoContextKeys.Memory(RepoId, "gotchas", "gotcha-000")];
            Assert.That(tagged.Tags.Elements(), Is.Not.Empty, "Tags must survive the round trip.");
        });
    }

    [Test]
    public async Task A_truncated_archive_is_detected_rather_than_restored_as_if_intact()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var (directory, _, _) = await ExportSeededArchiveAsync(harness, serializer, scratch);
        var damaged = await TruncateAsync(directory, 0.6);

        var intact = await new RepoContextMemoryArchive(Options(directory))
            .RestoreAsync(Target(harness), serializer, Ct);
        var broken = await new RepoContextMemoryArchive(Options(damaged))
            .RestoreAsync(Target(harness), serializer, Ct);

        TestContext.WriteLine(
            $"INTACT     outcome={intact.Outcome} read={intact.RecordsRead} inStore={intact.RecordsInStore}");
        TestContext.WriteLine(
            $"TRUNCATED  outcome={broken.Outcome} read={broken.RecordsRead} inStore={broken.RecordsInStore}");

        // The check on the check. If the perturbation produced an identical result the
        // control cannot fire, and its pass would be an artefact of the harness rather
        // than a detection.
        Assert.That(
            (broken.Outcome, broken.RecordsRead, broken.RecordsInStore),
            Is.Not.EqualTo((intact.Outcome, intact.RecordsRead, intact.RecordsInStore)),
            "Truncating 40 percent of the snapshot produced an identical result to the intact "
            + "file, so this control cannot distinguish them and its pass would mean nothing.");

        Assert.Multiple(() =>
        {
            Assert.That(intact.Outcome, Is.EqualTo(RepoContextMemoryRestoreOutcome.Restored));
            Assert.That(
                broken.Outcome, Is.Not.EqualTo(RepoContextMemoryRestoreOutcome.Restored),
                "A truncated snapshot must never report a complete restore.");
            Assert.That(
                broken.RecordsInStore, Is.LessThan(intact.RecordsInStore),
                "The truncated import must land strictly fewer records than the intact one.");
        });
    }

    /// <summary>
    /// The regression for issue #2641. A restore that fails partway leaves records
    /// behind, so the store is no longer empty. Before the fix, Auto mode declined the
    /// retry because of the wreckage the first attempt left - the recovery action
    /// disarmed by the failure it was recovering from - and reported a populated store.
    /// </summary>
    [Test]
    public async Task A_retry_after_a_partial_restore_heals_the_tree_instead_of_declining()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var (directory, seeded, _) = await ExportSeededArchiveAsync(harness, serializer, scratch);
        var damaged = await TruncateAsync(directory, 0.6);

        // One tree, two attempts: the operational sequence. A bad restore, then the
        // operator replaces the archive with a good one and restarts.
        var tree = Target(harness);

        var first = await new RepoContextMemoryArchive(Options(damaged))
            .RestoreAsync(tree, serializer, Ct);
        var afterFirst = RepoContextMemoryRestoreState.Decode(
            await tree.GetAsync(RepoContextMemoryRestoreState.Key, Ct));
        TestContext.WriteLine(
            $"ATTEMPT 1  outcome={first.Outcome} read={first.RecordsRead} inStore={first.RecordsInStore} "
            + $"marker={afterFirst?.Outcome.ToString() ?? "<absent>"}");

        var second = await new RepoContextMemoryArchive(Options(directory))
            .RestoreAsync(tree, serializer, Ct);
        var (records, undecodable) = await ReadAllAsync(tree, serializer);
        TestContext.WriteLine(
            $"ATTEMPT 2  outcome={second.Outcome} read={second.RecordsRead} "
            + $"inStore={second.RecordsInStore} decoded={records.Count}");

        Assert.Multiple(() =>
        {
            // The first attempt must report the partial rather than a bare failure,
            // and must report what actually landed rather than zero.
            Assert.That(
                first.Outcome, Is.EqualTo(RepoContextMemoryRestoreOutcome.Partial),
                "A failed import that wrote records must report Partial, not Failed.");
            Assert.That(
                first.RecordsInStore, Is.GreaterThan(0),
                "Precondition: the truncated import is expected to land records before failing.");
            Assert.That(
                first.RecordsInStore, Is.LessThan(seeded),
                "Precondition: the truncated import must not land the whole archive.");

            // The marker is the whole mechanism, so assert it landed rather than
            // inferring it from the retry succeeding. If the retry heals but this is
            // absent, the heal came from somewhere else and the test is not measuring
            // what it claims to measure.
            Assert.That(
                afterFirst, Is.Not.Null,
                "A restore that mutated the tree must leave a restore-state marker: without "
                + "one the retry has nothing to distinguish wreckage from legitimate state.");
            Assert.That(
                afterFirst!.Value.Outcome, Is.EqualTo(RepoContextMemoryRestoreOutcome.Partial));

            // The retry must heal rather than decline.
            Assert.That(
                second.Outcome, Is.EqualTo(RepoContextMemoryRestoreOutcome.Restored),
                "The retry with an intact archive must restore, not decline because the "
                + "failed attempt left the store non-empty.");
            Assert.That(records, Has.Count.EqualTo(seeded), "The healed tree must hold every record.");
            Assert.That(undecodable, Is.Empty);
        });
    }

    /// <summary>
    /// The other half of the marker's job: it must not turn Auto into Always. A store
    /// populated by ordinary use carries no partial marker, and restoring over it would
    /// replay an archived snapshot across live state, which can resurrect records that
    /// were deliberately forgotten.
    /// </summary>
    [Test]
    public async Task A_normally_populated_store_is_still_declined()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var (directory, _, _) = await ExportSeededArchiveAsync(harness, serializer, scratch);

        var tree = Target(harness);
        await WriteAsync(tree, serializer, "gotchas", "written-by-an-agent", "live body");

        var restore = await new RepoContextMemoryArchive(Options(directory))
            .RestoreAsync(tree, serializer, Ct);
        var (records, _) = await ReadAllAsync(tree, serializer);

        TestContext.WriteLine(
            $"DECLINED   outcome={restore.Outcome} inStore={restore.RecordsInStore} "
            + $"decoded={records.Count} reason={restore.Reason}");

        Assert.Multiple(() =>
        {
            Assert.That(
                restore.Outcome, Is.EqualTo(RepoContextMemoryRestoreOutcome.NothingToRestore),
                "A store populated by ordinary use carries no partial marker and must be declined.");
            Assert.That(
                records, Has.Count.EqualTo(1),
                "Declining must leave the live store untouched rather than merging the archive over it.");
            Assert.That(
                restore.RecordsInStore, Is.EqualTo(1),
                "The declined result must report what the store holds rather than zero.");
        });
    }

    [Test]
    public async Task The_restore_state_marker_is_neither_archived_nor_counted_as_memory()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var (directory, seeded, _) = await ExportSeededArchiveAsync(harness, serializer, scratch);

        var tree = Target(harness);
        var restore = await new RepoContextMemoryArchive(Options(directory))
            .RestoreAsync(tree, serializer, Ct);

        var marker = RepoContextMemoryRestoreState.Decode(
            await tree.GetAsync(RepoContextMemoryRestoreState.Key, Ct));

        // Re-export the restored tree and confirm the marker did not travel with it.
        var second = Path.Combine(scratch, "second");
        Directory.CreateDirectory(second);
        var export = await new RepoContextMemoryArchive(Options(second))
            .ExportAsync(tree, serializer, Ct);
        var held = await RepoContextMemoryArchive.CountMemoryAsync(tree, Ct);

        TestContext.WriteLine($"MARKER     {marker?.Outcome} records={marker?.Records}");
        TestContext.WriteLine($"REEXPORT   {export.Outcome} {export.RecordCount} records held={held}");

        Assert.Multiple(() =>
        {
            Assert.That(restore.Outcome, Is.EqualTo(RepoContextMemoryRestoreOutcome.Restored));
            Assert.That(marker, Is.Not.Null, "A completed restore must leave a marker.");
            Assert.That(marker!.Value.Outcome, Is.EqualTo(RepoContextMemoryRestoreOutcome.Restored));
            Assert.That(
                export.RecordCount, Is.EqualTo(seeded),
                "The marker must not be exported: it describes this store, and importing a "
                + "foreign store's completion stamp would let a partial tree present as complete.");
            Assert.That(held, Is.EqualTo(seeded), "The marker must not count as a memory record.");
        });
    }

    [Test]
    public void An_unreadable_marker_decodes_to_absent_rather_than_to_a_partial()
    {
        // Asymmetric on purpose. Reading a damaged marker as a partial would authorise
        // a restore over a store that may be legitimately populated, and importing an
        // archive over live memory can resurrect deliberately forgotten records.
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextMemoryRestoreState.Decode(null), Is.Null);
            Assert.That(RepoContextMemoryRestoreState.Decode([]), Is.Null);
            Assert.That(RepoContextMemoryRestoreState.Decode(Encoding.UTF8.GetBytes("garbage")), Is.Null);
            Assert.That(
                RepoContextMemoryRestoreState.Decode(Encoding.UTF8.GetBytes("v9|Restored|1|1|x")),
                Is.Null,
                "An unrecognised version must read as absent rather than be misparsed.");

            var round = new RepoContextMemoryRestoreState(
                RepoContextMemoryRestoreOutcome.Partial, 70, 12345, "snap");
            var decoded = RepoContextMemoryRestoreState.Decode(round.Encode());
            Assert.That(decoded, Is.EqualTo(round));
        });
    }
}
