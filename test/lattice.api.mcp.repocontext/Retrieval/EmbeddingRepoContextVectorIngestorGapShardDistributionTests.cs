namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for
/// <see cref="EmbeddingRepoContextVectorIngestor.SummariseGapShardDistribution"/>, the
/// physical-shard diagnostic issue #2287 turns on. Issue #2208 established that the
/// membership writes for a fixed subset of sources return success and are then not
/// observable to the next pass, and left two candidates: the stranded set is
/// hash-partitioned onto particular physical shards, or the write has a visibility
/// property unrelated to where it lands. This summary is the instrument that separates
/// them, so these tests pin the properties any conclusion drawn from it depends on.
/// <para>
/// The load-bearing one is <see cref="Resolution_is_physical_not_virtual"/>. Routing is
/// two-stage - hash into one of ~4096 virtual slots, then map the slot onto a physical
/// shard - and with ~64 virtual slots per physical shard, keys that share a physical
/// shard still occupy distinct virtual slots. A virtual-slot histogram therefore reads
/// as "scattered" under BOTH candidates, so substituting one would not merely be weaker
/// evidence, it would be a false refutation of the clustering candidate. That test
/// constructs a map under which the two stages give different answers and pins that the
/// summary reports the physical one.
/// </para>
/// </summary>
/// <remarks>
/// Pure in-process function test: it builds <see cref="ShardMap"/> and
/// <see cref="RepoFileEntry"/> values directly and calls the static summariser,
/// standing up no silo and touching no store, so it needs no slow category.
/// </remarks>
[TestFixture]
public sealed class EmbeddingRepoContextVectorIngestorGapShardDistributionTests
{
    private const string RepoId = "acme";

    private static RepoFileEntry Entry(string relativePath)
        => new(relativePath, "digest-" + relativePath, relativePath.Length, "csharp");

    private static RepoFileEntry[] Entries(int count)
    {
        var entries = new RepoFileEntry[count];
        for (var i = 0; i < count; i++)
        {
            entries[i] = Entry($"src/pkg/File{i:D4}.cs");
        }

        return entries;
    }

    /// <summary>
    /// The membership key the diagnostic must resolve, derived here independently of
    /// the production helper's call sequence so the tests check the derivation rather
    /// than restate it.
    /// </summary>
    private static string MembershipKeyFor(string relativePath)
        => RepoContextKeys.VectorMembership(
            RepoId, VectorCodec.SourceId(RepoContextKeys.File(RepoId, relativePath)));

    /// <summary>
    /// A map that sends every virtual slot to one physical shard, so the physical
    /// answer is knowable without reimplementing the hash.
    /// </summary>
    private static ShardMap SingleShardMap(int virtualShardCount, int physicalShard)
    {
        var slots = new int[virtualShardCount];
        Array.Fill(slots, physicalShard);
        return new ShardMap { Slots = slots, Version = 7 };
    }

    [Test]
    public void An_empty_gap_set_reports_zeroes_and_does_not_throw()
    {
        var summary = EmbeddingRepoContextVectorIngestor.SummariseGapShardDistribution(
            RepoId, Array.Empty<RepoFileEntry>(), ShardMap.CreateDefault(4096, 64));

        Assert.Multiple(() =>
        {
            Assert.That(summary.Sources, Is.Zero);
            Assert.That(summary.DistinctShards, Is.Zero);
            Assert.That(summary.LargestShardGroup, Is.Zero);
            Assert.That(summary.GroupDetail, Is.Empty);
            Assert.That(summary.SourceDetail, Is.Empty);

            // The tree's own parameters are still reported: P is what parameterises the
            // null the statistics are judged against, so it must be present even on a
            // pass that measured nothing.
            Assert.That(summary.PhysicalShardCount, Is.EqualTo(64));
            Assert.That(summary.VirtualShardCount, Is.EqualTo(4096));
        });
    }

    [Test]
    public void Resolution_is_physical_not_virtual()
    {
        // The whole point of the diagnostic. Every key is sent to physical shard 5
        // regardless of which virtual slot it hashes into, so a summary that reported
        // virtual slots would show many distinct groups here and a summary that
        // correctly applies both routing stages shows exactly one.
        var files = Entries(40);
        var map = SingleShardMap(virtualShardCount: 4096, physicalShard: 5);

        // Control: these keys really do occupy many DISTINCT virtual slots, so the
        // single group below is the map collapsing them and not an artefact of forty
        // keys happening to hash alike. Without this the test would pass vacuously.
        var distinctVirtualSlots = files
            .Select(f => ShardMap.GetVirtualSlot(MembershipKeyFor(f.RelativePath), 4096))
            .Distinct()
            .Count();
        Assert.That(
            distinctVirtualSlots,
            Is.GreaterThan(1),
            "the fixture's keys must scatter across virtual slots or the test proves nothing");

        var summary = EmbeddingRepoContextVectorIngestor.SummariseGapShardDistribution(
            RepoId, files, map);

        Assert.Multiple(() =>
        {
            Assert.That(summary.Sources, Is.EqualTo(40));
            Assert.That(summary.DistinctShards, Is.EqualTo(1), "every key routes to physical shard 5");
            Assert.That(summary.LargestShardGroup, Is.EqualTo(40));
            Assert.That(summary.GroupDetail, Is.EqualTo("5:40"));
        });
    }

    [Test]
    public void The_statistics_cover_the_whole_gap_set_and_not_a_prefix()
    {
        // D and M are the measurement, so the counting arm must never truncate: a
        // prefix would bias both downward and manufacture apparent clustering. The
        // previous diagnostic sampled the ordinal-first 16, which is exactly the bound
        // this must not inherit.
        var files = Entries(200);
        var map = ShardMap.CreateDefault(4096, 64);

        var summary = EmbeddingRepoContextVectorIngestor.SummariseGapShardDistribution(
            RepoId, files, map);

        Assert.Multiple(() =>
        {
            Assert.That(summary.Sources, Is.EqualTo(200), "K counts every gap source");

            // Independently recomputed group total, to check the summary counted the
            // whole set rather than reporting a plausible number.
            var expectedDistinct = files
                .Select(f => map.Resolve(MembershipKeyFor(f.RelativePath)))
                .Distinct()
                .Count();
            Assert.That(summary.DistinctShards, Is.EqualTo(expectedDistinct));

            var expectedLargest = files
                .GroupBy(f => map.Resolve(MembershipKeyFor(f.RelativePath)))
                .Max(g => g.Count());
            Assert.That(summary.LargestShardGroup, Is.EqualTo(expectedLargest));
        });
    }

    [Test]
    public void The_enumerated_detail_is_bounded_and_reports_the_total_it_was_drawn_from()
    {
        // The bound is the point: this workstream has already filed three bugs caused
        // by an unbounded or wrongly-bounded loop. A cap that hid the total would be
        // the fourth, because a truncated enumeration would be indistinguishable from a
        // complete one.
        var files = Entries(500);
        var map = ShardMap.CreateDefault(4096, 64);

        var summary = EmbeddingRepoContextVectorIngestor.SummariseGapShardDistribution(
            RepoId, files, map, maxReportedGroups: 4, maxReportedSources: 3);

        Assert.Multiple(() =>
        {
            Assert.That(summary.ReportedGroups, Is.EqualTo(4));
            Assert.That(summary.ReportedSources, Is.EqualTo(3));
            Assert.That(summary.GroupDetail.Split(',').Length, Is.EqualTo(4));
            Assert.That(summary.SourceDetail.Split(',').Length, Is.EqualTo(3));

            // The totals are untruncated, so the log line can show that it truncated.
            Assert.That(summary.Sources, Is.EqualTo(500));
            Assert.That(summary.DistinctShards, Is.GreaterThan(4));
        });
    }

    [Test]
    public void Groups_are_ordered_densest_first_so_the_largest_group_is_readable_from_the_detail()
    {
        // M is one of the two pre-registered statistics, so the enumeration must lead
        // with the group it names; a bounded list ordered any other way could omit the
        // very group the rejection rule turns on.
        var slots = new int[16];
        for (var i = 0; i < slots.Length; i++)
        {
            slots[i] = i % 4;
        }

        var map = new ShardMap { Slots = slots, Version = 1 };
        var files = Entries(120);

        var summary = EmbeddingRepoContextVectorIngestor.SummariseGapShardDistribution(
            RepoId, files, map, maxReportedGroups: 2, maxReportedSources: 0);

        var counts = summary.GroupDetail
            .Split(", ", StringSplitOptions.RemoveEmptyEntries)
            .Select(g => int.Parse(g.Split(':')[1], System.Globalization.CultureInfo.InvariantCulture))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(counts, Has.Length.EqualTo(2));
            Assert.That(counts[0], Is.GreaterThanOrEqualTo(counts[1]), "densest first");
            Assert.That(counts[0], Is.EqualTo(summary.LargestShardGroup), "M leads the enumeration");
        });
    }

    [Test]
    public void The_map_version_and_shard_counts_are_reported_so_the_null_is_recomputable()
    {
        // Every term of E[D] = P * (1 - (1 - 1/P)^K) must be readable from the log, or
        // the threshold gets assumed instead of recomputed - which is the failure mode
        // #2287 records as more dangerous than not pre-registering at all. MapVersion
        // is what invalidates a comparison across passes if the tree was remapped.
        var map = ShardMap.CreateDefault(4096, 63);
        map.Version = 42;

        var summary = EmbeddingRepoContextVectorIngestor.SummariseGapShardDistribution(
            RepoId, Entries(43), map);

        Assert.Multiple(() =>
        {
            Assert.That(summary.PhysicalShardCount, Is.EqualTo(63), "P");
            Assert.That(summary.Sources, Is.EqualTo(43), "K");
            Assert.That(summary.VirtualShardCount, Is.EqualTo(4096));
            Assert.That(summary.MapVersion, Is.EqualTo(42));
        });
    }

    [Test]
    public void A_proportional_set_is_not_reported_as_clustered()
    {
        // The guard against the corrected pre-registration error. Independent hashing
        // of K=43 keys over P=63 shards occupies about 31 distinct shards, NOT 43, so a
        // criterion of "far fewer than K" fires on the null and would confirm the
        // clustering candidate when nothing is wrong. This pins that the summary
        // reports the honest occupancy rather than anything that flatters that reading.
        const int K = 43;
        const int P = 63;

        var summary = EmbeddingRepoContextVectorIngestor.SummariseGapShardDistribution(
            RepoId, Entries(K), ShardMap.CreateDefault(4096, P));

        var expectedOccupancy = P * (1 - Math.Pow(1 - (1.0 / P), K));

        Assert.Multiple(() =>
        {
            Assert.That(expectedOccupancy, Is.EqualTo(31.3).Within(0.1), "the closed form, restated here");

            // A hash-scattered set of this size lands near the occupancy expectation and
            // nowhere near K. The window is wide because this is a single draw, not a
            // distributional claim; the point is only that D is far below K under the
            // null, which is what makes a bare "far fewer than K" criterion unsound.
            Assert.That(summary.DistinctShards, Is.LessThan(K));
            Assert.That(summary.DistinctShards, Is.EqualTo(31).Within(8));
            Assert.That(summary.LargestShardGroup, Is.LessThan(K));
        });
    }
}
