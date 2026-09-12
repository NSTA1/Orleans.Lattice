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

    /// <summary>
    /// The physical shard count P is a <b>lower bound from observation</b>, and it has
    /// moved twice: this workstream first assumed 16, then read shard 62 out of a
    /// deployed log, then read shard 63 (observed 2026-09-09 in boot-2 logs of the
    /// repo-context-vector-metadata tree), which puts the floor at P &gt;= 64. Any
    /// threshold written against one day's best guess is wrong the next time a higher
    /// shard id appears, so the diagnostic reports the map's own
    /// <see cref="EmbeddingRepoContextVectorIngestor.GapShardDistribution.PhysicalShardCount"/>
    /// and the criterion is computed from it rather than baked in.
    /// <para>
    /// E[D] = P * (1 - (1 - 1/P)^K) is monotonically increasing in P, so a larger P
    /// raises expected occupancy and therefore RAISES the bar for concluding
    /// "concentrated". The direction is not harmless, which is why this asserts the
    /// monotonicity rather than assuming it.
    /// </para>
    /// </summary>
    [Test]
    public void The_occupancy_criterion_is_computed_from_the_reported_P_and_does_not_flip_across_its_plausible_range()
    {
        const int K = 43;
        int[] plausibleP = [16, 32, 63, 64, 128, 256];

        var previousExpected = double.NegativeInfinity;

        Assert.Multiple(() =>
        {
            foreach (var p in plausibleP)
            {
                var summary = EmbeddingRepoContextVectorIngestor.SummariseGapShardDistribution(
                    RepoId, Entries(K), ShardMap.CreateDefault(4096, p));

                var expected = p * (1 - Math.Pow(1 - (1.0 / p), K));

                // The instrument reports P, so a reader can recompute the threshold for
                // whatever P the deployment actually had. Nothing is assumed.
                Assert.That(
                    summary.PhysicalShardCount,
                    Is.EqualTo(p),
                    "the diagnostic must report the map's own P so the threshold is derived, not assumed");

                // Monotone in P: a bigger P means more expected distinct shards.
                Assert.That(
                    expected,
                    Is.GreaterThan(previousExpected),
                    $"E[D] must increase with P; it did not at P={p}");
                previousExpected = expected;

                // The verdict under the null must be the SAME at every P in range:
                // a hash-scattered set is never reported as occupying a single shard,
                // and never as occupying all K. If either flipped anywhere in this
                // range, an observed value could not be read without knowing P exactly.
                Assert.That(
                    summary.DistinctShards,
                    Is.GreaterThan(1),
                    $"a scattered set must not read as concentrated at P={p}");
                Assert.That(
                    summary.DistinctShards,
                    Is.LessThanOrEqualTo(Math.Min(K, p)),
                    $"D cannot exceed min(K, P) at P={p}");

                // And the unsound bare-percentage criterion fires on the null at EVERY
                // P in the range, not merely at the P this workstream happened to guess.
                // That is the whole reason a bare percentage was rejected.
                Assert.That(
                    summary.DistinctShards,
                    Is.LessThan(K),
                    $"D < K under the null at P={p}, so 'far fewer than K' is unsound here too");
            }
        });
    }

    /// <summary>
    /// The empirical result issue #2287 was opened to obtain, pinned as an executable
    /// artifact so it cannot be lost with the log that produced it.
    /// <para>
    /// These are the real gap-set paths sampled from two deployed passes (2026-09-09,
    /// passes 1 and 2 of the pre-window run). Every one of them hashes to a
    /// <b>distinct</b> virtual slot, and every one of those slots is congruent to 32
    /// modulo 64, so under any contiguous virtual-to-physical map they collapse onto a
    /// single physical shard. That is the exact signature the virtual-slot proxy would
    /// have hidden: the proxy sees 20 distinct slots and reports "scattered", which
    /// would have been a false refutation of the hash-partitioned candidate.
    /// </para>
    /// <para>
    /// The contrast is what carries the inference, so the control is asserted in the
    /// same test: an equally path-ordered sample of ordinary repository paths spreads
    /// across the whole residue space. Note the clustering claim is map-independent
    /// (it is a property of the keys' slot residues); only naming the shard "32"
    /// depends on the default contiguous map.
    /// </para>
    /// </summary>
    [Test]
    public void The_deployed_gap_set_shares_one_physical_shard_while_occupying_distinct_virtual_slots()
    {
        string[] deployedGapSample =
        [
            "benchmark/host/Bench.Microbench/Orleans.Lattice.Benchmark.Microbench.csproj",
            "docs/lattice/bulk-loading.md",
            "src/lattice.api.abstractions/TreeAdmin/Model/LatticeTreeAdminCapabilities.cs",
            "src/lattice.api.mcp.repocontext/Source/IRepoContextSourceScanner.cs",
            "src/lattice.api.mcp.telemetry/LatticeMcpTelemetryServiceCollectionExtensions.cs",
            "src/lattice.api.tenantadmin/TenantAdminAccessAuthorizer.cs",
            "src/lattice.explorer/DesignSystem/Layout/LatticeAdaptiveContext.cs",
            "src/lattice.explorer/Plugins/Tenants/Views/TenantRegionView.razor",
            "src/lattice.grainindex/GrainIndexOptionsValidator.cs",
            "src/lattice.scaling/LatticeScalingEndpointRouteBuilderExtensions.cs",
            "src/lattice.storage.azuretable/AzureTableWalEntity.cs",
            "src/lattice.vector/Persistence/VectorIndexManifest.cs",
            "src/lattice/BPlusTree/Grains/LatticeGrain.Idempotency.cs",
            "src/lattice/BPlusTree/State/LeafSnapshotBlob.cs",
            "src/lattice/Crdt/CrdtMemberValue.cs",
            "src/lattice/Crdt/MvRegisterProvenanceDecoder.cs",
            "src/lattice.api.treeadmin.grpc/Model/TreeAdminResizeRequest.cs",
            "src/lattice/LatticeAuthorizationDeniedException.cs",
            "src/lattice/LatticeIdempotencyContext.cs",
            "src/lattice/Views/RuntimeViewProjectionProviderCatalog.cs",
        ];

        // The deployed repository indexes itself under this id, and the id is part of
        // the hashed key, so the residue only reproduces under the real one.
        const string DeployedRepoId = "lattice";
        const int P = 64;

        static string KeyFor(string repoId, string relativePath)
            => RepoContextKeys.VectorMembership(
                repoId, VectorCodec.SourceId(RepoContextKeys.File(repoId, relativePath)));

        var gapKeys = deployedGapSample.Select(p => KeyFor(DeployedRepoId, p)).ToArray();
        var gapSlots = gapKeys.Select(k => ShardMap.GetVirtualSlot(k, 4096)).ToArray();
        var map = ShardMap.CreateDefault(4096, P);
        var gapShards = gapKeys.Select(map.Resolve).Distinct().ToArray();

        // The control: an equally path-ordered sample of ordinary paths from the same
        // repository. Without this contrast a single-shard result could just as well be
        // a property of every membership key, which would carry no information at all.
        var controlPaths = Enumerable.Range(0, 400)
            .Select(i => $"src/lattice/Generated/Control{i:D4}.cs")
            .ToArray();
        var controlResidues = controlPaths
            .Select(p => ShardMap.GetVirtualSlot(KeyFor(DeployedRepoId, p), 4096) % 64)
            .Distinct()
            .Count();

        Assert.Multiple(() =>
        {
            Assert.That(
                gapKeys.Distinct(StringComparer.Ordinal).Count(),
                Is.EqualTo(deployedGapSample.Length),
                "the sample must be distinct keys, or a single-shard result is a duplication artifact");

            Assert.That(
                gapSlots.Distinct().Count(),
                Is.EqualTo(deployedGapSample.Length),
                "every gap key occupies its OWN virtual slot, which is exactly why a "
                + "virtual-slot histogram reads as scattered and cannot discriminate");

            Assert.That(
                gapSlots.Select(v => v % 64).Distinct().ToArray(),
                Is.EqualTo(new[] { 32 }),
                "every deployed gap key shares the virtual-slot residue class 32 mod 64");

            Assert.That(
                gapShards,
                Is.EqualTo(new[] { 32 }),
                "so they collapse onto a single physical shard under the default map");

            Assert.That(
                controlResidues,
                Is.GreaterThan(32),
                "the control must spread across the residue space, or the gap result is "
                + "a property of all membership keys rather than of the gap set");
        });
    }
}
