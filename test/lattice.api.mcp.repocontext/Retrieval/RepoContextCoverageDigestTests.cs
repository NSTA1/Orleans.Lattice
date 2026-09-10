using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Integration tests for the per-page coverage digest (issue #2486): the record
/// that turns gap <b>detection</b> from O(sources) membership point-reads into a
/// fixed O(pages) read, and turns gap <b>repair</b> from a whole-repository pass
/// into a targeted one.
/// <para>
/// The two assertions this fixture exists for, stated plainly because either one
/// failing would leave a cheaper scan that is merely a cheaper way of finding
/// nothing:
/// </para>
/// <list type="number">
/// <item>A gap planted deliberately is <b>found</b> by the digest path, with the
/// identity of the uncovered source, not merely a boolean.</item>
/// <item>Detection cost is measured in <b>keys read</b> at two corpus sizes and is
/// flat in the corpus size, which a single measurement could never show - one
/// number is equally consistent with O(sources).</item>
/// </list>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo (the
/// membership and coverage trees) via <see cref="RepoContextMcpHarness"/>, so it is
/// excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextCoverageDigestTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    /// <summary>
    /// A counter over <b>every</b> tree, so one harness can be asked separately how
    /// many keys the membership tree served and how many the coverage tree did. The
    /// per-tree split is the whole point: issue #2486 requires the effect on the
    /// membership hotspot (issue #2071) reported in both directions.
    /// </summary>
    private static RepoContextMcpHarnessOptions CountingOptions()
    {
        var counter = new LatticeTreeCallCounter();
        return new RepoContextMcpHarnessOptions
        {
            Posture = RepoContextMcpAuthPosture.Writer,
            ConfigureSilo = silo =>
            {
                silo.Services.AddSingleton(counter);
                silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeCallCountingFilter>();
            },
        };
    }

    private static RepoContextVectorWriter Writer(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextVectorWriter>();

    private static LatticeTreeCallCounter Counter(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<LatticeTreeCallCounter>();

    private static string[] FileKeys(int count)
    {
        var keys = new string[count];
        for (var i = 0; i < count; i++)
        {
            keys[i] = RepoContextKeys.File(RepoId, $"src/File{i:d6}.cs");
        }

        return keys;
    }

    /// <summary>
    /// Seeds <paramref name="count"/> embedded sources and leaves the digest built
    /// and converged, which is the steady state detection actually runs against.
    /// </summary>
    private async Task<string[]> SeedCoveredAsync(RepoContextMcpHarness harness, int count)
    {
        var keys = FileKeys(count);
        await Writer(harness).AddMembersAsync(RepoId, keys, Ct);

        // The digest is seeded from membership on first load, then maintained on the
        // write path. Loading here means every measurement below runs against a built
        // digest rather than paying the one-off bootstrap.
        var digest = await Writer(harness).LoadCoverageDigestAsync(RepoId, Ct);
        Assert.That(digest.IsBuilt, Is.True, "The digest must be built before it is measured.");
        return keys;
    }

    [Test]
    public async Task The_digest_finds_a_planted_gap_and_names_it()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        var keys = await SeedCoveredAsync(harness, 64);

        // Plant the gap: this source is a known member of the corpus and is
        // deliberately NOT covered. Everything else is. A detection path that cannot
        // see this is not a detection path.
        var planted = RepoContextKeys.File(RepoId, "src/Planted.cs");
        var candidates = keys.Append(planted).ToArray();

        var digest = await Writer(harness).LoadCoverageDigestAsync(RepoId, Ct);
        var coverage = digest.ProjectOnto(candidates.Select(VectorCodec.SourceId));

        var missing = candidates
            .Where(key => !coverage.IsCovered(VectorCodec.SourceId(key)))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(digest.IsBuilt, Is.True);
            Assert.That(
                missing,
                Is.EqualTo(new[] { planted }),
                "The digest must report the planted gap AND report nothing else. A path "
                + "that reported every source missing would also 'find' the gap, and would "
                + "be worthless - so the negative half of this assertion is the load-bearing "
                + "one.");
        });
    }

    [Test]
    public async Task The_digest_scan_finds_a_planted_gap_across_the_whole_repository()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        var structural = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);

        var keys = await SeedCoveredAsync(harness, 32);
        foreach (var key in keys)
        {
            await structural.SetAsync(key, new byte[] { 1 }, Ct);
        }

        // A structural file record with no coverage: the exact shape of a vector that
        // never landed, which is what the sweep exists to find.
        var planted = RepoContextKeys.File(RepoId, "src/Planted.cs");
        await structural.SetAsync(planted, new byte[] { 1 }, Ct);

        var scanner = harness.Services.GetRequiredService<RepoContextEmbeddingGapScanner>();
        var scan = await scanner.ScanWithDigestAsync(RepoId, maxMissing: 256, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(scan.DigestAvailable, Is.True);
            Assert.That(scan.GapFound, Is.True);
            Assert.That(
                scan.MissingFileKeys,
                Is.EqualTo(new[] { planted }),
                "The scan reports the identity of the uncovered file, which is what makes "
                + "the repair targeted rather than a whole-repository pass.");
            Assert.That(scan.FilesConsidered, Is.EqualTo(keys.Length + 1));
            Assert.That(scan.Truncated, Is.False);
        });
    }

    [Test]
    public async Task The_digest_scan_reports_no_gap_when_every_file_is_covered()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        var structural = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);

        var keys = await SeedCoveredAsync(harness, 32);
        foreach (var key in keys)
        {
            await structural.SetAsync(key, new byte[] { 1 }, Ct);
        }

        var scanner = harness.Services.GetRequiredService<RepoContextEmbeddingGapScanner>();
        var scan = await scanner.ScanWithDigestAsync(RepoId, maxMissing: 256, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(scan.DigestAvailable, Is.True);
            Assert.That(scan.GapFound, Is.False, "This is the negative control for the planted-gap test.");
            Assert.That(scan.MissingFileKeys, Is.Empty);
        });
    }

    [Test]
    public async Task Detection_cost_is_flat_in_the_corpus_size_while_the_probe_is_linear()
    {
        // Two sizes, deliberately. A single measurement showing "257 rows" is equally
        // consistent with O(sources) at that one corpus size; only the ratio across
        // two sizes distinguishes the two cost models.
        var small = await MeasureDetectionAsync(sources: 100);
        var large = await MeasureDetectionAsync(sources: 800);

        TestContext.Out.WriteLine(
            $"corpus=100 sources, pages={RepoContextCoveragePage.PageCount}: "
            + $"probe reads {small.ProbeMembershipKeys} membership keys, "
            + $"digest reads {small.DigestCoverageKeys} coverage keys "
            + $"and {small.DigestMembershipKeys} membership keys.");
        TestContext.Out.WriteLine(
            $"corpus=800 sources, pages={RepoContextCoveragePage.PageCount}: "
            + $"probe reads {large.ProbeMembershipKeys} membership keys, "
            + $"digest reads {large.DigestCoverageKeys} coverage keys "
            + $"and {large.DigestMembershipKeys} membership keys.");

        Assert.Multiple(() =>
        {
            Assert.That(
                small.ProbeMembershipKeys,
                Is.EqualTo(200),
                "The baseline is two membership point-reads per source: the embedded flag "
                + "and the contentless marker.");
            Assert.That(
                large.ProbeMembershipKeys,
                Is.EqualTo(1600),
                "Eight times the corpus, eight times the reads - the O(sources) cost this "
                + "item exists to remove, measured rather than asserted.");

            Assert.That(
                small.DigestCoverageKeys,
                Is.EqualTo(large.DigestCoverageKeys),
                "Detection reads the same fixed row count at both corpus sizes. This "
                + "equality, not either value on its own, is the O(pages) evidence.");
            Assert.That(
                small.DigestCoverageKeys,
                Is.EqualTo(RepoContextCoveragePage.PageCount + 1),
                "256 pages plus the built-state marker.");

            Assert.That(
                small.DigestMembershipKeys,
                Is.Zero,
                "The read-path saving lands where issue #2071 needs it: detection now "
                + "touches the membership tree not less, but NOT AT ALL.");
            Assert.That(large.DigestMembershipKeys, Is.Zero);
        });
    }

    private sealed record DetectionCost(
        int ProbeMembershipKeys, int DigestCoverageKeys, int DigestMembershipKeys);

    private async Task<DetectionCost> MeasureDetectionAsync(int sources)
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        var keys = await SeedCoveredAsync(harness, sources);
        var counter = Counter(harness);

        // BASELINE: the existing whole-set point probe, measured on this exact corpus
        // in this exact process, so the comparison is not against a remembered figure.
        //
        // Measured in READ keys specifically. A raw per-tree call tally also picks up
        // shard-routing lookups and a saga's inner write, neither of which is a read
        // and neither of which scales with the corpus, so including them would inflate
        // both arms by a constant that has nothing to do with the cost model under
        // test. That contamination is not hypothetical - it showed up as 6 where 3 was
        // correct on the write-path fixture below, which is how it was found.
        counter.Reset();
        await Writer(harness).ProbeCoverageAsync(RepoId, keys, Ct);
        var probeMembershipKeys = counter.ReadKeyCountForTree(RepoContextTrees.VectorMembership);

        // NEW: the same question answered from the digest.
        counter.Reset();
        var digest = await Writer(harness).LoadCoverageDigestAsync(RepoId, Ct);
        Assert.That(digest.IsBuilt, Is.True);

        TestContext.Out.WriteLine(
            $"  [{sources} sources] digest load, coverage tree: "
            + counter.DescribeTree(RepoContextTrees.VectorCoverage));

        return new DetectionCost(
            probeMembershipKeys,
            counter.ReadKeyCountForTree(RepoContextTrees.VectorCoverage),
            counter.ReadKeyCountForTree(RepoContextTrees.VectorMembership));
    }

    [Test]
    public async Task Digest_maintenance_costs_the_membership_tree_nothing_and_suppresses_no_op_writes()
    {
        // The write-path direction of the issue-#2071 trade-off, reported rather than
        // waved away. Maintaining the digest is not free - it is a read-modify-write
        // over the touched pages - but it lands on a tree of its own, so the
        // membership hotspot's write volume is unchanged by this item.
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        await SeedCoveredAsync(harness, 64);
        var counter = Counter(harness);

        counter.Reset();
        await Writer(harness).AddMembersAsync(RepoId, FileKeys(64), Ct);

        var coverageCalls = counter.CountForTree(RepoContextTrees.VectorCoverage);
        var coverageWrites = counter.Count("SetManyAtomicAsync");
        TestContext.Out.WriteLine(
            $"re-adding 64 already-covered sources: {coverageCalls} coverage-tree call(s), "
            + $"{coverageWrites} coverage-tree write(s), "
            + $"{counter.ReadKeyCountForTree(RepoContextTrees.VectorMembership)} membership read key(s). "
            + "Breakdown: " + counter.DescribeTree(RepoContextTrees.VectorCoverage));

        Assert.Multiple(() =>
        {
            Assert.That(
                coverageWrites,
                Is.Zero,
                "A no-op re-add must not rewrite a page. Every reconcile re-offers its "
                + "unchanged files, so a digest that rewrote a page per re-offered source "
                + "would add write amplification on every pass forever.");
            Assert.That(
                coverageCalls,
                Is.LessThanOrEqualTo(4),
                "The read-modify-write reads the touched pages in batches, not one round "
                + "trip per page. Left unbatched this would be ~57 round trips on the "
                + "ingest's critical path, which would hand back much of the read-path "
                + "saving the digest bought.");
        });
    }

    [Test]
    public async Task Adding_one_new_source_rewrites_one_page()
    {
        // The targeted-repair claim in its purest form, asserted on the write side:
        // healing one gap must not cost a whole-repository pass.
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        await SeedCoveredAsync(harness, 64);
        var counter = Counter(harness);

        counter.Reset();
        await Writer(harness).AddMembersAsync(
            RepoId, new[] { RepoContextKeys.File(RepoId, "src/Repaired.cs") }, Ct);

        TestContext.Out.WriteLine(
            "adding 1 new source, coverage tree: "
            + counter.DescribeTree(RepoContextTrees.VectorCoverage));

        Assert.Multiple(() =>
        {
            Assert.That(
                counter.Count("SetManyAtomicAsync"),
                Is.EqualTo(1),
                "One source, one page write.");
            Assert.That(
                counter.ReadKeyCountForTree(RepoContextTrees.VectorCoverage),
                Is.EqualTo(2),
                "The built-state marker and the one touched page. The page WRITE is not "
                + "counted here because this figure is reads; it is asserted separately "
                + "above as exactly one atomic write.");
        });
    }

    [Test]
    public async Task A_digest_with_pages_but_no_marker_reports_present_but_unconsumed_not_absent()
    {
        // The middle state, planted deliberately. RebuildAsync writes the pages first
        // and the built-state marker last, so a crash in that window leaves exactly
        // this: correct, decodable pages that every consumer ignores.
        //
        // Behaviourally it is identical to "no digest" - both fall back to the
        // membership probe - which is precisely why it is dangerous to report them
        // the same way. A digest that has silently stopped being read would otherwise
        // be indistinguishable from one that was never built, and a marker that is
        // present, well-formed, and consumed by nothing is exactly as good as no
        // marker while looking exactly as good as the right one.
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        await SeedCoveredAsync(harness, 32);
        var writer = Writer(harness);
        Assert.That((await writer.LoadCoverageDigestAsync(RepoId, Ct)).IsBuilt, Is.True);

        // Strip ONLY the marker, leaving every page in place.
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.VectorCoverage);
        await tree.DeleteAsync(RepoContextKeys.VectorCoverageState(RepoId), Ct);

        var store = harness.Services.GetRequiredService<RepoContextCoverageDigestStore>();
        var digest = await store.LoadAsync(RepoId, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                digest.Presence,
                Is.EqualTo(RepoContextCoverageDigestPresence.PresentButUnconsumed),
                "Present and consumed by nothing must be its own reported state. "
                + "Collapsing it into Absent is what makes it invisible.");
            Assert.That(
                digest.IsBuilt,
                Is.False,
                "It must still assert nothing: distinguishing the state is a reporting "
                + "change, never a licence to read pages the marker does not vouch for.");
            Assert.That(digest.Embedded, Is.Empty);
        });
    }

    [Test]
    public async Task A_repository_with_no_digest_at_all_reports_absent()
    {
        // The other half of the distinction. Without this, a classifier that returned
        // PresentButUnconsumed unconditionally would pass the test above and would
        // have collapsed the three states just as badly, in the other direction.
        //
        // Note this uses a repository that was never seeded: the shared seed helper
        // builds the digest as part of its setup, so calling it here would have
        // produced a Consumed digest and quietly tested nothing. It did, on the first
        // run of this test.
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);

        var store = harness.Services.GetRequiredService<RepoContextCoverageDigestStore>();
        var digest = await store.LoadAsync("never-indexed", Ct);

        Assert.Multiple(() =>
        {
            Assert.That(digest.Presence, Is.EqualTo(RepoContextCoverageDigestPresence.Absent));
            Assert.That(digest.IsBuilt, Is.False);
        });
    }

    [Test]
    public async Task A_built_digest_reports_consumed()
    {
        // And the third, so all three members are asserted somewhere. A digest that
        // never reported Consumed would make the enum unfalsifiable.
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        await SeedCoveredAsync(harness, 8);
        await Writer(harness).LoadCoverageDigestAsync(RepoId, Ct);

        var store = harness.Services.GetRequiredService<RepoContextCoverageDigestStore>();
        var digest = await store.LoadAsync(RepoId, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(digest.Presence, Is.EqualTo(RepoContextCoverageDigestPresence.Consumed));
            Assert.That(digest.IsBuilt, Is.True);
        });
    }

    [Test]
    public async Task Removing_a_member_uncovers_it_in_the_digest()    {
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        var keys = await SeedCoveredAsync(harness, 16);

        await Writer(harness).RetireAsync(RepoId, keys[3], Ct);

        var digest = await Writer(harness).LoadCoverageDigestAsync(RepoId, Ct);
        var coverage = digest.ProjectOnto(keys.Select(VectorCodec.SourceId));

        Assert.Multiple(() =>
        {
            Assert.That(
                coverage.IsCovered(VectorCodec.SourceId(keys[3])),
                Is.False,
                "A retired vector must become a gap again, or the digest would mask it "
                + "permanently - the one failure direction that is silent.");
            Assert.That(coverage.IsCovered(VectorCodec.SourceId(keys[4])), Is.True);
        });
    }

    [Test]
    public async Task A_contentless_source_is_covered_and_keeps_its_classification()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        var empty = RepoContextKeys.File(RepoId, "src/Empty.cs");

        await Writer(harness).MarkContentlessAsync(RepoId, new[] { empty }, Ct);

        var digest = await Writer(harness).LoadCoverageDigestAsync(RepoId, Ct);
        var sourceId = VectorCodec.SourceId(empty);
        var coverage = digest.ProjectOnto(new[] { sourceId });

        Assert.Multiple(() =>
        {
            Assert.That(coverage.IsCovered(sourceId), Is.True, "A contentless file is not a gap.");
            Assert.That(
                coverage.Contentless,
                Does.Contain(sourceId),
                "The ingestor drives contentless unmarking from this set, so collapsing "
                + "the two sets into one 'covered' set would break the unmark path.");
            Assert.That(coverage.Embedded, Does.Not.Contain(sourceId));
        });
    }

    [Test]
    public async Task The_audit_re_derives_a_digest_that_drifted_out_of_step()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        var keys = await SeedCoveredAsync(harness, 16);

        // Simulate drift in the ONLY direction that is dangerous: a digest claiming
        // coverage that membership no longer has. This cannot arise from the write
        // ordering, but a re-derivation of the membership plane could produce it, so
        // the audit is the backstop and this test is what proves it works.
        var membership = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
        var stranded = VectorCodec.SourceId(keys[2]);
        await membership.DeleteAsync(RepoContextKeys.VectorMembership(RepoId, stranded), Ct);

        var before = await Writer(harness).LoadCoverageDigestAsync(RepoId, Ct);
        Assert.That(
            before.IsCovered(stranded),
            Is.True,
            "Precondition: the digest still claims the source that membership lost.");

        var audited = await Writer(harness).AuditCoverageDigestAsync(RepoId, Ct);
        var after = await Writer(harness).LoadCoverageDigestAsync(RepoId, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(audited, Is.True);
            Assert.That(
                after.IsCovered(stranded),
                Is.False,
                "The audit re-derives the digest from the authoritative membership scan, "
                + "so an over-reporting digest is corrected rather than trusted forever.");
            Assert.That(after.IsCovered(VectorCodec.SourceId(keys[5])), Is.True, "Nothing else moved.");
        });
    }

    [Test]
    public async Task An_unbuilt_digest_asserts_nothing_rather_than_reporting_a_whole_repository_gap()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);

        // Membership exists; the digest has never been built. This is the state every
        // already-onboarded deployment upgrades into, and reading it as "nothing is
        // covered" would re-embed the entire repository on the first pass after the
        // upgrade.
        await Writer(harness).AddMembersAsync(RepoId, FileKeys(8), Ct);

        var scanner = harness.Services.GetRequiredService<RepoContextEmbeddingGapScanner>();
        var scan = await scanner.ScanWithDigestAsync(RepoId, maxMissing: 256, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(scan.DigestAvailable, Is.True, "The first read seeds the digest from membership.");
            Assert.That(
                scan.GapFound,
                Is.False,
                "Seeding from the authoritative membership set means the bootstrap pass "
                + "sees the true coverage, not an empty digest.");
        });
    }
}
