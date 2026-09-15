using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Integration coverage for the embedding-coverage probe under a real read-path
/// access gate (issue #2277).
/// <para>
/// The defect is latent rather than live - no gate is configured on the current
/// deployment - so these tests stand a real <see cref="ILatticeAccessGate"/> in
/// front of the membership tree and drive the shipped probe and sweep through it,
/// rather than substituting a store that merely simulates a short read. The
/// distinction matters: the whole argument for fixing this at the grain is that a
/// pruned key and a never-written key are indistinguishable downstream, and a
/// simulated short read would be a test of the simulation.
/// </para>
/// <para>
/// The failure being pinned is a FALSE GAP, not a missed one. The self-heal
/// sweep's response to a gap is to re-drive the entire repository index, so a
/// probe whose silence is misread as "unembedded" costs a full re-index on every
/// sweep and heals nothing, forever, with a clean log at every layer.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit dev
/// loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextGatePrunedCoverageTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    // One gate instance per harness, so nothing leaks between tests even when
    // fixtures run in parallel. It starts allow-all: every test seeds its files and
    // memberships through the ordinary write path first, then arms the prune, so
    // the gate only ever affects the read under test.
    private static RepoContextMcpHarnessOptions WithGate(PruningGate gate) =>
        new()
        {
            Posture = RepoContextMcpAuthPosture.Writer,
            ConfigureSilo = silo =>
                // Last-wins override of the null gate AddLattice registered above.
                silo.Services.AddSingleton<ILatticeAccessGate>(gate),
        };

    private async Task SeedFileAsync(RepoContextMcpHarness harness, string relativePath)
    {
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        await tree.SetAsync(RepoContextKeys.File(RepoId, relativePath), new byte[] { 1 }, Ct);
    }

    private static RepoContextEmbeddingGapScanner Scanner(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextEmbeddingGapScanner>();

    private static RepoContextVectorWriter Writer(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextVectorWriter>();

    [Test]
    public async Task ProbeCoverageAsync_reports_the_prune_count_so_absence_is_not_conclusive()
    {
        var gate = new PruningGate();
        await using var harness = await RepoContextMcpHarness.StartAsync(WithGate(gate), Ct);

        var fileKey = RepoContextKeys.File(RepoId, "src/A.cs");
        await Writer(harness).AddMembersAsync(RepoId, new[] { fileKey }, Ct);
        gate.PruneMembershipReads = true;

        var coverage = await Writer(harness).ProbeCoverageAsync(RepoId, new[] { fileKey }, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                coverage.PrunedByAccessGate,
                Is.GreaterThan(0),
                "The gate removed the membership key, and the grain reported how many.");
            Assert.That(
                coverage.AbsenceIsConclusive,
                Is.False,
                "A pruned probe is INCOMPLETE, not negative: absence from it proves nothing.");
            Assert.That(
                coverage.IsCovered(VectorCodec.SourceId(fileKey)),
                Is.False,
                "The source really is absent from the probe - which is exactly why the count, "
                + "and not the returned set, is what makes the absence interpretable.");
        });
    }

    [Test]
    public async Task ProbeCoverageAsync_reports_nothing_pruned_when_the_gate_admits_the_read()
    {
        // The control arm. Without it a prune count that was always non-zero - or an
        // AbsenceIsConclusive that was always false - would pass the test above and
        // stand the gap sweep down permanently on every healthy deployment, which is
        // a far worse defect than the one being fixed.
        var gate = new PruningGate();
        await using var harness = await RepoContextMcpHarness.StartAsync(WithGate(gate), Ct);

        var fileKey = RepoContextKeys.File(RepoId, "src/A.cs");
        await Writer(harness).AddMembersAsync(RepoId, new[] { fileKey }, Ct);

        var coverage = await Writer(harness).ProbeCoverageAsync(RepoId, new[] { fileKey }, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(coverage.PrunedByAccessGate, Is.Zero);
            Assert.That(coverage.AbsenceIsConclusive, Is.True);
            Assert.That(
                coverage.IsCovered(VectorCodec.SourceId(fileKey)),
                Is.True,
                "The ungated probe still sees the member, so the seam did not change what is read.");
        });
    }

    [Test]
    public async Task ProbeCoveredSourceIdsAsync_carries_the_prune_count_to_the_sweep()
    {
        var gate = new PruningGate();
        await using var harness = await RepoContextMcpHarness.StartAsync(WithGate(gate), Ct);

        var fileKey = RepoContextKeys.File(RepoId, "src/A.cs");
        await Writer(harness).AddMembersAsync(RepoId, new[] { fileKey }, Ct);
        gate.PruneMembershipReads = true;

        var probed = await Writer(harness).ProbeCoveredSourceIdsAsync(RepoId, new[] { fileKey }, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(probed.PrunedByAccessGate, Is.GreaterThan(0));
            Assert.That(probed.AbsenceIsConclusive, Is.False);
        });
    }

    [Test]
    public async Task ScanFilePageAsync_does_not_claim_a_gap_when_the_coverage_probe_was_pruned()
    {
        // The decisive regression. Both files ARE embedded, so the repository has no
        // gap at all; only the probe's view of that fact is pruned. Before the fix
        // the sweep read the pruned silence as "no live embedding" and reported a
        // gap, and the caller re-drove the whole repository index - on every sweep,
        // forever, healing nothing.
        var gate = new PruningGate();
        await using var harness = await RepoContextMcpHarness.StartAsync(WithGate(gate), Ct);

        await SeedFileAsync(harness, "src/A.cs");
        await SeedFileAsync(harness, "src/B.cs");
        await Writer(harness).AddMembersAsync(
            RepoId,
            new[] { RepoContextKeys.File(RepoId, "src/A.cs"), RepoContextKeys.File(RepoId, "src/B.cs") },
            Ct);
        gate.PruneMembershipReads = true;

        var page = await Scanner(harness).ScanFilePageAsync(RepoId, resumeKeyInclusive: null, pageSize: 100, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                page.GapFound,
                Is.False,
                "Both files are live members; the probe simply could not see them, which is not a gap.");
            Assert.That(
                page.CoverageUnavailable,
                Is.True,
                "And the page says WHY it found nothing, so 'no gap' is not recorded as a clean sweep.");
            Assert.That(page.PrunedByAccessGate, Is.GreaterThan(0));
            Assert.That(page.HasMore, Is.False, "An unclassifiable page ends the walk rather than paging on.");
            Assert.That(page.NextResumeKey, Is.Null);
        });
    }

    [Test]
    public async Task ScanFilePageAsync_still_reports_a_real_gap_when_the_gate_admits_the_read()
    {
        // The counterpart control: the fix must not buy its safety by suppressing
        // genuine gaps. With the same gate present but not pruning, the unembedded
        // file is still found.
        var gate = new PruningGate();
        await using var harness = await RepoContextMcpHarness.StartAsync(WithGate(gate), Ct);

        await SeedFileAsync(harness, "src/A.cs");
        await SeedFileAsync(harness, "src/B.cs");
        await Writer(harness).AddMembersAsync(RepoId, new[] { RepoContextKeys.File(RepoId, "src/A.cs") }, Ct);

        var page = await Scanner(harness).ScanFilePageAsync(RepoId, resumeKeyInclusive: null, pageSize: 100, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(page.GapFound, Is.True, "B has no live embedding: a real gap, still reported.");
            Assert.That(page.CoverageUnavailable, Is.False);
            Assert.That(page.PrunedByAccessGate, Is.Zero);
        });
    }

    /// <summary>
    /// A read-path gate that prunes every membership-tree key once armed, and
    /// leaves every other tree - the structural tree the sweep walks in particular -
    /// completely alone.
    /// </summary>
    private sealed class PruningGate : ILatticeAccessGate
    {
        /// <summary>Whether membership reads are currently pruned to nothing.</summary>
        public bool PruneMembershipReads { get; set; }

        /// <inheritdoc />
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            var prune = PruneMembershipReads
                && string.Equals(request.TreeId, RepoContextTrees.VectorMembership, StringComparison.Ordinal);
            return new ValueTask<LatticeAccessDecision>(
                prune
                    ? LatticeAccessDecision.Filtered(static _ => false)
                    : LatticeAccessDecision.Allow());
        }
    }
}
