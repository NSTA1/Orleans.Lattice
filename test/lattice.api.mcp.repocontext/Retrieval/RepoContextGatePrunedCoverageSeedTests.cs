using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// The whole-set membership scan that seeds and audits the durable coverage digest
/// is the one coverage read that had no gate accounting at all. A read-path access
/// gate that withheld membership rows produced a coverage reporting zero pruning and
/// <c>AbsenceIsConclusive</c> true - so the digest mirrored it, marked itself built,
/// and turned a transient authorization condition into a durable, authoritative
/// claim that the withheld sources were never covered.
/// <para>
/// These tests pin the producer's honesty in both directions, because a fix that
/// simply refused to ever build the digest would satisfy a one-legged test while
/// disabling the feature. The unrestricted leg is therefore as load-bearing as the
/// pruned ones, and the partial-prune leg is the one that distinguishes consulting
/// the gate unconditionally from consulting it only when the scan came back empty.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextGatePrunedCoverageSeedTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static RepoContextMcpHarnessOptions WithGate(MembershipReadGate gate) =>
        new()
        {
            Posture = RepoContextMcpAuthPosture.Writer,
            ConfigureSilo = silo => silo.Services.AddSingleton<ILatticeAccessGate>(gate),
        };

    private static RepoContextVectorWriter Writer(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextVectorWriter>();

    private static async Task<RepoContextVectorWriter> SeedTwoMembersAsync(
        RepoContextMcpHarness harness, CancellationToken cancellationToken)
    {
        var writer = Writer(harness);
        await writer.AddMembersAsync(
            RepoId,
            [RepoContextKeys.File(RepoId, "src/A.cs"), RepoContextKeys.File(RepoId, "src/B.cs")],
            cancellationToken);
        return writer;
    }

    [Test]
    public async Task An_unrestricted_membership_scan_still_seeds_and_marks_the_digest_built()
    {
        var gate = new MembershipReadGate();
        await using var harness = await RepoContextMcpHarness.StartAsync(WithGate(gate), Ct);
        var writer = await SeedTwoMembersAsync(harness, Ct);

        var coverage = await writer.LoadCoverageAsync(RepoId, Ct);
        var digest = await writer.LoadCoverageDigestAsync(RepoId, Ct);
        var audited = await writer.AuditCoverageDigestAsync(RepoId, Ct);

        // The open-path readings are pinned exactly, not merely as "not refused".
        // Making a read sometimes inconclusive is only safe if it is never
        // inconclusive when the gate withheld nothing, and a fix that degraded the
        // open path would pass every pruned leg below while breaking the feature.
        Assert.Multiple(() =>
        {
            Assert.That(coverage.Embedded, Has.Count.EqualTo(2), "open-path embedded count");
            Assert.That(coverage.PrunedByAccessGate, Is.Zero, "open-path pruned count");
            Assert.That(
                coverage.RangeGateCoverage,
                Is.EqualTo(LatticeRangeReadGateCoverage.Unrestricted),
                "open-path range coverage");
            Assert.That(coverage.AbsenceIsConclusive, Is.True, "open-path conclusiveness");
            Assert.That(digest.IsBuilt, Is.True, "digest built from an unrestricted scan");
            Assert.That(digest.Embedded, Has.Count.EqualTo(2), "digest mirrors the two members");
            Assert.That(audited, Is.True, "the audit re-derives from an unrestricted scan");
        });
    }

    [Test]
    public async Task A_totally_pruned_membership_scan_is_refused_rather_than_seeded_as_empty_coverage()
    {
        var gate = new MembershipReadGate();
        await using var harness = await RepoContextMcpHarness.StartAsync(WithGate(gate), Ct);
        var writer = await SeedTwoMembersAsync(harness, Ct);

        gate.Withhold = static _ => true;

        var coverage = await writer.LoadCoverageAsync(RepoId, Ct);
        var digest = await writer.LoadCoverageDigestAsync(RepoId, Ct);

        Assert.Multiple(() =>
        {
            // The empty reading is the defect's raw material. It is fine for the scan
            // to come back empty; what is not fine is for it to come back empty and
            // claim that absence is conclusive.
            Assert.That(coverage.Embedded, Is.Empty, "a total prune withholds every row");
            Assert.That(
                coverage.RangeGateCoverage,
                Is.Not.EqualTo(LatticeRangeReadGateCoverage.Unrestricted),
                "the scan must report that the gate restricted the range");
            Assert.That(
                coverage.AbsenceIsConclusive,
                Is.False,
                "an absence produced by the gate is not evidence the sources are uncovered");
            Assert.That(
                digest.IsBuilt,
                Is.False,
                "seeding a durable digest from a pruned read would publish the absence as authoritative");
        });
    }

    [Test]
    public async Task A_partially_pruned_membership_scan_is_refused_even_though_it_returns_rows()
    {
        var gate = new MembershipReadGate();
        await using var harness = await RepoContextMcpHarness.StartAsync(WithGate(gate), Ct);
        var writer = await SeedTwoMembersAsync(harness, Ct);

        // Withhold exactly one of the two members, so the scan returns a populated,
        // entirely plausible, and quietly wrong coverage. This is the leg that earns
        // consulting the gate unconditionally: the documented guidance is to consult
        // it only when a read comes back empty, and this read does not.
        var open = await writer.LoadCoverageAsync(RepoId, Ct);
        var withheld = open.Embedded.Order(StringComparer.Ordinal).First();
        gate.Withhold = key => key.Contains(withheld, StringComparison.Ordinal);

        var coverage = await writer.LoadCoverageAsync(RepoId, Ct);
        var digest = await writer.LoadCoverageDigestAsync(RepoId, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                coverage.Embedded,
                Has.Count.EqualTo(1),
                "the partial prune must leave a non-empty result, or this leg tests the total case again");
            Assert.That(
                coverage.AbsenceIsConclusive,
                Is.False,
                "a populated result is not a complete one when the gate withheld rows");
            Assert.That(
                digest.IsBuilt,
                Is.False,
                "a digest seeded from a partial prune records the withheld source as uncovered forever");
        });
    }

    [Test]
    public async Task The_periodic_audit_refuses_a_pruned_scan_instead_of_overwriting_a_healthy_digest()
    {
        var gate = new MembershipReadGate();
        await using var harness = await RepoContextMcpHarness.StartAsync(WithGate(gate), Ct);
        var writer = await SeedTwoMembersAsync(harness, Ct);

        var seeded = await writer.LoadCoverageDigestAsync(RepoId, Ct);
        Assert.That(seeded.IsBuilt, Is.True, "precondition: a healthy digest exists to be corrupted");

        gate.Withhold = static _ => true;

        // The audit is the repair path, so it is also the reinfection vector: it
        // rebuilds unconditionally and previously reported success either way, which
        // means a pruned audit did not merely fail to repair a corrupt digest, it
        // corrupted a healthy one and said it had re-derived it.
        var audited = await writer.AuditCoverageDigestAsync(RepoId, Ct);

        gate.Withhold = null;
        var after = await writer.LoadCoverageDigestAsync(RepoId, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(audited, Is.False, "the audit must report that it did not re-derive");
            Assert.That(after.IsBuilt, Is.True, "the pre-existing digest survives a refused audit");
            Assert.That(after.Embedded, Has.Count.EqualTo(2), "and still names both members");
        });
    }

    /// <summary>
    /// Prunes read-path access to the vector membership tree only, and only for reads:
    /// the ingest's own membership writes must still land, or the fixture would be
    /// measuring an empty tree rather than a withheld one.
    /// </summary>
    private sealed class MembershipReadGate : ILatticeAccessGate
    {
        /// <summary>Returns true for a key the gate should withhold. Null allows everything.</summary>
        public Func<string, bool>? Withhold { get; set; }

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            var withhold = Withhold;
            var applies = withhold is not null
                && string.Equals(request.TreeId, RepoContextTrees.VectorMembership, StringComparison.Ordinal)
                && (request.Operation & (LatticeOperation.Read | LatticeOperation.RangeRead)) != LatticeOperation.None;

            return new ValueTask<LatticeAccessDecision>(
                applies
                    ? LatticeAccessDecision.Filtered(key => !withhold!(key))
                    : LatticeAccessDecision.Allow());
        }
    }
}
