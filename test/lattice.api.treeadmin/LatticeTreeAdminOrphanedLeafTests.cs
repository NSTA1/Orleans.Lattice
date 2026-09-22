using System.Collections.Immutable;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for the orphaned-leaf audit and repair verbs on
/// <see cref="LatticeTreeAdmin"/>. The core library shipped the two verbs with no
/// caller outside its own package, so a tree already carrying an orphaned leaf had
/// no way to reach its documented remedy; these tests pin the control-facade path
/// that makes it reachable. The audit is a pure read gated on the whole-tree
/// <c>Read</c> capability; the repair is an irreversible structural change gated on
/// the strictly narrower <c>TreeLifecycle</c> capability and refused outright for a
/// reserved system tree id. Both project the core report onto its transport-agnostic
/// mirror and echo the caller's own unqualified tree name. Driven purely with
/// substitutes and a hand-written access gate - no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminOrphanedLeafTests
{
    private const string Tree = "orders";

    private sealed class FixedGate : ILatticeAccessGate
    {
        private readonly bool _allow;
        public FixedGate(bool allow) => _allow = allow;

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(_allow ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("denied by test"));
    }

    private static LatticeTreeAdmin Create(IGrainFactory factory, bool allow = true)
        => new(
            Substitute.For<ILatticeSchemaControl>(),
            factory,
            new TreeAdminAccessAuthorizer(new FixedGate(allow)),
            Options.Create(new LatticeApiTreeAdminOptions()),
            new NullTenantContextResolver());

    private static ILattice WireTree(IGrainFactory factory, string treeId = Tree)
    {
        var tree = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>(treeId).Returns(tree);
        return tree;
    }

    private static OrphanedLeafRepairReport CoreReport(bool dryRun) => new()
    {
        DryRun = dryRun,
        LeavesWalked = 9,
        Findings = new[]
        {
            new OrphanedLeafFinding
            {
                ShardIndex = 3,
                LeafId = "leaf-7",
                LowKeyInclusive = "a",
                HighKeyExclusive = "m",
                KeyCount = 12,
                VerifiedKeyCount = 12,
                Disposition = dryRun ? OrphanedLeafDisposition.Repairable : OrphanedLeafDisposition.Repaired,
            },
            new OrphanedLeafFinding
            {
                ShardIndex = 4,
                LeafId = "leaf-8",
                KeyCount = 5,
                VerifiedKeyCount = 3,
                Disposition = OrphanedLeafDisposition.RefusedUnverifiedKeys,
                UnverifiedKey = "zebra",
            },
        },
    };

    // ----- Audit -----

    [Test]
    public async Task Survey_projects_all_counts_and_uses_read_only_core_verb()
    {
        var factory = Substitute.For<IGrainFactory>();
        var tree = WireTree(factory);
        var core = CoreReport(true) with
        {
            Survey = true,
            Findings = [new OrphanedLeafFinding
            {
                ShardIndex = 3, LeafId = "leaf", LowKeyInclusive = "a", HighKeyExclusive = "z",
                KeyCount = 6, VerifiedKeyCount = 1, UnverifiedKey = "b",
                SurveyVerifiedKeyCount = 2, SurveyMissingKeyCount = 3,
                SurveyRoutingContradictionKeyCount = 1,
                Disposition = OrphanedLeafDisposition.RefusedUnverifiedKeys,
            }],
        };
        tree.SurveyOrphanedLeavesAsync("cursor", Arg.Any<CancellationToken>()).Returns(core);

        var report = await Create(factory).SurveyOrphanedLeavesAsync(Tree, "cursor");
        Assert.Multiple(() =>
        {
            Assert.That(report.TreeId, Is.EqualTo(Tree));
            Assert.That(report.Survey, Is.True);
            Assert.That(report.DryRun, Is.True);
            Assert.That(report.SurveyMissingKeyCount, Is.EqualTo(3));
            Assert.That(report.OrphanedLeafCount, Is.EqualTo(1));
            Assert.That(report.Findings[0].SurveyVerifiedKeyCount, Is.EqualTo(2));
            Assert.That(report.Findings[0].SurveyRoutingContradictionKeyCount, Is.EqualTo(1));
            Assert.That(report.Findings[0].VerifiedKeyCount, Is.EqualTo(1));
            Assert.That(report.Findings[0].UnverifiedKey, Is.EqualTo("b"));
            Assert.That(report.Findings[0].ShardIndex, Is.EqualTo(3));
        });
        await tree.Received(1).SurveyOrphanedLeavesAsync("cursor", Arg.Any<CancellationToken>());
        await tree.DidNotReceive().RepairOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void Survey_denied_by_read_gate_never_dials_and_rejects_empty_tree_id()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory, allow: false);
        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.SurveyOrphanedLeavesAsync(Tree),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(async () => await facade.SurveyOrphanedLeavesAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await facade.SurveyOrphanedLeavesAsync(""), Throws.ArgumentException);
        });
        factory.DidNotReceive().GetGrain<ILattice>(Arg.Any<string>());
    }

    [Test]
    public async Task AuditOrphanedLeavesAsync_projects_the_core_report()
    {
        var factory = Substitute.For<IGrainFactory>();
        var tree = WireTree(factory);
        tree.InspectOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>()).Returns(CoreReport(dryRun: true));
        var facade = Create(factory);

        var report = await facade.AuditOrphanedLeavesAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeId, Is.EqualTo(Tree));
            Assert.That(report.DryRun, Is.True);
            Assert.That(report.LeavesWalked, Is.EqualTo(9));
            Assert.That(report.Findings, Has.Length.EqualTo(2));
            Assert.That(report.Findings[0].LeafId, Is.EqualTo("leaf-7"));
            Assert.That(report.Findings[0].ShardIndex, Is.EqualTo(3));
            Assert.That(report.Findings[0].LowKeyInclusive, Is.EqualTo("a"));
            Assert.That(report.Findings[0].HighKeyExclusive, Is.EqualTo("m"));
            Assert.That(report.Findings[0].Disposition, Is.EqualTo(TreeOrphanedLeafDisposition.Repairable));
            Assert.That(report.Findings[0].IsRefusal, Is.False);
            Assert.That(report.Findings[1].Disposition, Is.EqualTo(TreeOrphanedLeafDisposition.RefusedUnverifiedKeys));
            Assert.That(report.Findings[1].UnverifiedKey, Is.EqualTo("zebra"));
            Assert.That(report.Findings[1].IsRefusal, Is.True);
            Assert.That(report.RefusedCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task AuditOrphanedLeavesAsync_reports_a_clean_tree_as_no_findings()
    {
        var factory = Substitute.For<IGrainFactory>();
        var tree = WireTree(factory);
        tree.InspectOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new OrphanedLeafRepairReport { DryRun = true, LeavesWalked = 40, Findings = Array.Empty<OrphanedLeafFinding>() });
        var facade = Create(factory);

        var report = await facade.AuditOrphanedLeavesAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.Findings, Is.Empty, "A tree with no orphans is a verdict, not a failure.");
            Assert.That(report.LeavesWalked, Is.EqualTo(40));
            Assert.That(report.RepairedCount, Is.Zero);
            Assert.That(report.RefusedCount, Is.Zero);
        });
    }

    /// <summary>
    /// The resume token and the completeness flag are what let an operator drive
    /// a bounded pass to the end and tell a clean verdict from a partial one
    /// (issue 3302). A facade that dropped either on the way through would turn
    /// every pass into its first batch while still looking correct: the report
    /// would read complete, and "no findings" would be indistinguishable from
    /// "no findings yet".
    /// </summary>
    [Test]
    public async Task Both_verbs_carry_the_resume_token_and_completeness_through_the_projection()
    {
        const string Token = "olp1:3:kb3JkZXJz";
        var factory = Substitute.For<IGrainFactory>();
        var tree = WireTree(factory);
        tree.InspectOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new OrphanedLeafRepairReport { DryRun = true, LeavesWalked = 9, ResumeFrom = Token });
        tree.RepairOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new OrphanedLeafRepairReport { DryRun = false, LeavesWalked = 9, ResumeFrom = null });
        var facade = Create(factory);

        var partial = await facade.AuditOrphanedLeavesAsync(Tree, Token);
        var complete = await facade.RepairOrphanedLeavesAsync(Tree, Token);

        Assert.Multiple(() =>
        {
            Assert.That(partial.ResumeFrom, Is.EqualTo(Token));
            Assert.That(partial.IsComplete, Is.False, "a partial batch must not read as a clean verdict");
            Assert.That(complete.ResumeFrom, Is.Null);
            Assert.That(complete.IsComplete, Is.True);
        });

        await tree.Received(1).InspectOrphanedLeavesAsync(Token, Arg.Any<CancellationToken>());
        await tree.Received(1).RepairOrphanedLeavesAsync(Token, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AuditOrphanedLeavesAsync_tolerates_a_null_findings_list()    {
        var factory = Substitute.For<IGrainFactory>();
        var tree = WireTree(factory);
        tree.InspectOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new OrphanedLeafRepairReport { DryRun = true, LeavesWalked = 2, Findings = null! });
        var facade = Create(factory);

        var report = await facade.AuditOrphanedLeavesAsync(Tree);

        Assert.That(report.Findings, Is.Empty);
    }

    [Test]
    public async Task AuditOrphanedLeavesAsync_carries_the_gaps_out_to_the_operator()
    {
        // The gap channel is only worth anything if it survives the facade.
        // A tree-level report whose gaps were dropped here would present the
        // same false clean bill of health the core now refuses to give
        // (issue 3301).
        var factory = Substitute.For<IGrainFactory>();
        var tree = WireTree(factory);
        tree.InspectOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new OrphanedLeafRepairReport
            {
                DryRun = true,
                LeavesWalked = 2588,
                Findings = Array.Empty<OrphanedLeafFinding>(),
                Gaps = new[]
                {
                    new OrphanedLeafAuditGap
                    {
                        ShardIndex = 17,
                        Reason = OrphanedLeafAuditGapReason.ChainTruncated,
                        LeafId = "leaf-9",
                        KeyHint = "m",
                    },
                },
            });
        var facade = Create(factory);

        var report = await facade.AuditOrphanedLeavesAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.Findings, Is.Empty);
            Assert.That(report.Gaps, Has.Length.EqualTo(1));
            Assert.That(report.Gaps[0].ShardIndex, Is.EqualTo(17));
            Assert.That(report.Gaps[0].Reason,
                Is.EqualTo(TreeOrphanedLeafGapReason.ChainTruncated));
            Assert.That(report.Gaps[0].LeafId, Is.EqualTo("leaf-9"));
            Assert.That(report.Gaps[0].KeyHint, Is.EqualTo("m"));
            Assert.That(report.VerdictComplete, Is.False,
                "2588 leaves walked and no findings is not a clean tree when part of it was never reached");
        });
    }

    [Test]
    public async Task AuditOrphanedLeavesAsync_tolerates_a_null_gap_list()
    {
        var factory = Substitute.For<IGrainFactory>();
        var tree = WireTree(factory);
        tree.InspectOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new OrphanedLeafRepairReport
            {
                DryRun = true,
                LeavesWalked = 2,
                Findings = Array.Empty<OrphanedLeafFinding>(),
                Gaps = null!,
            });
        var facade = Create(factory);

        var report = await facade.AuditOrphanedLeavesAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.Gaps, Is.Empty);
            Assert.That(report.VerdictComplete, Is.True);
        });
    }

    [Test]
    public void AuditOrphanedLeavesAsync_denied_by_read_gate_throws_and_does_not_dial()
    {
        var factory = Substitute.For<IGrainFactory>();
        var tree = WireTree(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.AuditOrphanedLeavesAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        tree.DidNotReceive().InspectOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void AuditOrphanedLeavesAsync_rejects_an_empty_tree_id()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.AuditOrphanedLeavesAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await facade.AuditOrphanedLeavesAsync(string.Empty), Throws.ArgumentException);
        });
    }

    // ----- Repair -----

    [Test]
    public async Task RepairOrphanedLeavesAsync_projects_the_core_report()
    {
        var factory = Substitute.For<IGrainFactory>();
        var tree = WireTree(factory);
        tree.RepairOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>()).Returns(CoreReport(dryRun: false));
        var facade = Create(factory);

        var report = await facade.RepairOrphanedLeavesAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeId, Is.EqualTo(Tree));
            Assert.That(report.DryRun, Is.False);
            Assert.That(report.Findings[0].Disposition, Is.EqualTo(TreeOrphanedLeafDisposition.Repaired));
            Assert.That(report.RepairedCount, Is.EqualTo(1));
            Assert.That(report.RefusedCount, Is.EqualTo(1));
        });
    }

    [Test]
    public void RepairOrphanedLeavesAsync_denied_by_lifecycle_gate_throws_and_does_not_dial()
    {
        var factory = Substitute.For<IGrainFactory>();
        var tree = WireTree(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.RepairOrphanedLeavesAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        tree.DidNotReceive().RepairOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void RepairOrphanedLeavesAsync_rejects_a_reserved_system_tree_id()
    {
        var reserved = LatticeConstants.SystemTreePrefix + "wal";
        var factory = Substitute.For<IGrainFactory>();
        var tree = WireTree(factory, reserved);
        var facade = Create(factory);

        Assert.That(async () => await facade.RepairOrphanedLeavesAsync(reserved),
            Throws.ArgumentException);
        tree.DidNotReceive().RepairOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void RepairOrphanedLeavesAsync_rejects_an_empty_tree_id()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.RepairOrphanedLeavesAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await facade.RepairOrphanedLeavesAsync(string.Empty), Throws.ArgumentException);
        });
    }

    // ----- Enum parity -----

    /// <summary>
    /// The projection casts the core disposition straight onto its mirror, so the two
    /// enums must agree on every name and every value. A silent divergence would
    /// mislabel a refusal as a repair in the operator's report.
    /// </summary>
    [Test]
    public void The_mirrored_disposition_enum_matches_the_core_enum_exactly()
    {
        var core = Enum.GetValues<OrphanedLeafDisposition>()
            .ToImmutableSortedDictionary(v => v.ToString(), v => (int)v, StringComparer.Ordinal);
        var mirror = Enum.GetValues<TreeOrphanedLeafDisposition>()
            .ToImmutableSortedDictionary(v => v.ToString(), v => (int)v, StringComparer.Ordinal);

        Assert.That(mirror, Is.EqualTo(core),
            "TreeOrphanedLeafDisposition mirrors OrphanedLeafDisposition by direct cast; names and values must match.");
    }
}
