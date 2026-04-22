using MultiSiteManufacturing.Host.Domain;
using Orleans.Lattice.Primitives;

namespace MultiSiteManufacturing.Tests.Domain;

[TestFixture]
public class ComplianceFoldTests
{
    private static readonly PartSerialNumber Serial = new("HPT-BLD-S1-2028-00001");
    private static readonly OperatorId Op = OperatorId.Demo;

    [Test]
    public void Fold_of_empty_fact_set_returns_Nominal()
    {
        Assert.That(ComplianceFold.Fold([]), Is.EqualTo(ComplianceState.Nominal));
    }

    [Test]
    public void ProcessStepCompleted_alone_keeps_Nominal()
    {
        var facts = new Fact[]
        {
            Step(1, ProcessStage.Forge, ProcessSite.OhioForge),
            Step(2, ProcessStage.HeatTreat, ProcessSite.NagoyaHeatTreat),
        };
        Assert.That(ComplianceFold.Fold(facts), Is.EqualTo(ComplianceState.Nominal));
    }

    [Test]
    public void Failed_inspection_escalates_to_FlaggedForReview()
    {
        var facts = new Fact[]
        {
            Step(1, ProcessStage.NDT, ProcessSite.ToulouseNdtLab),
            Inspect(2, Inspection.FPI, InspectionOutcome.Fail, ProcessSite.ToulouseNdtLab),
        };
        Assert.That(ComplianceFold.Fold(facts), Is.EqualTo(ComplianceState.FlaggedForReview));
    }

    [Test]
    public void Passed_inspection_alone_keeps_Nominal()
    {
        var facts = new Fact[]
        {
            Inspect(1, Inspection.CMM, InspectionOutcome.Pass, ProcessSite.StuttgartCmmLab),
        };
        Assert.That(ComplianceFold.Fold(facts), Is.EqualTo(ComplianceState.Nominal));
    }

    [TestCase(NcSeverity.Minor, ComplianceState.FlaggedForReview)]
    [TestCase(NcSeverity.Major, ComplianceState.Rework)]
    [TestCase(NcSeverity.Critical, ComplianceState.Scrap)]
    public void NonConformance_escalates_by_severity(NcSeverity severity, ComplianceState expected)
    {
        var facts = new Fact[] { Nc(1, "NC-1", severity, ProcessSite.ToulouseNdtLab) };
        Assert.That(ComplianceFold.Fold(facts), Is.EqualTo(expected));
    }

    [Test]
    public void MrbDisposition_UseAsIs_demotes_FlaggedForReview_to_Nominal()
    {
        var facts = new Fact[]
        {
            Nc(1, "NC-1", NcSeverity.Minor, ProcessSite.ToulouseNdtLab),
            Mrb(2, "NC-1", MrbDispositionKind.UseAsIs, ProcessSite.CincinnatiMrb),
        };
        Assert.That(ComplianceFold.Fold(facts), Is.EqualTo(ComplianceState.Nominal));
    }

    [Test]
    public void MrbDisposition_UseAsIs_does_not_demote_Rework_without_retest()
    {
        var facts = new Fact[]
        {
            Nc(1, "NC-1", NcSeverity.Major, ProcessSite.ToulouseNdtLab),
            Mrb(2, "NC-1", MrbDispositionKind.UseAsIs, ProcessSite.CincinnatiMrb),
        };
        Assert.That(ComplianceFold.Fold(facts), Is.EqualTo(ComplianceState.Rework));
    }

    [Test]
    public void ReworkCompleted_then_Pass_then_UseAsIs_demotes_Rework_to_Nominal()
    {
        var facts = new Fact[]
        {
            Nc(1, "NC-1", NcSeverity.Major, ProcessSite.ToulouseNdtLab),
            new ReworkCompleted
            {
                Serial = Serial, FactId = Guid.NewGuid(), Hlc = Hlc(2),
                Site = ProcessSite.StuttgartMachining, Operator = Op,
                Description = "rework op", ReworkOperation = "re-blend", RetestPassed = true,
            },
            Inspect(3, Inspection.CMM, InspectionOutcome.Pass, ProcessSite.StuttgartCmmLab),
            Mrb(4, "NC-1", MrbDispositionKind.UseAsIs, ProcessSite.CincinnatiMrb),
        };
        Assert.That(ComplianceFold.Fold(facts), Is.EqualTo(ComplianceState.Nominal));
    }

    [Test]
    public void MrbDisposition_Scrap_is_terminal()
    {
        var facts = new Fact[]
        {
            Nc(1, "NC-1", NcSeverity.Major, ProcessSite.ToulouseNdtLab),
            Mrb(2, "NC-1", MrbDispositionKind.Scrap, ProcessSite.CincinnatiMrb),
            // Late-arriving UseAsIs must not revive a scrapped part.
            Mrb(3, "NC-1", MrbDispositionKind.UseAsIs, ProcessSite.CincinnatiMrb),
            Inspect(4, Inspection.CMM, InspectionOutcome.Pass, ProcessSite.StuttgartCmmLab),
        };
        Assert.That(ComplianceFold.Fold(facts), Is.EqualTo(ComplianceState.Scrap));
    }

    [Test]
    public void NonConformance_Critical_is_terminal()
    {
        var facts = new Fact[]
        {
            Nc(1, "NC-1", NcSeverity.Critical, ProcessSite.ToulouseNdtLab),
            Mrb(2, "NC-1", MrbDispositionKind.UseAsIs, ProcessSite.CincinnatiMrb),
        };
        Assert.That(ComplianceFold.Fold(facts), Is.EqualTo(ComplianceState.Scrap));
    }

    [Test]
    public void Fold_is_order_independent()
    {
        var a = Nc(1, "NC-1", NcSeverity.Minor, ProcessSite.ToulouseNdtLab);
        var b = Mrb(2, "NC-1", MrbDispositionKind.UseAsIs, ProcessSite.CincinnatiMrb);
        var c = Inspect(3, Inspection.CMM, InspectionOutcome.Pass, ProcessSite.StuttgartCmmLab);

        var forward = ComplianceFold.Fold([a, b, c]);
        var reversed = ComplianceFold.Fold([c, b, a]);
        var shuffled = ComplianceFold.Fold([b, a, c]);

        Assert.Multiple(() =>
        {
            Assert.That(forward, Is.EqualTo(ComplianceState.Nominal));
            Assert.That(reversed, Is.EqualTo(forward));
            Assert.That(shuffled, Is.EqualTo(forward));
        });
    }

    [Test]
    public void Fold_is_idempotent_under_duplicate_facts()
    {
        var nc = Nc(1, "NC-1", NcSeverity.Major, ProcessSite.ToulouseNdtLab);
        var single = ComplianceFold.Fold([nc]);
        var doubled = ComplianceFold.Fold([nc, nc, nc]);

        Assert.That(doubled, Is.EqualTo(single));
    }

    [Test]
    public void Late_InspectionPass_under_Flagged_stays_Flagged()
    {
        // The divergence scenario: a late CMM Pass arrives after a Minor NC.
        // Under the fold (HLC ordered), the NC happened after the pass, so we stay Flagged.
        var facts = new Fact[]
        {
            Inspect(2, Inspection.CMM, InspectionOutcome.Pass, ProcessSite.StuttgartCmmLab),
            Nc(3, "NC-1", NcSeverity.Minor, ProcessSite.ToulouseNdtLab),
        };
        Assert.That(ComplianceFold.Fold(facts), Is.EqualTo(ComplianceState.FlaggedForReview));
    }

    // --- helpers ------------------------------------------------------------

    private static HybridLogicalClock Hlc(long tick) =>
        new() { WallClockTicks = tick, Counter = 0 };

    private static ProcessStepCompleted Step(long tick, ProcessStage stage, ProcessSite site) =>
        new()
        {
            Serial = Serial, FactId = Guid.NewGuid(), Hlc = Hlc(tick),
            Site = site, Operator = Op, Description = $"{stage} completed",
            Stage = stage,
        };

    private static InspectionRecorded Inspect(long tick, Inspection inspection, InspectionOutcome outcome, ProcessSite site) =>
        new()
        {
            Serial = Serial, FactId = Guid.NewGuid(), Hlc = Hlc(tick),
            Site = site, Operator = Op, Description = $"{inspection} {outcome}",
            Inspection = inspection, Outcome = outcome,
        };

    private static NonConformanceRaised Nc(long tick, string ncNumber, NcSeverity severity, ProcessSite site) =>
        new()
        {
            Serial = Serial, FactId = Guid.NewGuid(), Hlc = Hlc(tick),
            Site = site, Operator = Op, Description = $"{severity} NC {ncNumber}",
            NcNumber = ncNumber, DefectCode = "D-001", Severity = severity,
        };

    private static MrbDisposition Mrb(long tick, string ncNumber, MrbDispositionKind disposition, ProcessSite site) =>
        new()
        {
            Serial = Serial, FactId = Guid.NewGuid(), Hlc = Hlc(tick),
            Site = site, Operator = Op, Description = $"MRB {disposition}",
            NcNumber = ncNumber, Disposition = disposition,
        };
}
