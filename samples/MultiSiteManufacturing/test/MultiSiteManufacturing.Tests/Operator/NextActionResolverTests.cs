using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Operator;
using Orleans.Lattice.Primitives;

namespace MultiSiteManufacturing.Tests.Operator;

/// <summary>
/// Verifies <see cref="NextActionResolver.Resolve"/> maps every observable
/// part-state to the correct suggested next action driving the part-detail
/// page primary button.
/// </summary>
[TestFixture]
public class NextActionResolverTests
{
    private static readonly PartSerialNumber Serial = new("HPT-BLD-S1-2025-00001");
    private static readonly OperatorId Op = OperatorId.Demo;

    private long _ticks = 1;

    [SetUp]
    public void SetUp() => _ticks = 1;

    private HybridLogicalClock Next() => new() { WallClockTicks = _ticks++, Counter = 0 };

    private ProcessStepCompleted Step(ProcessStage s) => new()
    {
        Serial = Serial,
        FactId = Guid.NewGuid(),
        Hlc = Next(),
        Site = ProcessSite.OhioForge,
        Operator = Op,
        Description = $"{s} complete",
        Stage = s,
    };

    private InspectionRecorded Inspect(Inspection i, InspectionOutcome o) => new()
    {
        Serial = Serial,
        FactId = Guid.NewGuid(),
        Hlc = Next(),
        Site = ProcessSite.StuttgartCmmLab,
        Operator = Op,
        Description = $"{i} {o}",
        Inspection = i,
        Outcome = o,
    };

    private NonConformanceRaised Ncr(string nc, NcSeverity sev = NcSeverity.Major) => new()
    {
        Serial = Serial,
        FactId = Guid.NewGuid(),
        Hlc = Next(),
        Site = ProcessSite.ToulouseNdtLab,
        Operator = Op,
        Description = $"NCR {nc}",
        NcNumber = nc,
        DefectCode = "DEMO",
        Severity = sev,
    };

    private MrbDisposition Mrb(string nc, MrbDispositionKind d) => new()
    {
        Serial = Serial,
        FactId = Guid.NewGuid(),
        Hlc = Next(),
        Site = ProcessSite.CincinnatiMrb,
        Operator = Op,
        Description = $"MRB {d} {nc}",
        NcNumber = nc,
        Disposition = d,
    };

    private ReworkCompleted Rework(bool retestPassed) => new()
    {
        Serial = Serial,
        FactId = Guid.NewGuid(),
        Hlc = Next(),
        Site = ProcessSite.StuttgartMachining,
        Operator = Op,
        Description = retestPassed ? "rework pass" : "rework fail",
        ReworkOperation = "Weld repair",
        RetestPassed = retestPassed,
    };

    private FinalAcceptance Fai() => new()
    {
        Serial = Serial,
        FactId = Guid.NewGuid(),
        Hlc = Next(),
        Site = ProcessSite.BristolFai,
        Operator = Op,
        Description = "FAI",
        FaiReportId = "FAI-1",
        InspectorId = "insp",
        CertificateIssued = true,
    };

    [Test]
    public void Resolve_throws_when_facts_null()
    {
        Assert.That(
            () => NextActionResolver.Resolve(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Resolve_empty_facts_suggests_heat_treat()
    {
        var s = NextActionResolver.Resolve(Array.Empty<Fact>());
        Assert.That(s.Action, Is.EqualTo(NextAction.CompleteHeatTreat));
    }

    [Test]
    public void Resolve_after_forge_suggests_heat_treat()
    {
        var s = NextActionResolver.Resolve([Step(ProcessStage.Forge)]);
        Assert.That(s.Action, Is.EqualTo(NextAction.CompleteHeatTreat));
    }

    [Test]
    public void Resolve_after_heat_treat_suggests_machining()
    {
        var s = NextActionResolver.Resolve([Step(ProcessStage.Forge), Step(ProcessStage.HeatTreat)]);
        Assert.That(s.Action, Is.EqualTo(NextAction.CompleteMachining));
    }

    [Test]
    public void Resolve_after_machining_suggests_cmm()
    {
        var s = NextActionResolver.Resolve([
            Step(ProcessStage.Forge), Step(ProcessStage.HeatTreat), Step(ProcessStage.Machining)]);
        Assert.That(s.Action, Is.EqualTo(NextAction.RecordCmmInspection));
    }

    [Test]
    public void Resolve_after_cmm_pass_suggests_ndt()
    {
        var s = NextActionResolver.Resolve([
            Step(ProcessStage.Forge), Step(ProcessStage.HeatTreat), Step(ProcessStage.Machining),
            Inspect(Inspection.CMM, InspectionOutcome.Pass)]);
        Assert.That(s.Action, Is.EqualTo(NextAction.RecordNdtInspection));
    }

    [Test]
    public void Resolve_after_ndt_pass_suggests_fai()
    {
        var s = NextActionResolver.Resolve([
            Step(ProcessStage.Forge), Step(ProcessStage.HeatTreat), Step(ProcessStage.Machining),
            Inspect(Inspection.CMM, InspectionOutcome.Pass),
            Inspect(Inspection.FPI, InspectionOutcome.Pass)]);
        Assert.That(s.Action, Is.EqualTo(NextAction.SignOffFai));
    }

    [Test]
    public void Resolve_after_fai_is_terminal()
    {
        var s = NextActionResolver.Resolve([
            Step(ProcessStage.Forge), Step(ProcessStage.HeatTreat), Step(ProcessStage.Machining),
            Inspect(Inspection.CMM, InspectionOutcome.Pass),
            Inspect(Inspection.FPI, InspectionOutcome.Pass), Fai()]);
        Assert.That(s.Action, Is.EqualTo(NextAction.Terminal));
    }

    [Test]
    public void Resolve_with_open_ncr_suggests_issue_mrb_carrying_nc_number()
    {
        var s = NextActionResolver.Resolve([
            Step(ProcessStage.Forge), Step(ProcessStage.HeatTreat), Step(ProcessStage.Machining),
            Inspect(Inspection.CMM, InspectionOutcome.Pass),
            Ncr("NC-001")]);
        Assert.Multiple(() =>
        {
            Assert.That(s.Action, Is.EqualTo(NextAction.IssueMrb));
            Assert.That(s.OpenNcNumber, Is.EqualTo("NC-001"));
        });
    }

    [Test]
    public void Resolve_with_rework_ordered_suggests_complete_rework()
    {
        var s = NextActionResolver.Resolve([
            Step(ProcessStage.Forge), Step(ProcessStage.HeatTreat), Step(ProcessStage.Machining),
            Inspect(Inspection.CMM, InspectionOutcome.Pass),
            Ncr("NC-002"), Mrb("NC-002", MrbDispositionKind.Rework)]);
        Assert.That(s.Action, Is.EqualTo(NextAction.CompleteRework));
    }

    [Test]
    public void Resolve_with_rework_done_retest_passed_suggests_fai()
    {
        var s = NextActionResolver.Resolve([
            Step(ProcessStage.Forge), Step(ProcessStage.HeatTreat), Step(ProcessStage.Machining),
            Inspect(Inspection.CMM, InspectionOutcome.Pass),
            Ncr("NC-003"), Mrb("NC-003", MrbDispositionKind.Rework), Rework(retestPassed: true)]);
        Assert.That(s.Action, Is.EqualTo(NextAction.SignOffFai));
    }

    [Test]
    public void Resolve_with_rework_done_retest_failed_suggests_ndt_retest()
    {
        var s = NextActionResolver.Resolve([
            Step(ProcessStage.Forge), Step(ProcessStage.HeatTreat), Step(ProcessStage.Machining),
            Inspect(Inspection.CMM, InspectionOutcome.Pass),
            Ncr("NC-004"), Mrb("NC-004", MrbDispositionKind.Rework), Rework(retestPassed: false)]);
        Assert.That(s.Action, Is.EqualTo(NextAction.RecordNdtInspection));
    }

    [Test]
    public void Resolve_after_mrb_scrap_is_terminal()
    {
        var s = NextActionResolver.Resolve([
            Step(ProcessStage.Forge), Step(ProcessStage.HeatTreat), Step(ProcessStage.Machining),
            Inspect(Inspection.CMM, InspectionOutcome.Pass),
            Ncr("NC-005"), Mrb("NC-005", MrbDispositionKind.Scrap)]);
        Assert.That(s.Action, Is.EqualTo(NextAction.Terminal));
    }

    [Test]
    public void Resolve_after_mrb_return_to_vendor_is_terminal()
    {
        var s = NextActionResolver.Resolve([
            Step(ProcessStage.Forge), Step(ProcessStage.HeatTreat), Step(ProcessStage.Machining),
            Ncr("NC-006"), Mrb("NC-006", MrbDispositionKind.ReturnToVendor)]);
        Assert.That(s.Action, Is.EqualTo(NextAction.Terminal));
    }

    [Test]
    public void Resolve_after_mrb_use_as_is_resumes_main_path()
    {
        var s = NextActionResolver.Resolve([
            Step(ProcessStage.Forge), Step(ProcessStage.HeatTreat), Step(ProcessStage.Machining),
            Inspect(Inspection.CMM, InspectionOutcome.Pass),
            Ncr("NC-007"), Mrb("NC-007", MrbDispositionKind.UseAsIs)]);
        Assert.That(s.Action, Is.EqualTo(NextAction.RecordNdtInspection));
    }

    [Test]
    public void Resolve_sorts_facts_by_hlc_before_resolving()
    {
        // Identical facts, shuffled — resolver must sort internally.
        var forge = Step(ProcessStage.Forge);
        var ht = Step(ProcessStage.HeatTreat);
        var mach = Step(ProcessStage.Machining);
        var cmm = Inspect(Inspection.CMM, InspectionOutcome.Pass);
        var ncr = Ncr("NC-008");
        var mrb = Mrb("NC-008", MrbDispositionKind.Rework);

        var s = NextActionResolver.Resolve([mrb, forge, ncr, cmm, ht, mach]);
        Assert.That(s.Action, Is.EqualTo(NextAction.CompleteRework));
    }

    [Test]
    public void Resolve_ignores_failed_cmm_without_pass()
    {
        var s = NextActionResolver.Resolve([
            Step(ProcessStage.Forge), Step(ProcessStage.HeatTreat), Step(ProcessStage.Machining),
            Inspect(Inspection.CMM, InspectionOutcome.Fail)]);
        Assert.That(s.Action, Is.EqualTo(NextAction.RecordCmmInspection));
    }
}
