using MultiSiteManufacturing.Host.Domain;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace MultiSiteManufacturing.Tests.Domain;

/// <summary>
/// Unit tests for <see cref="ProcessStageMap.Of(Fact)"/>, the shared fact-to-stage
/// mapping used by both the folded compliance projection and the dashboard
/// broadcaster's per-part summary build so the two paths cannot drift.
/// </summary>
[TestFixture]
public class ProcessStageMapTests
{
    private static readonly PartSerialNumber Serial = new("HPT-BLD-S1-2028-00001");
    private static readonly OperatorId Op = OperatorId.Demo;

    private static HybridLogicalClock Hlc(long tick) => new() { WallClockTicks = tick, Counter = 0 };

    [Test]
    public void Of_ProcessStepCompleted_returns_the_step_stage()
    {
        var fact = new ProcessStepCompleted
        {
            Serial = Serial, FactId = Guid.NewGuid(), Hlc = Hlc(1),
            Site = ProcessSite.StuttgartMachining, Operator = Op, Description = "Machining completed",
            Stage = ProcessStage.Machining,
        };

        Assert.That(ProcessStageMap.Of(fact), Is.EqualTo(ProcessStage.Machining));
    }

    [Test]
    public void Of_InspectionRecorded_returns_NDT()
    {
        var fact = new InspectionRecorded
        {
            Serial = Serial, FactId = Guid.NewGuid(), Hlc = Hlc(1),
            Site = ProcessSite.ToulouseNdtLab, Operator = Op, Description = "FPI Pass",
            Inspection = Inspection.FPI, Outcome = InspectionOutcome.Pass,
        };

        Assert.That(ProcessStageMap.Of(fact), Is.EqualTo(ProcessStage.NDT));
    }

    [Test]
    public void Of_NonConformanceRaised_returns_MRB()
    {
        var fact = new NonConformanceRaised
        {
            Serial = Serial, FactId = Guid.NewGuid(), Hlc = Hlc(1),
            Site = ProcessSite.ToulouseNdtLab, Operator = Op, Description = "Minor NC",
            NcNumber = "NC-1", DefectCode = "D-001", Severity = NcSeverity.Minor,
        };

        Assert.That(ProcessStageMap.Of(fact), Is.EqualTo(ProcessStage.MRB));
    }

    [Test]
    public void Of_MrbDisposition_returns_MRB()
    {
        var fact = new MrbDisposition
        {
            Serial = Serial, FactId = Guid.NewGuid(), Hlc = Hlc(1),
            Site = ProcessSite.CincinnatiMrb, Operator = Op, Description = "MRB UseAsIs",
            NcNumber = "NC-1", Disposition = MrbDispositionKind.UseAsIs,
        };

        Assert.That(ProcessStageMap.Of(fact), Is.EqualTo(ProcessStage.MRB));
    }

    [Test]
    public void Of_ReworkCompleted_returns_MRB()
    {
        var fact = new ReworkCompleted
        {
            Serial = Serial, FactId = Guid.NewGuid(), Hlc = Hlc(1),
            Site = ProcessSite.StuttgartMachining, Operator = Op, Description = "rework op",
            ReworkOperation = "re-blend", RetestPassed = true,
        };

        Assert.That(ProcessStageMap.Of(fact), Is.EqualTo(ProcessStage.MRB));
    }

    [Test]
    public void Of_FinalAcceptance_returns_FAI()
    {
        var fact = new FinalAcceptance
        {
            Serial = Serial, FactId = Guid.NewGuid(), Hlc = Hlc(1),
            Site = ProcessSite.CincinnatiMrb, Operator = Op, Description = "FAI signed",
            FaiReportId = "FAI-1", InspectorId = "inspector:1", CertificateIssued = true,
        };

        Assert.That(ProcessStageMap.Of(fact), Is.EqualTo(ProcessStage.FAI));
    }

    [Test]
    public void Of_null_fact_throws_ArgumentNullException()
    {
        Assert.That(() => ProcessStageMap.Of(null!), Throws.ArgumentNullException);
    }
}
