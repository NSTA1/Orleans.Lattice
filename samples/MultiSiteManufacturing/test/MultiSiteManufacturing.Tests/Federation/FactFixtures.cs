using MultiSiteManufacturing.Host.Baseline;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Lattice;
using Orleans.Lattice.Primitives;

namespace MultiSiteManufacturing.Tests.Federation;

/// <summary>
/// Shared test helpers for building <see cref="Fact"/> fixtures with a
/// controlled HLC so reorder scenarios are deterministic.
/// </summary>
internal static class FactFixtures
{
    public static readonly OperatorId Op = OperatorId.Demo;

    public static HybridLogicalClock Hlc(long tick) =>
        new() { WallClockTicks = tick, Counter = 0 };

    public static ProcessStepCompleted Step(PartSerialNumber serial, long tick, ProcessStage stage, ProcessSite site) =>
        new()
        {
            Serial = serial, FactId = Guid.NewGuid(), Hlc = Hlc(tick),
            Site = site, Operator = Op, Description = $"{stage} completed",
            Stage = stage,
        };

    public static NonConformanceRaised Nc(PartSerialNumber serial, long tick, string ncNumber, NcSeverity severity, ProcessSite site) =>
        new()
        {
            Serial = serial, FactId = Guid.NewGuid(), Hlc = Hlc(tick),
            Site = site, Operator = Op, Description = $"{severity} NC {ncNumber}",
            NcNumber = ncNumber, DefectCode = "D-001", Severity = severity,
        };

    public static MrbDisposition Mrb(PartSerialNumber serial, long tick, string ncNumber, MrbDispositionKind disposition, ProcessSite site) =>
        new()
        {
            Serial = serial, FactId = Guid.NewGuid(), Hlc = Hlc(tick),
            Site = site, Operator = Op, Description = $"MRB {disposition}",
            NcNumber = ncNumber, Disposition = disposition,
        };
}
