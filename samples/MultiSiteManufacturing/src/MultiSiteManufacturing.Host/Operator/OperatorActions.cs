using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;

namespace MultiSiteManufacturing.Host.Operator;

/// <summary>
/// Facade used by the Blazor UI (and any other operator-facing surface) to
/// emit the six fact kinds that drive a part's compliance state. Each
/// method stamps a monotonic HLC via <see cref="OperatorClock"/>, picks a
/// canonical <see cref="ProcessSite"/>, and routes through the
/// <see cref="FederationRouter"/> so chaos controls and fan-out apply.
/// </summary>
public sealed class OperatorActions(FederationRouter router, OperatorClock clock)
{
    private const string LatticeBackendName = "lattice";

    /// <summary>
    /// Creates a brand-new part by allocating the next serial in the
    /// <paramref name="family"/> / current year and emitting the first
    /// <see cref="ProcessStepCompleted"/> fact at <paramref name="initialStage"/>.
    /// </summary>
    public async Task<PartSerialNumber> CreatePartAsync(
        PartFamily family,
        ProcessStage initialStage,
        OperatorId op,
        CancellationToken cancellationToken = default)
    {
        var lattice = router.GetBackend(LatticeBackendName);
        var existing = await lattice.ListPartsAsync(cancellationToken);
        var year = DateTime.UtcNow.Year;
        var sequence = existing.Count + 1;
        var serial = PartSerialNumber.From(family, year, sequence);

        var fact = new ProcessStepCompleted
        {
            Serial = serial,
            FactId = Guid.NewGuid(),
            Hlc = clock.Next(),
            Site = SiteForStage(initialStage),
            Operator = op,
            Description = $"{initialStage} step completed",
            Stage = initialStage,
        };
        await router.EmitAsync(fact, cancellationToken);
        return serial;
    }

    /// <summary>Emits a <see cref="ProcessStepCompleted"/> fact for an existing part.</summary>
    public Task CompleteProcessStepAsync(
        PartSerialNumber serial,
        ProcessStage stage,
        OperatorId op,
        string? heatLot = null,
        IReadOnlyDictionary<string, string>? processParameters = null,
        CancellationToken cancellationToken = default)
    {
        var fact = new ProcessStepCompleted
        {
            Serial = serial,
            FactId = Guid.NewGuid(),
            Hlc = clock.Next(),
            Site = SiteForStage(stage),
            Operator = op,
            Description = $"{stage} step completed",
            Stage = stage,
            HeatLot = heatLot,
            ProcessParameters = processParameters ?? new Dictionary<string, string>(),
        };
        return router.EmitAsync(fact, cancellationToken);
    }

    /// <summary>Emits an <see cref="InspectionRecorded"/> fact.</summary>
    public Task RecordInspectionAsync(
        PartSerialNumber serial,
        Inspection inspection,
        InspectionOutcome outcome,
        ProcessSite site,
        OperatorId op,
        IReadOnlyDictionary<string, string>? measurements = null,
        DateTimeOffset? instrumentCalibrationDate = null,
        CancellationToken cancellationToken = default)
    {
        var fact = new InspectionRecorded
        {
            Serial = serial,
            FactId = Guid.NewGuid(),
            Hlc = clock.Next(),
            Site = site,
            Operator = op,
            Description = $"{inspection} inspection: {outcome}",
            Inspection = inspection,
            Outcome = outcome,
            Measurements = measurements ?? new Dictionary<string, string>(),
            InstrumentCalibrationDate = instrumentCalibrationDate,
        };
        return router.EmitAsync(fact, cancellationToken);
    }

    /// <summary>Emits a <see cref="NonConformanceRaised"/> fact.</summary>
    public Task RaiseNonConformanceAsync(
        PartSerialNumber serial,
        string ncNumber,
        string defectCode,
        NcSeverity severity,
        ProcessSite site,
        OperatorId op,
        CancellationToken cancellationToken = default)
    {
        var fact = new NonConformanceRaised
        {
            Serial = serial,
            FactId = Guid.NewGuid(),
            Hlc = clock.Next(),
            Site = site,
            Operator = op,
            Description = $"NCR {ncNumber} raised ({severity}): {defectCode}",
            NcNumber = ncNumber,
            DefectCode = defectCode,
            Severity = severity,
        };
        return router.EmitAsync(fact, cancellationToken);
    }

    /// <summary>Emits an <see cref="MrbDisposition"/> fact against an existing NC.</summary>
    public Task DispositionMrbAsync(
        PartSerialNumber serial,
        string ncNumber,
        MrbDispositionKind disposition,
        OperatorId op,
        CancellationToken cancellationToken = default)
    {
        var fact = new MrbDisposition
        {
            Serial = serial,
            FactId = Guid.NewGuid(),
            Hlc = clock.Next(),
            Site = ProcessSite.CincinnatiMrb,
            Operator = op,
            Description = $"MRB disposition for {ncNumber}: {disposition}",
            NcNumber = ncNumber,
            Disposition = disposition,
        };
        return router.EmitAsync(fact, cancellationToken);
    }

    /// <summary>Emits a <see cref="ReworkCompleted"/> fact.</summary>
    public Task CompleteReworkAsync(
        PartSerialNumber serial,
        string reworkOperation,
        bool retestPassed,
        OperatorId op,
        CancellationToken cancellationToken = default)
    {
        var fact = new ReworkCompleted
        {
            Serial = serial,
            FactId = Guid.NewGuid(),
            Hlc = clock.Next(),
            Site = ProcessSite.StuttgartMachining,
            Operator = op,
            Description = retestPassed
                ? $"Rework '{reworkOperation}' completed; retest passed"
                : $"Rework '{reworkOperation}' completed; retest FAILED",
            ReworkOperation = reworkOperation,
            RetestPassed = retestPassed,
        };
        return router.EmitAsync(fact, cancellationToken);
    }

    /// <summary>Emits a <see cref="FinalAcceptance"/> fact.</summary>
    public Task SignOffFaiAsync(
        PartSerialNumber serial,
        string faiReportId,
        string inspectorId,
        bool certificateIssued,
        OperatorId op,
        CancellationToken cancellationToken = default)
    {
        var fact = new FinalAcceptance
        {
            Serial = serial,
            FactId = Guid.NewGuid(),
            Hlc = clock.Next(),
            Site = ProcessSite.BristolFai,
            Operator = op,
            Description = certificateIssued
                ? $"FAI {faiReportId} signed off by {inspectorId}; certificate issued"
                : $"FAI {faiReportId} signed off by {inspectorId}; no certificate",
            FaiReportId = faiReportId,
            InspectorId = inspectorId,
            CertificateIssued = certificateIssued,
        };
        return router.EmitAsync(fact, cancellationToken);
    }

    /// <summary>
    /// Canonical <see cref="ProcessSite"/> for each <see cref="ProcessStage"/>. Mirrors
    /// the mapping in <c>InventoryServiceImpl</c> so gRPC- and UI-driven facts land at the
    /// same physical site.
    /// </summary>
    internal static ProcessSite SiteForStage(ProcessStage stage) => stage switch
    {
        ProcessStage.Forge => ProcessSite.OhioForge,
        ProcessStage.HeatTreat => ProcessSite.NagoyaHeatTreat,
        ProcessStage.Machining => ProcessSite.StuttgartMachining,
        ProcessStage.NDT => ProcessSite.ToulouseNdtLab,
        ProcessStage.MRB => ProcessSite.CincinnatiMrb,
        ProcessStage.FAI => ProcessSite.BristolFai,
        _ => ProcessSite.OhioForge,
    };
}
