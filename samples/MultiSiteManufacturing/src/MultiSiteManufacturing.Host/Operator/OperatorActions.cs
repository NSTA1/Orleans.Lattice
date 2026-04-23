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
    /// Emits a burst of <paramref name="count"/> benign
    /// <see cref="InspectionRecorded"/> facts (Visual / Pass at
    /// <see cref="ProcessSite.StuttgartCmmLab"/>) against
    /// <paramref name="serial"/>. Intended as a UI-driven helper to
    /// generate fact volume — for example, to surface baseline-vs-lattice
    /// divergence when a lattice-only chaos preset is active. All facts
    /// flow through <see cref="FederationRouter"/> and therefore honour
    /// any active site- or backend-level chaos.
    /// </summary>
    /// <param name="serial">Target part serial.</param>
    /// <param name="count">Number of facts to emit. Must be positive.</param>
    /// <param name="op">Operator stamped onto each fact.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A <see cref="BurstResult"/> summarising how many of the
    /// <paramref name="count"/> emitted facts were forwarded through the
    /// federation fan-out versus held by the origin site grain (paused
    /// or buffered for reorder).
    /// </returns>
    public async Task<BurstResult> BurstAsync(
        PartSerialNumber serial,
        int count,
        OperatorId op,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(count);

        const ProcessSite site = ProcessSite.StuttgartCmmLab;
        var forwarded = 0;
        var held = 0;
        for (var i = 0; i < count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var fact = new InspectionRecorded
            {
                Serial = serial,
                FactId = Guid.NewGuid(),
                Hlc = clock.Next(),
                Site = site,
                Operator = op,
                Description = $"Burst visual inspection {i + 1}/{count}: Pass",
                Inspection = Inspection.Visual,
                Outcome = InspectionOutcome.Pass,
                Measurements = new Dictionary<string, string>(),
            };
            if (await router.EmitAsync(fact, cancellationToken))
            {
                forwarded++;
            }
            else
            {
                held++;
            }
        }
        return new BurstResult(forwarded, held, site);
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

/// <summary>
/// Outcome of <see cref="OperatorActions.BurstAsync"/>: how many of the
/// requested facts were forwarded through federation fan-out versus
/// held at the origin site grain (paused or buffered for reorder).
/// </summary>
/// <param name="Forwarded">Count of facts that reached the backends and raised <c>FactRouted</c>.</param>
/// <param name="Held">Count of facts held by the origin site grain's chaos config.</param>
/// <param name="Site">Origin site all burst facts were emitted at.</param>
public readonly record struct BurstResult(int Forwarded, int Held, ProcessSite Site)
{
    /// <summary>True when every emitted fact was held (no downstream side effects).</summary>
    public bool AllHeld => Forwarded == 0 && Held > 0;

    /// <summary>Total facts attempted (<see cref="Forwarded"/> + <see cref="Held"/>).</summary>
    public int Total => Forwarded + Held;
}
