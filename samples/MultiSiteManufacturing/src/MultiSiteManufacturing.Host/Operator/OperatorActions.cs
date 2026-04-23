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
    /// Emits an order-sensitive three-fact "race" trio against
    /// <paramref name="serial"/> at three different origin sites:
    /// <list type="number">
    ///   <item><see cref="NonConformanceRaised"/> (Minor) at <see cref="ProcessSite.ToulouseNdtLab"/>.</item>
    ///   <item><see cref="InspectionRecorded"/> (Visual / Pass) at <see cref="ProcessSite.StuttgartCmmLab"/>.</item>
    ///   <item><see cref="MrbDisposition"/> (UseAsIs) at <see cref="ProcessSite.CincinnatiMrb"/>.</item>
    /// </list>
    /// </summary>
    /// <remarks>
    /// <para>
    /// HLCs are strictly monotonic in the listed order, so <c>ComplianceFold</c>
    /// (lattice backend) always reads the trio as
    /// <c>Nominal → Flagged → Flagged → Nominal</c>. <c>NaiveFold</c>
    /// (baseline backend) applies facts in <i>arrival</i> order; whenever
    /// the three facts land in any order other than the emission order
    /// — e.g. under the <see cref="ChaosPreset.BaselineReorderStorm"/>
    /// preset or when <see cref="SiteConfig.ReorderEnabled"/> flushes a
    /// previously-paused site's queue — baseline cannot demote a flag
    /// that has not yet been raised, and the part diverges from lattice.
    /// This is the canonical UI-driven way to produce a row in the
    /// divergence feed.
    /// </para>
    /// <para>
    /// All three facts flow through <see cref="FederationRouter.EmitAsync"/>
    /// and therefore honour any active site- or backend-level chaos.
    /// </para>
    /// </remarks>
    /// <returns>
    /// A <see cref="RaceResult"/> summarising how many of the three
    /// facts were forwarded versus held at an origin site grain. The
    /// <see cref="RaceResult.Site"/> reports
    /// <see cref="ProcessSite.CincinnatiMrb"/> as the principal site
    /// (the decisive MRB disposition).
    /// </returns>
    public async Task<RaceResult> RaceAsync(
        PartSerialNumber serial,
        OperatorId op,
        CancellationToken cancellationToken = default)
    {
        var ncNumber = $"NCR-{Guid.NewGuid().ToString("N")[..6].ToUpperInvariant()}";
        var facts = new Fact[]
        {
            new NonConformanceRaised
            {
                Serial = serial,
                FactId = Guid.NewGuid(),
                Hlc = clock.Next(),
                Site = ProcessSite.ToulouseNdtLab,
                Operator = op,
                Description = $"Race: NCR {ncNumber} raised (Minor)",
                NcNumber = ncNumber,
                DefectCode = "DEMO-RACE",
                Severity = NcSeverity.Minor,
            },
            new InspectionRecorded
            {
                Serial = serial,
                FactId = Guid.NewGuid(),
                Hlc = clock.Next(),
                Site = ProcessSite.StuttgartCmmLab,
                Operator = op,
                Description = "Race: re-check visual inspection Pass",
                Inspection = Inspection.Visual,
                Outcome = InspectionOutcome.Pass,
                Measurements = new Dictionary<string, string>(),
            },
            new MrbDisposition
            {
                Serial = serial,
                FactId = Guid.NewGuid(),
                Hlc = clock.Next(),
                Site = ProcessSite.CincinnatiMrb,
                Operator = op,
                Description = $"Race: MRB UseAsIs for {ncNumber}",
                NcNumber = ncNumber,
                Disposition = MrbDispositionKind.UseAsIs,
            },
        };

        // Fire all three concurrently. HLCs were stamped sequentially
        // above so the lattice (HLC-sorted) fold remains deterministic
        // (Nominal -> Flagged -> Flagged -> Nominal). Baseline applies
        // facts in arrival order, so racing emissions + per-site delays
        // + (optionally) the backend reorder buffer can deliver the
        // MRB or Inspection <i>before</i> the NCR — at which point
        // baseline cannot demote a flag that has not yet been raised
        // and the part diverges from lattice.
        var tasks = new Task<bool>[facts.Length];
        for (var i = 0; i < facts.Length; i++)
        {
            tasks[i] = router.EmitAsync(facts[i], cancellationToken);
        }
        var results = await Task.WhenAll(tasks);

        var forwarded = 0;
        foreach (var r in results)
        {
            if (r)
            {
                forwarded++;
            }
        }
        return new RaceResult(forwarded, results.Length - forwarded, ProcessSite.CincinnatiMrb);
    }

    /// <summary>
    /// Emits a single <see cref="MrbDisposition"/> (UseAsIs) at
    /// <see cref="ProcessSite.CincinnatiMrb"/> with a fresh monotonic
    /// HLC, intended to restore agreement between the baseline and
    /// lattice backends for a part that is currently diverging in the
    /// <see cref="ComplianceState.FlaggedForReview"/> state. Exposed on
    /// the dashboard as a per-row <b>✓ Fix</b> button that replaces the
    /// <b>⚠ Race</b> button whenever
    /// <c>PartSummaryUpdate.Diverges</c> is true.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Only one fact is emitted (no concurrency, no race), so backend
    /// reorder buffers see a singleton batch and cannot shuffle it. The
    /// HLC is strictly greater than every prior fact for the part, so
    /// both folds see the UseAsIs as the final disposition:
    /// </para>
    /// <list type="bullet">
    ///   <item><c>NaiveFold</c> (baseline, arrival order) appends the
    ///   UseAsIs after every prior fact and demotes
    ///   <see cref="ComplianceState.FlaggedForReview"/> to
    ///   <see cref="ComplianceState.Nominal"/>.</item>
    ///   <item><c>ComplianceFold</c> (lattice, HLC order) applies the
    ///   UseAsIs last and reaches the same terminal state.</item>
    /// </list>
    /// <para>
    /// This fixes the common divergence produced by the
    /// <see cref="RaceAsync"/> trio under chaos (baseline Flagged vs.
    /// lattice Nominal). It does <i>not</i> demote unarmed
    /// <see cref="ComplianceState.Rework"/> or terminal
    /// <see cref="ComplianceState.Scrap"/> — those divergences require
    /// a proper retest + re-disposition sequence and are deliberately
    /// not auto-fixable from a single click.
    /// </para>
    /// </remarks>
    public async Task<FixResult> FixAsync(
        PartSerialNumber serial,
        OperatorId op,
        CancellationToken cancellationToken = default)
    {
        var ncNumber = $"FIX-{Guid.NewGuid().ToString("N")[..6].ToUpperInvariant()}";
        var fact = new MrbDisposition
        {
            Serial = serial,
            FactId = Guid.NewGuid(),
            Hlc = clock.Next(),
            Site = ProcessSite.CincinnatiMrb,
            Operator = op,
            Description = $"Fix: MRB UseAsIs {ncNumber} to restore baseline ↔ lattice agreement",
            NcNumber = ncNumber,
            Disposition = MrbDispositionKind.UseAsIs,
        };

        var forwarded = await router.EmitAsync(fact, cancellationToken);
        return new FixResult(forwarded, ProcessSite.CincinnatiMrb);
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
/// Outcome of <see cref="OperatorActions.RaceAsync"/>: how many of the
/// three race-trio facts were forwarded through federation fan-out
/// versus held at an origin site grain (paused or buffered for reorder).
/// </summary>
/// <param name="Forwarded">Count of facts that reached the backends and raised <c>FactRouted</c>.</param>
/// <param name="Held">Count of facts held by an origin site grain's chaos config.</param>
/// <param name="Site">Principal origin site for the race (Cincinnati MRB — the decisive disposition).</param>
public readonly record struct RaceResult(int Forwarded, int Held, ProcessSite Site)
{
    /// <summary>True when every emitted fact was held (no downstream side effects).</summary>
    public bool AllHeld => Forwarded == 0 && Held > 0;

    /// <summary>Total facts attempted (<see cref="Forwarded"/> + <see cref="Held"/>).</summary>
    public int Total => Forwarded + Held;
}

/// <summary>
/// Outcome of <see cref="OperatorActions.FixAsync"/>: whether the single
/// MRB UseAsIs disposition reached the backends or was held at the
/// origin site grain (paused or buffered for reorder).
/// </summary>
/// <param name="Forwarded">True when the fact fanned out to both backends and raised <c>FactRouted</c>.</param>
/// <param name="Site">Origin site the fix was emitted at (Cincinnati MRB).</param>
public readonly record struct FixResult(bool Forwarded, ProcessSite Site)
{
    /// <summary>True when the fact was held by the origin site grain.</summary>
    public bool Held => !Forwarded;
}
