using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Lattice;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace MultiSiteManufacturing.Host.Inventory;

/// <summary>
/// Bulk-loads a small, diverse spread of parts into the federation on
/// silo startup. The seed is deterministic - same serial numbers, same
/// facts, same HLCs across runs - so demos look identical every time
/// the host starts against a fresh storage account.
/// </summary>
/// <remarks>
/// <para>
/// The seeder gates on <see cref="IInventorySeedStateGrain"/> together
/// with the lattice fact backend: it skips only when the durable seed
/// marker is set <em>and</em> the lattice tree still holds parts, so
/// operator mutations are preserved across restarts. If the marker
/// survives but the (non-durable) lattice data was lost on an abrupt
/// silo restart, the seeder re-seeds rather than leaving the cluster
/// flagged "seeded" over empty trees. To force a full re-seed from a
/// clean slate, delete the <c>msmfgGrainState</c> and
/// <c>msmfgLatticeFacts*</c> tables from Azurite (or the target storage
/// account).
/// </para>
/// <para>
/// Before emitting, the seeder snapshots every site's chaos configuration
/// and forces them all to nominal (unpaused, zero delay). After emission
/// it restores the snapshot, so a previous session's chaos preset cannot
/// interfere with seed determinism.
/// </para>
/// <para>
/// The seed produces 5 parts - one per reachable <see cref="ComplianceState"/>
/// terminal / representative state - chosen to keep the demo inventory
/// small and readable while still exercising every fold transition the UI
/// needs to render (Nominal early, Nominal complete, FlaggedForReview,
/// Rework, Scrap).
/// </para>
/// </remarks>
public sealed class InventorySeeder(
    FederationRouter router,
    IGrainFactory grains,
    PartCrdtStore crdt,
    ILogger<InventorySeeder> logger) : IHostedService
{
    private const string Family = "HPT-BLD-S1";
    private const int SeedYear = 2028;

    /// <summary>Total number of parts the seeder produces against an empty store.</summary>
    public const int TotalParts = 5;

    /// <summary>
    /// Sequence number of the part the seeder drives CRDT change-history into,
    /// so the Explorer History tab has a non-trivial timeline to render for
    /// both CRDT shapes (operator last-writer-wins register and process-label
    /// OR-Set) the moment the sample starts.
    /// </summary>
    private const int CrdtHistorySeq = 2;

    /// <summary>
    /// Ordered operator handoffs written to the same operator-register key so
    /// the last-writer-wins timeline shows successive values plus diffs. The
    /// final entry is <see cref="OperatorId.Demo"/> so the part's current
    /// operator matches the rest of the seeded inventory.
    /// </summary>
    private static readonly OperatorId[] CrdtHistoryOperators =
    [
        new("operator:alice"),
        new("operator:bob"),
        new("operator:carol"),
        OperatorId.Demo,
    ];

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken) => SeedAsync(cancellationToken);

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>
    /// Seeds the inventory if the <see cref="IInventorySeedStateGrain"/>
    /// flag is unset; otherwise no-ops. Public so tests can invoke it
    /// directly against a <see cref="FederationRouter"/>.
    /// </summary>
    public async Task SeedAsync(CancellationToken cancellationToken)
    {
        var seedFlag = grains.GetGrain<IInventorySeedStateGrain>(IInventorySeedStateGrain.SingletonKey);
        var alreadyMarked = await seedFlag.HasSeededAsync();

        // The seed marker lives in durable grain storage, but the lattice
        // fact tree it guards is not guaranteed to survive an abrupt silo
        // restart (for example recreating the container to pick up a new
        // image). Trusting the marker alone can leave the cluster flagged
        // "seeded" over empty trees, so verify the lattice backend actually
        // holds parts before skipping; re-seed when it does not.
        if (alreadyMarked && await LatticeInventoryPresentAsync(cancellationToken))
        {
            logger.LogInformation("Inventory already seeded; skipping bulk load.");
            return;
        }

        if (alreadyMarked)
        {
            logger.LogWarning(
                "Inventory seed marker is set but the lattice fact tree is empty; the seeded data did not survive a restart. Re-seeding.");
        }

        await seedFlag.TryMarkSeededAsync();

        logger.LogInformation("Seeding {Count} parts into the federation…", TotalParts);

        var snapshot = await router.ListSitesAsync();
        try
        {
            // Force every site to nominal so chaos cannot perturb the seed.
            foreach (var state in snapshot)
            {
                if (!state.Config.Equals(SiteConfig.Nominal))
                {
                    await router.ConfigureSiteAsync(state.Site, SiteConfig.Nominal, cancellationToken);
                }
            }

            await EmitAllAsync(cancellationToken);
        }
        finally
        {
            // Restore whatever chaos config was in effect before seeding.
            foreach (var state in snapshot)
            {
                if (!state.Config.Equals(SiteConfig.Nominal))
                {
                    await router.ConfigureSiteAsync(state.Site, state.Config, cancellationToken);
                }
            }
        }

        logger.LogInformation("Inventory seed complete.");

        // Drive change-history into the CRDT trees so the Explorer History
        // tab has a populated, multi-revision timeline the moment the sample
        // starts - independent of the fact log seeded above.
        await SeedCrdtHistoryAsync(cancellationToken);
    }

    /// <summary>
    /// Writes several revisions into the same CRDT keys for one representative
    /// part so the Explorer History tab shows a non-trivial timeline for both
    /// CRDT shapes:
    /// <list type="bullet">
    ///   <item>
    ///     the operator last-writer-wins register in the
    ///     <see cref="PartCrdtStore.OperatorTreeId"/> tree - a sequence of
    ///     operator handoffs, so the History tab renders successive values
    ///     plus diffs;
    ///   </item>
    ///   <item>
    ///     the process-label OR-Set in the
    ///     <see cref="PartCrdtStore.LabelsTreeId"/> tree - interleaved add and
    ///     remove operations, so the History tab renders element-level member
    ///     changes.
    ///   </item>
    /// </list>
    /// Each lattice write advances the source tree's hybrid logical clock, so
    /// every call below lands as a distinct, time-ordered revision. The
    /// <see cref="MultiSiteManufacturing.Host.Lattice.HistoryShowcaseActivator"/>
    /// enables a durable history view (with value-retaining retention) over both
    /// trees ahead of this seeding step, so these revisions are captured into a
    /// durable, retention-bounded timeline rather than only the transient
    /// write-ahead-log window.
    /// </summary>
    private async Task SeedCrdtHistoryAsync(CancellationToken cancellationToken)
    {
        var serial = PartSerialNumber.From(new PartFamily(Family), SeedYear, CrdtHistorySeq);

        // Operator LWW register: one key, four successive values (handoffs).
        foreach (var op in CrdtHistoryOperators)
        {
            await crdt.AssignOperatorAsync(serial, op, cancellationToken);
        }

        // Process-label OR-Set: one key, interleaved add / remove operations.
        await crdt.AddLabelAsync(serial, "priority", cancellationToken);
        await crdt.AddLabelAsync(serial, "expedite", cancellationToken);
        await crdt.AddLabelAsync(serial, "rework-watch", cancellationToken);
        await crdt.RemoveLabelAsync(serial, "expedite", cancellationToken);
        await crdt.AddLabelAsync(serial, "qa-hold", cancellationToken);
        await crdt.RemoveLabelAsync(serial, "rework-watch", cancellationToken);

        logger.LogInformation(
            "Seeded CRDT change-history for {Serial}: {OperatorRevisions} operator revisions, 6 label operations.",
            serial.Value,
            CrdtHistoryOperators.Length);
    }

    /// <summary>
    /// Returns <c>true</c> when the lattice fact backend already holds at
    /// least one part, used to distinguish a genuinely seeded cluster from
    /// one whose durable seed marker outlived the (non-durable) lattice
    /// data across a restart.
    /// </summary>
    private async Task<bool> LatticeInventoryPresentAsync(CancellationToken cancellationToken)
    {
        var parts = await router.GetBackend(LatticeFactBackend.BackendName).ListPartsAsync(cancellationToken);
        return parts.Count > 0;
    }

    private async Task EmitAllAsync(CancellationToken cancellationToken)
    {
        // 5 parts total, one representative per reachable fold outcome:
        //   seq 1 - Forge only                         → Nominal
        //   seq 2 - Full lifecycle incl. FAI           → Nominal
        //   seq 3 - NDT raised minor NC                → FlaggedForReview
        //   seq 4 - NDT major NC + MRB Rework          → Rework
        //   seq 5 - NDT critical NC                    → Scrap
        await EmitForgeOnlyAsync(1, cancellationToken);
        await EmitFullLifecycleAsync(2, cancellationToken);
        await EmitFlaggedForReviewAsync(3, cancellationToken);
        await EmitReworkAsync(4, cancellationToken);
        await EmitScrapAsync(5, cancellationToken);
    }

    private async Task EmitForgeOnlyAsync(int seq, CancellationToken ct)
    {
        var ctx = PartContext.Create(seq);
        await EmitForgeAsync(ctx, ct);
    }

    private async Task EmitFlaggedForReviewAsync(int seq, CancellationToken ct)
    {
        var ctx = PartContext.Create(seq);
        await EmitForgeAsync(ctx, ct);
        await EmitStageAsync(ctx, ProcessStage.HeatTreat, ProcessSite.NagoyaHeatTreat, "heat-treat complete", ct);
        await EmitStageAsync(ctx, ProcessStage.Machining, ProcessSite.StuttgartMachining, "machining complete", ct);
        await EmitInspectionAsync(ctx, Inspection.CMM, InspectionOutcome.Pass,
            ProcessSite.StuttgartCmmLab, "CMM pass", ct);
        await EmitNonConformanceAsync(ctx, NcSeverity.Minor, $"NC-{seq:D5}-A",
            ProcessSite.ToulouseNdtLab, "FPI indication (minor)", ct);
    }

    private async Task EmitReworkAsync(int seq, CancellationToken ct)
    {
        var ctx = PartContext.Create(seq);
        await EmitForgeAsync(ctx, ct);
        await EmitStageAsync(ctx, ProcessStage.HeatTreat, ProcessSite.NagoyaHeatTreat, "heat-treat complete", ct);
        await EmitStageAsync(ctx, ProcessStage.Machining, ProcessSite.StuttgartMachining, "machining complete", ct);
        await EmitInspectionAsync(ctx, Inspection.CMM, InspectionOutcome.Pass,
            ProcessSite.StuttgartCmmLab, "CMM pass", ct);
        await EmitNonConformanceAsync(ctx, NcSeverity.Major, $"NC-{seq:D5}-B",
            ProcessSite.ToulouseNdtLab, "eddy-current indication (major)", ct);
        await EmitMrbAsync(ctx, MrbDispositionKind.Rework, $"NC-{seq:D5}-B",
            ProcessSite.CincinnatiMrb, "MRB: rework", ct);
    }

    private async Task EmitFullLifecycleAsync(int seq, CancellationToken ct)
    {
        var ctx = PartContext.Create(seq);
        await EmitForgeAsync(ctx, ct);
        await EmitStageAsync(ctx, ProcessStage.HeatTreat, ProcessSite.NagoyaHeatTreat, "heat-treat complete", ct);
        await EmitStageAsync(ctx, ProcessStage.Machining, ProcessSite.StuttgartMachining, "machining complete", ct);
        await EmitInspectionAsync(ctx, Inspection.CMM, InspectionOutcome.Pass,
            ProcessSite.StuttgartCmmLab, "CMM pass", ct);
        await EmitInspectionAsync(ctx, Inspection.FPI, InspectionOutcome.Pass,
            ProcessSite.ToulouseNdtLab, "FPI pass", ct);
        await EmitFaiAsync(ctx, ct);
    }

    private async Task EmitScrapAsync(int seq, CancellationToken ct)
    {
        var ctx = PartContext.Create(seq);
        await EmitForgeAsync(ctx, ct);
        await EmitStageAsync(ctx, ProcessStage.HeatTreat, ProcessSite.NagoyaHeatTreat, "heat-treat complete", ct);
        await EmitStageAsync(ctx, ProcessStage.Machining, ProcessSite.StuttgartMachining, "machining complete", ct);
        await EmitInspectionAsync(ctx, Inspection.CMM, InspectionOutcome.Pass,
            ProcessSite.StuttgartCmmLab, "CMM pass", ct);
        await EmitNonConformanceAsync(ctx, NcSeverity.Critical, $"NC-{seq:D5}-X",
            ProcessSite.ToulouseNdtLab, "X-ray: critical void", ct);
    }

    private Task EmitForgeAsync(PartContext ctx, CancellationToken ct) =>
        EmitStageAsync(ctx, ProcessStage.Forge, ProcessSite.OhioForge, "forge complete", ct);

    private Task EmitStageAsync(
        PartContext ctx,
        ProcessStage stage,
        ProcessSite site,
        string description,
        CancellationToken ct)
    {
        var fact = new ProcessStepCompleted
        {
            Serial = ctx.Serial,
            FactId = ctx.NextFactId(),
            Hlc = ctx.NextHlc(),
            Site = site,
            Operator = OperatorId.Demo,
            Description = description,
            Stage = stage,
        };
        return router.EmitAsync(fact, ct);
    }

    private Task EmitInspectionAsync(
        PartContext ctx,
        Inspection inspection,
        InspectionOutcome outcome,
        ProcessSite site,
        string description,
        CancellationToken ct)
    {
        var fact = new InspectionRecorded
        {
            Serial = ctx.Serial,
            FactId = ctx.NextFactId(),
            Hlc = ctx.NextHlc(),
            Site = site,
            Operator = OperatorId.Demo,
            Description = description,
            Inspection = inspection,
            Outcome = outcome,
        };
        return router.EmitAsync(fact, ct);
    }

    private Task EmitNonConformanceAsync(
        PartContext ctx,
        NcSeverity severity,
        string ncNumber,
        ProcessSite site,
        string description,
        CancellationToken ct)
    {
        var fact = new NonConformanceRaised
        {
            Serial = ctx.Serial,
            FactId = ctx.NextFactId(),
            Hlc = ctx.NextHlc(),
            Site = site,
            Operator = OperatorId.Demo,
            Description = description,
            NcNumber = ncNumber,
            DefectCode = "DEF-SEED",
            Severity = severity,
        };
        return router.EmitAsync(fact, ct);
    }

    private Task EmitMrbAsync(
        PartContext ctx,
        MrbDispositionKind disposition,
        string ncNumber,
        ProcessSite site,
        string description,
        CancellationToken ct)
    {
        var fact = new MrbDisposition
        {
            Serial = ctx.Serial,
            FactId = ctx.NextFactId(),
            Hlc = ctx.NextHlc(),
            Site = site,
            Operator = OperatorId.Demo,
            Description = description,
            NcNumber = ncNumber,
            Disposition = disposition,
        };
        return router.EmitAsync(fact, ct);
    }

    private Task EmitFaiAsync(PartContext ctx, CancellationToken ct)
    {
        var fact = new FinalAcceptance
        {
            Serial = ctx.Serial,
            FactId = ctx.NextFactId(),
            Hlc = ctx.NextHlc(),
            Site = ProcessSite.BristolFai,
            Operator = OperatorId.Demo,
            Description = "FAI signed off",
            FaiReportId = $"FAI-{ctx.Serial.Value}",
            InspectorId = "inspector:demo",
            CertificateIssued = true,
        };
        return router.EmitAsync(fact, ct);
    }

    /// <summary>
    /// Per-part mutable context that hands out monotonic HLCs and
    /// fact ids. Each part starts in its own slot of a rolling 5-day
    /// seed window - the slot is derived from the sequence number -
    /// and <see cref="NextHlc"/> advances by a jittered 60–180 minute
    /// gap per fact. Determinism is preserved across reseeds that
    /// happen inside the same 5-day window; the window itself scrolls
    /// with wall-clock time so the dashboard always shows "recent"
    /// activity, which means two reseeds hours apart will differ in
    /// absolute timestamps while the intra-part cadence stays stable.
    /// </summary>
    private sealed class PartContext
    {
        // Spread seeded parts across the last ~5 days so per-part activity
        // streams have room to breathe without spilling into the future. Each
        // part starts ~(5d / TotalParts) after the previous one, and facts
        // inside a part advance by a jittered 10m–2h gap - so the dashboard
        // "When" column shows realistic spacing (e.g. Forge 3d ago, Heat Treat
        // 2d ago, Machining 18h ago, FAI 1h ago) instead of everything landing
        // in the same millisecond.
        private static readonly long SeedWindowSpanTicks = TimeSpan.FromDays(5).Ticks;
        private static readonly long SeedWindowStartTicks =
            DateTimeOffset.UtcNow.Ticks - SeedWindowSpanTicks;
        private static readonly long PerPartStrideTicks =
            SeedWindowSpanTicks / Math.Max(1, TotalParts);

        public PartSerialNumber Serial { get; }
        private HybridLogicalClock _hlc;
        private int _factOrdinal;
        private readonly int _seq;

        private PartContext(int seq, PartSerialNumber serial)
        {
            _seq = seq;
            Serial = serial;
            _hlc = new HybridLogicalClock
            {
                WallClockTicks = SeedWindowStartTicks + (long)(seq - 1) * PerPartStrideTicks,
                Counter = 0,
            };
        }

        public static PartContext Create(int seq) =>
            new(seq, PartSerialNumber.From(new PartFamily(Family), SeedYear, seq));

        public HybridLogicalClock NextHlc()
        {
            // Deterministic 60–180 minute gap between consecutive facts,
            // seeded by the current HLC so reseeds over the same data
            // produce the same schedule. Clamped to "now" so the final
            // facts of late parts don't drift into the future if the
            // seeder is slow.
            var hash = (uint)(_hlc.WallClockTicks ^ ((long)_seq * 2654435761L));
            var gapMinutes = 60 + (int)(hash % 121U);
            var nextTicks = _hlc.WallClockTicks + TimeSpan.FromMinutes(gapMinutes).Ticks;
            var nowTicks = DateTimeOffset.UtcNow.Ticks;
            if (nextTicks > nowTicks)
            {
                nextTicks = nowTicks;
            }
            _hlc = new HybridLogicalClock
            {
                WallClockTicks = nextTicks,
                Counter = 0,
            };
            return _hlc;
        }

        public Guid NextFactId()
        {
            // Deterministic GUID derived from (seq, ordinal): byte 0..3 = seq, byte 4..7 = ordinal.
            Span<byte> bytes = stackalloc byte[16];
            BitConverter.TryWriteBytes(bytes[..4], _seq);
            BitConverter.TryWriteBytes(bytes.Slice(4, 4), _factOrdinal++);
            return new Guid(bytes);
        }
    }
}
