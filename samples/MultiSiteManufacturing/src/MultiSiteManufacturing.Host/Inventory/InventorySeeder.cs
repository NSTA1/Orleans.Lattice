using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using Orleans.Lattice.Primitives;

namespace MultiSiteManufacturing.Host.Inventory;

/// <summary>
/// Bulk-loads a realistic spread of parts into the federation on silo
/// startup (plan §8). The seed is deterministic — same serial numbers,
/// same facts, same HLCs across runs — so demos look identical every time
/// the host starts against a fresh storage account.
/// </summary>
/// <remarks>
/// <para>
/// The seeder gates on <see cref="IInventorySeedStateGrain"/>: once the
/// flag is set against a given Azure Table Storage account, subsequent
/// host starts no-op and preserve any operator mutations.
/// </para>
/// <para>
/// Before emitting, the seeder snapshots every site's chaos configuration
/// and forces them all to nominal (unpaused, zero delay). After emission
/// it restores the snapshot, so a previous session's chaos preset cannot
/// interfere with seed determinism.
/// </para>
/// <para>
/// The plan §8.1 shape calls for 6 parts in <c>UnderInspection</c>, but
/// the current fact grammar has no transition to that state (there is no
/// <c>InspectionStarted</c> fact). Those 6 slots are currently folded
/// into the <c>Machining complete, CMM pass</c> bucket so the seed still
/// produces 50 parts and exercises every reachable state. If/when an
/// inspection-in-progress fact is added, this balance can shift back.
/// </para>
/// </remarks>
public sealed class InventorySeeder(
    FederationRouter router,
    IGrainFactory grains,
    ILogger<InventorySeeder> logger) : IHostedService
{
    private const string Family = "HPT-BLD-S1";
    private const int SeedYear = 2028;

    /// <summary>Total number of parts the seeder produces against an empty store.</summary>
    public const int TotalParts = 50;

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
        if (!await seedFlag.TryMarkSeededAsync())
        {
            logger.LogInformation("Inventory already seeded; skipping bulk load.");
            return;
        }

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
    }

    private async Task EmitAllAsync(CancellationToken cancellationToken)
    {
        var sequence = 1;
        // 10: Forge complete only.
        for (var i = 0; i < 10; i++, sequence++)
        {
            await EmitForgeOnlyAsync(sequence, cancellationToken);
        }
        // 8: HeatTreat complete.
        for (var i = 0; i < 8; i++, sequence++)
        {
            await EmitThroughHeatTreatAsync(sequence, cancellationToken);
        }
        // 14 (8 base + 6 folded from UnderInspection bucket): Machining + CMM pass.
        for (var i = 0; i < 14; i++, sequence++)
        {
            await EmitThroughMachiningAsync(sequence, cancellationToken);
        }
        // 6: FlaggedForReview (NDT raised a minor NC).
        for (var i = 0; i < 6; i++, sequence++)
        {
            await EmitFlaggedForReviewAsync(sequence, cancellationToken);
        }
        // 5: Rework (MRB disposition).
        for (var i = 0; i < 5; i++, sequence++)
        {
            await EmitReworkAsync(sequence, cancellationToken);
        }
        // 4: FAI signed off, nominal.
        for (var i = 0; i < 4; i++, sequence++)
        {
            await EmitFullLifecycleAsync(sequence, cancellationToken);
        }
        // 3: Scrap (critical NC).
        for (var i = 0; i < 3; i++, sequence++)
        {
            await EmitScrapAsync(sequence, cancellationToken);
        }
    }

    private async Task EmitForgeOnlyAsync(int seq, CancellationToken ct)
    {
        var ctx = PartContext.Create(seq);
        await EmitForgeAsync(ctx, ct);
    }

    private async Task EmitThroughHeatTreatAsync(int seq, CancellationToken ct)
    {
        var ctx = PartContext.Create(seq);
        await EmitForgeAsync(ctx, ct);
        await EmitStageAsync(ctx, ProcessStage.HeatTreat, ProcessSite.NagoyaHeatTreat, "heat-treat complete", ct);
    }

    private async Task EmitThroughMachiningAsync(int seq, CancellationToken ct)
    {
        var ctx = PartContext.Create(seq);
        await EmitForgeAsync(ctx, ct);
        await EmitStageAsync(ctx, ProcessStage.HeatTreat, ProcessSite.NagoyaHeatTreat, "heat-treat complete", ct);
        await EmitStageAsync(ctx, ProcessStage.Machining, ProcessSite.StuttgartMachining, "machining complete", ct);
        await EmitInspectionAsync(ctx, Inspection.CMM, InspectionOutcome.Pass,
            ProcessSite.StuttgartCmmLab, "CMM pass", ct);
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
    /// Per-part mutable context that hands out deterministic HLCs and
    /// fact ids. Wall-clock ticks are derived from the sequence number
    /// (<c>seq * 1_000_000</c>) so every part occupies its own HLC band
    /// and a re-run against a fresh store produces identical timestamps.
    /// </summary>
    private sealed class PartContext
    {
        public PartSerialNumber Serial { get; }
        private HybridLogicalClock _hlc;
        private int _factOrdinal;
        private readonly int _seq;

        private PartContext(int seq, PartSerialNumber serial)
        {
            _seq = seq;
            Serial = serial;
            _hlc = new HybridLogicalClock { WallClockTicks = (long)seq * 1_000_000, Counter = 0 };
        }

        public static PartContext Create(int seq) =>
            new(seq, PartSerialNumber.From(new PartFamily(Family), SeedYear, seq));

        public HybridLogicalClock NextHlc()
        {
            _hlc = new HybridLogicalClock
            {
                WallClockTicks = _hlc.WallClockTicks + 1,
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
