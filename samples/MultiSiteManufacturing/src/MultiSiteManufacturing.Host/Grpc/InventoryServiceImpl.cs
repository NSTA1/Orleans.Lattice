using Grpc.Core;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using Orleans.Lattice.Primitives;
using V1 = MultiSiteManufacturing.Contracts.V1;

namespace MultiSiteManufacturing.Host.Grpc;

/// <summary>
/// gRPC surface for operator-facing inventory operations: create a part,
/// read a part view (with fact trail), enumerate/watch inventory. All
/// mutations flow through the federation router so chaos controls apply.
/// </summary>
/// <remarks>
/// <c>WatchPart</c> and <c>WatchInventory</c> currently emit a single
/// snapshot then await cancellation — real-time push lands in M7/M8
/// alongside the Blazor dashboard wiring.
/// </remarks>
public sealed class InventoryServiceImpl(FederationRouter router)
    : V1.InventoryService.InventoryServiceBase
{
    private const string LatticeBackendName = "lattice";
    private const string BaselineBackendName = "baseline";

    /// <inheritdoc />
    public override async Task<V1.Part> CreatePart(V1.CreatePartRequest request, ServerCallContext context)
    {
        var family = new PartFamily(request.Family);
        var operatorId = string.IsNullOrEmpty(request.OperatorId)
            ? OperatorId.Demo
            : new OperatorId(request.OperatorId);

        var initialStage = request.InitialStage == V1.ProcessStage.Unspecified
            ? ProcessStage.Forge
            : ProtoMappings.FromProto(request.InitialStage);

        var lattice = router.GetBackend(LatticeBackendName);
        var existing = await lattice.ListPartsAsync(context.CancellationToken);
        var year = DateTime.UtcNow.Year;
        var sequence = existing.Count + 1;
        var serial = PartSerialNumber.From(family, year, sequence);

        var fact = new ProcessStepCompleted
        {
            Serial = serial,
            FactId = Guid.NewGuid(),
            Hlc = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            Site = ProcessSiteForStage(initialStage),
            Operator = operatorId,
            Description = $"{initialStage} step completed",
            Stage = initialStage,
        };
        await router.EmitAsync(fact, context.CancellationToken);

        return new V1.Part { Serial = serial.Value, Family = family.Value };
    }

    /// <inheritdoc />
    public override async Task<V1.PartView> GetPart(V1.GetPartRequest request, ServerCallContext context)
    {
        var serial = new PartSerialNumber(request.Serial);
        var lattice = router.GetBackend(LatticeBackendName);
        var baseline = router.GetBackend(BaselineBackendName);

        var facts = await lattice.GetFactsAsync(serial, context.CancellationToken);
        var baselineState = await baseline.GetStateAsync(serial, context.CancellationToken);
        var latticeState = await lattice.GetStateAsync(serial, context.CancellationToken);

        var view = new V1.PartView
        {
            Serial = serial.Value,
            Family = InferFamily(serial),
            BaselineState = ProtoMappings.ToProto(baselineState),
            LatticeState = ProtoMappings.ToProto(latticeState),
        };
        foreach (var fact in facts)
        {
            view.Facts.Add(ProtoMappings.ToSummary(fact));
        }
        return view;
    }

    /// <inheritdoc />
    public override async Task ListParts(
        V1.ListPartsRequest request,
        IServerStreamWriter<V1.PartSummary> responseStream,
        ServerCallContext context)
    {
        var lattice = router.GetBackend(LatticeBackendName);
        var parts = await lattice.ListPartsAsync(context.CancellationToken);
        foreach (var serial in parts)
        {
            var summary = await BuildSummaryAsync(serial, context.CancellationToken);
            await responseStream.WriteAsync(summary, context.CancellationToken);
        }
    }

    /// <inheritdoc />
    public override async Task WatchPart(
        V1.WatchPartRequest request,
        IServerStreamWriter<V1.PartView> responseStream,
        ServerCallContext context)
    {
        var snapshot = await GetPart(new V1.GetPartRequest { Serial = request.Serial }, context);
        await responseStream.WriteAsync(snapshot, context.CancellationToken);

        try
        {
            await Task.Delay(Timeout.Infinite, context.CancellationToken);
        }
        catch (OperationCanceledException)
        {
        }
    }

    /// <inheritdoc />
    public override async Task WatchInventory(
        V1.WatchInventoryRequest request,
        IServerStreamWriter<V1.PartSummary> responseStream,
        ServerCallContext context)
    {
        await ListParts(new V1.ListPartsRequest(), responseStream, context);

        try
        {
            await Task.Delay(Timeout.Infinite, context.CancellationToken);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task<V1.PartSummary> BuildSummaryAsync(PartSerialNumber serial, CancellationToken cancellationToken)
    {
        var lattice = router.GetBackend(LatticeBackendName);
        var state = await lattice.GetStateAsync(serial, cancellationToken);
        var facts = await lattice.GetFactsAsync(serial, cancellationToken);

        // See DashboardBroadcaster.StageOf — "latest stage" is the
        // lifecycle milestone of the newest fact (HLC-ascending list,
        // so facts[^1]), not merely the last ProcessStepCompleted.
        var latestStage = facts.Count == 0
            ? V1.ProcessStage.Unspecified
            : StageOf(facts[^1]) is { } stage
                ? ProtoMappings.ToProto(stage)
                : V1.ProcessStage.Unspecified;

        return new V1.PartSummary
        {
            Serial = serial.Value,
            Family = InferFamily(serial),
            LatestStage = latestStage,
            State = ProtoMappings.ToProto(state),
        };
    }

    private static ProcessStage? StageOf(Fact fact) => fact switch
    {
        ProcessStepCompleted step => step.Stage,
        InspectionRecorded => ProcessStage.NDT,
        NonConformanceRaised => ProcessStage.MRB,
        MrbDisposition => ProcessStage.MRB,
        ReworkCompleted => ProcessStage.MRB,
        FinalAcceptance => ProcessStage.FAI,
        _ => null,
    };

    private static string InferFamily(PartSerialNumber serial)
    {
        // Serial shape: {family}-{year}-{seq:D5}. Family may itself contain hyphens
        // (e.g. "HPT-BLD-S1"), so strip the last two hyphen-delimited segments.
        var value = serial.Value;
        var lastDash = value.LastIndexOf('-');
        if (lastDash <= 0)
        {
            return value;
        }
        var yearDash = value.LastIndexOf('-', lastDash - 1);
        return yearDash > 0 ? value[..yearDash] : value;
    }

    private static ProcessSite ProcessSiteForStage(ProcessStage stage) => stage switch
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
