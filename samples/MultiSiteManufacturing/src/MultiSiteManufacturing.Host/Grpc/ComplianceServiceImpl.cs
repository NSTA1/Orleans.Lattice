using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using MultiSiteManufacturing.Host.Dashboard;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using V1 = MultiSiteManufacturing.Contracts.V1;

namespace MultiSiteManufacturing.Host.Grpc;

/// <summary>
/// gRPC surface for baseline-vs-lattice compliance comparison. Reads both
/// backends by their stable names (<c>baseline</c>, <c>lattice</c>) and
/// surfaces per-part divergence.
/// </summary>
/// <remarks>
/// <c>WatchDivergence</c> emits an initial snapshot of every currently
/// divergent part, then forwards <see cref="DivergenceEvent"/>s pushed by
/// <see cref="DashboardBroadcaster"/> until the caller cancels.
/// </remarks>
public sealed class ComplianceServiceImpl(FederationRouter router, DashboardBroadcaster broadcaster)
    : V1.ComplianceService.ComplianceServiceBase
{
    private const string BaselineBackendName = "baseline";
    private const string LatticeBackendName = "lattice";

    /// <inheritdoc />
    public override async Task<V1.PartComplianceView> GetPartCompliance(
        V1.GetPartComplianceRequest request,
        ServerCallContext context)
    {
        var serial = new PartSerialNumber(request.Serial);
        var baseline = router.GetBackend(BaselineBackendName);
        var lattice = router.GetBackend(LatticeBackendName);

        var baselineState = await baseline.GetStateAsync(serial, context.CancellationToken);
        var latticeState = await lattice.GetStateAsync(serial, context.CancellationToken);

        return new V1.PartComplianceView
        {
            Serial = serial.Value,
            BaselineState = ProtoMappings.ToProto(baselineState),
            LatticeState = ProtoMappings.ToProto(latticeState),
            Diverges = baselineState != latticeState,
        };
    }

    /// <inheritdoc />
    public override async Task WatchDivergence(
        Empty request,
        IServerStreamWriter<V1.DivergenceReport> responseStream,
        ServerCallContext context)
    {
        var initial = await broadcaster.GetInitialDivergenceAsync(context.CancellationToken);
        foreach (var evt in initial)
        {
            await responseStream.WriteAsync(ToProto(evt), context.CancellationToken);
        }

        try
        {
            await foreach (var evt in broadcaster.SubscribeDivergence(context.CancellationToken))
            {
                await responseStream.WriteAsync(ToProto(evt), context.CancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private static V1.DivergenceReport ToProto(DivergenceEvent evt) => new()
    {
        Serial = evt.Serial.Value,
        BaselineState = ProtoMappings.ToProto(evt.BaselineState),
        LatticeState = ProtoMappings.ToProto(evt.LatticeState),
        Resolved = evt.Resolved,
    };
}
