using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
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
/// <c>WatchDivergence</c> currently emits the current divergence set once
/// then awaits cancellation. Live push lands in M10 alongside the
/// divergence feed component.
/// </remarks>
public sealed class ComplianceServiceImpl(FederationRouter router)
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
        var baseline = router.GetBackend(BaselineBackendName);
        var lattice = router.GetBackend(LatticeBackendName);

        var parts = await lattice.ListPartsAsync(context.CancellationToken);
        foreach (var serial in parts)
        {
            var baselineState = await baseline.GetStateAsync(serial, context.CancellationToken);
            var latticeState = await lattice.GetStateAsync(serial, context.CancellationToken);
            if (baselineState == latticeState)
            {
                continue;
            }
            await responseStream.WriteAsync(new V1.DivergenceReport
            {
                Serial = serial.Value,
                BaselineState = ProtoMappings.ToProto(baselineState),
                LatticeState = ProtoMappings.ToProto(latticeState),
            }, context.CancellationToken);
        }

        try
        {
            await Task.Delay(Timeout.Infinite, context.CancellationToken);
        }
        catch (OperationCanceledException)
        {
        }
    }
}
