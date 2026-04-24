using Grpc.Core;
using MultiSiteManufacturing.Host.Federation;
using V1 = MultiSiteManufacturing.Contracts.V1;

namespace MultiSiteManufacturing.Host.Grpc;

/// <summary>
/// gRPC surface for fact ingress. Thin adapter over
/// <see cref="FederationRouter.EmitAsync"/>; the router handles chaos
/// admission and fan-out to every backend.
/// </summary>
public sealed class FactIngressServiceImpl(FederationRouter router)
    : V1.FactIngressService.FactIngressServiceBase
{
    /// <inheritdoc />
    public override async Task<V1.EmitResult> EmitFact(V1.FactEnvelope request, ServerCallContext context)
    {
        var fact = ProtoMappings.FromProto(request);
        await router.EmitAsync(fact, context.CancellationToken);
        return new V1.EmitResult { FactId = fact.FactId.ToString(), Accepted = true };
    }

    /// <inheritdoc />
    public override async Task<V1.EmitSummary> EmitFactStream(
        IAsyncStreamReader<V1.FactEnvelope> requestStream,
        ServerCallContext context)
    {
        var count = 0;
        await foreach (var envelope in requestStream.ReadAllAsync(context.CancellationToken))
        {
            var fact = ProtoMappings.FromProto(envelope);
            await router.EmitAsync(fact, context.CancellationToken);
            count++;
        }
        return new V1.EmitSummary { AcceptedCount = count };
    }
}
