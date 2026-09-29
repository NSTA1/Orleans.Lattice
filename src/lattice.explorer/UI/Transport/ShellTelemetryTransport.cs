using Grpc.Core;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Api.Telemetry.Grpc;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeTelemetry"/> over gRPC: a per-circuit adapter
/// over <see cref="LatticeTelemetryApiGrpcClient"/>, ported from the Telemetry
/// plugin's <c>GrpcTelemetryQueryClient</c>.
/// </summary>
/// <remarks>
/// Faults map through <see cref="ShellTransportFaults"/>, refined for a query the
/// way the telemetry binding maps its facade's exceptions: <c>NotFound</c> is
/// rebuilt into <see cref="TelemetryQueryNotFoundException"/> and
/// <c>Unavailable</c> into <see cref="TelemetryBackendException"/>, both carrying
/// the query id. <c>Unavailable</c> is also what the transport reports for an
/// endpoint it cannot reach; both readings are retryable, which is what the
/// backend exception tells an area.
/// </remarks>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellTelemetryTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeTelemetryApiGrpcClient>(channel, LatticeTelemetryApiGrpcClient.Create), ILatticeTelemetry
{
    /// <inheritdoc />
    public Task<TelemetryQueryCatalog> GetCatalogAsync(CancellationToken cancellationToken = default) =>
        CallAsync((object?)null, static (client, _, ct) => client.GetCatalogAsync(ct), null, cancellationToken);

    /// <inheritdoc />
    public Task<TelemetryQueryResponse> QueryAsync(TelemetryQueryRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(request, static (client, state, ct) => client.QueryAsync(state, ct), request.QueryId, cancellationToken);
    }

    /// <inheritdoc />
    protected override Exception MapFault(RpcException exception, string? subject, CancellationToken cancellationToken)
    {
        if (subject is null)
        {
            return ShellTransportFaults.Map(exception, cancellationToken);
        }

        return exception.StatusCode switch
        {
            StatusCode.NotFound => new TelemetryQueryNotFoundException(subject, ShellTransportFaults.Detail(exception)),
            StatusCode.Unavailable => new TelemetryBackendException(subject, ShellTransportFaults.Detail(exception), exception),
            _ => ShellTransportFaults.Map(exception, cancellationToken),
        };
    }
}
