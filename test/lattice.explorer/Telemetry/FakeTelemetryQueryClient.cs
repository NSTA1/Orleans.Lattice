using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// A hand-rolled <see cref="ITelemetryQueryClient"/> fake that lets a test script
/// each RPC independently: a canned catalogue or response, or a fault - one of
/// the facade's typed refusals, a translated denial, a
/// <see cref="TelemetryUnavailableException"/>, or a residual
/// <see cref="Grpc.Core.RpcException"/>.
/// <para>
/// Every reply is a fixed literal from <see cref="SampleTelemetry"/> and every
/// counter is incremented synchronously, so no test here depends on timing,
/// ordering, or a live cluster.
/// </para>
/// </summary>
internal sealed class FakeTelemetryQueryClient : ITelemetryQueryClient
{
    /// <summary>Thrown by the catalogue read when set.</summary>
    public Exception? CatalogThrows { get; set; }

    /// <summary>Thrown by the query evaluation when set.</summary>
    public Exception? QueryThrows { get; set; }

    /// <summary>The catalogue the read returns.</summary>
    public TelemetryQueryCatalog CatalogResult { get; set; } = SampleTelemetry.Catalog();

    /// <summary>The scope the synthesised response reports when no explicit response is set.</summary>
    public TelemetryTenantScope Scope { get; set; } = SampleTelemetry.ActiveScope();

    /// <summary>The series the synthesised response carries.</summary>
    public IReadOnlyList<TelemetryTimeSeries> Series { get; set; } = [];

    /// <summary>An explicit response, overriding the synthesised one.</summary>
    public TelemetryQueryResponse? QueryResult { get; set; }

    /// <summary>How many times the catalogue was read.</summary>
    public int CatalogCallCount { get; private set; }

    /// <summary>How many times a query was evaluated.</summary>
    public int QueryCallCount { get; private set; }

    /// <summary>The request the last evaluation actually sent, exactly as it went.</summary>
    public TelemetryQueryRequest? LastRequest { get; private set; }

    /// <summary>Makes both RPCs fail with <paramref name="exception"/>.</summary>
    public void FailWith(Exception exception)
    {
        CatalogThrows = exception;
        QueryThrows = exception;
    }

    /// <inheritdoc />
    public Task<TelemetryQueryCatalog> GetCatalogAsync(CancellationToken cancellationToken = default)
    {
        CatalogCallCount++;
        return CatalogThrows is not null
            ? Task.FromException<TelemetryQueryCatalog>(CatalogThrows)
            : Task.FromResult(CatalogResult);
    }

    /// <inheritdoc />
    public Task<TelemetryQueryResponse> QueryAsync(
        TelemetryQueryRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        QueryCallCount++;
        LastRequest = request;

        if (QueryThrows is not null)
        {
            return Task.FromException<TelemetryQueryResponse>(QueryThrows);
        }

        return Task.FromResult(
            QueryResult ?? SampleTelemetry.Response(request.QueryId, Scope, Series));
    }
}
