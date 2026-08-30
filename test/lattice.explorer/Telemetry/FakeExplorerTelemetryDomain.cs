using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// A scriptable <see cref="ITelemetryDomain"/> over an in-memory operations
/// surface: the whole reach of a telemetry panel, under a test's control.
/// <para>
/// Because a panel declares this one contract and receives nothing else from the
/// host, substituting it here exercises the surface exactly as the shell drives
/// it - there is no second channel to the cluster to stub out.
/// </para>
/// <para>
/// Every reply is returned through an already-completed
/// <see cref="ValueTask{TResult}"/> and every counter is incremented
/// synchronously, so nothing here depends on timing or ordering.
/// </para>
/// </summary>
internal sealed class FakeExplorerTelemetryDomain : ITelemetryDomain, ITelemetryQueryService
{
    /// <summary>The catalogue discovery returns.</summary>
    public ExplorerTelemetryCatalog Catalog { get; set; } = ExplorerTelemetrySample.Catalog();

    /// <summary>A failure discovery returns instead of a catalogue, when set.</summary>
    public TelemetryOperationResult<ExplorerTelemetryCatalog>? CatalogFailure { get; set; }

    /// <summary>The result an evaluation returns.</summary>
    public ExplorerTelemetryResult Result { get; set; } = ExplorerTelemetrySample.Result();

    /// <summary>A failure an evaluation returns instead of a result, when set.</summary>
    public TelemetryOperationResult<ExplorerTelemetryResult>? QueryFailure { get; set; }

    /// <summary>
    /// When set, an evaluation waits on this before answering, so a test can
    /// hold one request open and land a newer one first.
    /// <para>
    /// A completion source the test itself signals, never a delay - the overlap
    /// is produced deterministically rather than raced for.
    /// </para>
    /// </summary>
    public Task? Gate { get; set; }

    /// <summary>How many times the catalogue was read from the cluster.</summary>
    public int CatalogReads { get; private set; }

    /// <summary>How many times the catalogue was force-refreshed.</summary>
    public int CatalogRefreshes { get; private set; }

    /// <summary>Every request an evaluation actually sent, in call order.</summary>
    public List<ExplorerTelemetryRequest> Requests { get; } = [];

    /// <summary>The last request sent, or <see langword="null"/> when none was.</summary>
    public ExplorerTelemetryRequest? LastRequest => Requests.Count == 0 ? null : Requests[^1];

    /// <inheritdoc />
    public ITelemetryQueryService Queries => this;

    /// <inheritdoc />
    public bool IsTenancyEnabled { get; set; } = true;

    /// <inheritdoc />
    public ExplorerTelemetryVisibility RequestedVisibility { get; set; } =
        ExplorerTelemetryVisibility.ActiveTenant;

    /// <summary>The decision the availability probe returns.</summary>
    public ExplorerPluginAccess Availability { get; set; } = ExplorerPluginAccess.Allowed;

    /// <inheritdoc />
    public ValueTask<ExplorerPluginAccess> ProbeAvailabilityAsync(
        CancellationToken cancellationToken = default) => new(Availability);

    /// <inheritdoc />
    public ValueTask<TelemetryOperationResult<ExplorerTelemetryCatalog>> GetCatalogAsync(
        CancellationToken cancellationToken = default)
    {
        CatalogReads++;
        return new ValueTask<TelemetryOperationResult<ExplorerTelemetryCatalog>>(
            CatalogFailure
            ?? TelemetryOperationResult<ExplorerTelemetryCatalog>.Success(Catalog, "Read the telemetry catalogue."));
    }

    /// <inheritdoc />
    public ValueTask<TelemetryOperationResult<ExplorerTelemetryCatalog>> RefreshCatalogAsync(
        CancellationToken cancellationToken = default)
    {
        CatalogRefreshes++;
        return GetCatalogAsync(cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<TelemetryOperationResult<ExplorerTelemetryResult>> QueryAsync(
        ExplorerTelemetryRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        Requests.Add(request);

        // Captured before the await so a later mutation of the fake's script
        // cannot retroactively change what this in-flight call answers with.
        var answer = QueryFailure
            ?? TelemetryOperationResult<ExplorerTelemetryResult>.Success(
                Result with { QueryId = request.QueryId },
                "Evaluated the telemetry query.");

        return Gate is { } gate
            ? new ValueTask<TelemetryOperationResult<ExplorerTelemetryResult>>(AwaitGateAsync(gate, answer))
            : new ValueTask<TelemetryOperationResult<ExplorerTelemetryResult>>(answer);
    }

    private static async Task<TelemetryOperationResult<ExplorerTelemetryResult>> AwaitGateAsync(
        Task gate,
        TelemetryOperationResult<ExplorerTelemetryResult> answer)
    {
        await gate.ConfigureAwait(false);
        return answer;
    }
}
