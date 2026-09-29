using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Telemetry;

/// <summary>
/// A scripted <see cref="ILatticeTelemetry"/>: a catalogue (or a fault, or a read the
/// test completes itself) and a per-query answer, recording every call so a test can
/// assert what the area asked for. Nothing here waits on a clock.
/// </summary>
internal sealed class FakeTelemetry : ILatticeTelemetry
{
    private readonly Dictionary<string, Func<TelemetryQueryRequest, CancellationToken, Task<TelemetryQueryResponse>>> _answers =
        new(StringComparer.Ordinal);

    /// <summary>The catalogue <see cref="GetCatalogAsync"/> returns.</summary>
    public TelemetryQueryCatalog Catalog { get; set; } = TelemetryTestData.FullCatalog();

    /// <summary>When set, <see cref="GetCatalogAsync"/> throws this instead.</summary>
    public Exception? CatalogFailure { get; set; }

    /// <summary>When set, <see cref="GetCatalogAsync"/> returns this pending read.</summary>
    public TaskCompletionSource<TelemetryQueryCatalog>? PendingCatalog { get; set; }

    /// <summary>How many times the catalogue was read.</summary>
    public int CatalogReads { get; private set; }

    /// <summary>Every request the area sent, in order.</summary>
    public List<TelemetryQueryRequest> Requests { get; } = [];

    /// <summary>Answers <paramref name="queryId"/> with <paramref name="answer"/>.</summary>
    /// <param name="queryId">The query.</param>
    /// <param name="answer">Builds the answer from the request.</param>
    /// <returns>This fake, for chaining.</returns>
    public FakeTelemetry Answer(string queryId, Func<TelemetryQueryRequest, TelemetryQueryResponse> answer)
    {
        _answers[queryId] = (request, _) => Task.FromResult(answer(request));
        return this;
    }

    /// <summary>Answers <paramref name="queryId"/> with a pending task the test completes.</summary>
    /// <param name="queryId">The query.</param>
    /// <param name="pending">The pending answer.</param>
    /// <returns>This fake, for chaining.</returns>
    public FakeTelemetry Pending(string queryId, TaskCompletionSource<TelemetryQueryResponse> pending)
    {
        _answers[queryId] = (_, _) => pending.Task;
        return this;
    }

    /// <summary>Fails <paramref name="queryId"/> with <paramref name="failure"/>.</summary>
    /// <param name="queryId">The query.</param>
    /// <param name="failure">The fault.</param>
    /// <returns>This fake, for chaining.</returns>
    public FakeTelemetry Fail(string queryId, Exception failure)
    {
        _answers[queryId] = (_, _) => Task.FromException<TelemetryQueryResponse>(failure);
        return this;
    }

    /// <inheritdoc />
    public Task<TelemetryQueryCatalog> GetCatalogAsync(CancellationToken cancellationToken = default)
    {
        CatalogReads++;
        if (PendingCatalog is { } pending)
        {
            return pending.Task;
        }

        return CatalogFailure is { } failure ? Task.FromException<TelemetryQueryCatalog>(failure) : Task.FromResult(Catalog);
    }

    /// <inheritdoc />
    public Task<TelemetryQueryResponse> QueryAsync(TelemetryQueryRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        Requests.Add(request);
        return _answers.TryGetValue(request.QueryId, out var answer)
            ? answer(request, cancellationToken)
            : Task.FromResult(TelemetryTestData.Empty(request));
    }
}
