using System.Text.Json;

namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// A recording <see cref="IPrometheusQueryClient"/> stand-in: it captures every
/// query text and range the facade sent and returns a canned envelope, so a test
/// can assert on the <em>exact</em> expression that would have reached the backend
/// without running one.
/// </summary>
internal sealed class RecordingPrometheusQueryClient : IPrometheusQueryClient
{
    private readonly List<string> _queries = [];

    /// <summary>The envelope every query returns. Defaults to an empty vector.</summary>
    public PrometheusQueryResponse Response { get; set; } = EmptyVector();

    /// <summary>An exception to throw instead of answering, or <see langword="null"/>.</summary>
    public Exception? Fault { get; set; }

    /// <summary>Every query text the facade sent, in call order.</summary>
    public IReadOnlyList<string> Queries => _queries;

    /// <summary>The single query text sent, asserting exactly one call was made.</summary>
    public string SingleQuery
    {
        get
        {
            Assert.That(_queries, Has.Count.EqualTo(1), "Expected exactly one backend call.");
            return _queries[0];
        }
    }

    /// <summary>The start instant of the last range query.</summary>
    public DateTimeOffset LastStart { get; private set; }

    /// <summary>The end instant of the last range query, or the last instant query's instant.</summary>
    public DateTimeOffset? LastEnd { get; private set; }

    /// <summary>The step of the last range query.</summary>
    public TimeSpan LastStep { get; private set; }

    /// <summary>Whether the last call was a range query.</summary>
    public bool LastWasRange { get; private set; }

    /// <inheritdoc />
    public Task<PrometheusQueryResponse> InstantQueryAsync(
        string query, DateTimeOffset? time, CancellationToken cancellationToken)
    {
        _queries.Add(query);
        LastEnd = time;
        LastWasRange = false;
        return Answer();
    }

    /// <inheritdoc />
    public Task<PrometheusQueryResponse> RangeQueryAsync(
        string query, DateTimeOffset start, DateTimeOffset end, TimeSpan step, CancellationToken cancellationToken)
    {
        _queries.Add(query);
        LastStart = start;
        LastEnd = end;
        LastStep = step;
        LastWasRange = true;
        return Answer();
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<string>> ListMetricNamesAsync(CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<string>>([]);

    /// <inheritdoc />
    public Task<PrometheusMetadataResponse> MetricMetadataAsync(
        string? metric, CancellationToken cancellationToken) =>
        Task.FromResult(new PrometheusMetadataResponse("success", default));

    /// <summary>Builds a success envelope carrying <paramref name="json"/> as its data payload.</summary>
    /// <param name="json">The <c>data</c> payload JSON.</param>
    /// <returns>The envelope.</returns>
    public static PrometheusQueryResponse Success(string json) =>
        new("success", JsonDocument.Parse(json).RootElement.Clone());

    private static PrometheusQueryResponse EmptyVector() =>
        Success("""{"resultType":"vector","result":[]}""");

    private Task<PrometheusQueryResponse> Answer() =>
        Fault is null ? Task.FromResult(Response) : Task.FromException<PrometheusQueryResponse>(Fault);
}
