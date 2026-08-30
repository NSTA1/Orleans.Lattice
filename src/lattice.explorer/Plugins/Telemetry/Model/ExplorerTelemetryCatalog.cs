using System.Diagnostics.CodeAnalysis;

namespace Orleans.Lattice.Explorer.Telemetry;

/// <summary>
/// The set of curated queries the cluster offers this caller, in the server's own
/// ascending id order. It is the whole of the telemetry surface a panel can
/// reach: a panel picks an entry from here, and there is no way to compose one.
/// </summary>
/// <remarks>
/// <para>
/// <b>An empty catalogue is a normal answer, not a failure.</b> The facade
/// reports one when the cluster has no telemetry backend configured, and also
/// when the caller may run none of the entries - the two are deliberately
/// indistinguishable, so a caller cannot probe for queries outside its
/// entitlement. Either way there is nothing to render.
/// </para>
/// </remarks>
public sealed record ExplorerTelemetryCatalog
{
    private static readonly ExplorerTelemetryCatalog EmptyCatalog = new() { Version = 0, Queries = [] };

    /// <summary>The catalogue version, which changes when the offered set changes.</summary>
    public required int Version { get; init; }

    /// <summary>The offered entries, in ascending <see cref="ExplorerTelemetryQuery.QueryId"/> order.</summary>
    public required IReadOnlyList<ExplorerTelemetryQuery> Queries { get; init; }

    /// <summary>The shared empty catalogue.</summary>
    public static ExplorerTelemetryCatalog Empty => EmptyCatalog;

    /// <summary>The number of offered entries.</summary>
    public int Count => Queries.Count;

    /// <summary>
    /// <see langword="true"/> when the caller is offered nothing, so a telemetry
    /// surface has no panel to render.
    /// </summary>
    public bool IsEmpty => Queries.Count == 0;

    /// <summary>Finds the entry with id <paramref name="queryId"/>.</summary>
    /// <param name="queryId">The catalogue id, compared ordinally.</param>
    /// <param name="query">The entry when found.</param>
    /// <returns><see langword="true"/> when the catalogue offers it.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="queryId"/> is <see langword="null"/>.</exception>
    public bool TryGetQuery(string queryId, [NotNullWhen(true)] out ExplorerTelemetryQuery? query)
    {
        ArgumentNullException.ThrowIfNull(queryId);

        var queries = Queries;
        for (var i = 0; i < queries.Count; i++)
        {
            var candidate = queries[i];
            if (string.Equals(candidate.QueryId, queryId, StringComparison.Ordinal))
            {
                query = candidate;
                return true;
            }
        }

        query = null;
        return false;
    }

    /// <summary>Whether the catalogue offers <paramref name="queryId"/>.</summary>
    /// <param name="queryId">The catalogue id, compared ordinally.</param>
    /// <returns><see langword="true"/> when the catalogue offers it.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="queryId"/> is <see langword="null"/>.</exception>
    public bool Contains(string queryId) => TryGetQuery(queryId, out _);
}
