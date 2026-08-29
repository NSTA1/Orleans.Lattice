using System.Diagnostics.CodeAnalysis;

namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The versioned set of curated named queries a caller may select from. It is the
/// whole of the query surface: a caller picks an entry by
/// <see cref="TelemetryQueryDescriptor.QueryId"/> and supplies only the bounded
/// parameters that entry declares, so no query outside this catalogue is
/// reachable over the facade.
/// </summary>
/// <remarks>
/// <para>
/// <b>Versioning.</b> <see cref="Version"/> is a monotonically increasing
/// revision the server bumps whenever the catalogue's content changes. A client
/// caches the catalogue against it and refetches when it advances; it never has
/// to diff the entries to notice a change. Because a query's identity is stable
/// (see <see cref="TelemetryQueryDescriptor"/>), a bump means entries were added,
/// removed, or re-described - never that an existing id started measuring
/// something else.
/// </para>
/// <para>
/// <b>Scoped to the caller.</b> A catalogue is produced for one caller: the facade
/// omits any entry the caller is not entitled to run, so an absent entry and an
/// entry that does not exist are indistinguishable, exactly as the fail-closed
/// convention elsewhere in this contract requires.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTelemetryTypeAliases.TelemetryQueryCatalog)]
[Immutable]
public sealed record TelemetryQueryCatalog
{
    private static readonly TelemetryQueryCatalog EmptyCatalog =
        new() { Version = 0, Queries = [] };

    /// <summary>
    /// The catalogue revision. Monotonically increasing; a client refetches when
    /// it advances past the revision it cached.
    /// </summary>
    [Id(0)] public required int Version { get; init; }

    /// <summary>
    /// The entries this caller may select from, in ascending
    /// <see cref="TelemetryQueryDescriptor.QueryId"/> order so a rendered picker is
    /// stable across calls. Empty when the caller is entitled to none.
    /// </summary>
    [Id(1)] public required IReadOnlyList<TelemetryQueryDescriptor> Queries { get; init; }

    /// <summary>
    /// The empty catalogue at revision <c>0</c>: what a caller entitled to no query
    /// receives, and what a cluster with no telemetry backend configured reports.
    /// A cached singleton, so the fail-closed path allocates nothing.
    /// </summary>
    public static TelemetryQueryCatalog Empty => EmptyCatalog;

    /// <summary>The number of entries in the catalogue.</summary>
    public int Count => Queries.Count;

    /// <summary>
    /// Looks up the entry with <paramref name="queryId"/>, compared ordinally.
    /// Scans by index, so a lookup allocates nothing.
    /// </summary>
    /// <param name="queryId">The catalogue-stable query id to resolve.</param>
    /// <param name="descriptor">The resolved entry, or <see langword="null"/> when absent.</param>
    /// <returns><see langword="true"/> when the catalogue contains the entry.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="queryId"/> is <see langword="null"/>.</exception>
    public bool TryGetQuery(string queryId, [NotNullWhen(true)] out TelemetryQueryDescriptor? descriptor)
    {
        ArgumentNullException.ThrowIfNull(queryId);

        var queries = Queries;
        for (var i = 0; i < queries.Count; i++)
        {
            var candidate = queries[i];
            if (string.Equals(candidate.QueryId, queryId, StringComparison.Ordinal))
            {
                descriptor = candidate;
                return true;
            }
        }

        descriptor = null;
        return false;
    }

    /// <summary>
    /// <see langword="true"/> when the catalogue contains an entry with
    /// <paramref name="queryId"/>.
    /// </summary>
    /// <param name="queryId">The catalogue-stable query id to test for.</param>
    /// <returns><see langword="true"/> when the entry is present.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="queryId"/> is <see langword="null"/>.</exception>
    public bool Contains(string queryId) => TryGetQuery(queryId, out _);
}
