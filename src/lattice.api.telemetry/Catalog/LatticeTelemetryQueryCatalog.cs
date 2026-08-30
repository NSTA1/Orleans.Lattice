using System.Diagnostics.CodeAnalysis;

namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The compiled, versioned named-query catalogue the facade serves: the
/// server-authored definitions turned into executable plans, indexed by id, with
/// the client-facing <see cref="TelemetryQueryCatalog"/> materialised once.
/// </summary>
/// <remarks>
/// <para>
/// <b>Built once, read per request.</b> Registered as a singleton, so template
/// parsing, metric-name extraction, and allow-list evaluation happen at
/// construction. A request performs a dictionary lookup and a render.
/// </para>
/// <para>
/// <b>Entitlement is structural.</b> An entry whose metrics the configured
/// allow-list does not admit is absent from <see cref="Catalog"/> <em>and</em>
/// unresolvable through <see cref="TryGetPlan"/>, so discovery and execution agree
/// and an unentitled id is indistinguishable from an id that does not exist.
/// </para>
/// <para>
/// <b>One catalogue in both deployment modes.</b> Nothing here consults the tenancy
/// add-on: the same entries are compiled and served whether or not it is
/// registered, because the derived <c>tenant</c> dimension is emitted either way.
/// </para>
/// </remarks>
public sealed class LatticeTelemetryQueryCatalog
{
    private readonly Dictionary<string, TelemetryQueryPlan> _plans;

    /// <summary>
    /// Builds the catalogue from the built-in query definitions, filtered by
    /// <paramref name="policy"/>.
    /// </summary>
    /// <param name="policy">The configured metric-access policy.</param>
    /// <exception cref="ArgumentNullException"><paramref name="policy"/> is <see langword="null"/>.</exception>
    public LatticeTelemetryQueryCatalog(TelemetryMetricAccessPolicy policy)
        : this(LatticeTelemetryQueries.Definitions, LatticeTelemetryQueries.Version, policy)
    {
    }

    /// <summary>
    /// Builds the catalogue from an explicit definition set, for a host that
    /// curates its own queries and for tests.
    /// </summary>
    /// <param name="definitions">The server-authored definitions.</param>
    /// <param name="version">The catalogue revision to report.</param>
    /// <param name="policy">The configured metric-access policy.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="definitions"/> or <paramref name="policy"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Two definitions share a query id, or a template carries no scope placeholder.
    /// </exception>
    public LatticeTelemetryQueryCatalog(
        IEnumerable<TelemetryQueryDefinition> definitions,
        int version,
        TelemetryMetricAccessPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(definitions);
        ArgumentNullException.ThrowIfNull(policy);

        _plans = new Dictionary<string, TelemetryQueryPlan>(StringComparer.Ordinal);
        var admitted = new List<TelemetryQueryDescriptor>();

        foreach (var definition in definitions)
        {
            var plan = TelemetryQueryPlan.Compile(definition, policy);
            if (!_plans.TryAdd(plan.QueryId, plan))
            {
                throw new ArgumentException(
                    $"Telemetry query id '{plan.QueryId}' is declared more than once. A query id "
                    + "is the catalogue's stable identity and must be unique.",
                    nameof(definitions));
            }

            if (plan.IsAdmitted)
            {
                admitted.Add(plan.Descriptor);
            }
        }

        admitted.Sort(static (left, right) => string.CompareOrdinal(left.QueryId, right.QueryId));

        Version = version;
        Catalog = admitted.Count == 0
            ? TelemetryQueryCatalog.Empty
            : new TelemetryQueryCatalog { Version = version, Queries = admitted };
    }

    /// <summary>The catalogue revision reported to clients.</summary>
    public int Version { get; }

    /// <summary>
    /// The client-facing catalogue of admitted entries, in ascending
    /// <see cref="TelemetryQueryDescriptor.QueryId"/> order. Materialised once, so
    /// serving discovery allocates nothing.
    /// </summary>
    public TelemetryQueryCatalog Catalog { get; }

    /// <summary>The number of admitted entries.</summary>
    public int Count => Catalog.Count;

    /// <summary>
    /// Resolves the executable plan for <paramref name="queryId"/>, succeeding only
    /// for an entry the configured allow-list admits.
    /// </summary>
    /// <param name="queryId">The catalogue-stable query id.</param>
    /// <param name="plan">The resolved plan, or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when an admitted entry with that id exists.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="queryId"/> is <see langword="null"/>.</exception>
    internal bool TryGetPlan(string queryId, [NotNullWhen(true)] out TelemetryQueryPlan? plan)
    {
        ArgumentNullException.ThrowIfNull(queryId);

        if (_plans.TryGetValue(queryId, out var candidate) && candidate.IsAdmitted)
        {
            plan = candidate;
            return true;
        }

        plan = null;
        return false;
    }

    /// <summary>
    /// <see langword="true"/> when the catalogue offers <paramref name="queryId"/>
    /// to callers.
    /// </summary>
    /// <param name="queryId">The catalogue-stable query id.</param>
    /// <returns><see langword="true"/> when the entry is offered.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="queryId"/> is <see langword="null"/>.</exception>
    public bool Offers(string queryId) => TryGetPlan(queryId, out _);
}
