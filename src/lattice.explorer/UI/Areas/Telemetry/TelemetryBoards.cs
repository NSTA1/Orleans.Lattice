using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Explorer.UI.Areas.Telemetry;

/// <summary>
/// The Telemetry area's boards: the built-in catalogue grouped by the question each
/// group answers, plus an Other board for any query a host adds, resolved against
/// the catalogue a caller was actually served.
/// </summary>
/// <remarks>
/// The catalogue is server-authored and already narrowed to what the caller may
/// read: an entry the metric allow-list does not admit is absent, indistinguishable
/// from an unknown id. A board therefore never errors over a missing entry; it
/// draws what it was given and names what it was not.
/// </remarks>
internal static class TelemetryBoards
{
    /// <summary>The board of catalogue queries no curated board claims.</summary>
    public const string OtherKey = "other";

    /// <summary>The tenant board, which exists only while tenancy is on.</summary>
    public const string TenantKey = "tenant";

    /// <summary>The curated boards, in the order the area lists them.</summary>
    public static IReadOnlyList<TelemetryBoard> Curated { get; } =
    [
        new("throughput", "Throughput", "Reads, writes and atomic writes per second, per tree.",
        [
            new("tree.read.operation_rate", "Read operations"),
            new("tree.write.operation_rate", "Write operations"),
            new("tree.write.record_rate", "Records written"),
            new("tree.atomic_write.outcome_rate", "Atomic write outcomes"),
        ]),
        new("latency", "Latency", "How long writes and scans take, at the 95th percentile.",
        [
            new("tree.write.latency_p95", "Write latency (p95)"),
            new("tree.scan.latency_p95", "Scan latency (p95)"),
        ]),
        new("storage", "Storage", "Stored bytes per tree and how fast tombstones come and go.",
        [
            new("tree.storage.bytes_trend", "Stored bytes over time"),
            new("tree.storage.bytes", "Stored bytes now"),
            new("tree.tombstones.created_rate", "Tombstones created"),
            new("tree.tombstones.reaped_rate", "Tombstones reaped"),
        ]),
        new("pressure", "Pressure", "Admission, write-ahead-log saturation and cache hits.",
        [
            new("tree.admission.utilization", "Admission utilisation"),
            new("tree.wal.saturation_state", "Write-ahead-log saturation"),
            new("tree.cache.hit_ratio", "Cache hit ratio"),
        ]),
        new(TenantKey, "Tenant", "The tenant's stored bytes against its quota.",
        [
            new("tenant.usage.bytes", "Tenant stored bytes"),
            new("tenant.quota.byte_utilization", "Tenant quota used"),
        ],
        TenancyOnly: true),
    ];

    /// <summary>The board that collects catalogue queries no curated board claims.</summary>
    public static TelemetryBoard Other { get; } =
        new(OtherKey, "Other", "Queries this cluster adds to the built-in catalogue.", []);

    /// <summary>
    /// Resolves every board against <paramref name="catalog"/>: each curated board
    /// that applies to this tenancy mode, then Other when the catalogue holds a
    /// query no curated board claims.
    /// </summary>
    /// <param name="catalog">The catalogue the caller was served.</param>
    /// <param name="tenancyActive">Whether tenancy is on.</param>
    /// <returns>The resolved boards, in listing order.</returns>
    public static IReadOnlyList<TelemetryBoardPlan> Plan(TelemetryQueryCatalog catalog, bool tenancyActive)
    {
        ArgumentNullException.ThrowIfNull(catalog);

        var plans = new List<TelemetryBoardPlan>(Curated.Count + 1);
        var claimed = new HashSet<string>(StringComparer.Ordinal);

        foreach (var board in Curated)
        {
            foreach (var query in board.Queries)
            {
                claimed.Add(query.QueryId);
            }

            if (board.TenancyOnly && !tenancyActive)
            {
                continue;
            }

            var charts = new List<TelemetryQueryDescriptor>(board.Queries.Count);
            var omitted = new List<string>();
            foreach (var query in board.Queries)
            {
                if (catalog.TryGetQuery(query.QueryId, out var descriptor))
                {
                    charts.Add(descriptor);
                }
                else
                {
                    omitted.Add(query.Title);
                }
            }

            plans.Add(new TelemetryBoardPlan(board, charts, omitted));
        }

        var others = catalog.Queries.Where(query => !claimed.Contains(query.QueryId)).ToArray();
        if (others.Length > 0)
        {
            plans.Add(new TelemetryBoardPlan(Other, others, []));
        }

        return plans;
    }

    /// <summary>Finds the resolved board addressed by <paramref name="key"/>.</summary>
    /// <param name="plans">The resolved boards.</param>
    /// <param name="key">The board's address segment.</param>
    /// <returns>The board, or <see langword="null"/> when no board has that key.</returns>
    public static TelemetryBoardPlan? Find(IReadOnlyList<TelemetryBoardPlan> plans, string? key) =>
        key is null ? null : plans.FirstOrDefault(plan => string.Equals(plan.Board.Key, key, StringComparison.Ordinal));
}
