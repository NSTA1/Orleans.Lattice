using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// The regions a tenant may be allowed: every region the cluster knows, plus any
/// region the tenant already lists (a region can be allowed before this cluster
/// replicates with it). When the cluster's regions cannot be listed the answer is
/// unavailable, not the tenant's own list alone, so a picker never refuses a real
/// region merely because the replication report is not served.
/// </summary>
/// <param name="regions">The cluster's region source.</param>
/// <param name="tenantRegions">The regions the tenant lists now, read at query time.</param>
internal sealed class TenancyRegionSuggestionSource(ILtSuggestionSource regions, Func<IReadOnlyList<string>> tenantRegions) : ILtSuggestionSource
{
    /// <summary>The detail beside a region only the tenant lists.</summary>
    public const string TenantDetail = "Listed for this tenant";

    /// <summary>The most regions asked of the cluster's source to merge with the tenant's.</summary>
    public const int ClusterLimit = 200;

    /// <inheritdoc />
    public async ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        var cluster = await regions.SuggestAsync(string.Empty, ClusterLimit, cancellationToken).ConfigureAwait(false);
        if (!cluster.IsAvailable)
        {
            return cluster;
        }

        var merged = new List<LtSuggestion>(cluster.Items.Count);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var item in cluster.Items)
        {
            if (seen.Add(item.Value))
            {
                merged.Add(item);
            }
        }

        foreach (var region in tenantRegions())
        {
            if (seen.Add(region))
            {
                merged.Add(new LtSuggestion(region, TenantDetail));
            }
        }

        return SuggestionMatcher.Match(merged, text, limit);
    }
}
