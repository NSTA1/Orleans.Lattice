using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// The groups an app role can be bound to: the installing tenant's own groups,
/// labelled <c>Tenant</c> and offered by their full id (<c>t/{tenant}/{name}</c>),
/// beside the cluster's groups, labelled <c>Cluster</c>. Another tenant's group is
/// never offered: the tenant directory lists only the named tenant's groups, and
/// every reserved tenant group id the cluster source answers with is left out.
/// </summary>
/// <remarks>
/// It remembers nothing itself: the tenant's groups are the caller-keyed
/// catalogue's first page and the cluster's are each query's. Without the
/// tenant's groups (no installing tenant, or delegated tenant access
/// administration is not open to the caller) it offers the cluster's groups only.
/// </remarks>
/// <param name="cluster">The cluster's groups, from the identity directory or the auth store.</param>
/// <param name="tenantAccess">The circuit's tenant access catalogue.</param>
/// <param name="tenant">The tenant whose own groups are offered, or <see langword="null"/> to offer none.</param>
internal sealed class AppGroupSuggestionSource(ILtSuggestionSource cluster, TenantAccessCatalog tenantAccess, string? tenant) : ILtSuggestionSource
{
    /// <summary>The tenant whose own groups are offered, or <see langword="null"/>.</summary>
    public string? Tenant => tenant;

    /// <inheritdoc />
    public async ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        var own = tenant is null ? null : await TenantGroupsAsync(tenant, text, limit, cancellationToken).ConfigureAwait(false);
        var shared = await cluster.SuggestAsync(text, limit, cancellationToken).ConfigureAwait(false);
        if (own is null && !shared.IsAvailable)
        {
            return shared;
        }

        var values = new List<LtSuggestion>(Math.Min(limit, (own?.Items.Count ?? 0) + (shared.IsAvailable ? shared.Items.Count : 0)));
        if (own is not null)
        {
            for (var i = 0; i < own.Items.Count && values.Count < limit; i++)
            {
                values.Add(own.Items[i]);
            }
        }

        if (shared.IsAvailable)
        {
            for (var i = 0; i < shared.Items.Count && values.Count < limit; i++)
            {
                var item = shared.Items[i];
                if (!ClusterSubjectSuggestionSource.IsTenantGroupId(item.Value))
                {
                    values.Add(item with { Detail = TenantGroupSuggestionSource.Describe(ClusterSubjectSuggestionSource.SourceLabel, item.Detail) });
                }
            }
        }

        return LtSuggestionSet.Of(values, (own?.Truncated ?? false) || (shared.IsAvailable && shared.Truncated));
    }

    private async Task<LtSuggestionSet?> TenantGroupsAsync(string owner, string text, int limit, CancellationToken cancellationToken)
    {
        IReadOnlyList<TenantGroupDescriptor> groups;
        try
        {
            groups = await tenantAccess.GetGroupsAsync(owner, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            // The cluster's groups are still offered; the tenant's are simply missing.
            return null;
        }

        var values = new List<LtSuggestion>(groups.Count);
        for (var i = 0; i < groups.Count; i++)
        {
            var group = groups[i];

            // A local name never holds a separator; anything that does is not this tenant's own.
            if (string.IsNullOrEmpty(group.Name) || group.Name.Contains('/', StringComparison.Ordinal))
            {
                continue;
            }

            values.Add(new LtSuggestion(
                AccessSubjectPicker.TenantGroupId(owner, group.Name),
                TenantGroupSuggestionSource.Describe(TenantGroupSuggestionSource.SourceLabel, group.DisplayName)));
        }

        return SuggestionMatcher.Match(values, text.Trim(), limit);
    }
}
