using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;

/// <summary>
/// One tenant's own groups, by tenant-local name, for the subject picker's
/// <c>Tenant</c> source: the first page the tenant directory lists, matched as
/// typed. The directory composes every id under the tenant named, so another
/// tenant's group can never be offered.
/// </summary>
/// <remarks>
/// It remembers nothing itself: the list is the caller-keyed catalogue's, so no
/// answer can outlive the caller or tenant it was read for. A group's display
/// name is the detail, rendered as text, behind its <see cref="SourceLabel"/>.
/// </remarks>
/// <param name="catalog">The circuit's tenant access catalogue.</param>
/// <param name="tenant">The tenant whose groups are offered.</param>
internal sealed class TenantGroupSuggestionSource(TenantAccessCatalog catalog, string tenant) : ILtSuggestionSource
{
    /// <summary>The label naming this source's provenance.</summary>
    public const string SourceLabel = "Tenant";

    /// <summary>The note shown when the tenant's groups cannot be listed.</summary>
    public const string UnavailableReason = "This tenant's groups cannot be listed right now, so the name is used as typed.";

    /// <summary>The tenant whose groups are offered.</summary>
    public string Tenant => tenant;

    /// <inheritdoc />
    public async ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        IReadOnlyList<TenantGroupDescriptor> groups;
        try
        {
            groups = await catalog.GetGroupsAsync(tenant, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return LtSuggestionSet.Unavailable(UnavailableReason);
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

            values.Add(new LtSuggestion(group.Name, Describe(SourceLabel, group.DisplayName)));
        }

        return SuggestionMatcher.Match(values, text.Trim(), limit);
    }

    /// <summary>The detail shown beside a value: its provenance, then its display name when it has one.</summary>
    /// <param name="source">The provenance label.</param>
    /// <param name="displayName">The display name, or <see langword="null"/>.</param>
    /// <returns>The detail text.</returns>
    internal static string Describe(string source, string? displayName) =>
        string.IsNullOrWhiteSpace(displayName) ? source : $"{source} - {displayName}";
}
