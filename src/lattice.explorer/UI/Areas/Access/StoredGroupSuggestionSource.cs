using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// The cluster groups administered in the Access area itself - the auth store's
/// own groups, whether or not the identity directory lists them - matched as
/// typed: the catalogue's first page, and, when that page may not hold every
/// group, an exact look-up of the typed id. Every reserved tenant group id is
/// left out, so a tenant's group is never offered as a cluster group.
/// </summary>
/// <remarks>
/// It holds nothing of its own: the page is <see cref="AccessCatalog"/>'s memo,
/// keyed on the circuit's caller and forgotten on every Access write. The listing
/// is the auth facade's, which only an access administrator may read, so for
/// anyone else the source answers unavailable.
/// </remarks>
/// <param name="catalog">The circuit's access catalogue, or <see langword="null"/> when the head serves no auth facade.</param>
internal sealed class StoredGroupSuggestionSource(AccessCatalog? catalog) : ILtSuggestionSource
{
    /// <summary>The note shown when the stored groups cannot be listed.</summary>
    public const string UnavailableReason = "The cluster's stored groups cannot be listed right now.";

    /// <inheritdoc />
    public async ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        if (limit <= 0)
        {
            return LtSuggestionSet.Empty;
        }

        if (catalog is null)
        {
            return LtSuggestionSet.Unavailable(UnavailableReason);
        }

        try
        {
            var groups = await catalog.GetGroupsAsync(cancellationToken).ConfigureAwait(false);
            var values = new List<LtSuggestion>(groups.Count);
            for (var i = 0; i < groups.Count; i++)
            {
                if (!ClusterSubjectSuggestionSource.IsTenantGroupId(groups[i].GroupId))
                {
                    values.Add(Describe(groups[i]));
                }
            }

            var term = text.Trim();
            var matched = SuggestionMatcher.Match(values, term, limit);
            var full = groups.Count >= AuthPageRequest.MaxPageSize;
            if (!full || term.Length == 0 || matched.Find(term) is not null || ClusterSubjectSuggestionSource.IsTenantGroupId(term))
            {
                return full ? LtSuggestionSet.Of(matched.Items, truncated: true) : matched;
            }

            // The first page is full, so the typed id may be a group beyond it.
            var exact = await catalog.Admin.GetGroupAsync(term, cancellationToken).ConfigureAwait(false);
            if (exact is null)
            {
                return LtSuggestionSet.Of(matched.Items, truncated: true);
            }

            var items = new List<LtSuggestion>(Math.Min(limit, matched.Items.Count + 1)) { Describe(exact) };
            for (var i = 0; i < matched.Items.Count && items.Count < limit; i++)
            {
                items.Add(matched.Items[i]);
            }

            return LtSuggestionSet.Of(items, truncated: true);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return LtSuggestionSet.Unavailable(UnavailableReason);
        }
    }

    private static LtSuggestion Describe(AuthGroup group) =>
        new(group.GroupId, string.IsNullOrWhiteSpace(group.DisplayName) || group.DisplayName == group.GroupId ? null : group.DisplayName);
}
