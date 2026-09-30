using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// The groups already defined on the cluster, which the New group form checks a
/// typed id against so a duplicate is refused as it is typed: the catalogue's
/// first page, memoised per caller, and - when that page may not hold every
/// group - an exact look-up of the id itself.
/// </summary>
/// <remarks>
/// It holds nothing of its own: the memo is <see cref="AccessCatalog"/>'s, which
/// is keyed on the circuit's caller. A read that fails answers unavailable, so the
/// field says the id could not be checked; the form checks it again before it
/// writes, and the server refuses nothing it did not validate.
/// </remarks>
/// <param name="catalog">The circuit's access catalogue.</param>
internal sealed class AccessGroupNameSource(AccessCatalog catalog) : ILtSuggestionSource
{
    /// <summary>The note shown when the defined groups could not be read.</summary>
    public const string UnavailableReason = "The cluster's groups could not be read, so a duplicate id is caught when the group is created.";

    /// <inheritdoc />
    public async ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        var id = text.Trim();
        if (id.Length == 0)
        {
            return LtSuggestionSet.Empty;
        }

        try
        {
            var groups = await catalog.GetGroupsAsync(cancellationToken).ConfigureAwait(false);
            for (var i = 0; i < groups.Count; i++)
            {
                if (string.Equals(groups[i].GroupId, id, StringComparison.Ordinal))
                {
                    return Of(groups[i]);
                }
            }

            if (groups.Count < AuthPageRequest.MaxPageSize)
            {
                return LtSuggestionSet.Empty;
            }

            // The first page is full, so the id may be a group beyond it.
            var group = await catalog.Admin.GetGroupAsync(id, cancellationToken).ConfigureAwait(false);
            return group is null ? LtSuggestionSet.Empty : Of(group);
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

    private static LtSuggestionSet Of(AuthGroup group) =>
        LtSuggestionSet.Of([new LtSuggestion(group.GroupId, string.IsNullOrWhiteSpace(group.DisplayName) ? null : group.DisplayName)], truncated: false);
}
