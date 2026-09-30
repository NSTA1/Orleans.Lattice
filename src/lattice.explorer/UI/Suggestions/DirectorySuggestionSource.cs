using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.UI.Suggestions;

/// <summary>
/// Users or groups from the cluster's identity directory, searched by what is
/// typed: a principal's id is the value and its display name, rendered as text,
/// the detail.
/// </summary>
/// <remarks>
/// <para>
/// Nothing is remembered: each query is one bounded directory search, and the
/// combobox coalesces keystrokes so a burst of typing costs one search. So no
/// answer can outlive the tenant it was read under.
/// </para>
/// <para>
/// Without a directory (a token-only deployment, or none configured) the source
/// answers unavailable and the field accepts the id as typed, saying the id is not
/// validated. For groups it may instead list the auth store's own groups when
/// asked to, for a field where a group need not be in the directory.
/// </para>
/// </remarks>
/// <param name="admin">The auth facade, or <see langword="null"/> when the head serves none.</param>
/// <param name="kind">Whether users or groups are searched, or <see langword="null"/> for both.</param>
/// <param name="listStoredGroups">For groups: list the auth store's groups when the directory is unavailable.</param>
internal sealed class DirectorySuggestionSource(ILatticeAuthAdmin? admin, DirectoryPrincipalKind? kind, bool listStoredGroups = false)
    : ILtSuggestionSource
{
    /// <summary>The note shown when no directory can be searched.</summary>
    public const string UnavailableReason = "No identity directory can be searched, so the id is used as typed and is not validated.";

    /// <summary>Whether users or groups are searched, or <see langword="null"/> for both.</summary>
    public DirectoryPrincipalKind? Kind => kind;

    /// <inheritdoc />
    public async ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        if (admin is null)
        {
            return LtSuggestionSet.Unavailable(UnavailableReason);
        }

        DirectorySearchResult result;
        try
        {
            result = await admin.SearchDirectoryAsync(
                new DirectorySearchRequest { Term = text.Trim(), Kind = kind, PageSize = Math.Clamp(limit, 1, 100) },
                cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            result = DirectorySearchResult.Unavailable;
        }

        if (result is not { Available: true, Principals: not null })
        {
            return listStoredGroups && kind == DirectoryPrincipalKind.Group
                ? await ListStoredGroupsAsync(admin, text, limit, cancellationToken).ConfigureAwait(false)
                : LtSuggestionSet.Unavailable(UnavailableReason);
        }

        var term = text.Trim();
        var more = result.ContinuationToken is not null || result.Principals.Count > limit;
        DirectoryPrincipalDescriptor? exact = null;
        if (more && term.Length > 0 && !Contains(result.Principals, term))
        {
            // A bounded page can miss the exact id among many partial matches; the
            // exact principal is resolved directly so pick-existing can accept it.
            exact = await ResolveAsync(admin, term, cancellationToken).ConfigureAwait(false);
        }

        return Rank(result.Principals, exact, term, limit, more);
    }

    private static LtSuggestionSet Rank(IReadOnlyList<DirectoryPrincipalDescriptor> principals, DirectoryPrincipalDescriptor? exact, string text, int limit, bool more)
    {
        var values = new List<LtSuggestion>(Math.Min(principals.Count + 1, limit));
        var first = -1;
        if (exact is not null)
        {
            values.Add(Describe(exact));
        }
        else
        {
            for (var i = 0; i < principals.Count; i++)
            {
                if (string.Equals(principals[i].Id, text, StringComparison.Ordinal))
                {
                    first = i;
                    values.Add(Describe(principals[i]));
                    break;
                }
            }
        }

        for (var i = 0; i < principals.Count && values.Count < limit; i++)
        {
            if (i != first)
            {
                values.Add(Describe(principals[i]));
            }
        }

        return LtSuggestionSet.Of(values, more);
    }

    private static bool Contains(IReadOnlyList<DirectoryPrincipalDescriptor> principals, string id)
    {
        for (var i = 0; i < principals.Count; i++)
        {
            if (string.Equals(principals[i].Id, id, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private async Task<DirectoryPrincipalDescriptor?> ResolveAsync(ILatticeAuthAdmin auth, string id, CancellationToken cancellationToken)
    {
        try
        {
            var principal = await auth.ResolveDirectoryPrincipalAsync(id, cancellationToken).ConfigureAwait(false);
            return principal is not null && (kind is null || principal.Kind == kind) && string.Equals(principal.Id, id, StringComparison.Ordinal) ? principal : null;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return null;
        }
    }
    private static LtSuggestion Describe(DirectoryPrincipalDescriptor principal) =>
        new(principal.Id, string.IsNullOrWhiteSpace(principal.DisplayName) || principal.DisplayName == principal.Id ? null : principal.DisplayName);

    private static async Task<LtSuggestionSet> ListStoredGroupsAsync(ILatticeAuthAdmin admin, string text, int limit, CancellationToken cancellationToken)
    {
        try
        {
            var page = await admin.ListGroupsAsync(new AuthPageRequest { PageSize = AuthPageRequest.MaxPageSize }, cancellationToken).ConfigureAwait(false);
            var values = new List<LtSuggestion>();
            foreach (var group in page?.Entries ?? [])
            {
                values.Add(new LtSuggestion(group.GroupId, string.IsNullOrWhiteSpace(group.DisplayName) ? null : group.DisplayName));
            }

            return SuggestionMatcher.Match(values, text.Trim(), limit);
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
}
