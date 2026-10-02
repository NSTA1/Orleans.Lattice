using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// Reads what each entry of a tenant's admin set names, so the admin-set editor
/// can show it. A tenant group id (<c>t/{tenant}/{name}</c>) is known from its
/// grammar alone; any other entry is a cluster group when the auth store or the
/// identity directory records a group under that id, and a user otherwise. Where
/// neither could be read the entry is shown as a user or group, never guessed.
/// </summary>
internal static class TenancyAdminEntries
{
    private static readonly AuthPageRequest GroupPage = new() { PageSize = AuthPageRequest.MaxPageSize };

    /// <summary>Classifies <paramref name="subjects"/>, the admin set of <paramref name="tenant"/>.</summary>
    /// <param name="auth">The cluster auth facade, or <see langword="null"/> when the head serves none.</param>
    /// <param name="tenant">The tenant whose admin set it is.</param>
    /// <param name="subjects">The admin set's entries, in the order to show.</param>
    /// <param name="cancellationToken">Cancels the reads.</param>
    /// <returns>Every entry with its kind, in the same order.</returns>
    /// <exception cref="OperationCanceledException">The reads were cancelled.</exception>
    public static async Task<IReadOnlyList<TenancyAdminEntry>> ClassifyAsync(
        ILatticeAuthAdmin? auth, string tenant, IReadOnlyList<string> subjects, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(subjects);
        var entries = new TenancyAdminEntry[subjects.Count];
        HashSet<string>? clusterGroups = null;
        var groupsRead = false;
        for (var i = 0; i < subjects.Count; i++)
        {
            var subject = subjects[i];
            if (KindFromGrammar(tenant, subject) is { } known)
            {
                entries[i] = new TenancyAdminEntry(subject, known);
                continue;
            }

            if (auth is null)
            {
                entries[i] = new TenancyAdminEntry(subject, TenancyAdminEntryKind.Unknown);
                continue;
            }

            if (!groupsRead)
            {
                groupsRead = true;
                clusterGroups = await ReadClusterGroupsAsync(auth, cancellationToken).ConfigureAwait(true);
            }

            entries[i] = new TenancyAdminEntry(subject, clusterGroups?.Contains(subject) == true
                ? TenancyAdminEntryKind.ClusterGroup
                : await ResolveAsync(auth, subject, clusterGroups is not null, cancellationToken).ConfigureAwait(true));
        }

        return entries;
    }

    /// <summary>
    /// The kind of <paramref name="subject"/> when its grammar alone says it - a
    /// group in the reserved tenant namespace, this tenant's own or another's -
    /// or <see langword="null"/> for a user or cluster group id.
    /// </summary>
    /// <param name="tenant">The tenant whose admin set it is.</param>
    /// <param name="subject">The entry.</param>
    public static TenancyAdminEntryKind? KindFromGrammar(string tenant, string subject)
    {
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(subject);
        if (!subject.StartsWith("t/", StringComparison.Ordinal))
        {
            return null;
        }

        return LatticeTenantGroupId.TryParse(subject, out var group) && string.Equals(group.Tenant.Value, tenant, StringComparison.Ordinal)
            ? TenancyAdminEntryKind.TenantGroup
            : TenancyAdminEntryKind.OtherTenantGroup;
    }

    private static async Task<HashSet<string>?> ReadClusterGroupsAsync(ILatticeAuthAdmin auth, CancellationToken cancellationToken)
    {
        try
        {
            var page = await auth.ListGroupsAsync(GroupPage, cancellationToken).ConfigureAwait(true);
            var groups = new HashSet<string>(StringComparer.Ordinal);
            foreach (var group in page?.Entries ?? [])
            {
                if (group?.GroupId is { } id)
                {
                    groups.Add(id);
                }
            }

            return groups;
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

    private static async Task<TenancyAdminEntryKind> ResolveAsync(ILatticeAuthAdmin auth, string subject, bool groupsKnown, CancellationToken cancellationToken)
    {
        try
        {
            var principal = await auth.ResolveDirectoryPrincipalAsync(subject, cancellationToken).ConfigureAwait(true);
            if (principal is not null && string.Equals(principal.Id, subject, StringComparison.Ordinal))
            {
                return principal.Kind == DirectoryPrincipalKind.Group ? TenancyAdminEntryKind.ClusterGroup : TenancyAdminEntryKind.User;
            }

            // Not in the directory: a user, unless the auth store's groups could not be read either.
            return groupsKnown ? TenancyAdminEntryKind.User : TenancyAdminEntryKind.Unknown;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return groupsKnown ? TenancyAdminEntryKind.User : TenancyAdminEntryKind.Unknown;
        }
    }
}
