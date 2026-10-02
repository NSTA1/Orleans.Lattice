using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups;

/// <summary>
/// The preview a tenant group's deletion is confirmed with: what the tenant
/// directory's cascade will take with it - its member entries, the tenant rules
/// and app role bindings that name it, and its entries in the tenant's member and
/// administrator sets - read before anything is written.
/// </summary>
/// <param name="Usage">The group's members and the rules naming it.</param>
/// <param name="InMemberSet">Whether the group is an entry of the tenant's member set, or <see langword="null"/> when unread.</param>
/// <param name="InAdminSet">Whether the group is an entry of the tenant's administrator set, or <see langword="null"/> when unread.</param>
internal sealed record TenantGroupCascade(TenantGroupUsage Usage, bool? InMemberSet, bool? InAdminSet)
{
    /// <summary>
    /// The sentence the confirmation opens with, such as
    /// <c>Deleting ops removes 3 member entries, 2 rules, 1 app binding.</c>
    /// </summary>
    /// <param name="name">The group's tenant-local name.</param>
    /// <returns>The sentence.</returns>
    public string Text(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        if (Usage.MemberCount is not { } members || Usage.TenantRuleCount is not { } rules || Usage.AppRoleCount is not { } bindings)
        {
            return $"What deleting {name} removes could not be read in full. Its member entries, the tenant rules that name it, and its member and administrator set entries are removed with it.";
        }

        var parts = new List<string>(5)
        {
            TenantGroupFormat.Count(members, "member entry", "member entries"),
            TenantGroupFormat.Count(rules, "rule", "rules"),
            TenantGroupFormat.Count(bindings, "app binding", "app bindings"),
        };
        if (InMemberSet == true)
        {
            parts.Add("its member-set entry");
        }

        if (InAdminSet == true)
        {
            parts.Add("its administrator entry");
        }

        return $"Deleting {name} removes {string.Join(", ", parts)}.";
    }

    /// <summary>
    /// Reads the preview for <paramref name="name"/>: its usage, and whether it is
    /// itself an entry of the tenant's member or administrator set.
    /// </summary>
    /// <param name="access">The circuit's tenant access catalogue.</param>
    /// <param name="tenant">The tenant.</param>
    /// <param name="name">The group's tenant-local name.</param>
    /// <param name="cancellationToken">Cancels the reads.</param>
    /// <returns>The preview; never <see langword="null"/>.</returns>
    /// <exception cref="OperationCanceledException">The reads were cancelled.</exception>
    public static async Task<TenantGroupCascade> ReadAsync(
        TenantAccessCatalog access, string tenant, string name, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(access);
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(name);
        var usage = await TenantGroupUsage.ReadAsync(access, tenant, name, cancellationToken).ConfigureAwait(true);
        bool? inMembers = null;
        bool? inAdmins = null;
        if (access.Directory is { } directory)
        {
            try
            {
                var resolution = await directory.ResolveSubjectAsync(tenant, name, TenantSubjectKind.TenantGroup, cancellationToken).ConfigureAwait(true);
                if (resolution is not null)
                {
                    inMembers = Names(resolution.MemberEntries, name);
                    inAdmins = Names(resolution.AdminEntries, name);
                }
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                // Unknown, said as such: the confirmation does not guess.
            }
        }

        return new TenantGroupCascade(usage, inMembers, inAdmins);
    }

    /// <summary>Whether <paramref name="entries"/> holds the group <paramref name="name"/> itself, not a group containing it.</summary>
    private static bool Names(IReadOnlyList<TenantMemberEntry> entries, string name)
    {
        for (var i = 0; i < entries.Count; i++)
        {
            if (entries[i].Kind == TenantSubjectKind.TenantGroup && string.Equals(entries[i].SubjectId, name, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
