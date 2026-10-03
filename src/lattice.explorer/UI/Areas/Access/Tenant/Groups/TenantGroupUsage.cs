using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups;

/// <summary>
/// What references one of the tenant's groups, read from the delegated contracts:
/// its direct members (the tenant directory) and the rules that name it (the tenant
/// policy's effective permissions for the group), split into rules and app role
/// bindings. A part that could not be read is <see langword="null"/>, never zero.
/// </summary>
/// <param name="Members">The group's direct members, or <see langword="null"/> when they could not be read.</param>
/// <param name="References">The rules naming the group, or <see langword="null"/> when they could not be read.</param>
internal sealed record TenantGroupUsage(IReadOnlyList<TenantGroupMember>? Members, IReadOnlyList<TenantRuleView>? References)
{
    /// <summary>Nothing read.</summary>
    public static TenantGroupUsage Unknown { get; } = new(null, null);

    /// <summary>How many direct members the group has, or <see langword="null"/> when unread.</summary>
    public int? MemberCount => Members?.Count;

    /// <summary>How many rules (tenant and platform, not app role bindings) name the group, or <see langword="null"/> when unread.</summary>
    public int? RuleCount => References is null ? null : CountWhere(References, appRoles: false);

    /// <summary>How many app role bindings name the group, or <see langword="null"/> when unread.</summary>
    public int? AppRoleCount => References is null ? null : CountWhere(References, appRoles: true);

    /// <summary>How many of the tenant's own rules name the group - the ones its removal deletes - or <see langword="null"/> when unread.</summary>
    public int? TenantRuleCount
    {
        get
        {
            if (References is null)
            {
                return null;
            }

            var count = 0;
            for (var i = 0; i < References.Count; i++)
            {
                if (References[i].Origin == TenantRuleOrigin.Tenant)
                {
                    count++;
                }
            }

            return count;
        }
    }

    /// <summary>
    /// Reads what references <paramref name="name"/>. Each part is read on its own,
    /// so a policy the head does not serve still leaves the members shown.
    /// </summary>
    /// <param name="access">The circuit's tenant access catalogue.</param>
    /// <param name="tenant">The tenant.</param>
    /// <param name="name">The group's tenant-local name.</param>
    /// <param name="cancellationToken">Cancels the reads.</param>
    /// <returns>The usage; never <see langword="null"/>.</returns>
    /// <exception cref="OperationCanceledException">The reads were cancelled.</exception>
    public static async Task<TenantGroupUsage> ReadAsync(TenantAccessCatalog access, string tenant, string name, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(access);
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(name);
        IReadOnlyList<TenantGroupMember>? members = null;
        IReadOnlyList<TenantRuleView>? references = null;
        if (access.Directory is { } directory)
        {
            try
            {
                members = await directory.ListGroupMembersAsync(tenant, name, cancellationToken).ConfigureAwait(true) ?? [];
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                members = null;
            }
        }

        if (access.Policy is not null)
        {
            references = await ReadReferencesAsync(access, tenant, name, cancellationToken).ConfigureAwait(true);
        }

        return new TenantGroupUsage(members, references);
    }

    /// <summary>
    /// Reads the rules that name <paramref name="name"/> - the tenant policy's
    /// effective permissions for the group - or <see langword="null"/> when they
    /// cannot be read, including when the head serves no tenant policy.
    /// </summary>
    /// <param name="access">The circuit's tenant access catalogue.</param>
    /// <param name="tenant">The tenant.</param>
    /// <param name="name">The group's tenant-local name.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The rules, or <see langword="null"/>.</returns>
    /// <exception cref="OperationCanceledException">The read was cancelled.</exception>
    public static async Task<IReadOnlyList<TenantRuleView>?> ReadReferencesAsync(
        TenantAccessCatalog access, string tenant, string name, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(access);
        if (access.Policy is not { } policy)
        {
            return null;
        }

        try
        {
            var permissions = await policy.EffectivePermissionsAsync(tenant, name, null, TenantSubjectKind.TenantGroup, cancellationToken).ConfigureAwait(true);
            return permissions?.Rules ?? [];
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return null;
        }
    }

    private static int CountWhere(IReadOnlyList<TenantRuleView> rules, bool appRoles)
    {
        var count = 0;
        for (var i = 0; i < rules.Count; i++)
        {
            if ((rules[i].Origin == TenantRuleOrigin.AppRole) == appRoles)
            {
                count++;
            }
        }

        return count;
    }
}
