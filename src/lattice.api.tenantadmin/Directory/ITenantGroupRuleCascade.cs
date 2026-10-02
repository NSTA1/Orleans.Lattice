namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The policy underlay of a tenant group removal: removes the tenant-tier rules
/// (<c>tenant:{tenant}:...</c>) whose subject is the removed group, so no rule is
/// left naming a group that no longer exists. It performs no authorization of its
/// own and writes under system origin, the only origin the policy store admits for a
/// tenant-tier rule id.
/// </summary>
internal interface ITenantGroupRuleCascade
{
    /// <summary>
    /// Removes every tenant-tier rule of <paramref name="tenant"/> whose subject is
    /// the group <paramref name="groupId"/>. Idempotent.
    /// </summary>
    /// <param name="tenant">The owning tenant (never the reserved default tenant).</param>
    /// <param name="groupId">The full id of the removed group.</param>
    /// <param name="cancellationToken">Cancels the cascade.</param>
    /// <returns>The full ids of the rules removed, in ordinal order.</returns>
    Task<IReadOnlyList<string>> RemoveRulesNamingGroupAsync(
        TenantId tenant, string groupId, CancellationToken cancellationToken);
}
