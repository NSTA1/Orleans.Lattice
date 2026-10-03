namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The result of <see cref="ILatticeTenantDirectoryAdmin.RemoveGroupAsync"/>:
/// whether the group existed and was removed, and everything the removal
/// cascaded to. Removing a group removes its edges in both directions and its
/// entries in the tenant's member set, its admin set, and its tenant-tier rules,
/// so no dangling reference to the group survives.
/// </summary>
/// <remarks>
/// Removal is idempotent: removing a group that does not exist reports
/// <see cref="Removed"/> <see langword="false"/>, cascades nothing, and is not an
/// error. A group of another tenant reads as not existing.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantGroupRemovalResult)]
[Immutable]
public sealed record TenantGroupRemovalResult
{
    /// <summary>The tenant id the removal targeted.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The local name of the group the removal targeted.</summary>
    [Id(1)] public required string GroupName { get; init; }

    /// <summary><see langword="true"/> when the group existed and was removed; <see langword="false"/> for an idempotent no-op.</summary>
    [Id(2)] public bool Removed { get; init; }

    /// <summary>The number of membership edges removed, counting the group's own members and the groups it was a member of.</summary>
    [Id(3)] public int EdgesRemoved { get; init; }

    /// <summary><see langword="true"/> when the group was an entry of the tenant's member set and that entry was removed.</summary>
    [Id(4)] public bool RemovedFromMemberSet { get; init; }

    /// <summary><see langword="true"/> when the group was an entry of the tenant's admin set and that entry was removed.</summary>
    [Id(5)] public bool RemovedFromAdminSet { get; init; }

    /// <summary>The local ids of the tenant-tier rules naming the group that were removed, in ordinal order.</summary>
    [Id(6)] public IReadOnlyList<string> RemovedRuleIds { get; init; } = Array.Empty<string>();
}
