namespace Orleans.Lattice;

/// <summary>
/// What kind of object <see cref="LatticeAdminAuditRecord.TargetId"/> names.
/// Values are explicit and persisted, so they are only ever appended.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeAdminAuditTargetKind)]
public enum LatticeAdminAuditTargetKind
{
    /// <summary>No specific object (for example a denied listing call).</summary>
    None = 0,

    /// <summary>An authorization rule, named <c>{treeId}/{ruleId}</c>.</summary>
    Rule = 1,

    /// <summary>A group, named by its group id.</summary>
    Group = 2,

    /// <summary>A group membership edge, named <c>{groupId}/{memberId}</c>.</summary>
    GroupMember = 3,

    /// <summary>A tenant member-set entry, named by the member subject id.</summary>
    TenantMember = 4,

    /// <summary>A tenant administrator-set entry, named by the administrator subject id.</summary>
    TenantAdministrator = 5,

    /// <summary>A tenant, named by its tenant id.</summary>
    Tenant = 6,

    /// <summary>A cross-tenant grant, named <c>{grantor}/{grantee}</c>.</summary>
    Grant = 7,

    /// <summary>A tenant's region configuration, named by the tenant id (and region where one applies).</summary>
    TenantRegion = 8,

    /// <summary>An installed app, named <c>{tenant}/{slug}</c>.</summary>
    App = 9,

    /// <summary>A logical tree alias, named by the logical tree id.</summary>
    Alias = 10,
}
