namespace Orleans.Lattice;

/// <summary>
/// The privileged administrative action a <see cref="LatticeAdminAuditRecord"/>
/// attributes. The set is closed: every value names one verb on one of the
/// administration facades, and adding a value is a reviewed change. Values are
/// explicit and persisted, so they are only ever appended, never renumbered.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeAdminAuditAction)]
public enum LatticeAdminAuditAction
{
    /// <summary>No action; never written by a facade.</summary>
    Unspecified = 0,

    /// <summary>An authorization rule was created or replaced.</summary>
    RuleUpsert = 1,

    /// <summary>An authorization rule was removed (including an operator's break-glass removal of a tenant-tier rule).</summary>
    RuleRemove = 2,

    /// <summary>A group record was created or replaced.</summary>
    GroupUpsert = 3,

    /// <summary>A group was removed (for a tenant group, with its cascade).</summary>
    GroupRemove = 4,

    /// <summary>A member (user or nested group) was added to a group.</summary>
    GroupMemberAdd = 5,

    /// <summary>A member was removed from a group.</summary>
    GroupMemberRemove = 6,

    /// <summary>A subject was added to a tenant's member set.</summary>
    TenantMemberAdd = 7,

    /// <summary>A subject was removed from a tenant's member set.</summary>
    TenantMemberRemove = 8,

    /// <summary>A subject was added to a tenant's administrator set.</summary>
    TenantAdministratorAdd = 9,

    /// <summary>A subject was removed from a tenant's administrator set.</summary>
    TenantAdministratorRemove = 10,

    /// <summary>A tenant was created.</summary>
    TenantCreate = 11,

    /// <summary>A tenant was suspended.</summary>
    TenantSuspend = 12,

    /// <summary>A suspended tenant was resumed.</summary>
    TenantResume = 13,

    /// <summary>A tenant was deleted.</summary>
    TenantDelete = 14,

    /// <summary>A tenant's quotas were replaced.</summary>
    TenantQuotasSet = 15,

    /// <summary>A cross-tenant grant was offered.</summary>
    GrantOffer = 16,

    /// <summary>A cross-tenant grant offer was approved.</summary>
    GrantApprove = 17,

    /// <summary>A cross-tenant grant offer was rejected.</summary>
    GrantReject = 18,

    /// <summary>A cross-tenant grant was revoked.</summary>
    GrantRevoke = 19,

    /// <summary>A tenant's allowed regions were set.</summary>
    TenantRegionsAllow = 20,

    /// <summary>A tenant's residency was set.</summary>
    TenantResidencySet = 21,

    /// <summary>A tenant's region lifecycle was advanced.</summary>
    TenantRegionAdvance = 22,

    /// <summary>An app was installed for a tenant.</summary>
    AppInstall = 23,

    /// <summary>An installed app was upgraded to another version.</summary>
    AppUpgrade = 24,

    /// <summary>An installed app was enabled.</summary>
    AppEnable = 25,

    /// <summary>An enabled app was disabled.</summary>
    AppDisable = 26,

    /// <summary>An app was uninstalled.</summary>
    AppUninstall = 27,

    /// <summary>An installed app's consented grant ceiling was replaced.</summary>
    AppConsentUpdate = 28,

    /// <summary>An installed app's role bindings were replaced.</summary>
    AppRoleBindingsUpdate = 29,

    /// <summary>A logical tree was aliased to a physical tree.</summary>
    AliasSet = 30,

    /// <summary>
    /// A read or introspection call on an administration facade. Recorded only
    /// when the call is denied; successful reads are not audited.
    /// </summary>
    Read = 31,
}
