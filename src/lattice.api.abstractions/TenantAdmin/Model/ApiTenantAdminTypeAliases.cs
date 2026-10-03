namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Centralized Orleans serialization alias constants for the tenant-administration
/// control-API surface (the transport-agnostic contract in
/// <c>Orleans.Lattice.Api.Abstractions</c> and the sibling gRPC / MCP bindings
/// that reuse this registry). Mirrors the sibling <c>ApiReplicationTypeAliases</c>
/// / <c>ApiTreeAdminTypeAliases</c> tables: every constant must use the reserved
/// <c>oitn.</c> prefix and be unique.
/// <para>
/// The <c>oitn.</c> prefix namespace keeps the tenant-admin control-API DTO types
/// from colliding with the core (<c>ol.</c>), the tree-admin control-API
/// (<c>oit.</c>), the replication control-API (<c>oir.</c>), or the tenancy
/// engine (<c>olt.</c>) namespaces. New serializable types append new
/// <c>oitn.</c>-prefixed constants here.
/// </para>
/// </summary>
public static class ApiTenantAdminTypeAliases
{
    /// <summary>
    /// The reserved alias prefix owned by the tenant-admin control-API surface.
    /// Every alias constant added here must start with this value.
    /// </summary>
    public const string AliasPrefix = "oitn.";

    /// <summary>Alias for <see cref="TenantLifecycleStatus"/>.</summary>
    public const string TenantLifecycleStatus = "oitn.st";

    /// <summary>Alias for <see cref="TenantCreationResult"/>.</summary>
    public const string TenantCreationResult = "oitn.cr";

    /// <summary>Alias for <see cref="TenantStatusChangeResult"/>.</summary>
    public const string TenantStatusChangeResult = "oitn.sc";

    /// <summary>Alias for <see cref="TenantDeletionResult"/>.</summary>
    public const string TenantDeletionResult = "oitn.dl";

    /// <summary>Alias for <see cref="TenantRegionLifecycleStatus"/>.</summary>
    public const string TenantRegionLifecycleStatus = "oitn.rl";

    /// <summary>Alias for <see cref="TenantRegionStatusDescriptor"/>.</summary>
    public const string TenantRegionStatusDescriptor = "oitn.rd";

    /// <summary>Alias for <see cref="TenantRegionStatusReport"/>.</summary>
    public const string TenantRegionStatusReport = "oitn.rr";

    /// <summary>Alias for <see cref="TenantRegionAuthorizationResult"/>.</summary>
    public const string TenantRegionAuthorizationResult = "oitn.ra";

    /// <summary>Alias for <see cref="TenantResidencyChangeResult"/>.</summary>
    public const string TenantResidencyChangeResult = "oitn.rc";

    /// <summary>Alias for <see cref="TenantDescriptor"/>.</summary>
    public const string TenantDescriptor = "oitn.td";

    /// <summary>Alias for <see cref="TenantStatusReport"/>.</summary>
    public const string TenantStatusReport = "oitn.ts";

    /// <summary>Alias for <see cref="TenantQuotasDescriptor"/>.</summary>
    public const string TenantQuotasDescriptor = "oitn.qd";

    /// <summary>Alias for <see cref="TenantQuotasUpdateResult"/>.</summary>
    public const string TenantQuotasUpdateResult = "oitn.qu";

    /// <summary>Alias for <see cref="TenantQuotaEnforcementScope"/>.</summary>
    public const string TenantQuotaEnforcementScope = "oitn.qe";

    /// <summary>Alias for <see cref="TenantQuotaDimensionUsage"/>.</summary>
    public const string TenantQuotaDimensionUsage = "oitn.qx";

    /// <summary>Alias for <see cref="TenantQuotaUsageReport"/>.</summary>
    public const string TenantQuotaUsageReport = "oitn.qr";

    /// <summary>Alias for <see cref="TenantAdminSubjectReport"/>.</summary>
    public const string TenantAdminSubjectReport = "oitn.sr";

    /// <summary>Alias for <see cref="TenantAdminSubjectChangeResult"/>.</summary>
    public const string TenantAdminSubjectChangeResult = "oitn.su";

    /// <summary>Alias for <see cref="TenantGrantAccess"/>.</summary>
    public const string TenantGrantAccess = "oitn.ga";

    /// <summary>Alias for <see cref="TenantGrantLifecycleState"/>.</summary>
    public const string TenantGrantLifecycleState = "oitn.gl";

    /// <summary>Alias for <see cref="TenantGrantDescriptor"/>.</summary>
    public const string TenantGrantDescriptor = "oitn.gd";

    /// <summary>Alias for <see cref="TenantGrantReport"/>.</summary>
    public const string TenantGrantReport = "oitn.gr";

    /// <summary>Alias for <see cref="TenantGrantChangeResult"/>.</summary>
    public const string TenantGrantChangeResult = "oitn.gc";

    // ----- Delegated tenant access administration (directory and policy) -----

    /// <summary>Alias for <see cref="TenantSubjectKind"/>.</summary>
    public const string TenantSubjectKind = "oitn.sk";

    /// <summary>Alias for <see cref="TenantRuleLayer"/>.</summary>
    public const string TenantRuleLayer = "oitn.ly";

    /// <summary>Alias for <see cref="TenantRuleOrigin"/>.</summary>
    public const string TenantRuleOrigin = "oitn.ro";

    /// <summary>Alias for <see cref="TenantRuleScopeKind"/>.</summary>
    public const string TenantRuleScopeKind = "oitn.rs";

    /// <summary>Alias for <see cref="TenantAccessPageRequest"/>.</summary>
    public const string TenantAccessPageRequest = "oitn.pq";

    /// <summary>Alias for <see cref="TenantGroupDescriptor"/>.</summary>
    public const string TenantGroupDescriptor = "oitn.gp";

    /// <summary>Alias for <see cref="TenantGroupPage"/>.</summary>
    public const string TenantGroupPage = "oitn.gg";

    /// <summary>Alias for <see cref="TenantGroupMember"/>.</summary>
    public const string TenantGroupMember = "oitn.gm";

    /// <summary>Alias for <see cref="TenantGroupRemovalResult"/>.</summary>
    public const string TenantGroupRemovalResult = "oitn.gx";

    /// <summary>Alias for <see cref="TenantMemberEntry"/>.</summary>
    public const string TenantMemberEntry = "oitn.me";

    /// <summary>Alias for <see cref="TenantMemberPage"/>.</summary>
    public const string TenantMemberPage = "oitn.mp";

    /// <summary>Alias for <see cref="TenantMembershipChangeResult"/>.</summary>
    public const string TenantMembershipChangeResult = "oitn.mc";

    /// <summary>Alias for <see cref="TenantSubjectResolution"/>.</summary>
    public const string TenantSubjectResolution = "oitn.sv";

    /// <summary>Alias for <see cref="TenantRuleDraft"/>.</summary>
    public const string TenantRuleDraft = "oitn.rf";

    /// <summary>Alias for <see cref="TenantRuleView"/>.</summary>
    public const string TenantRuleView = "oitn.rv";

    /// <summary>Alias for <see cref="TenantRulePage"/>.</summary>
    public const string TenantRulePage = "oitn.rg";

    /// <summary>Alias for <see cref="TenantExplanation"/>.</summary>
    public const string TenantExplanation = "oitn.ex";

    /// <summary>Alias for <see cref="TenantEffectivePermissions"/>.</summary>
    public const string TenantEffectivePermissions = "oitn.ep";

    /// <summary>Alias for <see cref="TenantAccessPosture"/>.</summary>
    public const string TenantAccessPosture = "oitn.ap";
}
