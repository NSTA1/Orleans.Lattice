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
}
