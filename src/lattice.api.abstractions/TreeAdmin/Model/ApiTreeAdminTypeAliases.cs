namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// Centralized Orleans serialization alias constants for the
/// <c>Orleans.Lattice.Api.TreeAdmin</c> control-API package. Mirrors the sibling
/// <c>ApiSchemaTypeAliases</c> table: every constant must use the reserved
/// <c>oit.</c> prefix, be at most 6 characters, and be unique.
/// <para>
/// The <c>oit.</c> prefix namespace keeps the tree-administration control-API DTO
/// types from colliding with the core (<c>ol.</c>), the schema engine
/// (<c>ols.</c>), the schema control-API (<c>ois.</c>), or any other sibling
/// control-API namespace. New serializable types append new <c>oit.</c>-prefixed
/// constants here.
/// </para>
/// </summary>
public static class ApiTreeAdminTypeAliases
{
    /// <summary>
    /// The reserved alias prefix owned by the tree-administration control-API
    /// package. Every alias constant added here must start with this value.
    /// </summary>
    public const string AliasPrefix = "oit.";

    /// <summary>Alias for <see cref="LatticeTreeAdminCapabilities"/>.</summary>
    public const string LatticeTreeAdminCapabilities = "oit.ca";

    /// <summary>Alias for <see cref="ShardHotnessSnapshot"/>.</summary>
    public const string ShardHotnessSnapshot = "oit.hs";

    /// <summary>Alias for <see cref="TreeHotnessReport"/>.</summary>
    public const string TreeHotnessReport = "oit.hr";

    /// <summary>Alias for <see cref="ShardDiagnosticSnapshot"/>.</summary>
    public const string ShardDiagnosticSnapshot = "oit.ds";

    /// <summary>Alias for <see cref="TreeAdminDiagnosticReport"/>.</summary>
    public const string TreeAdminDiagnosticReport = "oit.dr";

    /// <summary>Alias for <see cref="ShardMapInspection"/>.</summary>
    public const string ShardMapInspection = "oit.mi";

    /// <summary>Alias for <see cref="ShardProjectionDigestReport"/>.</summary>
    public const string ShardProjectionDigestReport = "oit.pd";

    /// <summary>Alias for <see cref="TreeStatsReport"/>.</summary>
    public const string TreeStatsReport = "oit.st";

    /// <summary>Alias for <see cref="TreeStorageUsageSnapshot"/>.</summary>
    public const string TreeStorageUsageSnapshot = "oit.us";

    /// <summary>Alias for <see cref="ClusterStorageUsageSummary"/>.</summary>
    public const string ClusterStorageUsageSummary = "oit.cs";
}
