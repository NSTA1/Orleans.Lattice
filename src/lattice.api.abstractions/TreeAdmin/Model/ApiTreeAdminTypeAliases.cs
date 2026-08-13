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

    /// <summary>Alias for <see cref="TreeCreationResult"/>.</summary>
    public const string TreeCreationResult = "oit.cr";

    /// <summary>Alias for <see cref="TreeExistenceResult"/>.</summary>
    public const string TreeExistenceResult = "oit.ex";

    /// <summary>Alias for <see cref="TreeAliasResolution"/>.</summary>
    public const string TreeAliasResolution = "oit.al";

    /// <summary>Alias for <see cref="TreeConfigurationReport"/>.</summary>
    public const string TreeConfigurationReport = "oit.cg";

    /// <summary>Alias for <see cref="TreeConfigurationUpdate"/>.</summary>
    public const string TreeConfigurationUpdate = "oit.cu";

    /// <summary>Alias for <see cref="TreeShardMapView"/>.</summary>
    public const string TreeShardMapView = "oit.sm";

    /// <summary>Alias for <see cref="TreeDeletionStatus"/>.</summary>
    public const string TreeDeletionStatus = "oit.dl";

    /// <summary>Alias for <see cref="TreeBulkLoadSession"/>.</summary>
    public const string TreeBulkLoadSession = "oit.bs";

    /// <summary>Alias for <see cref="TreeBulkLoadChunkAck"/>.</summary>
    public const string TreeBulkLoadChunkAck = "oit.bk";

    /// <summary>Alias for <see cref="TreeBulkLoadResult"/>.</summary>
    public const string TreeBulkLoadResult = "oit.bo";

    /// <summary>Alias for <see cref="TreeRestoreMode"/>.</summary>
    public const string TreeRestoreMode = "oit.rm";

    /// <summary>Alias for <see cref="TreeRestoreResult"/>.</summary>
    public const string TreeRestoreResult = "oit.rr";

    /// <summary>Alias for <see cref="TreeRestoreSetResult"/>.</summary>
    public const string TreeRestoreSetResult = "oit.rs";

    /// <summary>Alias for <see cref="TreeReshardStatus"/>.</summary>
    public const string TreeReshardStatus = "oit.re";

    /// <summary>Alias for <see cref="TreeResizeStatus"/>.</summary>
    public const string TreeResizeStatus = "oit.rz";

    /// <summary>Alias for <see cref="TreeSnapshotMode"/>.</summary>
    public const string TreeSnapshotMode = "oit.sn";

    /// <summary>Alias for <see cref="TreeSnapshotStatus"/>.</summary>
    public const string TreeSnapshotStatus = "oit.ss";
}
