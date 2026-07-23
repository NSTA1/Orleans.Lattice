namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// Centralized Orleans serialization alias constants for the replication
/// control-API surface (the transport-agnostic contract in
/// <c>Orleans.Lattice.Api.Abstractions</c> and the sibling gRPC / MCP bindings
/// that reuse this registry). Mirrors the sibling <c>ApiBackupTypeAliases</c>
/// table: every constant must use the reserved <c>oir.</c> prefix, be at most 6
/// characters, and be unique.
/// <para>
/// The <c>oir.</c> prefix namespace keeps the replication control-API DTO and
/// wire types from colliding with the core (<c>ol.</c>), the backup control-API
/// (<c>oib.</c>), or the schema control-API (<c>ois.</c>) namespaces. New
/// serializable types append new <c>oir.</c>-prefixed constants here.
/// </para>
/// </summary>
public static class ApiReplicationTypeAliases
{
    /// <summary>
    /// The reserved alias prefix owned by the replication control-API surface.
    /// Every alias constant added here must start with this value.
    /// </summary>
    public const string AliasPrefix = "oir.";

    /// <summary>Alias for <see cref="ReplicationConfigReport"/>.</summary>
    public const string ReplicationConfigReport = "oir.rp";

    /// <summary>Alias for <see cref="ReplicationTreeConfigEntry"/>.</summary>
    public const string ReplicationTreeConfigEntry = "oir.te";

    /// <summary>Alias for <see cref="ReplicationEnableResult"/>.</summary>
    public const string ReplicationEnableResult = "oir.er";

    /// <summary>Alias for <see cref="ReplicationDisableResult"/>.</summary>
    public const string ReplicationDisableResult = "oir.dr";
}
