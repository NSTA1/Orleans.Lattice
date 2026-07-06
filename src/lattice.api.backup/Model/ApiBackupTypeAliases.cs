namespace Orleans.Lattice.Api.Backup;

/// <summary>
/// Centralized Orleans serialization alias constants for the
/// <c>Orleans.Lattice.Api.Backup</c> control-API package. Mirrors the sibling
/// <c>ApiAuthTypeAliases</c> table: every constant must use the reserved
/// <c>oib.</c> prefix, be at most 6 characters, and be unique.
/// <para>
/// The <c>oib.</c> prefix namespace keeps the backup control-API DTO and gRPC
/// wire types (the sibling gRPC binding reuses this same registry) from
/// colliding with the core (<c>ol.</c>) or the sibling control-API
/// (<c>oli.</c>) namespaces. New serializable types append new
/// <c>oib.</c>-prefixed constants here.
/// </para>
/// </summary>
public static class ApiBackupTypeAliases
{
    /// <summary>
    /// The reserved alias prefix owned by the backup control-API package. Every
    /// alias constant added here must start with this value.
    /// </summary>
    public const string AliasPrefix = "oib.";

    /// <summary>Alias for <see cref="BackupCatalogRequest"/>.</summary>
    public const string BackupCatalogRequest = "oib.cr";

    /// <summary>Alias for <see cref="BackupCatalogPage"/>.</summary>
    public const string BackupCatalogPage = "oib.cp";

    /// <summary>Alias for <see cref="BackupChainDescription"/>.</summary>
    public const string BackupChainDescription = "oib.cd";
}
