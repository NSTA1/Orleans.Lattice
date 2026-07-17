namespace Orleans.Lattice.Api.Schema;

/// <summary>
/// Centralized Orleans serialization alias constants for the
/// <c>Orleans.Lattice.Api.Schema</c> control-API package. Mirrors the sibling
/// <c>ApiBackupTypeAliases</c> table: every constant must use the reserved
/// <c>ois.</c> prefix, be at most 6 characters, and be unique.
/// <para>
/// The <c>ois.</c> prefix namespace keeps the schema control-API DTO types from
/// colliding with the core (<c>ol.</c>), the schema engine (<c>ols.</c>), or the
/// sibling control-API (<c>oib.</c>) namespaces. New serializable types append new
/// <c>ois.</c>-prefixed constants here.
/// </para>
/// </summary>
public static class ApiSchemaTypeAliases
{
    /// <summary>
    /// The reserved alias prefix owned by the schema control-API package. Every
    /// alias constant added here must start with this value.
    /// </summary>
    public const string AliasPrefix = "ois.";

    /// <summary>Alias for <see cref="LatticeSchemaCapabilities"/>.</summary>
    public const string LatticeSchemaCapabilities = "ois.ca";
}
