namespace Orleans.Lattice.Backup;

/// <summary>
/// Centralized Orleans serialization alias constants for the
/// <c>Orleans.Lattice.Backup</c> package. Mirrors the core <c>TypeAliases</c>
/// table and the sibling <c>AuthTypeAliases</c> / <c>MembershipTypeAliases</c>:
/// every constant must use the reserved <c>olb.</c> prefix, be at most 6
/// characters, and be unique.
/// <para>
/// This scaffolding release declares no concrete aliases - it reserves the
/// <c>olb.</c> prefix namespace so later releases can add serializable backup
/// types without colliding with the core (<c>ol.</c>), membership (<c>olm.</c>),
/// authorization (<c>olz.</c>), or control-API (<c>oli.</c>) namespaces. New
/// serializable types append new <c>olb.</c>-prefixed constants here.
/// </para>
/// </summary>
internal static class BackupTypeAliases
{
    /// <summary>
    /// The reserved alias prefix owned by the backup package. Every alias
    /// constant added here must start with this value.
    /// </summary>
    internal const string AliasPrefix = "olb.";
}
