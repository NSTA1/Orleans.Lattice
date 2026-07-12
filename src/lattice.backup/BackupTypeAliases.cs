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

    /// <summary>Alias for <see cref="BackupManifest"/>.</summary>
    internal const string BackupManifest = "olb.mf";

    /// <summary>Alias for <see cref="BackupScopeSelector"/>.</summary>
    internal const string BackupScopeSelector = "olb.sc";

    /// <summary>Alias for <see cref="BackupConsistencyCut"/>.</summary>
    internal const string BackupConsistencyCut = "olb.cc";

    /// <summary>Alias for <see cref="BackupTopologySnapshot"/>.</summary>
    internal const string BackupTopologySnapshot = "olb.tp";

    /// <summary>Alias for <see cref="BackupKeyDescriptor"/>.</summary>
    internal const string BackupKeyDescriptor = "olb.kd";

    /// <summary>Alias for <see cref="BackupContentDescriptor"/>.</summary>
    internal const string BackupContentDescriptor = "olb.cd";

    /// <summary>Alias for <see cref="BackupOriginProvenance"/>.</summary>
    internal const string BackupOriginProvenance = "olb.pv";

    /// <summary>Alias for <see cref="BackupCompressionDictionaryRef"/>.</summary>
    internal const string BackupCompressionDictionaryRef = "olb.dr";

    /// <summary>Alias for <see cref="BackupSetFence"/>.</summary>
    internal const string BackupSetFence = "olb.fn";

    /// <summary>Alias for <see cref="BackupSetManifest"/>.</summary>
    internal const string BackupSetManifest = "olb.sm";

    /// <summary>Alias for <see cref="ILatticeBackupSchedulerGrain"/>.</summary>
    internal const string ILatticeBackupSchedulerGrain = "olb.gs";

    /// <summary>Alias for <see cref="BackupSchedulerState"/>.</summary>
    internal const string BackupSchedulerState = "olb.ss";

    /// <summary>Alias for <see cref="BackupRetentionReport"/>.</summary>
    internal const string BackupRetentionReport = "olb.rr";

    /// <summary>Alias for <see cref="BackupSchedulerRuntimeStatus"/>.</summary>
    internal const string BackupSchedulerRuntimeStatus = "olb.rs";

    /// <summary>Alias for <see cref="BackupCatalogIndexRow"/>.</summary>
    internal const string BackupCatalogIndexRow = "olb.ix";
}
