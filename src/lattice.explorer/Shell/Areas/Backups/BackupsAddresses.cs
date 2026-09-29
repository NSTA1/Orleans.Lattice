using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Backups;

/// <summary>
/// The Backups area's address grammar: <c>/backups</c> is the catalogue,
/// <c>/backups/{id}</c> one backup, and the literal segments <c>new</c>,
/// <c>schedules</c>, <c>health</c>, <c>maintenance</c> and
/// <c>operations/{id}</c> the area's other pages.
/// </summary>
internal static class BackupsAddresses
{
    /// <summary>The area's route segment and key.</summary>
    public const string AreaKey = "backups";

    /// <summary>The capture page's segment.</summary>
    public const string CaptureSegment = "new";

    /// <summary>The schedules page's segment.</summary>
    public const string SchedulesSegment = "schedules";

    /// <summary>The health page's segment.</summary>
    public const string HealthSegment = "health";

    /// <summary>The maintenance page's segment.</summary>
    public const string MaintenanceSegment = "maintenance";

    /// <summary>The segment under which staged operations have their status pages.</summary>
    public const string OperationsSegment = "operations";

    /// <summary>The catalogue's kind filter: <c>full</c> or <c>incremental</c>.</summary>
    public const string KindQuery = "kind";

    /// <summary>The catalogue's name-prefix filter.</summary>
    public const string NameQuery = "name";

    /// <summary>The tree a page is about: the catalogue's tree filter, the schedules page's tree, the capture page's first tree.</summary>
    public const string TreeQuery = "tree";

    /// <summary>The health page's focused backup.</summary>
    public const string BackupQuery = "backup";

    /// <summary>The catalogue.</summary>
    public static ExplorerAddress Root => ExplorerAddress.ForArea(AreaKey);

    /// <summary>The capture page.</summary>
    public static ExplorerAddress Capture => ExplorerAddress.ForArea(AreaKey, CaptureSegment);

    /// <summary>The schedules page.</summary>
    public static ExplorerAddress Schedules => ExplorerAddress.ForArea(AreaKey, SchedulesSegment);

    /// <summary>The health page.</summary>
    public static ExplorerAddress Health => ExplorerAddress.ForArea(AreaKey, HealthSegment);

    /// <summary>The maintenance page.</summary>
    public static ExplorerAddress Maintenance => ExplorerAddress.ForArea(AreaKey, MaintenanceSegment);

    /// <summary>One backup's page.</summary>
    /// <param name="backupId">The backup id.</param>
    public static ExplorerAddress Backup(string backupId)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        return ExplorerAddress.ForArea(AreaKey, backupId);
    }

    /// <summary>One staged operation's status page.</summary>
    /// <param name="operationId">The operation id.</param>
    public static ExplorerAddress Operation(string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        return ExplorerAddress.ForArea(AreaKey, OperationsSegment, operationId);
    }

    /// <summary>The health page focused on one backup.</summary>
    /// <param name="backupId">The backup id.</param>
    public static ExplorerAddress HealthOf(string backupId)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        return Health.WithQuery(BackupQuery, backupId);
    }

    /// <summary>The schedules page for one tree.</summary>
    /// <param name="treeId">The tree id.</param>
    public static ExplorerAddress SchedulesOf(string treeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return Schedules.WithQuery(TreeQuery, treeId);
    }
}
