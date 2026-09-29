namespace Orleans.Lattice.Explorer.Shell.Areas.Schema;

/// <summary>
/// The stages a long-running schema operation moves through (epic decision E15):
/// it is reviewed, started, runs in the cluster, and ends. The first stage lives
/// in the page that asks for confirmation; this enumerates the rest.
/// </summary>
internal enum SchemaOperationStage
{
    /// <summary>Reviewed and confirmed; being asked of the cluster.</summary>
    Starting = 0,

    /// <summary>Running in the cluster; its status is read from the cluster.</summary>
    Running = 1,

    /// <summary>Finished, and the tree serves the result.</summary>
    Completed = 2,

    /// <summary>Stopped at an offending value, with nothing cut over.</summary>
    Aborted = 3,

    /// <summary>The cluster refused or could not run it.</summary>
    Failed = 4,
}
