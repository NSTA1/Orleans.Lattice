namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The SQLite <c>auto_vacuum</c> mode the host applies to its database file, selected
/// by <see cref="RepoContextHostConfiguration.SqliteAutoVacuumKey"/>. The numeric values
/// are SQLite's own <c>PRAGMA auto_vacuum</c> codes.
/// </summary>
public enum SqliteAutoVacuumMode
{
    /// <summary>
    /// <c>auto_vacuum=NONE</c>: pages freed by deletes stay on SQLite's internal
    /// freelist, so the file never shrinks without a manual <c>VACUUM</c>.
    /// </summary>
    None = 0,

    /// <summary>
    /// <c>auto_vacuum=FULL</c>: every commit moves freed pages to the end of the file and
    /// truncates them. The file tracks live data exactly, at the cost of extra page
    /// relocation inside every write transaction.
    /// </summary>
    Full = 1,

    /// <summary>
    /// <c>auto_vacuum=INCREMENTAL</c> (the default): commits do no extra work, and the
    /// host's background reclaimer returns freed pages to the filesystem in small,
    /// paced batches.
    /// </summary>
    Incremental = 2,
}
