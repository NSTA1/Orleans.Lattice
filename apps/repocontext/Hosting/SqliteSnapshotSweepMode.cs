namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Whether the startup sweep of stranded leaf snapshot storage runs, and whether it
/// deletes. See <see cref="SqliteSnapshotOrphanSweep"/>.
/// </summary>
public enum SqliteSnapshotSweepMode
{
    /// <summary>The sweep does not run. The default.</summary>
    Off,

    /// <summary>The sweep classifies and reports what it would delete, and deletes nothing.</summary>
    Report,

    /// <summary>The sweep deletes what it finds and returns the freed pages to the filesystem.</summary>
    Delete,
}
