namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// What <see cref="SqliteSnapshotOrphanSweep.Run"/> found and, in
/// <see cref="SqliteSnapshotSweepMode.Delete"/> mode, removed.
/// </summary>
/// <param name="Mode">The mode the sweep ran in.</param>
/// <param name="LeavesScanned">Distinct leaf identities that own a leaf row, a snapshot manifest or a snapshot segment.</param>
/// <param name="StrandedLeaves">Of those, the identities no live row reaches, whose rows are stranded.</param>
/// <param name="LeafRows">Stranded <c>leaf</c> rows: rows written back without a tree id after the leaf was cleared.</param>
/// <param name="ManifestRows">Stranded <c>leaf-snapshot</c> manifest rows.</param>
/// <param name="SegmentRows">Stranded <c>leaf-snapshot-segment</c> rows.</param>
/// <param name="PayloadBytes">The payload bytes the stranded rows hold.</param>
/// <param name="RowsDeleted">The rows actually deleted; zero unless the mode is <see cref="SqliteSnapshotSweepMode.Delete"/>.</param>
/// <param name="BytesBefore">The database file size before the sweep.</param>
/// <param name="BytesAfter">The database file size after the sweep and any vacuum it ran.</param>
/// <param name="Elapsed">How long the sweep took.</param>
public sealed record SqliteSnapshotSweepOutcome(
    SqliteSnapshotSweepMode Mode,
    int LeavesScanned,
    int StrandedLeaves,
    int LeafRows,
    int ManifestRows,
    int SegmentRows,
    long PayloadBytes,
    int RowsDeleted,
    long BytesBefore,
    long BytesAfter,
    TimeSpan Elapsed)
{
    /// <summary>The outcome of a sweep that was not run.</summary>
    public static SqliteSnapshotSweepOutcome NotRun { get; } =
        new(SqliteSnapshotSweepMode.Off, 0, 0, 0, 0, 0, 0, 0, 0, 0, TimeSpan.Zero);
}
