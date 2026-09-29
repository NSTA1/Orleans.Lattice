namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// How a grain-index query walks the tree. Every mode returns the same rows; the
/// difference is what the scan survives and what it costs.
/// </summary>
public enum GrainIndexQueryExecution
{
    /// <summary>
    /// A durable server-side cursor, checkpointed after every page. The default:
    /// a long scan survives silo failovers and shard splits while the caller keeps
    /// enumerating it. The cursor id is not exposed, so a restarted caller starts a
    /// new query. Only one page is ever in flight, so a large result set never
    /// materialises client-side.
    /// </summary>
    DurableCursor = 0,

    /// <summary>
    /// A stateless streaming scan. Cheaper for a small result set because it
    /// opens no server-side cursor state, but it is bounded by the tree's
    /// scan-retry budget rather than checkpointed, so a long scan can be
    /// interrupted by topology change.
    /// </summary>
    Stream = 1,

    /// <summary>
    /// A durable cursor served from a tree snapshot captured when each key range
    /// begins scanning, so every page within that range sees the same index state
    /// and concurrent index maintenance cannot make a grain appear twice or not at
    /// all across that range's page boundaries. The snapshot is over the
    /// <i>index</i>, not over grain state.
    /// </summary>
    SnapshotCursor = 2,
}
