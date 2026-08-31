namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Defaults shared by every grain-index query.
/// </summary>
public static class GrainIndexQueryDefaults
{
    /// <summary>
    /// The number of entries a query fetches per round trip when the caller does
    /// not pick one. Large enough that a scan is not round-trip bound, small
    /// enough that only a bounded slice of a large result set is ever in flight.
    /// </summary>
    public const int PageSize = 256;

    /// <summary>
    /// How a query walks the tree when the caller does not pick a mode: a
    /// durable, checkpointed cursor, so a long scan survives failover and never
    /// materialises more than one page.
    /// </summary>
    public const GrainIndexQueryExecution Execution = GrainIndexQueryExecution.DurableCursor;
}
