namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// When an indexed grain's state write publishes its index entries.
/// </summary>
public enum GrainIndexProjectionMode
{
    /// <summary>
    /// The index entries are written as part of the grain's write path, and a
    /// failure is surfaced to the caller. This is the default.
    /// <para>
    /// The caller learns immediately that the index disagrees with the state it
    /// just wrote, which is the only way a failed index update cannot become
    /// invisible drift. The grain's own state is committed before the index
    /// batch is attempted, so the failure never rolls back or corrupts it, and
    /// the pending-projection outbox entry recorded beforehand is retried until
    /// the entries land regardless of what the caller does with the exception.
    /// </para>
    /// </summary>
    Synchronous = 0,

    /// <summary>
    /// The index write is recorded durably during the grain's write path but
    /// applied afterwards by the outbox drain, so the caller does not wait for
    /// it and does not see its failures.
    /// <para>
    /// The entries still land - the outbox is what guarantees that - but a
    /// query issued immediately after the write may not see them yet. Choose it
    /// for a latency-sensitive write path that tolerates a lagging index.
    /// </para>
    /// </summary>
    Eventual = 1,
}
