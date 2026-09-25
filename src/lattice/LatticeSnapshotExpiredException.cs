namespace Orleans.Lattice;

/// <summary>
/// Thrown by a zero-observable-writes snapshot cursor's
/// <c>Next*Async</c> when a shard's frozen baseline - the per-shard
/// projection captured when the cursor opened, which every page is
/// served from - can no longer be loaded. Either the in-memory copy
/// was lost (idle eviction or failover) before the cursor's first page
/// made it durable, or the durable copy was reclaimed after
/// <see cref="LatticeOptions.SnapshotBaselineTtl"/> without activity.
/// The cursor never falls back to replaying the WAL, which may have
/// been trimmed, so it fails rather than return an empty or partial
/// view. Callers should open a fresh snapshot cursor; the existing
/// cursor's persisted state is left intact and a subsequent
/// <c>CloseCursorAsync</c> still cleans it up.
/// </summary>
public sealed class LatticeSnapshotExpiredException : InvalidOperationException, ILatticeDomainFault
{
    /// <summary>
    /// Initialises a new instance with the specified message.
    /// </summary>
    public LatticeSnapshotExpiredException(string message) : base(message) { }

    /// <summary>
    /// Initialises a new instance with the specified message and inner
    /// exception.
    /// </summary>
    public LatticeSnapshotExpiredException(string message, Exception innerException) : base(message, innerException) { }
}
