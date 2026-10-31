namespace Orleans.Lattice.Replication;

/// <summary>
/// A <see cref="Orleans.Lattice.Backup.IReplicatedTreeMembership"/> whose answers
/// come from a per-silo snapshot that can lag a configuration change committed on
/// another silo, and that can be brought up to date on demand (#5462). The
/// coordinated-restore dispatcher refreshes it before it declines to coordinate a
/// restore, so a stale "not replicated" can never send a replicated tree down the
/// uncoordinated local path.
/// </summary>
internal interface IRefreshableReplicatedTreeMembership
{
    /// <summary>
    /// Re-reads the durable replication configuration and republishes the
    /// snapshot the membership answers from. Throws when the configuration cannot
    /// be read.
    /// </summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    Task RefreshAsync(CancellationToken cancellationToken = default);
}
