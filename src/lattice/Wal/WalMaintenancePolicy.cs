namespace Orleans.Lattice.Wal;

/// <summary>
/// Policy for how an <see cref="IWalSubscriber"/> treats
/// <see cref="MutationCategory.Maintenance"/> entries (resize / rebalance /
/// compaction / internal structural rewrite) while tailing a per-shard WAL.
/// </summary>
internal enum WalMaintenancePolicy
{
    /// <summary>
    /// Do not surface maintenance entries to the handler. The subscriber still
    /// advances its cursor past them so a maintenance entry at the source head
    /// does not leave the consumer pinned below a non-applicable head. This is
    /// the policy every shipped consumer uses: structural rewrites are replays
    /// of state already authored by user writes, so neither a view projection
    /// nor a replication producer should act on them.
    /// </summary>
    Skip = 0,

    /// <summary>
    /// Surface maintenance entries to the handler alongside user writes. Used
    /// by consumers (for example an audit / change-feed sink) that want the
    /// full unfiltered log.
    /// </summary>
    Include = 1,
}
