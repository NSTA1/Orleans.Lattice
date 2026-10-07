namespace Orleans.Lattice.Replication;

/// <summary>
/// The outcome of checking a replicated entry's causal dependencies against
/// the receiver's tree (<see cref="Grains.IReplicationHighWaterMarkGrain.CheckDependenciesAsync"/>).
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.CausalDependencyVerdict)]
internal enum CausalDependencyVerdict
{
    /// <summary>Not every dependency has arrived yet; the entry parks.</summary>
    Unmet = 0,

    /// <summary>Every dependency is satisfied; the entry may apply.</summary>
    Met = 1,

    /// <summary>
    /// A dependency names a write this tree acknowledged and then lost for good
    /// (#4603). It can never be satisfied, so the entry is dead-lettered with
    /// reason <see cref="LatticeReplicationMetrics.ReasonDependencyLost"/>.
    /// </summary>
    Lost = 2,
}
