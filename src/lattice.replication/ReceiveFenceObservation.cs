namespace Orleans.Lattice.Replication;

/// <summary>
/// One read of a tree's inbound receive fence (issue #4593): whether inbound
/// apply is paused, and the fence's epoch, which every pause bumps. An apply is
/// admitted under the epoch the observation that admitted it carried; a restored
/// copy refuses an apply admitted under an epoch older than the pause its restore
/// took.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReceiveFenceObservation)]
[Immutable]
internal readonly record struct ReceiveFenceObservation
{
    /// <summary>Whether inbound apply for the tree is paused.</summary>
    [Id(0)] public bool Paused { get; init; }

    /// <summary>The fence's epoch: the number of pauses it has taken.</summary>
    [Id(1)] public long Epoch { get; init; }
}