namespace Orleans.Lattice;

/// <summary>
/// The result of one coordinated WAL GC orphan-pin sweep of a tree, shared with
/// every silo by <see cref="BPlusTree.Grains.IWalOrphanPinSweepGrain"/>
/// (issue #5097).
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.WalOrphanPinSweepOutcome)]
internal sealed record WalOrphanPinSweepOutcome
{
    /// <summary>
    /// Identifies the coordinator activation that produced the outcome, so a
    /// generation from a later activation is never mistaken for one already seen.
    /// </summary>
    [Id(0)] public Guid Epoch { get; init; }

    /// <summary>Monotonic sweep number within <see cref="Epoch"/>.</summary>
    [Id(1)] public long Generation { get; init; }

    /// <summary>Whether this sweep classified the floor holders.</summary>
    [Id(2)] public bool Classified { get; init; }

    /// <summary>
    /// The repairable floor-holding consumer ids, or <see langword="null"/> when
    /// <see cref="Classified"/> is <see langword="false"/>.
    /// </summary>
    [Id(3)] public string[]? RepairConsumerIds { get; init; }

    /// <summary>
    /// The subset of <see cref="RepairConsumerIds"/> admitted on the offset axis,
    /// or <see langword="null"/> when <see cref="Classified"/> is <see langword="false"/>.
    /// </summary>
    [Id(4)] public string[]? RequireOffsetAdvance { get; init; }

    /// <summary>The measured floor-holding pin population of the tree.</summary>
    [Id(5)] public int PinPopulation { get; init; }

    /// <summary>
    /// The measured offset-bearing pin population per WAL partition, or
    /// <see langword="null"/> when no classifying sweep has measured it.
    /// </summary>
    [Id(6)] public Dictionary<int, int>? PartitionPopulations { get; init; }

    /// <summary>The sum of <see cref="PartitionPopulations"/>.</summary>
    [Id(7)] public int OffsetPopulation { get; init; }
}
