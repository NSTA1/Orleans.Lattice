using System.Collections.Immutable;

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
    /// <summary>A reachable coordinator whose runner cannot currently sweep.</summary>
    internal static readonly WalOrphanPinSweepOutcome Unavailable = new() { Available = false };

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
    /// The repairable floor-holding consumer ids, or empty when
    /// <see cref="Classified"/> is <see langword="false"/>.
    /// </summary>
    [Id(3)] public ImmutableArray<string> RepairConsumerIds { get; init; } = [];

    /// <summary>
    /// The subset of <see cref="RepairConsumerIds"/> admitted on the offset axis,
    /// or empty when <see cref="Classified"/> is <see langword="false"/>.
    /// </summary>
    [Id(4)] public ImmutableHashSet<string> RequireOffsetAdvance { get; init; } = ImmutableHashSet<string>.Empty;

    /// <summary>The measured floor-holding pin population of the tree.</summary>
    [Id(5)] public int PinPopulation { get; init; }

    /// <summary>
    /// The measured offset-bearing pin population per WAL partition, or
    /// <see langword="null"/> when no classifying sweep has measured it.
    /// </summary>
    [Id(6)] public ImmutableDictionary<int, int>? PartitionPopulations { get; init; }

    /// <summary>The sum of <see cref="PartitionPopulations"/>.</summary>
    [Id(7)] public int OffsetPopulation { get; init; }

    /// <summary>
    /// Whether this coordinator can sweep. False requests local fallback, whereas
    /// a null outcome means the first sweep is still pending.
    /// </summary>
    [Id(8)] public bool Available { get; init; } = true;
}
