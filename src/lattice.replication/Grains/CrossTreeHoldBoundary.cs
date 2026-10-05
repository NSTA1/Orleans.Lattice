using System.Collections.Immutable;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The write-ahead log boundary a cross-tree participant tree recorded with
/// <see cref="ICrossTreeHoldTrackerGrain"/>: each partition's next sequence of
/// <see cref="PhysicalTreeId"/>, read after the tree's decision was forgotten,
/// so the participant's terminal lies below it.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(ReplicationTypeAliases.CrossTreeHoldBoundary)]
internal sealed record CrossTreeHoldBoundary(
    [property: Id(0)] string PhysicalTreeId,
    [property: Id(1)] ImmutableArray<long> Tails);
