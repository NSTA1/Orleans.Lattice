using System.Collections.Immutable;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>A read of <see cref="ICrossTreeHoldTrackerGrain"/>.</summary>
[GenerateSerializer]
[Immutable]
[Alias(ReplicationTypeAliases.CrossTreeHoldSnapshot)]
internal sealed record CrossTreeHoldSnapshot(
    [property: Id(0)] bool Completed,
    [property: Id(1)] ImmutableDictionary<string, CrossTreeHoldBoundary> Boundaries);
