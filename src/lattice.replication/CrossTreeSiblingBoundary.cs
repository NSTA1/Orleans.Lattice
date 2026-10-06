using System.Collections.Immutable;

namespace Orleans.Lattice.Replication;

/// <summary>
/// A sibling tree's boundary an export captured at its end (issue #4684): the
/// sibling's physical write-ahead log, every partition's next sequence, and its
/// export epoch. A receiver that imported the exported tree keeps it read-fenced
/// until each sibling replicated there has passed its boundary: its shipper has
/// vouched acknowledged positions at or past the tails, or the receiver has
/// completed an import of the sibling from an export numbered above the epoch.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(ReplicationTypeAliases.CrossTreeSiblingBoundary)]
internal sealed record CrossTreeSiblingBoundary
{
    /// <summary>The sibling's physical write-ahead log.</summary>
    [Id(0)] public required string PhysicalTreeId { get; init; }

    /// <summary>Per partition, the next sequence at capture.</summary>
    [Id(1)] public required ImmutableArray<long> Tails { get; init; }

    /// <summary>The sibling's export epoch at capture.</summary>
    [Id(2)] public required long ExportEpoch { get; init; }

    /// <summary>Whether the sibling's log held nothing at capture, so it has nothing to acknowledge.</summary>
    public bool IsEmpty => Tails.All(static t => t <= 0);
}
