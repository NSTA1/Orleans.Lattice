using System.ComponentModel;
using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Metadata stored per tree in the internal registry tree.
/// Serialized as the <c>byte[]</c> value for each tree ID key.
/// Contains optional <see cref="LatticeOptions"/> overrides that take
/// priority over <see cref="IOptionsMonitor{LatticeOptions}"/> at runtime.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TreeRegistryEntry)]
[Immutable]
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record TreeRegistryEntry
{
    /// <summary>Maximum number of keys per leaf node, or <c>null</c> to use configured defaults.</summary>
    [Id(0)] public int? MaxLeafKeys { get; init; }

    /// <summary>Maximum number of children per internal node, or <c>null</c> to use configured defaults.</summary>
    [Id(1)] public int? MaxInternalChildren { get; init; }

    /// <summary>Number of shards, or <c>null</c> to use configured defaults.</summary>
    [Id(2)] public int? ShardCount { get; init; }

    /// <summary>
    /// Physical tree ID that this logical tree ID maps to, or <c>null</c> if the
    /// logical ID is the physical ID (the default). Used by tree aliasing to redirect
    /// reads and writes to a different physical tree after a resize operation.
    /// Only a single level of indirection is supported — a physical tree must not
    /// itself have a <see cref="PhysicalTreeId"/>.
    /// </summary>
    [Id(3)] public string? PhysicalTreeId { get; init; }
}
