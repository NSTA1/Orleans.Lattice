using System.Collections.Immutable;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The latest snapshot import of a receiver tree from one origin (issue
/// #4684): the export epoch of the export it drained and every cross-tree
/// operation a row of that export named. A cross-tree barrier waiting for the
/// tree records the tree's arrival with its siblings' verdict when the export
/// opened after the operation's decision (its epoch is greater than the
/// operation's decision stamp for the tree) and named the operation nowhere:
/// such an export carried the sub-saga's outcome as plain rows, because the
/// origin had purged it.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.CrossTreeImportRecord)]
internal sealed record CrossTreeImportRecord
{
    /// <summary>The export epoch of the import's source export.</summary>
    [Id(0)] public required long ExportEpoch { get; init; }

    /// <summary>The cross-tree operations the export named on any row.</summary>
    [Id(1)] public required ImmutableHashSet<string> NamedOperations { get; init; }
}
