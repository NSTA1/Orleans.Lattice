using System.Collections.Immutable;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The cross-tree atomic write a tree's sub-saga belongs to (issue #4683): the
/// operation id - the authoring coordinator's key, which every terminal of the
/// sub-saga carries as its cross-tree operation id - and the participating
/// trees. Recorded by the authoring tree's transaction registry for as long as
/// the sub-saga's decision is stored, and carried on the snapshot export's rows
/// of the sub-saga.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.CrossTreeMembership)]
internal sealed record CrossTreeMembership
{
    /// <summary>The cross-tree operation id.</summary>
    [Id(0)] public required string OperationId { get; init; }

    /// <summary>Every tree the cross-tree write touched, ordinal-sorted and de-duplicated.</summary>
    [Id(1)] public required ImmutableArray<string> Participants { get; init; }

    /// <summary>
    /// The operation's decision stamps (issue #4684): per participating tree,
    /// that tree's snapshot export epoch read after the decision was durable and
    /// before any participant finalized, or <see langword="null"/> for an
    /// operation decided by a silo that predates stamping. Carried on the
    /// export's rows and the shipped terminals of the sub-saga.
    /// </summary>
    [Id(2)] public ImmutableDictionary<string, long>? DecisionStamps { get; init; }
}
