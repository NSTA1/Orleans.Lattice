namespace Orleans.Lattice;

/// <summary>
/// The reduce an aggregation materialised view computes over the source entries
/// of each group. The view maintainer keys the source entries by a group key
/// derived from each entry and folds the group's members into a single
/// materialised value stored under the bare group key in the <c>view-{name}</c>
/// tree.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AggregationKind)]
public enum AggregationKind
{
    /// <summary>Number of distinct source keys in the group; materialised as an <see cref="long"/>.</summary>
    Count = 0,

    /// <summary>Sum of each member's numeric contribution; materialised as a <see cref="double"/>.</summary>
    Sum = 1,

    /// <summary>Smallest numeric contribution among the group's members; materialised as a <see cref="double"/>.</summary>
    Min = 2,

    /// <summary>Largest numeric contribution among the group's members; materialised as a <see cref="double"/>.</summary>
    Max = 3,

    /// <summary>
    /// Distinct-member cardinality of the union of every member's contributed
    /// value; materialised as an <see cref="long"/> (the number of distinct
    /// members). The exact mode counts distinct members precisely; the opt-in
    /// approximate mode estimates the cardinality.
    /// </summary>
    SetUnion = 4,

    /// <summary>
    /// A user-defined, non-commutative reduction (fold) over the group's
    /// surviving members, applied in ascending source-HLC order. The fold is
    /// supplied by an <see cref="ILatticeFoldProjection"/> (its
    /// <see cref="ILatticeFoldProjection.Initial"/> seed plus
    /// <see cref="ILatticeFoldProjection.Apply"/> step); the materialised group
    /// value is the resulting accumulator's opaque bytes. Because a general fold
    /// is not invertible, the maintainer re-folds the whole group whenever a
    /// member is added, retracted, or range-reconciled, so the value converges to
    /// a pure function of the group's converged member set regardless of delivery
    /// order.
    /// </summary>
    Fold = 5,
}
