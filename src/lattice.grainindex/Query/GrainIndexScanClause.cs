namespace Orleans.Lattice.GrainIndex.Query;

/// <summary>
/// One scan the executor performs: a single projected property, the ordinal key
/// ranges its comparisons narrowed to, and the residual predicate the server
/// applies to each entry inside those ranges.
/// <para>
/// A clause never spans two properties, because an index entry carries exactly
/// one. Conjunctions across properties are resolved by intersecting the grain
/// keys of several clauses, not by one wider predicate.
/// </para>
/// </summary>
internal sealed class GrainIndexScanClause
{
    internal GrainIndexScanClause(
        GrainIndexQueryProperty property,
        GrainIndexKeyRange[] ranges,
        LatticePredicateNode? residual,
        int selectivity)
    {
        Property = property;
        Ranges = ranges;
        Residual = residual;
        Selectivity = selectivity;
    }

    /// <summary>The property whose key range this clause scans.</summary>
    internal GrainIndexQueryProperty Property { get; }

    /// <summary>The ordinal key ranges to scan, ascending and disjoint.</summary>
    internal GrainIndexKeyRange[] Ranges { get; }

    /// <summary>
    /// The predicate the tree applies to every entry in range, or <c>null</c>
    /// when the ranges alone are exact and no server-side filtering is needed.
    /// </summary>
    internal LatticePredicateNode? Residual { get; }

    /// <summary>
    /// A lower-is-better selectivity estimate: <c>0</c> for a point lookup,
    /// <c>1</c> for a range bounded at both ends, <c>2</c> for a half-open range,
    /// and <c>3</c> for a whole-property scan. Clauses in a conjunction run in
    /// this order, so the narrowest scan seeds the candidate set.
    /// </summary>
    internal int Selectivity { get; }
}
