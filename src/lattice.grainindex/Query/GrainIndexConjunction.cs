namespace Orleans.Lattice.GrainIndex.Query;

/// <summary>
/// One conjunction of the planned predicate: the clauses whose grain keys must
/// all match, ordered most selective first so the narrowest scan seeds the
/// candidate set and every later clause only shrinks it.
/// </summary>
internal sealed class GrainIndexConjunction
{
    internal GrainIndexConjunction(GrainIndexScanClause[] clauses) => Clauses = clauses;

    /// <summary>The clauses to intersect, ordered most selective first.</summary>
    internal GrainIndexScanClause[] Clauses { get; }
}
