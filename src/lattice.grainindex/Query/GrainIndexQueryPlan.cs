namespace Orleans.Lattice.GrainIndex.Query;

/// <summary>
/// The lowered form of a user predicate: a union of conjunctions, produced once
/// per <c>Where</c> call and then read - never rebuilt - for every entry the
/// query touches.
/// </summary>
internal sealed class GrainIndexQueryPlan
{
    /// <summary>A plan that provably matches nothing, so no scan is issued at all.</summary>
    internal static GrainIndexQueryPlan Empty { get; } = new([]);

    internal GrainIndexQueryPlan(GrainIndexConjunction[] disjuncts) => Disjuncts = disjuncts;

    /// <summary>The conjunctions whose results are unioned.</summary>
    internal GrainIndexConjunction[] Disjuncts { get; }

    /// <summary>
    /// Whether the predicate was reduced to a contradiction, in which case the
    /// query completes without touching the tree.
    /// </summary>
    internal bool IsProvablyEmpty => Disjuncts.Length == 0;

    /// <summary>
    /// Whether the plan is a single clause over a single property. That is the
    /// shape that streams end to end: no de-duplication set and no candidate
    /// buffer, because one grain contributes exactly one entry per property and
    /// there is no second branch to union with.
    /// </summary>
    internal bool IsSingleScan => Disjuncts.Length == 1 && Disjuncts[0].Clauses.Length == 1;
}
