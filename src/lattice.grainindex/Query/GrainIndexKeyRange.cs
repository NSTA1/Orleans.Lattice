namespace Orleans.Lattice.GrainIndex.Query;

/// <summary>
/// A half-open ordinal key range <c>[StartInclusive, EndExclusive)</c> over an
/// index tree, handed straight to the range arguments of the core scan and
/// cursor surface.
/// </summary>
/// <param name="StartInclusive">The inclusive lower bound.</param>
/// <param name="EndExclusive">The exclusive upper bound.</param>
internal readonly record struct GrainIndexKeyRange(string StartInclusive, string EndExclusive)
{
    /// <summary>
    /// Reports whether the range can never contain a key, which lets the planner
    /// drop an unsatisfiable clause before it reaches the tree.
    /// </summary>
    internal bool IsEmpty => string.CompareOrdinal(StartInclusive, EndExclusive) >= 0;
}
