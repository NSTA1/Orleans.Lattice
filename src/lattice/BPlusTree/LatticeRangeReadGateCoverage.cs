namespace Orleans.Lattice;

/// <summary>
/// How much of a requested key range the read-path access gate admitted, reported
/// so a caller can tell an <em>empty</em> range read from a <em>restricted</em>
/// one. Returned by <see cref="ILattice.GetRangeReadGateCoverageAsync"/>.
/// </summary>
/// <remarks>
/// <para>
/// This type exists because the gated range-read surface reports denial and
/// emptiness identically. A denied <b>point</b> read throws, but a denied
/// <b>range</b> read resolves to a reject-all key filter, which yields a clean,
/// successful, empty result: no exception, no log, every instrument healthy. A
/// caller that reads "no rows" as "the store is empty" therefore treats an
/// authorization outcome as a fact about the data.
/// </para>
/// <para>
/// That is not hypothetical. It is the shared mechanism behind issues #2277,
/// #2252/#2407, #2406 and #2480, each of which cost hours precisely because an
/// empty result was read as a factual negative. Issue #2423 audits the class;
/// this enum is the observable difference the audit calls for.
/// </para>
/// <para>
/// A <b>coverage classification</b> and never the admitted keys or the gate's
/// reason. Naming the excluded keys would turn any range read into an
/// authorization oracle, which is the same disclosure argument that keeps
/// <see cref="GatedMultiReadResult.PrunedByAccessGate"/> a count.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.LatticeRangeReadGateCoverage)]
public enum LatticeRangeReadGateCoverage
{
    /// <summary>
    /// The gate admits the whole requested range, so no key was withheld. An
    /// empty range read is a genuine absence and a caller may safely conclude
    /// the range holds no live entries. Also reported on the default ungated
    /// path and inside a system-origin turn, where no gate applies.
    /// </summary>
    Unrestricted = 0,

    /// <summary>
    /// The gate allowed the range but narrowed it with a per-key filter, so an
    /// unknown subset of keys is withheld. An empty result is uninterpretable:
    /// the caller must not conclude the range is empty, because every live entry
    /// in it may simply be hidden.
    /// </summary>
    Filtered = 1,

    /// <summary>
    /// The gate denied the range outright, so every key is withheld and the read
    /// necessarily returns nothing. An empty result carries no information about
    /// the store at all. A caller that classifies on absence here is guaranteed
    /// to be wrong whenever the range is non-empty.
    /// </summary>
    Denied = 2,
}
