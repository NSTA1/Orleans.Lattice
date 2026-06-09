namespace Orleans.Lattice;

/// <summary>
/// Terminal result of a guarded atomic multi-key write
/// (<c>SetManyAtomicAsync&lt;T&gt;</c> with a predicate). Reported without an
/// exception so callers can branch on a precondition miss as ordinary control
/// flow rather than catching one.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AtomicWriteOutcome)]
public enum AtomicWriteOutcome
{
    /// <summary>
    /// Every targeted key's pre-saga value satisfied the guard predicate, so
    /// the whole batch was committed atomically.
    /// </summary>
    Committed = 0,

    /// <summary>
    /// At least one targeted key's pre-saga value failed the guard predicate
    /// (or the key had no live pre-saga value), so the saga aborted and
    /// committed nothing.
    /// </summary>
    PreconditionFailed = 1,
}
