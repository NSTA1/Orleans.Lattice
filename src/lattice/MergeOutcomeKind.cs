namespace Orleans.Lattice;

/// <summary>
/// The disposition an <see cref="ILatticeMergeObserver"/> chooses for a
/// completed CRDT / LWW merge. There is deliberately no hard-reject member: a
/// lock-free merge has already been applied and cannot be rolled back, so the
/// observer may only accept, accept-with-a-transformed-value, or
/// accept-and-annotate.
/// </summary>
public enum MergeOutcomeKind
{
    /// <summary>
    /// Accept the merged bytes verbatim - the default, zero-cost disposition.
    /// The canonical merged value stored by the grain is unchanged.
    /// </summary>
    Accept = 0,

    /// <summary>
    /// Accept, but replace the canonical merged bytes with the
    /// observer-supplied value (normalisation / re-encoding). Permitted
    /// <b>only</b> for <see cref="LatticeMergeMode.LwwRegister"/> records:
    /// rewriting the canonical merged bytes of a typed CRDT record would break
    /// WAL-replay determinism, so a merge observer that returns this kind for a
    /// non-LWW record faults with <see cref="System.InvalidOperationException"/>.
    /// </summary>
    AcceptTransformed = 1,

    /// <summary>
    /// Accept the merged bytes verbatim and additionally surface a
    /// non-mutating annotation (for example a validation-warning reason the
    /// host can log or emit as an event). The canonical merged value is
    /// unchanged.
    /// </summary>
    AcceptWithEvent = 2,
}
