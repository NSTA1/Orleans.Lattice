namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The reader-side verdict on whether a multi-key read computed under a
/// registry decision snapshot is still authoritative (issue #3641). Produced by
/// <see cref="ReaderStabilityGate.ClassifySnapshot"/> and by the production
/// post-fan-out probe, and combined with the "did any leaf resolve a prepared
/// key" signal by <see cref="ReaderStabilityGate.Decide"/>.
/// </summary>
internal enum ReaderStabilityVerdict
{
    /// <summary>
    /// No saga transitioned <see cref="TxStatus.InFlight"/> to
    /// <see cref="TxStatus.Committed"/> during the fan-out; the read is
    /// consistent.
    /// </summary>
    Stable,

    /// <summary>
    /// A saga committed during the fan-out; the read may be torn and must be
    /// retried under a fresh snapshot.
    /// </summary>
    Unstable,

    /// <summary>
    /// Stability could not be established: the registry could not be reached
    /// for the pre-fan-out snapshot, the revision probe, or the disambiguation
    /// snapshot. Safe to accept only when no key of the read depended on a saga
    /// decision.
    /// </summary>
    Unverifiable,
}
