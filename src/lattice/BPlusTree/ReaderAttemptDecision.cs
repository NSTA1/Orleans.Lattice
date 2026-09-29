namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// What a multi-key read does with one fan-out attempt, as decided by
/// <see cref="ReaderStabilityGate.Decide"/> (issue #3641).
/// </summary>
internal enum ReaderAttemptDecision
{
    /// <summary>Return the attempt's result: it cannot be torn.</summary>
    Accept,

    /// <summary>
    /// Discard the attempt's result and retry, within
    /// <see cref="LatticeOptions.MaxScanRetries"/>.
    /// </summary>
    Retry,
}
