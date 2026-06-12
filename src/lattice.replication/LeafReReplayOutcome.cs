namespace Orleans.Lattice.Replication;

/// <summary>
/// The result of a targeted leaf re-replay repair pass: whether it attempted a
/// re-ship, how many localised ranges it processed, how many write-ahead-log
/// entries it re-shipped, and - when it short-circuited - why. This is an
/// in-process result type and is not sent over the wire.
/// </summary>
public readonly record struct LeafReReplayOutcome
{
    /// <summary>
    /// <see langword="true"/> when the pass selected candidate entries and
    /// invoked the re-ship sink. <see langword="false"/> when it short-circuited
    /// (no candidates in range, or the local WAL was trimmed past the
    /// divergence point); inspect <see cref="SkipReason"/> for the cause.
    /// </summary>
    public bool Attempted { get; init; }

    /// <summary>The number of localised leaf ranges the pass considered.</summary>
    public int RangesProcessed { get; init; }

    /// <summary>
    /// The number of write-ahead-log entries the pass re-shipped to the peer
    /// (zero when the re-ship was rejected by the transport, or when the pass
    /// was skipped).
    /// </summary>
    public int EntriesReReplayed { get; init; }

    /// <summary>
    /// Why the pass was skipped without re-shipping, or
    /// <see cref="LeafReReplaySkipReason.None"/> when it ran.
    /// </summary>
    public LeafReReplaySkipReason SkipReason { get; init; }

    /// <summary>An outcome denoting the pass did not run and re-shipped nothing.</summary>
    public static LeafReReplayOutcome NotAttempted => default;
}
