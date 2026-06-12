namespace Orleans.Lattice.Replication;

/// <summary>
/// The result of a scoped bootstrap-snapshot fallback pass: whether it attempted
/// a re-ship, how many localised ranges it scoped the snapshot to, how many
/// committed-projection entries it re-shipped, and - when it short-circuited -
/// why. This is an in-process result type and is not sent over the wire.
/// </summary>
public readonly record struct BootstrapFallbackOutcome
{
    /// <summary>
    /// <see langword="true"/> when the pass exported the scoped snapshot and
    /// invoked the re-ship sink. <see langword="false"/> when it short-circuited
    /// (no ranges to scope to, or the scoped export yielded no committed
    /// entries); inspect <see cref="SkipReason"/> for the cause.
    /// </summary>
    public bool Attempted { get; init; }

    /// <summary>The number of localised leaf ranges the pass scoped the snapshot to.</summary>
    public int RangesProcessed { get; init; }

    /// <summary>
    /// The number of committed-projection entries the pass re-shipped to the
    /// peer (zero when the re-ship was rejected by the transport, or when the
    /// pass was skipped).
    /// </summary>
    public int EntriesShipped { get; init; }

    /// <summary>
    /// Why the pass was skipped without re-shipping, or
    /// <see cref="BootstrapFallbackSkipReason.None"/> when it ran.
    /// </summary>
    public BootstrapFallbackSkipReason SkipReason { get; init; }

    /// <summary>An outcome denoting the pass did not run and re-shipped nothing.</summary>
    public static BootstrapFallbackOutcome NotAttempted => default;
}
