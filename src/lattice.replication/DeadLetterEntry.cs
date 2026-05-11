using Orleans.Lattice.BPlusTree.Grains;
namespace Orleans.Lattice.Replication;

/// <summary>
/// A single <see cref="WalRecord"/> the inbound apply pipeline could
/// not install after exhausting
/// <see cref="LatticeReplicationOptions.MaxApplyRetries"/> consecutive
/// retries, parked on the per-tree dead-letter queue together with the
/// failure diagnostics needed to triage and (optionally) replay it.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.DeadLetterEntry)]
[Immutable]
public readonly record struct DeadLetterEntry
{
    /// <summary>
    /// Monotonic per-tree identifier assigned at enqueue time. Stable
    /// for the lifetime of the parked entry so operators can reference
    /// it from inspection / replay calls.
    /// </summary>
    [Id(0)]
    public long EntryId { get; init; }

    /// <summary>The replicated entry that failed to apply.</summary>
    [Id(1)]
    public WalRecord Entry { get; init; }

    /// <summary>
    /// Human-readable description of the terminal failure, typically the
    /// <see cref="Exception.Message"/> of the last apply attempt's
    /// exception. Carried verbatim into operator tooling for triage.
    /// </summary>
    [Id(2)]
    public string FailureReason { get; init; }

    /// <summary>
    /// Number of consecutive failed apply attempts the pipeline made
    /// against this entry before parking it. Equal to
    /// <see cref="LatticeReplicationOptions.MaxApplyRetries"/> on the
    /// canonical path; lower values indicate an enqueue forced by an
    /// out-of-band caller.
    /// </summary>
    [Id(3)]
    public int RetryCount { get; init; }

    /// <summary>
    /// UTC ticks captured at enqueue time
    /// (<see cref="DateTime.UtcNow"/>.<see cref="DateTime.Ticks"/>).
    /// Used by inspection tooling to age-out stale entries.
    /// </summary>
    [Id(4)]
    public long EnqueuedAtTicks { get; init; }
}
