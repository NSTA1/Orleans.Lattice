namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// What the index registry does at silo start when a declaration has drifted on
/// a field <see cref="GrainIndexDriftClassification"/> classifies as
/// drift-breaking. Configured per index through
/// <see cref="GrainIndexOptions.DriftPolicy"/>.
/// </summary>
/// <remarks>
/// A drift-safe change is not affected by this setting: it always refreshes the
/// stored record and logs, whichever policy is selected.
/// </remarks>
public enum GrainIndexDriftPolicy
{
    /// <summary>
    /// Reject the change: silo start fails with a
    /// <see cref="GrainIndexConfigurationDriftException"/> naming the index and
    /// the fields that drifted.
    /// <para>
    /// The default, because the alternative to failing loudly is serving queries
    /// from an index whose stored entries no longer match the declaration
    /// reading them - a silently wrong answer rather than an error.
    /// </para>
    /// </summary>
    Reject = 0,

    /// <summary>
    /// Accept the change and mark the index as needing a backfill rebuild: the
    /// stored record is updated to the new declaration with its
    /// needs-backfill flag raised, and silo start proceeds.
    /// <para>
    /// The opt-in for a deployment that would rather rebuild the index than
    /// block a rollout. Until the rebuild completes the index is incomplete, so
    /// queries against it can under-report.
    /// </para>
    /// </summary>
    Rebuild = 1,
}
