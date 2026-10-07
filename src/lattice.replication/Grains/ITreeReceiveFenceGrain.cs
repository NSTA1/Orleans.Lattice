namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-tree durable gate on <b>inbound</b> replication apply, keyed by tree id.
/// While paused, peer entries for the tree are not admitted so no laggard's
/// post-cut entries are union-merged into the local tree during a cross-cluster
/// restore saga.
/// <para>
/// This is the receive-side counterpart to the shipper's durable administrative
/// pause. Both stay engaged on every participant until the saga globally
/// completes; only then does the fence primitive resume shipping and receiving
/// together.
/// </para>
/// <para>
/// The fence keeps an epoch that every new pause bumps (issue #4593). An apply
/// is admitted under the epoch its admitting observation read, and a restored
/// copy refuses one admitted under an epoch older than its restore's pause.
/// </para>
/// </summary>
[Alias(ReplicationTypeAliases.ITreeReceiveFenceGrain)]
internal interface ITreeReceiveFenceGrain : IGrainWithStringKey
{
    /// <summary>
    /// Durably pauses inbound apply for the tree under
    /// <paramref name="sagaId"/> and returns the fence's epoch. A new pause -
    /// from unpaused, or a different saga taking over - bumps the epoch;
    /// re-pausing under the owning saga is idempotent and returns the epoch
    /// unchanged.
    /// </summary>
    /// <param name="sagaId">Engaging saga id. Must be non-empty.</param>
    /// <returns>The fence's epoch after the pause.</returns>
    Task<long> PauseAsync(string sagaId);

    /// <summary>
    /// Resumes inbound apply if <paramref name="sagaId"/> currently owns the
    /// pause. A resume for a non-owning saga is a no-op so a late resume from a
    /// superseded saga cannot unpause the tree. The epoch is unchanged.
    /// </summary>
    /// <param name="sagaId">Saga id lifting the pause. Must be non-empty.</param>
    Task ResumeAsync(string sagaId);

    /// <summary>
    /// Returns <see langword="true"/> while inbound apply for the tree is
    /// paused.
    /// </summary>
    [Orleans.Concurrency.AlwaysInterleave]
    Task<bool> IsPausedAsync();

    /// <summary>Returns whether inbound apply is paused, and the fence's epoch.</summary>
    [Orleans.Concurrency.AlwaysInterleave]
    Task<ReceiveFenceObservation> ObserveAsync();
}