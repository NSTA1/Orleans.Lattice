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
/// </summary>
[Alias(ReplicationTypeAliases.ITreeReceiveFenceGrain)]
internal interface ITreeReceiveFenceGrain : IGrainWithStringKey
{
    /// <summary>
    /// Durably pauses inbound apply for the tree under
    /// <paramref name="sagaId"/>. Idempotent for the same saga; a different
    /// saga id takes over the pause (the newest engaging saga owns it).
    /// </summary>
    /// <param name="sagaId">Engaging saga id. Must be non-empty.</param>
    Task PauseAsync(string sagaId);

    /// <summary>
    /// Resumes inbound apply if <paramref name="sagaId"/> currently owns the
    /// pause. A resume for a non-owning saga is a no-op so a late resume from a
    /// superseded saga cannot unpause the tree.
    /// </summary>
    /// <param name="sagaId">Saga id lifting the pause. Must be non-empty.</param>
    Task ResumeAsync(string sagaId);

    /// <summary>
    /// Returns <see langword="true"/> while inbound apply for the tree is
    /// paused.
    /// </summary>
    [Orleans.Concurrency.AlwaysInterleave]
    Task<bool> IsPausedAsync();
}
