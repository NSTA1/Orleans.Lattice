namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Durable receive fence on one <b>physical</b> tree copy, keyed by its physical
/// tree id (issue #4593). A coordinated restore closes its restored copy before
/// the alias swap makes it routable, and opens it only when the saga's fence
/// lifts. While the copy is closed, a replication apply that routes to it is
/// refused with <see cref="CopyReceiveFencedException"/>, and the replication
/// applier defers the entry so the sender re-ships it.
/// <para>
/// The close also records the copy's minimum admission epoch: the receive-fence
/// epoch of the pause the restore took before the copy became routable. It
/// survives the open. An apply admitted under an older epoch (see
/// <see cref="ReplicationAdmissionEpoch"/>) was admitted before that pause, so it
/// carries a pre-cutover write, and it is refused even after the copy opens.
/// </para>
/// <para>
/// A restored copy is born closed and is opened once, so a copy only ever moves
/// from closed to open, and its minimum admission epoch never changes once it is
/// open. That is what lets a routing activation cache an open copy's status and
/// re-read only a closed one. A copy that was never closed holds no state, reads
/// as open, and has a minimum admission epoch of zero.
/// </para>
/// </summary>
[Alias(TypeAliases.ICopyReceiveFenceGrain)]
internal interface ICopyReceiveFenceGrain : IGrainWithStringKey
{
    /// <summary>
    /// Closes the copy for <paramref name="sagaId"/>, durably, with
    /// <paramref name="minAdmissionEpoch"/> as its minimum admission epoch.
    /// Idempotent for the same saga. A close by a different saga takes over
    /// ownership, so only that saga's <see cref="OpenAsync"/> opens the copy. The
    /// minimum admission epoch only ever rises.
    /// </summary>
    /// <param name="sagaId">The restore saga that owns the restored copy.</param>
    /// <param name="minAdmissionEpoch">The receive-fence epoch of the pause the restore took.</param>
    Task CloseAsync(string sagaId, long minAdmissionEpoch);

    /// <summary>
    /// Opens the copy if <paramref name="sagaId"/> owns the close. Keeps the
    /// minimum admission epoch. A no-op when the copy is open or another saga
    /// owns the close.
    /// </summary>
    /// <param name="sagaId">The restore saga that closed the copy.</param>
    Task OpenAsync(string sagaId);

    /// <summary>Returns whether the copy is closed, and its minimum admission epoch.</summary>
    [Orleans.Concurrency.AlwaysInterleave]
    Task<CopyReceiveFenceStatus> GetStatusAsync();
}