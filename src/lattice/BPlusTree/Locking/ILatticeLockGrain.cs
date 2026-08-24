namespace Orleans.Lattice;

/// <summary>
/// A cluster-wide, FIFO-fair distributed lock / lease, keyed by lock name. Because
/// an Orleans grain activation is single-threaded and processes its inbox in
/// arrival order, a grain keyed by a lock name is a natural FIFO mutual-exclusion
/// point; this interface packages that pattern as a first-class primitive that
/// also gets the failure-mode details right - bounded leases so a crashed holder
/// cannot wedge the lock forever, and monotonic fencing tokens so a superseded
/// holder is detectable by the resource it guards.
/// <para>
/// Resolve a lock by name through the grain factory, for example
/// <c>grainFactory.GetGrain&lt;ILatticeLockGrain&gt;("inventory/sku-42")</c>. All
/// callers naming the same lock contend for the same activation and are serialized
/// FIFO.
/// </para>
/// <para>
/// <b>Fencing.</b> Every grant carries a strictly-increasing
/// <see cref="LockToken.FencingToken"/> that is never reused or decreased, even
/// across activations and crashes. Forward that token to any resource the lock
/// guards so the resource can reject a write from a holder that was presumed dead
/// and superseded (a GC pause or activation move that outlived its lease). This is
/// the load-bearing correctness property of a distributed lock.
/// </para>
/// <para>
/// This grain provides mutual exclusion only. It touches no tree, WAL, or
/// atomic-write saga, captures no pre-image, and offers no rollback.
/// </para>
/// </summary>
[Alias(TypeAliases.ILatticeLockGrain)]
public interface ILatticeLockGrain : IGrainWithStringKey
{
    /// <summary>
    /// Acquires the lock, waiting in the FIFO queue if it is currently held.
    /// Returns the granted <see cref="LockLease"/> when the caller reaches the head
    /// of the queue and the lock is free, or faults with a
    /// <see cref="TimeoutException"/> if <see cref="LockAcquireRequest.MaxWait"/>
    /// elapses first (the caller is then removed from the queue and waiters behind
    /// it are unaffected). The call never blocks the grain's activation turn: a
    /// contended caller is enqueued and its task completes from a later turn (a
    /// release, a lease expiry, or its own wait-timeout).
    /// </summary>
    /// <param name="request">The lease duration to grant and the maximum FIFO wait.</param>
    /// <returns>The granted lease, including the fencing token to present on renew / release.</returns>
    /// <exception cref="TimeoutException">
    /// The lock was not granted before <see cref="LockAcquireRequest.MaxWait"/> elapsed.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="LockAcquireRequest.MaxWait"/> is negative. A non-positive
    /// <see cref="LockAcquireRequest.LeaseDuration"/> is not rejected; it defaults to
    /// <see cref="LatticeOptions.DefaultLockLeaseDuration"/>.
    /// </exception>
    Task<LockLease> AcquireAsync(LockAcquireRequest request);

    /// <summary>
    /// Attempts to acquire the lock without queuing: returns the granted
    /// <see cref="LockLease"/> if the lock is free (or its lease has expired and is
    /// reclaimable) at the moment of the call, or <see langword="null"/> if it is
    /// currently held. Never waits and never enqueues the caller.
    /// </summary>
    /// <param name="leaseDuration">
    /// The lease duration to grant on success. A non-positive value defaults to
    /// <see cref="LatticeOptions.DefaultLockLeaseDuration"/>; any value is capped at
    /// <see cref="LatticeOptions.MaxLockLeaseDuration"/>.
    /// </param>
    /// <returns>The granted lease, or <see langword="null"/> under contention.</returns>
    Task<LockLease?> TryAcquireAsync(TimeSpan leaseDuration);

    /// <summary>
    /// Extends the current holder's lease by <paramref name="leaseDuration"/> from
    /// now, provided <paramref name="token"/> is the current holder's fencing
    /// token. Returns the updated lease (same fencing token, later expiry).
    /// </summary>
    /// <param name="token">The fencing token from the holder's <see cref="LockLease"/>.</param>
    /// <param name="leaseDuration">
    /// The new lease duration to extend to. A non-positive value defaults to
    /// <see cref="LatticeOptions.DefaultLockLeaseDuration"/>; any value is capped at
    /// <see cref="LatticeOptions.MaxLockLeaseDuration"/>.
    /// </param>
    /// <returns>The renewed lease.</returns>
    /// <exception cref="LatticeLockConflictException">
    /// <paramref name="token"/> is stale - the lease was already superseded (expired
    /// and reclaimed, then re-granted) or never held the lock.
    /// </exception>
    Task<LockLease> RenewAsync(LockToken token, TimeSpan leaseDuration);

    /// <summary>
    /// Releases the lock held under <paramref name="token"/> and grants it to the
    /// next FIFO waiter (if any). Idempotent: a release with a stale token - one
    /// that is not the current holder's - is a silent no-op and does not disturb
    /// the current holder.
    /// </summary>
    /// <param name="token">The fencing token from the holder's <see cref="LockLease"/>.</param>
    Task ReleaseAsync(LockToken token);

    /// <summary>
    /// Returns a point-in-time <see cref="LockStatus"/> snapshot of the lock -
    /// whether it is held, the current fencing token, the lease expiry, and the
    /// FIFO queue depth - for observability and tests. Not authoritative for
    /// acquire / release decisions; only the fencing token from an actual grant is.
    /// </summary>
    /// <returns>The current lock status.</returns>
    Task<LockStatus> GetStatusAsync();
}
