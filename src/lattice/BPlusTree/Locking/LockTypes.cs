namespace Orleans.Lattice;

/// <summary>
/// The fencing credential a lock holder presents to renew or release its lease.
/// It carries the strictly-increasing <see cref="FencingToken"/> minted when the
/// lease was granted; the lock grain honours a renew or release only for the
/// token that matches its current holder, so a superseded holder (one whose lease
/// expired and was reclaimed while it was paused) is rejected. The same token
/// value should be forwarded to any downstream resource the holder guards, so
/// that resource can reject a write bearing a stale (lower) token - the standard
/// distributed-lock fencing guarantee.
/// </summary>
/// <param name="FencingToken">
/// The monotonically increasing fencing token for this grant. Strictly greater
/// than the token of every prior grant of the same lock, and never reused, even
/// across activations and crashes.
/// </param>
[Immutable]
[GenerateSerializer]
[Alias(TypeAliases.LockToken)]
public readonly record struct LockToken(
    [property: Id(0)] long FencingToken);

/// <summary>
/// A granted distributed-lock lease: the fencing <see cref="Token"/> the holder
/// must present to renew or release, the absolute time the lease expires if
/// neither happens, and the lease duration that produced that expiry. A holder
/// that neither renews before <see cref="ExpiresAt"/> nor releases has its lease
/// reclaimed and handed to the next waiter, so a crashed holder cannot wedge the
/// lock forever.
/// </summary>
/// <param name="Token">The fencing credential identifying this holder.</param>
/// <param name="ExpiresAt">
/// The absolute UTC instant at which the lease expires unless renewed or
/// released.
/// </param>
/// <param name="LeaseDuration">
/// The duration the lease was granted for; a renew extends the lease by this
/// amount from the renew instant.
/// </param>
[Immutable]
[GenerateSerializer]
[Alias(TypeAliases.LockLease)]
public readonly record struct LockLease(
    [property: Id(0)] LockToken Token,
    [property: Id(1)] DateTimeOffset ExpiresAt,
    [property: Id(2)] TimeSpan LeaseDuration);

/// <summary>
/// The parameters of a FIFO <see cref="ILatticeLockGrain.AcquireAsync"/> attempt:
/// how long the granted lease should last, and how long the caller is willing to
/// wait in the FIFO queue for the lock to become available before giving up with
/// a <see cref="TimeoutException"/>.
/// </summary>
/// <param name="LeaseDuration">
/// The lease duration to grant once the caller reaches the head of the queue and
/// the lock is free. A non-positive value defaults to
/// <see cref="LatticeOptions.DefaultLockLeaseDuration"/>, and any value is capped
/// at <see cref="LatticeOptions.MaxLockLeaseDuration"/>. Orleans reminder
/// granularity means the durable lease-expiry backstop is minute-grained;
/// sub-minute leases still work but are reclaimed on the finer in-activation timer
/// while the grain stays activated.
/// </param>
/// <param name="MaxWait">
/// The maximum time to wait in the FIFO queue for the lock. When it elapses
/// before the caller is granted the lock, the acquire faults with a
/// <see cref="TimeoutException"/> and the caller is removed from the queue.
/// <see cref="TimeSpan.Zero"/> requests a non-blocking attempt (equivalent to
/// <see cref="ILatticeLockGrain.TryAcquireAsync"/>).
/// </param>
[Immutable]
[GenerateSerializer]
[Alias(TypeAliases.LockAcquireRequest)]
public readonly record struct LockAcquireRequest(
    [property: Id(0)] TimeSpan LeaseDuration,
    [property: Id(1)] TimeSpan MaxWait);

/// <summary>
/// An observability snapshot of a distributed lock: whether it is currently held,
/// the current holder's fencing token, when the current lease expires, and how
/// many callers are waiting in the FIFO queue. Returned by
/// <see cref="ILatticeLockGrain.GetStatusAsync"/> for diagnostics and tests; it
/// is a point-in-time read and must not be used to make an acquire / release
/// decision (only the fencing token from an actual grant is authoritative).
/// </summary>
/// <param name="IsHeld"><c>true</c> when the lock currently has a live holder.</param>
/// <param name="CurrentFencingToken">
/// The current holder's fencing token, or <c>0</c> when the lock is free.
/// </param>
/// <param name="LeaseExpiresAt">
/// The absolute UTC instant the current lease expires, or <see langword="null"/>
/// when the lock is free.
/// </param>
/// <param name="QueueDepth">
/// The number of callers currently waiting in the FIFO queue for the lock (not
/// counting the holder). Transient - it reflects in-memory waiters on the current
/// activation only.
/// </param>
[Immutable]
[GenerateSerializer]
[Alias(TypeAliases.LockStatus)]
public readonly record struct LockStatus(
    [property: Id(0)] bool IsHeld,
    [property: Id(1)] long CurrentFencingToken,
    [property: Id(2)] DateTimeOffset? LeaseExpiresAt,
    [property: Id(3)] int QueueDepth);
