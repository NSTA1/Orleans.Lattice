namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for <see cref="Grains.LatticeLockGrain"/> - the durable view
/// of one distributed lock, keyed by the lock name. Tracks the monotonic fencing
/// counter, the current holder (if any), and the current lease expiry so that a
/// reactivation after a silo restart or deactivation resumes a consistent view:
/// the same current holder, the same lease deadline, and a fencing counter that
/// never rewinds.
/// <para>
/// <b>What is deliberately not persisted.</b> The in-memory FIFO queue of
/// waiters (each backed by a <see cref="System.Threading.Tasks.TaskCompletionSource{TResult}"/>)
/// is transient and legitimately lost on deactivation - a queued acquirer's
/// <c>Task</c> cannot survive a process boundary, so on reactivation the queue is
/// empty and any in-flight <c>AcquireAsync</c> callers observe their wait-timeout
/// (or their grain call faults) and retry. Only the granted-lease invariants
/// (holder, fencing token, expiry) are durable; the waiting side is
/// reconstructed by callers retrying.
/// </para>
/// Key format: <c>{lockName}</c>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeLockState)]
internal sealed class LatticeLockState
{
    /// <summary>
    /// The highest fencing token ever issued for this lock. Persisted so the
    /// monotonic-fencing guarantee survives reactivation and crash recovery: the
    /// next grant after a reactivation still mints a strictly greater token than
    /// any previously handed out, even though the in-memory
    /// <c>LockCoreState.FencingCounter</c> was lost. Defaults to <c>0</c> (no
    /// token issued yet); the first grant mints <c>1</c>.
    /// </summary>
    [Id(0)] public long FencingCounter { get; set; }

    /// <summary><c>true</c> while the lock has a live holder.</summary>
    [Id(1)] public bool IsHeld { get; set; }

    /// <summary>
    /// The current holder's fencing token, or <c>0</c> when the lock is free.
    /// A renew or release is honoured only for a token equal to this value.
    /// </summary>
    [Id(2)] public long HolderToken { get; set; }

    /// <summary>
    /// The absolute UTC tick (<see cref="System.DateTimeOffset.UtcTicks"/>) at
    /// which the current lease expires. Meaningful only while <see cref="IsHeld"/>
    /// is <c>true</c>; <c>0</c> when the lock is free.
    /// </summary>
    [Id(3)] public long LeaseExpiresAtTicks { get; set; }

    /// <summary>
    /// The current holder's lease duration in ticks, captured on the grant that
    /// installed the holder. Persisted so a reminder-driven reactivation can
    /// re-arm the lease-expiry backstop with the same duration the holder was
    /// granted. <c>0</c> when the lock is free.
    /// </summary>
    [Id(4)] public long LeaseDurationTicks { get; set; }
}
