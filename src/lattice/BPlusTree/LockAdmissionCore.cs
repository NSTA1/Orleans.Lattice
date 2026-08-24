namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The mutable, caller-owned state a single distributed lock reduces to for the
/// purpose of its safety decisions: the monotonic fencing counter, whether the
/// lock is currently held, the current holder's fencing token, and the absolute
/// UTC tick at which the current lease expires. It is a plain value the caller
/// holds and mutates in place through <see cref="LockAdmissionCore"/>, so the
/// core stays allocation-free and free of any Orleans, timer, or wall-clock
/// dependency.
/// </summary>
/// <remarks>
/// This struct, together with <see cref="LockAdmissionCore"/>, is the
/// <b>dependency-free correctness core</b> of the distributed lock's fencing and
/// mutual-exclusion decisions. It is the exact state and rule the production
/// <c>LatticeLockGrain</c> runs to gate every grant, renew, release, and
/// lease-reclamation, and it is also the artifact the Coyote concurrency model
/// drives under systematic schedule exploration - so the safety properties the
/// model proves (a stale-token holder can never dislodge the current holder, and
/// fencing tokens strictly increase across grants) are properties of the code
/// that actually runs, not of a parallel mimic that can drift. The production
/// grain persists the same four scalars in <c>LatticeLockState</c> so the fencing
/// counter and current-holder view survive reactivation.
/// </remarks>
internal struct LockCoreState
{
    /// <summary>
    /// The highest fencing token ever issued for this lock. Every
    /// <see cref="LockAdmissionCore.Grant"/> mints the strictly-greater successor
    /// and never decreases it, so a token is never reused across grants,
    /// activations, or crashes once the counter is persisted.
    /// </summary>
    public long FencingCounter;

    /// <summary><c>true</c> while the lock has a live holder.</summary>
    public bool IsHeld;

    /// <summary>
    /// The current holder's fencing token, or <see cref="LockAdmissionCore.NoToken"/>
    /// when the lock is free. A renew or release is honoured only for a token
    /// equal to this value (see <see cref="LockAdmissionCore.IsCurrentHolder"/>).
    /// </summary>
    public long HolderToken;

    /// <summary>
    /// The absolute UTC tick (<see cref="System.DateTimeOffset.UtcTicks"/>) at
    /// which the current lease expires. Meaningful only while
    /// <see cref="IsHeld"/> is <c>true</c>.
    /// </summary>
    public long LeaseExpiresAtTicks;
}

/// <summary>
/// The admission verdict for a fresh acquire attempt against the current lock
/// state: either the lock can be granted now (it is free, or the current lease
/// has expired and may be reclaimed) or the caller must wait behind the holder.
/// </summary>
internal enum LockAdmissionDecision : byte
{
    /// <summary>
    /// The lock has a live, unexpired holder; a fresh acquirer must queue rather
    /// than be granted.
    /// </summary>
    Hold,

    /// <summary>
    /// The lock is free (or its lease has expired and is reclaimable), so the
    /// head waiter may be granted a new lease with a freshly minted fencing
    /// token.
    /// </summary>
    Grant,
}

/// <summary>
/// The pure, deterministic decision core of the distributed lock grain: given
/// the lock's current <see cref="LockCoreState"/> and the current time, decide
/// whether a fresh acquire may be admitted, mint the next strictly-increasing
/// fencing token on a grant, validate a presented token on renew / release, and
/// reclaim an expired lease. Extracted so the production coordinator
/// (<c>LatticeLockGrain</c>) and the Coyote lock model share one rule with no
/// possibility of drift.
/// <para>
/// The whole core is a total, deterministic function of explicit inputs with no
/// <c>Task</c>/<c>await</c>, timers, wall-clock, <c>RequestContext</c>, or Orleans
/// types - the caller passes the current time in as a tick count and owns the
/// <see cref="LockCoreState"/> value - exactly like
/// <see cref="SagaCoordinatorCore"/>. Modelling the state as a caller-owned value
/// keeps every decision allocation-free on the grain's hot acquire / renew /
/// release path.
/// </para>
/// </summary>
/// <remarks>
/// The safety weight of the core lives in three rules. <see cref="Grant"/> mints
/// a fencing token strictly greater than every token previously issued for the
/// lock (fencing monotonicity), so a downstream resource can always reject a
/// writer bearing a stale token. <see cref="IsCurrentHolder"/> honours a renew or
/// release only for the token equal to the current holder's, so a presumed-dead
/// holder that was superseded (a GC pause or activation move that outlived its
/// lease) can never release or extend a lock now held by someone else - the
/// standard distributed-lock fencing guarantee. <see cref="Decide"/> admits a
/// fresh grant only when the lock is free or its lease has demonstrably expired,
/// so the lock never has two live holders (mutual exclusion).
/// </remarks>
internal static class LockAdmissionCore
{
    /// <summary>
    /// The sentinel fencing value used when the lock is free. No real grant ever
    /// carries it because <see cref="Grant"/> mints strictly-positive tokens
    /// starting at <c>1</c>, so a token equal to <see cref="NoToken"/> can never
    /// match the current holder in <see cref="IsCurrentHolder"/>.
    /// </summary>
    public const long NoToken = 0;

    /// <summary>
    /// Returns the strictly-greater successor of <paramref name="lastIssued"/> -
    /// the next fencing token. The sequence never decreases and never repeats,
    /// which is the load-bearing fencing property; the production grain persists
    /// <paramref name="lastIssued"/> across activations so monotonicity survives
    /// reactivation and crash recovery.
    /// </summary>
    /// <param name="lastIssued">The highest fencing token issued so far; must be non-negative.</param>
    /// <returns><paramref name="lastIssued"/> + 1.</returns>
    /// <exception cref="System.ArgumentOutOfRangeException"><paramref name="lastIssued"/> is negative.</exception>
    /// <exception cref="System.OverflowException">
    /// <paramref name="lastIssued"/> is <see cref="long.MaxValue"/>, so no strictly
    /// greater token exists. Unreachable in practice (issuing 2^63 grants).
    /// </exception>
    public static long NextFencingToken(long lastIssued)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(lastIssued);
        if (lastIssued == long.MaxValue)
        {
            throw new OverflowException(
                "The lock fencing-token counter reached long.MaxValue; no strictly greater token can be minted.");
        }

        return lastIssued + 1;
    }

    /// <summary>
    /// <c>true</c> when the lock is held and its lease has expired at
    /// <paramref name="nowTicks"/> (a reclaimable dead-holder lease). A free lock
    /// is never "expired" - it has no lease to reclaim.
    /// </summary>
    /// <param name="state">The current lock state.</param>
    /// <param name="nowTicks">The current absolute UTC tick.</param>
    public static bool IsLeaseExpired(in LockCoreState state, long nowTicks)
        => state.IsHeld && nowTicks >= state.LeaseExpiresAtTicks;

    /// <summary>
    /// <c>true</c> when <paramref name="presentedToken"/> identifies the current
    /// live holder - the lock is held and the token equals the holder's fencing
    /// token. This is the fencing check: a stale token (a superseded holder's) or
    /// the <see cref="NoToken"/> sentinel is rejected, so a presumed-dead holder
    /// can never renew or release a lock that has moved on.
    /// </summary>
    /// <param name="state">The current lock state.</param>
    /// <param name="presentedToken">The fencing token the caller presented.</param>
    public static bool IsCurrentHolder(in LockCoreState state, long presentedToken)
        => state.IsHeld && presentedToken != NoToken && presentedToken == state.HolderToken;

    /// <summary>
    /// The admission verdict for a fresh acquire attempt: <see cref="LockAdmissionDecision.Grant"/>
    /// when the lock is free or its lease has expired (and so is reclaimable),
    /// otherwise <see cref="LockAdmissionDecision.Hold"/>. This is the
    /// mutual-exclusion gate - the production grain only hands the lock to a
    /// waiter when this returns <see cref="LockAdmissionDecision.Grant"/>.
    /// </summary>
    /// <param name="state">The current lock state.</param>
    /// <param name="nowTicks">The current absolute UTC tick.</param>
    public static LockAdmissionDecision Decide(in LockCoreState state, long nowTicks)
        => !state.IsHeld || IsLeaseExpired(state, nowTicks)
            ? LockAdmissionDecision.Grant
            : LockAdmissionDecision.Hold;

    /// <summary>
    /// Grants the lock, mutating <paramref name="state"/> in place: mints the next
    /// strictly-greater fencing token, marks the lock held by it, and sets the
    /// lease to expire <paramref name="leaseTicks"/> after <paramref name="nowTicks"/>.
    /// The caller is responsible for only calling this when <see cref="Decide"/>
    /// returned <see cref="LockAdmissionDecision.Grant"/> (or after
    /// <see cref="ReclaimIfExpired"/>), which is what preserves mutual exclusion.
    /// </summary>
    /// <param name="state">The lock state to mutate.</param>
    /// <param name="nowTicks">The current absolute UTC tick the lease is measured from.</param>
    /// <param name="leaseTicks">The lease duration in ticks; must be non-negative.</param>
    /// <returns>The freshly minted fencing token now identifying the holder.</returns>
    /// <exception cref="System.ArgumentOutOfRangeException"><paramref name="leaseTicks"/> is negative.</exception>
    public static long Grant(ref LockCoreState state, long nowTicks, long leaseTicks)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(leaseTicks);
        var token = NextFencingToken(state.FencingCounter);
        state.FencingCounter = token;
        state.IsHeld = true;
        state.HolderToken = token;
        state.LeaseExpiresAtTicks = nowTicks + leaseTicks;
        return token;
    }

    /// <summary>
    /// Releases the lock on behalf of the caller presenting
    /// <paramref name="presentedToken"/>. A no-op returning <c>false</c> when the
    /// token is not the current holder's (a stale or already-superseded token),
    /// which is what makes release idempotent and prevents a dead holder from
    /// freeing a lock now held by someone else.
    /// </summary>
    /// <param name="state">The lock state to mutate.</param>
    /// <param name="presentedToken">The fencing token the caller presented.</param>
    /// <returns><c>true</c> if the caller was the current holder and the lock was freed; otherwise <c>false</c>.</returns>
    public static bool Release(ref LockCoreState state, long presentedToken)
    {
        if (!IsCurrentHolder(state, presentedToken))
        {
            return false;
        }

        state.IsHeld = false;
        state.HolderToken = NoToken;
        state.LeaseExpiresAtTicks = 0;
        return true;
    }

    /// <summary>
    /// Extends the current holder's lease to expire <paramref name="leaseTicks"/>
    /// after <paramref name="nowTicks"/>, but only when
    /// <paramref name="presentedToken"/> is the current holder's token. A stale
    /// token is rejected (returns <c>false</c>) and the lease is left untouched,
    /// so a superseded holder can never keep a lock alive.
    /// </summary>
    /// <param name="state">The lock state to mutate.</param>
    /// <param name="presentedToken">The fencing token the caller presented.</param>
    /// <param name="nowTicks">The current absolute UTC tick the extended lease is measured from.</param>
    /// <param name="leaseTicks">The new lease duration in ticks; must be non-negative.</param>
    /// <returns><c>true</c> if the caller was the current holder and the lease was extended; otherwise <c>false</c>.</returns>
    /// <exception cref="System.ArgumentOutOfRangeException"><paramref name="leaseTicks"/> is negative.</exception>
    public static bool Renew(ref LockCoreState state, long presentedToken, long nowTicks, long leaseTicks)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(leaseTicks);
        if (!IsCurrentHolder(state, presentedToken))
        {
            return false;
        }

        state.LeaseExpiresAtTicks = nowTicks + leaseTicks;
        return true;
    }

    /// <summary>
    /// Reclaims the lock when the current holder's lease has expired at
    /// <paramref name="nowTicks"/>, freeing it so the next waiter can be granted.
    /// A no-op returning <c>false</c> when the lock is free or the lease is still
    /// live. Reclamation preserves the fencing counter, so the next
    /// <see cref="Grant"/> still mints a strictly greater token than the reclaimed
    /// holder's.
    /// </summary>
    /// <param name="state">The lock state to mutate.</param>
    /// <param name="nowTicks">The current absolute UTC tick.</param>
    /// <returns><c>true</c> if an expired lease was reclaimed; otherwise <c>false</c>.</returns>
    public static bool ReclaimIfExpired(ref LockCoreState state, long nowTicks)
    {
        if (!IsLeaseExpired(state, nowTicks))
        {
            return false;
        }

        state.IsHeld = false;
        state.HolderToken = NoToken;
        state.LeaseExpiresAtTicks = 0;
        return true;
    }
}
