using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The FIFO-fair distributed lock / lease grain behind
/// <see cref="ILatticeLockGrain"/>. One activation per lock name serializes all
/// contending callers through the single-threaded grain turn, and the safety
/// decisions (fencing-token monotonicity, mutual exclusion, lease reclamation)
/// are delegated to the pure <see cref="LockAdmissionCore"/> so the exact rule the
/// grain runs is the one the Coyote model checks.
/// <para>
/// <b>Non-blocking acquire.</b> The grain is <see cref="ReentrantAttribute">reentrant</see>
/// and a contended <see cref="AcquireAsync"/> parks on a per-waiter
/// <see cref="TaskCompletionSource{TResult}"/> that is completed from a <i>later</i>
/// turn - a release, a lease-expiry timer, or the caller's own wait-timeout. A
/// non-reentrant grain would deadlock here: the parked acquire's returned task
/// would block the activation from ever processing the release that completes it.
/// Reentrancy lets the release turn run while the acquire is parked; all state
/// mutation and persistence is serialized through an in-grain gate
/// (<see cref="_gate"/>) so the reentrant interleavings never overlap a
/// <c>WriteStateAsync</c> or corrupt the queue.
/// </para>
/// <para>
/// <b>Durability.</b> The fencing counter, current holder token, and lease expiry
/// are persisted in <see cref="LatticeLockState"/> after every transition, so a
/// reactivation resumes a consistent view and the fencing sequence never rewinds.
/// The in-memory waiter queue is deliberately transient: a queued acquirer's task
/// cannot cross a process boundary, so on deactivation waiters observe their
/// wait-timeout (or a faulted call) and retry. A minute-grained keepalive reminder
/// is registered while the lock is held so a crashed holder's lease is reclaimed
/// even with no live acquirer to drive it; a finer in-activation timer drives
/// sub-minute lease expiry while the grain stays activated.
/// </para>
/// </summary>
[Reentrant]
internal sealed class LatticeLockGrain(
    IGrainContext context,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<LatticeLockGrain> logger,
    [PersistentState("lattice-lock", LatticeOptions.StorageProviderName)]
    IPersistentState<LatticeLockState> state)
    : TtlGrain<LatticeLockGrain>(context, reminderRegistry, logger), ILatticeLockGrain
{
    /// <summary>
    /// Retention reminder name required by <see cref="TtlGrain{TSelf}"/>. The lock
    /// grain has no retention TTL (it persists for the life of the lock name), so
    /// <see cref="ResolveTtl"/> returns <see cref="Timeout.InfiniteTimeSpan"/> and
    /// this reminder is never registered; it exists only to satisfy the base's
    /// abstract contract.
    /// </summary>
    private const string RetentionReminderName = "lattice-lock-retention";

    /// <summary>
    /// Durable crash-recovery reminder registered while the lock is held. On a
    /// minute cadence it reactivates the grain (if collected) and reclaims an
    /// expired lease so a crashed holder cannot wedge the lock indefinitely even
    /// when no live acquirer is present to drive reclamation.
    /// </summary>
    private const string KeepaliveReminderName = "lattice-lock-keepalive";

    /// <summary>
    /// Serializes every state mutation + persistence region against the reentrant
    /// interleavings the parked-acquire pattern requires. Never held across the
    /// <c>await</c> on a waiter's completion task, so a release can always make
    /// progress while an acquire is parked.
    /// </summary>
    private readonly SemaphoreSlim _gate = new(1, 1);

    /// <summary>The FIFO queue of parked acquirers awaiting a grant.</summary>
    private readonly LinkedList<Waiter> _waiters = new();

    /// <summary>The in-memory safety state the grain and core operate on.</summary>
    private LockCoreState _core;

    /// <summary>Guards one-time rehydration of <see cref="_core"/> from persisted state.</summary>
    private bool _coreLoaded;

    /// <summary>The fine-grained in-activation lease-expiry timer, armed on each grant.</summary>
    private IDisposable? _leaseTimer;

    /// <summary>
    /// Test seam for the wall-clock source. Defaults to
    /// <see cref="TimeProvider.System"/>; unit tests substitute a controllable
    /// provider to drive lease expiry deterministically.
    /// </summary>
    internal TimeProvider Clock { get; set; } = TimeProvider.System;

    /// <summary>
    /// Test seam for one-shot timer creation. When <see langword="null"/> (the
    /// production default) timers are created through
    /// <c>RegisterGrainTimer</c>; unit tests inject a
    /// factory that captures the callback (tagged by <c>purpose</c>: <c>"lease"</c>
    /// or <c>"waiter-timeout"</c>) so a timer fire can be simulated without the
    /// Orleans runtime.
    /// </summary>
    internal Func<string, Func<CancellationToken, Task>, TimeSpan, IDisposable>? TimerFactory { get; set; }

    /// <summary>A single parked acquirer: its completion source, requested lease, and enqueue time.</summary>
    private sealed class Waiter(TaskCompletionSource<LockLease> completion, long leaseTicks, long enqueuedAtTicks)
    {
        /// <summary>The task handed back to the acquirer, completed on grant / timeout.</summary>
        public TaskCompletionSource<LockLease> Completion => completion;

        /// <summary>The (already validated and clamped) lease duration to grant this waiter, in ticks.</summary>
        public long LeaseTicks => leaseTicks;

        /// <summary>The UTC tick the waiter was enqueued, for wait-time telemetry.</summary>
        public long EnqueuedAtTicks => enqueuedAtTicks;

        /// <summary>The one-shot wait-timeout timer; disposed when the waiter is granted or removed.</summary>
        public IDisposable? TimeoutTimer { get; set; }

        /// <summary><c>true</c> once the waiter has been granted or timed out, so a racing callback no-ops.</summary>
        public bool Settled { get; set; }
    }

    private string LockName => GrainContext.GrainId.Key.ToString()!;

    private long NowTicks() => Clock.GetUtcNow().UtcTicks;

    /// <inheritdoc />
    protected override string TtlReminderName => RetentionReminderName;

    /// <inheritdoc />
    protected override TimeSpan ResolveTtl() => Timeout.InfiniteTimeSpan;

    /// <inheritdoc />
    protected override Task OnTtlExpiredAsync() => Task.CompletedTask;

    /// <inheritdoc />
    protected override async Task OnOtherReminderAsync(string reminderName, TickStatus status)
    {
        if (reminderName != KeepaliveReminderName)
        {
            return;
        }

        await _gate.WaitAsync().ConfigureAwait(true);
        try
        {
            EnsureCoreLoaded();
            await ReclaimAndDispatchAsync(NowTicks()).ConfigureAwait(true);
            if (!_core.IsHeld)
            {
                await UnregisterKeepaliveAsync().ConfigureAwait(true);
                this.DeactivateOnIdle();
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <inheritdoc />
    public async Task<LockLease> AcquireAsync(LockAcquireRequest request)
    {
        if (request.MaxWait < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request), request.MaxWait, "LockAcquireRequest.MaxWait must not be negative.");
        }

        var leaseTicks = ResolveLeaseTicks(request.LeaseDuration);

        Task<LockLease> parked;
        await _gate.WaitAsync().ConfigureAwait(true);
        try
        {
            EnsureCoreLoaded();
            var now = NowTicks();
            await ReclaimAndDispatchAsync(now).ConfigureAwait(true);

            if (_waiters.Count == 0 && LockAdmissionCore.Decide(_core, now) == LockAdmissionDecision.Grant)
            {
                var lease = await GrantCoreAsync(now, leaseTicks).ConfigureAwait(true);
                RecordGranted(0);
                return lease;
            }

            if (request.MaxWait == TimeSpan.Zero)
            {
                LatticeMetrics.LockAcquired.Add(1, new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, "timeout"));
                throw new TimeoutException(
                    $"Lock '{LockName}' is held and a non-blocking acquire (MaxWait = 0) could not be granted.");
            }

            var tcs = new TaskCompletionSource<LockLease>(TaskCreationOptions.RunContinuationsAsynchronously);
            var waiter = new Waiter(tcs, leaseTicks, now);
            var node = _waiters.AddLast(waiter);
            ArmWaiterTimeout(node, request.MaxWait);
            parked = tcs.Task;
        }
        finally
        {
            _gate.Release();
        }

        return await parked.ConfigureAwait(true);
    }

    /// <inheritdoc />
    public async Task<LockLease?> TryAcquireAsync(TimeSpan leaseDuration)
    {
        var leaseTicks = ResolveLeaseTicks(leaseDuration);

        await _gate.WaitAsync().ConfigureAwait(true);
        try
        {
            EnsureCoreLoaded();
            var now = NowTicks();
            await ReclaimAndDispatchAsync(now).ConfigureAwait(true);

            if (_waiters.Count == 0 && LockAdmissionCore.Decide(_core, now) == LockAdmissionDecision.Grant)
            {
                var lease = await GrantCoreAsync(now, leaseTicks).ConfigureAwait(true);
                RecordGranted(0);
                return lease;
            }

            LatticeMetrics.LockAcquired.Add(1, new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, "unavailable"));
            return null;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <inheritdoc />
    public async Task<LockLease> RenewAsync(LockToken token, TimeSpan leaseDuration)
    {
        var leaseTicks = ResolveLeaseTicks(leaseDuration);

        await _gate.WaitAsync().ConfigureAwait(true);
        try
        {
            EnsureCoreLoaded();
            var now = NowTicks();
            // Reclaim first so a holder renewing after its lease expired (and was
            // handed to a waiter) is correctly fenced out rather than extending a
            // lock it has lost.
            await ReclaimAndDispatchAsync(now).ConfigureAwait(true);

            if (!LockAdmissionCore.Renew(ref _core, token.FencingToken, now, leaseTicks))
            {
                throw new LatticeLockConflictException(
                    $"Renew rejected for lock '{LockName}': fencing token {token.FencingToken} is not the current holder.",
                    LockName);
            }

            await PersistCoreAsync().ConfigureAwait(true);
            ArmLeaseTimer(_core.LeaseExpiresAtTicks - now);
            return BuildLease(token.FencingToken, _core.LeaseExpiresAtTicks, leaseTicks);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <inheritdoc />
    public async Task ReleaseAsync(LockToken token)
    {
        await _gate.WaitAsync().ConfigureAwait(true);
        try
        {
            EnsureCoreLoaded();
            var now = NowTicks();
            // Handle an already-expired lease first: if it expired and was handed to
            // a waiter, this token is no longer current and the release below no-ops.
            await ReclaimAndDispatchAsync(now).ConfigureAwait(true);

            if (!LockAdmissionCore.Release(ref _core, token.FencingToken))
            {
                // Stale token: releasing a lease you no longer hold is a silent no-op.
                return;
            }

            DisposeLeaseTimer();
            LatticeMetrics.LockReleased.Add(1);

            if (_waiters.Count > 0)
            {
                await DispatchNextAsync(now).ConfigureAwait(true);
            }
            else
            {
                await PersistCoreAsync().ConfigureAwait(true);
                await UnregisterKeepaliveAsync().ConfigureAwait(true);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <inheritdoc />
    public async Task<LockStatus> GetStatusAsync()
    {
        await _gate.WaitAsync().ConfigureAwait(true);
        try
        {
            EnsureCoreLoaded();
            var now = NowTicks();
            await ReclaimAndDispatchAsync(now).ConfigureAwait(true);

            var expiresAt = _core.IsHeld
                ? new DateTimeOffset(_core.LeaseExpiresAtTicks, TimeSpan.Zero)
                : (DateTimeOffset?)null;
            return new LockStatus(_core.IsHeld, _core.HolderToken, expiresAt, _waiters.Count);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Reclaims an expired lease (if any) and grants the head FIFO waiter. Safe to
    /// call at the head of every operation: a no-op when the lease is live or the
    /// lock is free with no waiters. Assumes the caller holds <see cref="_gate"/>.
    /// </summary>
    private async Task ReclaimAndDispatchAsync(long nowTicks)
    {
        if (LockAdmissionCore.ReclaimIfExpired(ref _core, nowTicks))
        {
            DisposeLeaseTimer();
            LatticeMetrics.LockLeaseReclaimed.Add(1);
        }

        if (!_core.IsHeld && _waiters.Count > 0)
        {
            await DispatchNextAsync(nowTicks).ConfigureAwait(true);
        }
    }

    /// <summary>
    /// Grants the lock to the head FIFO waiter, persisting the transition and
    /// completing the waiter's task from this (later) turn. A single grant is
    /// possible per call because the grant marks the lock held. Assumes the caller
    /// holds <see cref="_gate"/> and that the lock is free.
    /// </summary>
    private async Task DispatchNextAsync(long nowTicks)
    {
        if (_core.IsHeld || _waiters.Count == 0)
        {
            return;
        }

        var node = _waiters.First!;
        var waiter = node.Value;
        _waiters.RemoveFirst();
        waiter.Settled = true;
        DisposeWaiterTimeout(waiter);

        var lease = await GrantCoreAsync(nowTicks, waiter.LeaseTicks).ConfigureAwait(true);

        var waitedMs = TimeSpan.FromTicks(Math.Max(0, nowTicks - waiter.EnqueuedAtTicks)).TotalMilliseconds;
        RecordGranted(waitedMs);
        waiter.Completion.TrySetResult(lease);
    }

    /// <summary>
    /// Mints the next fencing token, installs the holder with a lease expiring
    /// <paramref name="leaseTicks"/> from <paramref name="nowTicks"/>, persists the
    /// state, ensures the keepalive reminder is registered, and arms the fine
    /// lease-expiry timer. Assumes the caller holds <see cref="_gate"/>.
    /// </summary>
    private async Task<LockLease> GrantCoreAsync(long nowTicks, long leaseTicks)
    {
        var token = LockAdmissionCore.Grant(ref _core, nowTicks, leaseTicks);
        state.State.LeaseDurationTicks = leaseTicks;
        await PersistCoreAsync().ConfigureAwait(true);
        await EnsureKeepaliveAsync().ConfigureAwait(true);
        ArmLeaseTimer(_core.LeaseExpiresAtTicks - nowTicks);
        return BuildLease(token, _core.LeaseExpiresAtTicks, leaseTicks);
    }

    private static LockLease BuildLease(long token, long expiresAtTicks, long leaseTicks) =>
        new(new LockToken(token),
            new DateTimeOffset(expiresAtTicks, TimeSpan.Zero),
            TimeSpan.FromTicks(leaseTicks));

    private static void RecordGranted(double waitedMs)
    {
        LatticeMetrics.LockAcquired.Add(1, new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, "granted"));
        LatticeMetrics.LockAcquireWait.Record(waitedMs);
    }

    /// <summary>
    /// Validates and clamps a requested lease duration to ticks: a non-positive
    /// request defers to <see cref="LatticeOptions.DefaultLockLeaseDuration"/>, and
    /// every result is capped at <see cref="LatticeOptions.MaxLockLeaseDuration"/>.
    /// </summary>
    private long ResolveLeaseTicks(TimeSpan requested)
    {
        var options = optionsMonitor.CurrentValue;
        var lease = requested > TimeSpan.Zero ? requested : options.DefaultLockLeaseDuration;
        var max = options.MaxLockLeaseDuration;
        if (max > TimeSpan.Zero && lease > max)
        {
            lease = max;
        }

        return lease.Ticks;
    }

    private void EnsureCoreLoaded()
    {
        if (_coreLoaded)
        {
            return;
        }

        var s = state.State;
        _core = new LockCoreState
        {
            FencingCounter = s.FencingCounter,
            IsHeld = s.IsHeld,
            HolderToken = s.HolderToken,
            LeaseExpiresAtTicks = s.LeaseExpiresAtTicks,
        };
        _coreLoaded = true;
    }

    private async Task PersistCoreAsync()
    {
        var s = state.State;
        s.FencingCounter = _core.FencingCounter;
        s.IsHeld = _core.IsHeld;
        s.HolderToken = _core.HolderToken;
        s.LeaseExpiresAtTicks = _core.LeaseExpiresAtTicks;
        if (!_core.IsHeld)
        {
            s.LeaseDurationTicks = 0;
        }

        await state.WriteStateAsync().ConfigureAwait(true);
    }

    private IDisposable ArmTimer(string purpose, Func<CancellationToken, Task> callback, TimeSpan dueTime) =>
        TimerFactory is not null
            ? TimerFactory(purpose, callback, dueTime)
            : this.RegisterGrainTimer(callback, new GrainTimerCreationOptions(dueTime, Timeout.InfiniteTimeSpan));

    private void ArmLeaseTimer(long durationTicks)
    {
        DisposeLeaseTimer();
        var due = durationTicks <= 0 ? TimeSpan.Zero : TimeSpan.FromTicks(durationTicks);
        _leaseTimer = ArmTimer("lease", HandleLeaseTimerAsync, due);
    }

    private void DisposeLeaseTimer()
    {
        _leaseTimer?.Dispose();
        _leaseTimer = null;
    }

    private async Task HandleLeaseTimerAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(true);
        try
        {
            EnsureCoreLoaded();
            await ReclaimAndDispatchAsync(NowTicks()).ConfigureAwait(true);
            if (!_core.IsHeld)
            {
                await UnregisterKeepaliveAsync().ConfigureAwait(true);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    private void ArmWaiterTimeout(LinkedListNode<Waiter> node, TimeSpan maxWait)
    {
        var waiter = node.Value;
        waiter.TimeoutTimer = ArmTimer("waiter-timeout", _ => HandleWaiterTimeoutAsync(node), maxWait);
    }

    private async Task HandleWaiterTimeoutAsync(LinkedListNode<Waiter> node)
    {
        await _gate.WaitAsync().ConfigureAwait(true);
        try
        {
            var waiter = node.Value;
            if (waiter.Settled)
            {
                return;
            }

            waiter.Settled = true;
            if (node.List is not null)
            {
                _waiters.Remove(node);
            }

            DisposeWaiterTimeout(waiter);
            LatticeMetrics.LockAcquired.Add(1, new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, "timeout"));
            waiter.Completion.TrySetException(new TimeoutException(
                $"Acquiring lock '{LockName}' timed out before the caller reached the head of the FIFO queue."));
        }
        finally
        {
            _gate.Release();
        }
    }

    private static void DisposeWaiterTimeout(Waiter waiter)
    {
        waiter.TimeoutTimer?.Dispose();
        waiter.TimeoutTimer = null;
    }

    private async Task EnsureKeepaliveAsync()
    {
        try
        {
            await ReminderServiceReadiness.RetryWhileInitializingAsync(() =>
                ReminderRegistry.RegisterOrUpdateReminder(
                    callingGrainId: GrainContext.GrainId,
                    reminderName: KeepaliveReminderName,
                    dueTime: TimeSpan.FromMinutes(1),
                    period: TimeSpan.FromMinutes(1))).ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Lock '{LockName}': failed to register keepalive reminder (non-fatal; lazy reclamation still applies).",
                LockName);
        }
    }

    private async Task UnregisterKeepaliveAsync()
    {
        try
        {
            var reminder = await ReminderRegistry.GetReminder(GrainContext.GrainId, KeepaliveReminderName).ConfigureAwait(true);
            if (reminder is not null)
            {
                await ReminderRegistry.UnregisterReminder(GrainContext.GrainId, reminder).ConfigureAwait(true);
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Lock '{LockName}': failed to unregister keepalive reminder (non-fatal).",
                LockName);
        }
    }

    /// <inheritdoc />
    public async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        // In-flight waiters are in-memory only and legitimately lost on
        // deactivation. Fault them so parked callers observe a clean cancellation
        // (and retry) rather than hanging until their own wait-timeout.
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(true);
        try
        {
            DisposeLeaseTimer();
            foreach (var waiter in _waiters)
            {
                if (waiter.Settled)
                {
                    continue;
                }

                waiter.Settled = true;
                DisposeWaiterTimeout(waiter);
                waiter.Completion.TrySetException(new OperationCanceledException(
                    $"Lock '{LockName}' grain deactivated before the queued acquire was granted; retry the acquire."));
            }

            _waiters.Clear();
        }
        finally
        {
            _gate.Release();
        }
    }
}
