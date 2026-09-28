using System.Threading.Tasks.Sources;

namespace Orleans.Lattice;

/// <summary>
/// Runs one WAL GC reactivation pass's touches no wider than the replay gate's
/// starvation share, and re-drives a touch the gate refused once a sibling touch
/// of the same pass has finished (issue #3761 item 1(a)).
/// </summary>
/// <remarks>
/// <para>
/// A leaf's silo admits a sweep drive only while its process-wide starvation
/// share has a free slot, and it tests that with a non-queueing acquire, so a
/// drive beyond the share is refused at once. Launched all together, a pass of
/// thirty-two touches against a share of three was refused twenty-nine times,
/// and nothing retried the refusals inside the pass: measured on a live
/// container, 88% of touches were refused and the floor fell behind the head
/// without bound. Here the pass holds at most <c>concurrency</c> touches in
/// flight and starts the next as each one finishes, so the pass itself no longer
/// competes with its own drives for the share.
/// </para>
/// <para>
/// <b>The bounded wait is a sibling's completion, never a timer.</b> A refused
/// touch becomes eligible again only once another touch of this pass has
/// finished after the refusal, because that is the one freed slot the pass can
/// know about: it held it. A slot held elsewhere - by a coverage-lag timer
/// drive, say - frees at a moment the pass cannot observe, so a refusal with
/// nothing of the pass's own left in flight is not retried here; it falls to the
/// scheduler's escalating admission retry delay on a later pass. That also keeps
/// the pass off the scheduler's clock, whose only timer is the pass cadence.
/// Each touch is retried at most <see cref="MaxInPassRetries"/> times.
/// </para>
/// <para>
/// Touches are started lowest index first, including retries, so the ranking the
/// caller supplies (floor holders nearest the floor first, issue #3610) decides
/// who takes a freed slot.
/// </para>
/// </remarks>
internal static class ShareBoundedTouchRunner
{
    /// <summary>
    /// The most times one touch refused admission is re-driven within a single
    /// pass.
    /// </summary>
    /// <remarks>
    /// Small, because each retry waits only for a sibling's completion and a
    /// share that stays full after several of those is held by drives the pass
    /// does not own; those are better left to the scheduler's escalating
    /// cross-pass delay than retried in a tight loop.
    /// </remarks>
    internal const int MaxInPassRetries = 3;

    /// <summary>
    /// Drives touches <c>0 .. count-1</c> with at most
    /// <paramref name="concurrency"/> in flight at once.
    /// </summary>
    /// <typeparam name="TState">The caller's per-pass state, passed to both callbacks so they can be static.</typeparam>
    /// <param name="count">The number of touches in the pass.</param>
    /// <param name="concurrency">The most touches in flight at once; clamped to <c>[1, count]</c>.</param>
    /// <param name="leadAlone">
    /// Whether touch 0 runs by itself and to completion before any other is
    /// started (issue #3610). Ignored when <paramref name="count"/> is one.
    /// </param>
    /// <param name="state">The caller's state.</param>
    /// <param name="touch">
    /// Starts touch <c>i</c> and completes with <see langword="true"/> when its
    /// drive was refused admission and may be retried.
    /// </param>
    /// <param name="mayLaunch">
    /// Whether another touch may still be started; once it returns
    /// <see langword="false"/> nothing further is started, and touches never
    /// started are the caller's to restore. Not consulted for the first touch.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="touch"/> or <paramref name="mayLaunch"/> is null.</exception>
    /// <remarks>
    /// A touch that throws ends the run: the others in flight are awaited, their
    /// faults suppressed, and the first fault is rethrown. The scheduler's touch
    /// throws only on shutdown.
    /// </remarks>
    internal static async Task RunAsync<TState>(
        int count,
        int concurrency,
        bool leadAlone,
        TState state,
        Func<TState, int, Task<bool>> touch,
        Func<TState, bool> mayLaunch)
    {
        ArgumentNullException.ThrowIfNull(touch);
        ArgumentNullException.ThrowIfNull(mayLaunch);

        if (count <= 0)
        {
            return;
        }

        var run = new Run(count, Math.Clamp(concurrency, 1, count));

        if (leadAlone && count > 1)
        {
            run.Settle(0, await touch(state, 0).ConfigureAwait(false));
        }
        else
        {
            run.Launch(0, touch(state, 0));
        }

        while (true)
        {
            while (run.HasFreeSlot && run.LowestEligible() is var next && next >= 0 && mayLaunch(state))
            {
                run.Launch(next, touch(state, next));
            }

            if (run.Live == 0)
            {
                return;
            }

            // Wait for any touch in flight to finish. The wait is a single
            // reusable source each in-flight touch signals as it completes, so a
            // completion allocates nothing, where Task.WhenAny allocated a
            // promise and re-registered on every in-flight touch per completion.
            int slot;
            while ((slot = run.FinishedSlot()) < 0)
            {
                if (run.ArmWake(out var wake))
                {
                    await wake.ConfigureAwait(false);
                }
            }

            var done = run.RemoveAt(slot, out var index);

            bool refused;
            try
            {
                refused = await done.ConfigureAwait(false);
            }
            catch
            {
                await run.DrainAsync().ConfigureAwait(false);
                throw;
            }

            run.Settle(index, refused);
        }
    }


    /// <summary>The bookkeeping of one <see cref="RunAsync{TState}"/> call.</summary>
    /// <remarks>
    /// It is also the pass's wake source: every in-flight touch that is not yet
    /// complete when launched gets <see cref="Signal"/> as its one continuation,
    /// and the loop parks on this object only when no touch has finished, so
    /// neither registering nor waking allocates.
    /// </remarks>
    private sealed class Run : IValueTaskSource<bool>
    {
        private const byte Queued = 0;
        private const byte Flying = 1;
        private const byte Done = 2;
        private const byte Refused = 3;

        private readonly byte[] _status;
        private readonly byte[] _retries;
        private readonly int[] _waitPast;
        private readonly Task<bool>[] _slots;
        private readonly int[] _slotTouch;
        private readonly Action _signal;
        private ManualResetValueTaskSourceCore<bool> _wake;
        private int _waiting;
        private int _completions;

        /// <summary>Creates the bookkeeping for <paramref name="count"/> touches, <paramref name="concurrency"/> at a time.</summary>
        public Run(int count, int concurrency)
        {
            _status = new byte[count];
            _retries = new byte[count];
            _waitPast = new int[count];
            _slots = new Task<bool>[concurrency];
            _slotTouch = new int[concurrency];
            _signal = Signal;
        }

        /// <summary>Touches in flight.</summary>
        public int Live { get; private set; }

        /// <summary>Whether another touch may be started without exceeding the concurrency.</summary>
        public bool HasFreeSlot => Live < _slots.Length;

        /// <summary>
        /// The lowest-indexed touch that may be started now, or -1: a queued
        /// touch, or a refused one some sibling has finished since.
        /// </summary>
        public int LowestEligible()
        {
            for (var i = 0; i < _status.Length; i++)
            {
                if (_status[i] == Queued || (_status[i] == Refused && _completions > _waitPast[i]))
                {
                    return i;
                }
            }

            return -1;
        }

        /// <summary>
        /// Records touch <paramref name="index"/> as in flight, and arranges for
        /// its completion to wake the pass.
        /// </summary>
        public void Launch(int index, Task<bool> task)
        {
            _status[index] = Flying;
            _slots[Live] = task;
            _slotTouch[Live] = index;
            Live++;

            // A touch already complete is found by FinishedSlot; registering on
            // it would have the continuation queued to the thread pool instead.
            if (!task.IsCompleted)
            {
                task.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(_signal);
            }
        }

        /// <summary>The slot of an in-flight touch that has finished, or -1.</summary>
        public int FinishedSlot()
        {
            for (var i = 0; i < Live; i++)
            {
                if (_slots[i].IsCompleted)
                {
                    return i;
                }
            }

            return -1;
        }

        /// <summary>
        /// Arms the wake and reports whether the caller must await
        /// <paramref name="wake"/>: <see langword="false"/> only when a touch
        /// finished while arming and no signal has claimed the wake.
        /// </summary>
        /// <remarks>
        /// The interlocked write of <c>_waiting</c> is a full fence ahead of the
        /// re-scan, and <see cref="Signal"/> exchanges it after its touch has
        /// completed, so a completion is either seen by the re-scan or signals
        /// the wake - never neither. A signal that won the exchange completes the
        /// wake, so it is awaited even then, keeping the next reset strictly
        /// after that completion.
        /// </remarks>
        public bool ArmWake(out ValueTask<bool> wake)
        {
            _wake.Reset();
            wake = new ValueTask<bool>(this, _wake.Version);
            Interlocked.Exchange(ref _waiting, 1);

            if (FinishedSlot() < 0)
            {
                return true;
            }

            return Interlocked.Exchange(ref _waiting, 0) == 0;
        }

        /// <summary>
        /// Takes the touch in <paramref name="slot"/> out of the in-flight set,
        /// returning it and its touch index.
        /// </summary>
        public Task<bool> RemoveAt(int slot, out int index)
        {
            var task = _slots[slot];
            index = _slotTouch[slot];
            Live--;
            _slots[slot] = _slots[Live];
            _slotTouch[slot] = _slotTouch[Live];
            _slots[Live] = null!;
            return task;
        }

        /// <summary>
        /// Records that touch <paramref name="index"/> finished, re-queueing it
        /// behind the next sibling completion when it was refused and has retries
        /// left.
        /// </summary>
        public void Settle(int index, bool refused)
        {
            _completions++;
            if (refused && _retries[index] < MaxInPassRetries)
            {
                _retries[index]++;
                _status[index] = Refused;
                _waitPast[index] = _completions;
            }
            else
            {
                _status[index] = Done;
            }
        }

        /// <summary>Awaits every touch still in flight, suppressing their faults.</summary>
        public async Task DrainAsync()
        {
            for (var i = 0; i < Live; i++)
            {
                await ((Task)_slots[i]).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
        }

        /// <inheritdoc/>
        bool IValueTaskSource<bool>.GetResult(short token) => _wake.GetResult(token);

        /// <inheritdoc/>
        ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token) => _wake.GetStatus(token);

        /// <inheritdoc/>
        void IValueTaskSource<bool>.OnCompleted(
            Action<object?> continuation,
            object? state,
            short token,
            ValueTaskSourceOnCompletedFlags flags) =>
            _wake.OnCompleted(continuation, state, token, flags);

        /// <summary>
        /// A touch's completion continuation: wakes the pass if it is parked.
        /// Spurious wakes are harmless, because the loop re-scans.
        /// </summary>
        private void Signal()
        {
            if (Interlocked.Exchange(ref _waiting, 0) == 1)
            {
                _wake.SetResult(true);
            }
        }
    }
}