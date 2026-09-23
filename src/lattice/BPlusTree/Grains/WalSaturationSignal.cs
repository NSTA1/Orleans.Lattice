using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Globalization;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Internal singleton that backs <see cref="IWalSaturationSignal"/>.
/// Holds the per-tree saturation state cache populated by the
/// <see cref="WalSaturationSampler"/>, exposes the polling getters
/// and the await-able gate, and registers the
/// <see cref="LatticeMetrics.WalSaturationStateGaugeName"/> observable
/// gauge so dashboards can plot the current regime per tree.
/// <para>
/// Cache lookup is one
/// <see cref="ConcurrentDictionary{TKey, TValue}.TryGetValue(TKey, out TValue)"/>
/// returning a <see cref="WalSaturationState"/> enum - no allocation,
/// no grain call - so the polling shape costs effectively nothing
/// on the canonical TCP-read-loop caller pattern. The await-able gate
/// completes either synchronously (already-healthy fast path) or on
/// the next sample tick that flips the tree back to
/// <see cref="WalSaturationState.Healthy"/>; the bound is therefore
/// one <see cref="LatticeOptions.WalSaturationSampleInterval"/>
/// beyond the underlying recovery.
/// </para>
/// </summary>
internal sealed class WalSaturationSignal : IWalSaturationSignal, IWalPartitionSaturationSignal
{
    private static readonly object RegistrationLock = new();
    private static volatile WalSaturationSignal? _current;
    private static bool _gaugeRegistered;

    // Per-tree current state cache, populated by the sampler. Reads
    // are concurrent and lock-free; the sampler writes one entry per
    // observed tree on each transition (and every tick when no
    // transition has happened, the entry is left untouched).
    private readonly ConcurrentDictionary<string, WalSaturationState> _states
        = new(StringComparer.Ordinal);

    // (#3348) Per-(tree, partition) current state cache, populated by
    // the sampler alongside the per-tree roll-up above. The writer's
    // pre-admission gate consults this so one partition at its
    // admission cap cannot refuse appends routed at its idle siblings.
    // A ValueTuple key keeps the hot-path lookup allocation-free.
    private readonly ConcurrentDictionary<(string TreeId, int Partition), WalSaturationState> _partitionStates
        = new();

    // Per-tree wait registrations for WaitForHealthyAsync. The signal
    // completes every TCS for a tree the moment it transitions back to
    // Healthy. Keyed by tree id; the value is the list of pending
    // waiter entries (each registered by a single WaitForHealthyAsync
    // caller). Access is serialised under _waitGate so a tick observing
    // the recovery and a caller registering a wait cannot race.
    private readonly Dictionary<string, List<WaiterEntry>> _waiters
        = new(StringComparer.Ordinal);
    private readonly object _waitGate = new();

    /// <summary>
    /// One pending <see cref="WaitForHealthyAsync(string, CancellationToken)"/>
    /// registration. Pairs the <see cref="TaskCompletionSource"/> the
    /// caller awaits with the
    /// <see cref="CancellationTokenRegistration"/> that observes the
    /// caller's token, so both the recovery path and the cancellation
    /// path dispose the registration without a per-await
    /// <see cref="Task.ContinueWith(System.Action{Task})"/> chain.
    /// </summary>
    private sealed class WaiterEntry
    {
        public required TaskCompletionSource Tcs;
        public CancellationTokenRegistration Registration;
    }

    /// <summary>
    /// Initialises the signal and ensures the observable saturation-state
    /// gauge is registered on the shared meter. Registration is process-
    /// wide and idempotent; the most recently constructed instance backs
    /// every gauge scrape, matching the DI singleton model used by
    /// <c>AddLattice</c>.
    /// </summary>
    public WalSaturationSignal()
    {
        lock (RegistrationLock)
        {
            _current = this;
            if (!_gaugeRegistered)
            {
                RegisterGauge();
                _gaugeRegistered = true;
            }
        }
    }

    /// <inheritdoc />
    public WalSaturationState GetCurrentState(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return _states.TryGetValue(treeId, out var state) ? state : WalSaturationState.Healthy;
    }

    /// <inheritdoc />
    public WalSaturationState GetAggregateState()
    {
        // Worst-case across every observed tree. Enumerating the
        // dictionary is allocation-free (KeyValuePair<string,
        // WalSaturationState> is a struct) and short - bounded by the
        // number of trees the silo hosts, which is the same bound the
        // existing storage-usage and projection-digest aggregators
        // already operate against without complaint.
        var worst = WalSaturationState.Healthy;
        foreach (var kv in _states)
        {
            if (kv.Value > worst)
            {
                worst = kv.Value;
                if (worst == WalSaturationState.Saturated)
                {
                    // Cannot get any worse; short-circuit.
                    return worst;
                }
            }
        }
        return worst;
    }

    /// <inheritdoc />
    public WalSaturationState GetCurrentState(string treeId, int partition)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        // Absent entry means the sampler has never observed a tracker
        // for this partition, which in turn means nothing has ever been
        // admitted against it - it has no admission queue to be
        // saturated. Healthy is therefore the correct answer, and it is
        // also the only safe one: the tracker is created on first
        // admission, which happens AFTER this gate, so falling back to
        // the tree-wide verdict here would refuse a partition's very
        // first append forever whenever a sibling was saturated - the
        // refusal would prevent the admission that would create the
        // tracker that would publish the state.
        //
        // The cost is a bounded, self-healing window: if the tree is
        // saturated by a genuinely tree-wide cause, a partition that
        // has never been touched admits its first append rather than
        // being refused. That append creates the tracker, and the next
        // sampler tick (one WalSaturationSampleInterval) publishes the
        // partition at the tree's verdict, closing the gate. One append
        // on a cold partition is a far smaller price than a permanent
        // deadlock on it.
        return _partitionStates.TryGetValue((treeId, partition), out var state)
            ? state
            : WalSaturationState.Healthy;
    }

    /// <summary>
    /// Composes the <see cref="_waiters"/> key for a partition-scoped
    /// wait. Allocates, so it is only ever called on the slow path,
    /// after the caller has established the partition is not Healthy.
    /// </summary>
    private static string PartitionWaitKey(string treeId, int partition)
        => string.Create(CultureInfo.InvariantCulture, $"{treeId}\u0000{partition}");

    /// <summary>
    /// (#3348) Composes the <see cref="_waiters"/> key for a partition-scoped
    /// "no longer Saturated" wait. Kept distinct from
    /// <see cref="PartitionWaitKey"/> so releasing these waiters on a
    /// Throttled tick never releases a caller that asked for Healthy.
    /// </summary>
    private static string PartitionNotSaturatedWaitKey(string treeId, int partition)
        => string.Create(CultureInfo.InvariantCulture, $"{treeId}\u0000{partition}\u0000ns");

    /// <inheritdoc />
    public Task WaitForHealthyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        // Synchronous fast path: already healthy, complete inline with
        // no allocation (Task.CompletedTask is the singleton).
        if (GetCurrentState(treeId) == WalSaturationState.Healthy)
        {
            return Task.CompletedTask;
        }

        return WaitCoreAsync(treeId, treeId, partition: -1, cancellationToken);
    }

    /// <inheritdoc />
    public Task WaitForHealthyAsync(string treeId, int partition, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        if (GetCurrentState(treeId, partition) == WalSaturationState.Healthy)
        {
            return Task.CompletedTask;
        }

        return WaitCoreAsync(PartitionWaitKey(treeId, partition), treeId, partition, cancellationToken);
    }

    /// <inheritdoc />
    public Task WaitForNotSaturatedAsync(string treeId, int partition, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        if (GetCurrentState(treeId, partition) != WalSaturationState.Saturated)
        {
            return Task.CompletedTask;
        }

        return WaitCoreAsync(PartitionNotSaturatedWaitKey(treeId, partition), treeId, partition, cancellationToken, untilNotSaturated: true);
    }

    /// <summary>
    /// Shared slow path for both wait overloads. <paramref name="waitKey"/>
    /// is the <see cref="_waiters"/> bucket to register against;
    /// <paramref name="partition"/> is negative for a tree-scoped wait
    /// and selects which state probe the in-lock re-check uses.
    /// <paramref name="untilNotSaturated"/> relaxes that re-check from
    /// "Healthy" to "not Saturated", matching the release condition of the
    /// bucket it registers against.
    /// </summary>
    private Task WaitCoreAsync(string waitKey, string treeId, int partition, CancellationToken cancellationToken, bool untilNotSaturated = false)
    {
        // Slow path: allocate a TCS + WaiterEntry, register them, and
        // arm a cancellation hook that faults the TCS with
        // OperationCanceledException if the caller's token fires before
        // the sampler observes the recovery. RunContinuationsAsynchronously
        // keeps the TCS completion off the sampler thread.
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entry = new WaiterEntry { Tcs = tcs };
        lock (_waitGate)
        {
            // Re-check inside the lock so a tick that fired between the
            // fast-path check and the lock acquisition does not leave
            // us registered against an already-healthy tree (which
            // would only resolve on the next transition, defeating the
            // gate's contract).
            var current = partition < 0
                ? GetCurrentState(treeId)
                : GetCurrentState(treeId, partition);
            if (current == WalSaturationState.Healthy
                || (untilNotSaturated && current != WalSaturationState.Saturated))
            {
                return Task.CompletedTask;
            }
            if (!_waiters.TryGetValue(waitKey, out var list))
            {
                list = new List<WaiterEntry>(capacity: 2);
                _waiters[waitKey] = list;
            }
            list.Add(entry);
        }

        if (cancellationToken.CanBeCanceled)
        {
            // Arm cancellation after the entry is linked. The callback
            // disposes its own CTR and unlinks the entry from the
            // waiter list. No per-await ContinueWith chain is needed -
            // the recovery path disposes the CTR directly in
            // CompleteWaitersForRecovery, so the disposal cost is paid
            // exactly once whether the wait settled by recovery or by
            // cancellation.
            entry.Registration = cancellationToken.Register(static state =>
            {
                var pair = ((WalSaturationSignal Signal, string WaitKey, WaiterEntry Entry))state!;
                if (pair.Entry.Tcs.TrySetCanceled())
                {
                    // Remove the cancelled entry from the wait list so
                    // a later recovery does not see it.
                    lock (pair.Signal._waitGate)
                    {
                        if (pair.Signal._waiters.TryGetValue(pair.WaitKey, out var list))
                        {
                            list.Remove(pair.Entry);
                            if (list.Count == 0)
                            {
                                pair.Signal._waiters.Remove(pair.WaitKey);
                            }
                        }
                    }
                    pair.Entry.Registration.Dispose();
                }
            }, (this, waitKey, entry));
        }

        return tcs.Task;
    }

    /// <summary>
    /// Sampler-side write path. Updates the per-tree state cache, and
    /// when the new state is <see cref="WalSaturationState.Healthy"/>
    /// completes every pending <see cref="WaitForHealthyAsync(string, CancellationToken)"/>
    /// caller registered for the tree. Returns the previous state so
    /// the sampler can attribute a transition (or short-circuit when
    /// the state is unchanged).
    /// </summary>
    internal WalSaturationState UpdateState(string treeId, WalSaturationState newState, int releaseBatch = 0)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        // Atomic swap: AddOrUpdate returns the new value; we recover
        // the previous one via a second small lookup so callers can
        // attribute transitions without a Compare-Exchange dance on
        // the dictionary itself. The two-step is safe because the
        // sampler is single-threaded per tick (only one tick processes
        // a given tree at a time).
        var previous = _states.TryGetValue(treeId, out var existing) ? existing : WalSaturationState.Healthy;
        _states[treeId] = newState;

        // (#3402) Level-triggered for the same reason as the
        // partition-scoped sibling below: a paced release leaves a
        // residue that only a later tick can drain, and that tick sees
        // no transition. This path is the fallback used when no
        // per-partition signal is registered, so it carries the same
        // herd hazard and takes the same treatment.
        if (newState == WalSaturationState.Healthy)
        {
            // Drain pending waiters for this tree. The TCSs were
            // built with RunContinuationsAsynchronously so the
            // completion does not run inline on the sampler thread.
            // Disposing the cancellation registration inline keeps the
            // per-wait disposal cost off a separate continuation Task
            // (the per-await ContinueWith chain the entry was
            // explicitly designed to avoid).
            CompleteWaiters(treeId, releaseBatch);
        }

        return previous;
    }

    /// <summary>
    /// (#3348) Sampler-side write path for a single WAL partition.
    /// Mirrors <see cref="UpdateState"/> but scoped to one
    /// <c>(tree, partition)</c> pair: updates the per-partition cache
    /// and, on a transition back to
    /// <see cref="WalSaturationState.Healthy"/>, completes every
    /// partition-scoped waiter. Returns the previous state.
    /// </summary>
    internal WalSaturationState UpdatePartitionState(
        string treeId,
        int partition,
        WalSaturationState newState,
        int releaseBatch = 0)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var key = (treeId, partition);
        var previous = _partitionStates.TryGetValue(key, out var existing)
            ? existing
            : WalSaturationState.Healthy;
        _partitionStates[key] = newState;

        // (#3402) Level-triggered, not edge-triggered. A paced release
        // hands out at most `releaseBatch` waiters per call, so the
        // residue has to be drained by later ticks - and those ticks see
        // no Saturated -> Healthy transition, because the partition is
        // already Healthy. Releasing on the edge alone would strand the
        // residue until the next saturation cycle, which is a worse
        // failure than the herd this replaces. Releasing on the level
        // costs a dictionary miss under `_waitGate` per healthy
        // partition per tick, which at the 200 ms default cadence is
        // negligible.
        if (newState == WalSaturationState.Healthy)
        {
            CompleteWaiters(PartitionWaitKey(treeId, partition), releaseBatch);
        }

        // (#3348) Level-triggered for the same reason. These waiters asked
        // only for the partition to leave Saturated, so a Throttled tick -
        // including the recovery window's hysteresis - releases them too.
        // The bucket is empty unless WalSaturationAcuteOnly is set, so the
        // default configuration pays one dictionary miss per partition per
        // non-Saturated tick.
        if (newState != WalSaturationState.Saturated)
        {
            CompleteWaiters(PartitionNotSaturatedWaitKey(treeId, partition), releaseBatch);
        }

        return previous;
    }

    /// <summary>
    /// Completes and unregisters up to <paramref name="maxToRelease"/>
    /// waiters parked on <paramref name="waitKey"/>, oldest first.
    /// Shared by the tree-scoped and partition-scoped write paths.
    /// <para>
    /// (#3402) A value of zero or less releases every parked waiter, which
    /// is the pre-fix behaviour. A positive value paces the release so a
    /// recovered partition is not immediately re-saturated by the herd it
    /// just admitted; the residue stays parked and is drained by
    /// subsequent sampler ticks, which keep calling in while the
    /// partition reads Healthy.
    /// </para>
    /// <para>
    /// Release is oldest-first so a paced drain cannot starve the callers
    /// that have already waited longest, which are also the ones closest
    /// to exhausting their wait budget.
    /// </para>
    /// </summary>
    private void CompleteWaiters(string waitKey, int maxToRelease = 0)
    {
        List<WaiterEntry>? toComplete = null;
        lock (_waitGate)
        {
            if (_waiters.TryGetValue(waitKey, out var list))
            {
                if (maxToRelease <= 0 || list.Count <= maxToRelease)
                {
                    toComplete = list;
                    _waiters.Remove(waitKey);
                }
                else
                {
                    // Partial release: hand out the oldest `maxToRelease`
                    // entries and leave the remainder parked under the
                    // same key for the next tick to pick up.
                    toComplete = list.GetRange(0, maxToRelease);
                    list.RemoveRange(0, maxToRelease);
                }
            }
        }
        if (toComplete is not null)
        {
            for (var i = 0; i < toComplete.Count; i++)
            {
                var entry = toComplete[i];
                // Dispose the CTR first to remove the cancellation
                // hook before the TCS resolves; this prevents the
                // cancellation callback from seeing a settled TCS
                // and racing against the recovery completion.
                entry.Registration.Dispose();
                entry.Tcs.TrySetResult();
            }
        }
    }

    private static void RegisterGauge()
    {
        LatticeMetrics.Meter.CreateObservableGauge<long>(
            LatticeMetrics.WalSaturationStateGaugeName,
            static () => _current?.ObserveStateGauge() ?? Array.Empty<Measurement<long>>(),
            unit: "{state}",
            description: "Current per-tree WAL saturation state (0=Healthy, 1=Throttled, 2=Saturated).");
    }

    private IEnumerable<Measurement<long>> ObserveStateGauge()
    {
        foreach (var kv in _states)
        {
            // Emit only the tree tag: the ordinal value already encodes
            // the regime (0=Healthy, 1=Throttled, 2=Saturated), so a
            // redundant state-name label would just fragment the series.
            // With the state carried as a label, every transition changed
            // the series identity and the callback stopped emitting the
            // prior state's series without ever driving it back to zero,
            // so the stale elevated value lingered under Prometheus
            // staleness and a tree that had recovered still read as
            // Saturated (and max by (tree) over the orphaned series
            // returned the worst regime the tree had ever been in). One
            // series per tree whose value steps 0->1->2->0 keeps the
            // series identity stable across transitions, so nothing can
            // linger. The per-state breakdown stays on the
            // WalSaturationTransitions counter, where the state and
            // previous_state labels belong.
            yield return new Measurement<long>(
                (long)kv.Value,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, kv.Key),
                LatticeTenantLabel.ForTree(kv.Key));
        }
    }

    /// <summary>
    /// Lowercased tag value for the <see cref="LatticeMetrics.TagWalSaturationState"/>
    /// dimension on the saturation transitions counter. Centralised here
    /// so the writer-side metric site and the sampler agree on the
    /// spelling. The observable state gauge deliberately does not carry
    /// this label - its ordinal value already encodes the regime, and a
    /// redundant state label fragmented the per-tree series across
    /// transitions.
    /// </summary>
    internal static string StateTagValue(WalSaturationState state) => state switch
    {
        WalSaturationState.Healthy => "healthy",
        WalSaturationState.Throttled => "throttled",
        WalSaturationState.Saturated => "saturated",
        _ => "unknown",
    };

    /// <summary>
    /// Test-only reset. Clears the per-tree cache and faults every
    /// pending waiter so a successor test fixture sees a clean state.
    /// Intentionally <c>internal</c> so production code cannot call it.
    /// </summary>
    internal void ResetForTesting()
    {
        _states.Clear();
        _partitionStates.Clear();
        List<WaiterEntry> toCancel;
        lock (_waitGate)
        {
            toCancel = _waiters.SelectMany(static kv => kv.Value).ToList();
            _waiters.Clear();
        }
        foreach (var entry in toCancel)
        {
            entry.Registration.Dispose();
            entry.Tcs.TrySetCanceled();
        }
    }
}
