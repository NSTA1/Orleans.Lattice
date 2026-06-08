using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

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
internal sealed class WalSaturationSignal : IWalSaturationSignal
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
            if (GetCurrentState(treeId) == WalSaturationState.Healthy)
            {
                return Task.CompletedTask;
            }
            if (!_waiters.TryGetValue(treeId, out var list))
            {
                list = new List<WaiterEntry>(capacity: 2);
                _waiters[treeId] = list;
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
                var pair = ((WalSaturationSignal Signal, string TreeId, WaiterEntry Entry))state!;
                if (pair.Entry.Tcs.TrySetCanceled())
                {
                    // Remove the cancelled entry from the wait list so
                    // a later recovery does not see it.
                    lock (pair.Signal._waitGate)
                    {
                        if (pair.Signal._waiters.TryGetValue(pair.TreeId, out var list))
                        {
                            list.Remove(pair.Entry);
                            if (list.Count == 0)
                            {
                                pair.Signal._waiters.Remove(pair.TreeId);
                            }
                        }
                    }
                    pair.Entry.Registration.Dispose();
                }
            }, (this, treeId, entry));
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
    internal WalSaturationState UpdateState(string treeId, WalSaturationState newState)
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

        if (newState == WalSaturationState.Healthy && previous != WalSaturationState.Healthy)
        {
            // Drain every pending waiter for this tree. The TCSs were
            // built with RunContinuationsAsynchronously so the
            // completion does not run inline on the sampler thread.
            // Disposing the cancellation registration inline keeps the
            // per-wait disposal cost off a separate continuation Task
            // (the per-await ContinueWith chain the entry was
            // explicitly designed to avoid).
            List<WaiterEntry>? toComplete = null;
            lock (_waitGate)
            {
                if (_waiters.TryGetValue(treeId, out var list))
                {
                    toComplete = list;
                    _waiters.Remove(treeId);
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

        return previous;
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
            yield return new Measurement<long>(
                (long)kv.Value,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, kv.Key),
                new KeyValuePair<string, object?>(LatticeMetrics.TagWalSaturationState, StateTagValue(kv.Value)));
        }
    }

    /// <summary>
    /// Lowercased tag value for the <see cref="LatticeMetrics.TagWalSaturationState"/>
    /// dimension on the saturation transitions counter and the
    /// observable state gauge. Centralised here so the writer-side
    /// metric site and the sampler agree on the spelling.
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
