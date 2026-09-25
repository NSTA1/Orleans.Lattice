using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-silo single-flight coalescer for the tree-wide saga-registry reads a
/// multi-key read (<c>GetManyAsync</c>, key and entry scans, cursors) issues on
/// every call (issue #3501). With the registry sharded, one such read costs one
/// registry call per shard plus the legacy registry; concurrent reads on the
/// same tree on the same silo share one round of those calls instead of each
/// paying for its own.
/// <para>
/// Two sharing rules, one per read role:
/// </para>
/// <list type="bullet">
/// <item><description>
/// <b>Pre-fan-out snapshot (<see cref="GetSnapshotAsync"/>)</b>: a caller joins
/// any round already in flight. Sharing a snapshot captured slightly before the
/// caller arrived is safe, because the caller's post-fan-out stability check
/// still detects any decision that moved after the snapshot and retries.
/// </description></item>
/// <item><description>
/// <b>Post-fan-out probes (<see cref="GetRevisionAsync"/> and
/// <see cref="GetFreshSnapshotAsync"/>)</b>: a caller may only share a round
/// whose registry calls are issued after the caller arrived, because the probe
/// exists to observe every decision that moved during the caller's fan-out. A
/// round already in flight when a caller arrives was issued before that
/// caller's fan-out finished, so the caller never joins it; it joins (or opens)
/// the next round, which is issued only once the current one completes. At most
/// one round is in flight and one is queued per tree and probe kind.
/// </description></item>
/// </list>
/// <para>
/// Rounds are never cached: a round's entry is dropped as soon as it completes,
/// so a faulted round is observed only by the callers that shared it, and the
/// next caller always issues a fresh one. The shared work ignores any caller's
/// cancellation; a caller abandons its wait through
/// <see cref="Task.WaitAsync(CancellationToken)"/> without cancelling the round
/// for the others. Round work runs with a suppressed execution context, so no
/// caller's ambient <c>RequestContext</c> leaks into a call made on behalf of
/// another.
/// </para>
/// </summary>
internal sealed class TxRegistryReadCoalescer
{
    private readonly IGrainFactory _grainFactory;
    private readonly IOptionsMonitor<LatticeOptions> _options;
    private readonly ConcurrentDictionary<string, JoinSlot<TxRegistrySnapshot>> _snapshots = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, FreshSlot<TxRegistrySnapshot>> _freshSnapshots = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, FreshSlot<long>> _revisions = new(StringComparer.Ordinal);

    /// <summary>
    /// Creates the coalescer.
    /// </summary>
    /// <param name="grainFactory">The grain factory used to reach the registry.</param>
    /// <param name="options">The options monitor the shard count is read from.</param>
    public TxRegistryReadCoalescer(IGrainFactory grainFactory, IOptionsMonitor<LatticeOptions> options)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(options);
        _grainFactory = grainFactory;
        _options = options;
    }

    /// <summary>Number of trees with a round in flight or queued. Exposed for tests.</summary>
    internal int ActiveTreeCount => _snapshots.Count + _freshSnapshots.Count + _revisions.Count;

    /// <summary>
    /// Returns the tree-wide decisions snapshot and summed revision, joining any
    /// round already in flight for <paramref name="treeId"/>.
    /// </summary>
    /// <param name="treeId">The physical tree id.</param>
    /// <param name="cancellationToken">Abandons this caller's wait only.</param>
    /// <returns>The snapshot.</returns>
    public Task<TxRegistrySnapshot> GetSnapshotAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var shardCount = TxRegistryRouting.ResolveShardCount(_options);
        var task = JoinSlot<TxRegistrySnapshot>.Join(
            _snapshots,
            treeId,
            () => TxRegistryFanOut.SnapshotWithRevisionAsync(_grainFactory, treeId, shardCount));
        return cancellationToken.CanBeCanceled ? task.WaitAsync(cancellationToken) : task;
    }

    /// <summary>
    /// Returns the tree-wide decisions snapshot and summed revision from a round
    /// issued after this call began.
    /// </summary>
    /// <param name="treeId">The physical tree id.</param>
    /// <param name="cancellationToken">Abandons this caller's wait only.</param>
    /// <returns>The snapshot.</returns>
    public Task<TxRegistrySnapshot> GetFreshSnapshotAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var shardCount = TxRegistryRouting.ResolveShardCount(_options);
        var task = FreshSlot<TxRegistrySnapshot>.Join(
            _freshSnapshots,
            treeId,
            () => TxRegistryFanOut.SnapshotWithRevisionAsync(_grainFactory, treeId, shardCount));
        return cancellationToken.CanBeCanceled ? task.WaitAsync(cancellationToken) : task;
    }

    /// <summary>
    /// Returns the tree-wide summed decisions revision from a round issued after
    /// this call began.
    /// </summary>
    /// <param name="treeId">The physical tree id.</param>
    /// <param name="cancellationToken">Abandons this caller's wait only.</param>
    /// <returns>The summed revision.</returns>
    public Task<long> GetRevisionAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var shardCount = TxRegistryRouting.ResolveShardCount(_options);
        var task = FreshSlot<long>.Join(
            _revisions,
            treeId,
            () => TxRegistryFanOut.GetDecisionsRevisionAsync(_grainFactory, treeId, shardCount));
        return cancellationToken.CanBeCanceled ? task.WaitAsync(cancellationToken) : task;
    }

    /// <summary>
    /// Starts <paramref name="work"/> on the thread pool with a suppressed
    /// execution context, so no caller's ambient state flows into it.
    /// </summary>
    private static Task<T> Issue<T>(Func<Task<T>> work)
    {
        using (ExecutionContext.SuppressFlow())
        {
            return Task.Run(work);
        }
    }

    /// <summary>
    /// A slot any caller joins while its round is in flight.
    /// </summary>
    internal sealed class JoinSlot<T>
    {
        private Task<T>? _inFlight;

        /// <summary>
        /// Joins the round in flight for <paramref name="key"/>, or opens one.
        /// </summary>
        public static Task<T> Join(ConcurrentDictionary<string, JoinSlot<T>> slots, string key, Func<Task<T>> work)
        {
            while (true)
            {
                var slot = slots.GetOrAdd(key, static _ => new JoinSlot<T>());
                lock (slot)
                {
                    // A slot removed from the map after we fetched it is retired;
                    // retry against the live one.
                    if (!slots.TryGetValue(key, out var live) || !ReferenceEquals(live, slot))
                    {
                        continue;
                    }

                    if (slot._inFlight is { } existing)
                    {
                        return existing;
                    }

                    var round = Issue(work);
                    slot._inFlight = round;
                    round.ContinueWith(
                        static (_, state) =>
                        {
                            var (s, k, map) = ((JoinSlot<T>, string, ConcurrentDictionary<string, JoinSlot<T>>))state!;
                            lock (s)
                            {
                                s._inFlight = null;
                                map.TryRemove(new KeyValuePair<string, JoinSlot<T>>(k, s));
                            }
                        },
                        (slot, key, slots),
                        CancellationToken.None,
                        TaskContinuationOptions.ExecuteSynchronously,
                        TaskScheduler.Default);
                    return round;
                }
            }
        }
    }

    /// <summary>
    /// A slot whose callers share only a round issued after they arrived: one
    /// round in flight plus at most one queued round, issued when the in-flight
    /// one completes.
    /// </summary>
    internal sealed class FreshSlot<T>
    {
        private Task<T>? _inFlight;
        private TaskCompletionSource<T>? _queued;
        private Func<Task<T>>? _queuedWork;

        /// <summary>
        /// Joins the queued round for <paramref name="key"/>, queues one behind
        /// the round in flight, or issues one immediately when none is in flight.
        /// </summary>
        public static Task<T> Join(ConcurrentDictionary<string, FreshSlot<T>> slots, string key, Func<Task<T>> work)
        {
            while (true)
            {
                var slot = slots.GetOrAdd(key, static _ => new FreshSlot<T>());
                lock (slot)
                {
                    if (!slots.TryGetValue(key, out var live) || !ReferenceEquals(live, slot))
                    {
                        continue;
                    }

                    if (slot._inFlight is null)
                    {
                        // Nothing in flight: issue now. Every caller that joins
                        // from here on arrived after this issue, so it may not
                        // share it and queues instead.
                        return slot.Start(slots, key, work);
                    }

                    // A round is in flight and was issued before this caller
                    // arrived. Share the queued round, which is issued only after
                    // the in-flight one completes and therefore after this caller.
                    if (slot._queued is null)
                    {
                        slot._queued = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
                        slot._queuedWork = work;
                    }

                    return slot._queued.Task;
                }
            }
        }

        private Task<T> Start(ConcurrentDictionary<string, FreshSlot<T>> slots, string key, Func<Task<T>> work)
        {
            var round = Issue(work);
            _inFlight = round;
            round.ContinueWith(
                static (_, state) =>
                {
                    var (s, k, map) = ((FreshSlot<T>, string, ConcurrentDictionary<string, FreshSlot<T>>))state!;
                    s.OnRoundCompleted(map, k);
                },
                (this, key, slots),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
            return round;
        }

        private void OnRoundCompleted(ConcurrentDictionary<string, FreshSlot<T>> slots, string key)
        {
            lock (this)
            {
                _inFlight = null;
                if (_queued is { } queued)
                {
                    var work = _queuedWork!;
                    _queued = null;
                    _queuedWork = null;
                    var round = Start(slots, key, work);
                    round.ContinueWith(
                        static (t, state) =>
                        {
                            var tcs = (TaskCompletionSource<T>)state!;
                            if (t.IsFaulted) tcs.TrySetException(t.Exception!.InnerExceptions);
                            else if (t.IsCanceled) tcs.TrySetCanceled();
                            else tcs.TrySetResult(t.Result);
                        },
                        queued,
                        CancellationToken.None,
                        TaskContinuationOptions.ExecuteSynchronously,
                        TaskScheduler.Default);
                    return;
                }

                slots.TryRemove(new KeyValuePair<string, FreshSlot<T>>(key, this));
            }
        }
    }
}
