using System.Diagnostics;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The reserved system replay lane (issue #5569).
/// <para>
/// <b>Why it exists.</b> Every leaf activation replays its WAL tail behind one
/// per-silo permit gate, and that gate is first come, first served. When the WAL
/// store is slow - a schema migration rewriting every value, a resize, a view
/// rebuild reading a log GC could not trim - the replays that hold the permits
/// run for minutes, and the gate's <c>no_progress</c> arm then refuses every new
/// arrival. The small system trees that authorization and membership read on
/// every request (<c>sys-membership-groups</c>, <c>sys-auth-policy</c>) were
/// among the refused arrivals, so a bulk operation on one user tree took the
/// sign-in path down for minutes.
/// </para>
/// <para>
/// <b>The rule.</b> A replay of a tree <see cref="SystemTreeNamespace.IsSystemOwned"/>
/// accepts takes a shared permit if one is free. Otherwise it waits on this lane,
/// whose single permit no other replay can take, and it is never judged by the
/// shared gate's admission arms, because the shared gate's progress says nothing
/// about the lane's. Concurrency therefore stays bounded by the shared ceiling
/// plus <see cref="SystemReplayLaneCapacity"/>.
/// </para>
/// <para>
/// <b>Why lane waiters are not refused.</b> The replay barrier runs once per
/// activation and every retried request awaits the same barrier, so a silo
/// queues at most one lane waiter per system-tree leaf it hosts. That
/// population is small and bounded by the system trees, not by user load.
/// </para>
/// <para>
/// <b>The assumption the lane rests on</b> is that a system replay is bounded.
/// The lane is held only for the guarded replay region of one activation, and
/// is returned in the same <c>finally</c> as a shared permit. A system replay
/// that never ends would wedge the lane, exactly as it would hold a shared
/// permit; the lane protects system trees from user-tree replays, not from
/// themselves. <c>spec/scheduling/ReplayPermits.tla</c> states the same
/// premise on its <c>SystemLaneProgress</c> property.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>The number of permits reserved for system-tree replays.</summary>
    internal const int SystemReplayLaneCapacity = 1;

    /// <summary>
    /// The reserved lane. Sized once and never re-created, for the same reason
    /// the shared gate is not: a permit lost between acquisition and release
    /// would be lost for the lifetime of the process.
    /// </summary>
    private static readonly SemaphoreSlim _systemReplayLane =
        new(SystemReplayLaneCapacity, SystemReplayLaneCapacity);

    /// <summary>
    /// Count of system-tree replays that entered the lane wait and have not
    /// left it. Incremented before the wait and decremented in a <c>finally</c>.
    /// </summary>
    private static int _queuedSystemReplayLaneWaiters;

    /// <summary>Test-only view of the reserved lane.</summary>
    internal static SemaphoreSlim SystemReplayLaneForTest => _systemReplayLane;

    /// <summary>Test-only view of the lane's waiter count.</summary>
    internal static int QueuedSystemReplayLaneWaitersForTest => Volatile.Read(ref _queuedSystemReplayLaneWaiters);

    /// <summary>Whether <paramref name="permit"/> is the reserved lane rather than the shared gate.</summary>
    internal static bool IsSystemReplayLane(SemaphoreSlim? permit)
        => ReferenceEquals(permit, _systemReplayLane);

    /// <summary>
    /// Acquires a replay permit for a system-tree leaf: a free shared permit if
    /// there is one, else the reserved lane. Never refuses.
    /// </summary>
    /// <param name="gate">The shared replay gate.</param>
    /// <param name="cancellationToken">Cancels the lane wait.</param>
    /// <returns>The semaphore to release when the replay ends.</returns>
    private async Task<SemaphoreSlim> AcquireSystemReplayPermitAsync(
        SemaphoreSlim gate, CancellationToken cancellationToken)
    {
        var queuedAt = Stopwatch.GetTimestamp();
        if (gate.Wait(0))
        {
            // Took a free shared permit without waiting: account it exactly as an
            // uncontended shared acquisition, so the gate's own evidence is
            // unchanged by who took it.
            RecordReplayPermitQueueWait(queuedAt, LatticeMetrics.PermitQueueWaitAcquired);
            _replayAdmissionPhase = ReplayAdmissionPhase.HoldsPermit;
            return gate;
        }

        _replayAdmissionPhase = ReplayAdmissionPhase.QueuedForPermit;
        Interlocked.Increment(ref _queuedSystemReplayLaneWaiters);
        try
        {
            var permitWaitScope = LatticeMetrics.EnterWalReplayPermitWait(state.State.TreeId);
            try
            {
                try
                {
                    await _systemReplayLane.WaitAsync(cancellationToken).ConfigureAwait(true);
                }
                catch (OperationCanceledException)
                {
                    RecordSystemReplayLaneWait(queuedAt, LatticeMetrics.PermitQueueWaitCanceled);
                    throw;
                }

                RecordSystemReplayLaneWait(queuedAt, LatticeMetrics.PermitQueueWaitAcquired);
            }
            finally
            {
                permitWaitScope.Dispose();
            }
        }
        finally
        {
            Interlocked.Decrement(ref _queuedSystemReplayLaneWaiters);
        }

        _replayAdmissionPhase = ReplayAdmissionPhase.HoldsPermit;
        return _systemReplayLane;
    }

    /// <summary>
    /// Records a lane wait on the queue-wait histogram only. It deliberately does
    /// not fold into the shared gate's smoothed wait or stamp its progress: a
    /// lane acquisition is not evidence the shared queue is moving, and folding
    /// it in would let the lane mask a wedged shared pool from its own refusal
    /// arms.
    /// </summary>
    private void RecordSystemReplayLaneWait(long queuedAt, KeyValuePair<string, object?> outcome)
    {
        var treeId = state.State.TreeId;
        LatticeMetrics.WalReplayPermitQueueWait.Record(
            Stopwatch.GetElapsedTime(queuedAt).TotalMilliseconds,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, MetricTreeId),
            outcome,
            LatticeTenantLabel.ForTree(treeId));
    }

    /// <summary>
    /// Returns a lane permit. The lane takes no part in memory or service-rate
    /// withholding: those adapt the shared ceiling, and withholding the lane's
    /// only permit would reintroduce the starvation it exists to prevent.
    /// The hold is recorded, but not as a served shared permit, so the shared
    /// gate's service rate still describes the shared gate.
    /// </summary>
    private static void ReleaseSystemReplayLane(long acquiredAt, string metricTreeId, string? treeId)
    {
        _systemReplayLane.Release();
        try
        {
            LatticeMetrics.WalReplayPermitHold.Record(
                Stopwatch.GetElapsedTime(acquiredAt).TotalMilliseconds,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, metricTreeId),
                LatticeTenantLabel.ForTree(treeId));
        }
        catch
        {
            // Diagnostics never interrupt the finally that owns the release.
        }
    }
}
