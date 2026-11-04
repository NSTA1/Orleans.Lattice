using System.Diagnostics;
using System.Text;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Keeps a replay that holds a per-silo replay permit from waiting on a grain call
/// that may itself need one (issue #5775), and records who holds the permits.
/// <para>
/// A permit-holding replay made three such calls: the shard-map lookup on the
/// registry, which is backed by a system tree whose leaves replay under the same
/// gate; the forward of a replayed commit to the leaf that now declares the key,
/// whose first await is that leaf's replay barrier; and pass 2.5's status resolve
/// on the tx registry. Once every holder waits on a call whose target is queued
/// for a permit, the gate deadlocks until a response timeout fails the call, and
/// every other activation on the silo queues or is refused meanwhile.
/// </para>
/// <para>
/// The shard-map lookup, the forward and the status resolve each return the
/// permit to the gate for the length of the call and queue for it again
/// afterwards, in FIFO order behind any waiter already queued. That keeps the
/// replay's apply order and its flush and reconciliation steps unchanged. The
/// activation replay is still inside its barrier while the permit is out, so the
/// activation serves nothing in the gap. A starvation drive runs on a live leaf
/// whose barrier is already open; it keeps serving as before, and returning the
/// permit changes nothing it serves. The shard-map lookup stays after the acquisition
/// rather than moving ahead of the queue wait: a map read before the wait could
/// predate a reshard and filter out records for a slot this shard now owns.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// The permit the current replay holds, scoped to that replay's async flow.
    /// <para>
    /// An <see cref="AsyncLocal{T}"/> rather than a field, because the same
    /// seams (the forward and the status resolve) also run in ordinary request
    /// turns that hold no permit; a field would let such a turn release a permit
    /// a concurrent replay on the same activation holds. It is set once per
    /// acquisition, on the replay path only, never on a write.
    /// </para>
    /// </summary>
    private static readonly AsyncLocal<ReplayPermitLease?> HeldReplayPermit = new();

    /// <summary>Bound on the permit-holder census; holders beyond it are only counted.</summary>
    internal const int ReplayPermitCensusCapacity = 64;

    /// <summary>A frame's census slot before it has entered a permit scope.</summary>
    private const int NoReplayPermitScope = -2;

    private static readonly BPlusLeafGrain?[] ReplayPermitCensus = new BPlusLeafGrain?[ReplayPermitCensusCapacity];
    private static readonly long[] ReplayPermitCensusSince = new long[ReplayPermitCensusCapacity];
    private static int _replayPermitCensusOverflow;

    /// <summary>How many of this activation's replays have their permit out on a nested call.</summary>
    private int _replayPermitsSuspended;

    /// <summary>
    /// Test hook, invoked with the leaf at the moment a replay returns its permit
    /// for a nested call, before the permit is released.
    /// </summary>
    internal static Action<BPlusLeafGrain>? ReplayPermitSuspendedForTest;

    /// <summary>
    /// Test-only cap on the replay gate's ceiling, applied when the gate is sized;
    /// zero leaves the resolved ceiling. A low cap makes a nested permit wait
    /// deterministic rather than load-dependent (issue #5775).
    /// </summary>
    internal static int ReplayCeilingCapForTest;

    /// <summary>
    /// The tree id and metric tree id the permit path tags with, read from state
    /// before the queue wait. Everything between taking a permit and returning it
    /// reads these rather than grain state: an activation can be invalidated while
    /// its replay waits, and a state read then throws, which once stranded the
    /// permit for the life of the process (issue #5775).
    /// </summary>
    private string? _replayPermitTreeId;

    /// <inheritdoc cref="_replayPermitTreeId"/>
    private string? _replayPermitMetricTreeId;

    private GrainId CensusGrainId => context.GrainId;

    private string? CensusTreeId => _replayPermitTreeId;

    /// <summary>Test view of how many of this activation's replays have their permit out.</summary>
    internal int ReplayPermitsSuspendedForTest => Volatile.Read(ref _replayPermitsSuspended);

    /// <summary>Test view of whether this activation's replay barrier has been satisfied.</summary>
    internal bool ReplayBarrierSatisfiedForTest => _replayBarrierSatisfied;

    /// <summary>Test view of the census: the activations currently recorded as holding a permit.</summary>
    internal static int ReplayPermitCensusCountForTest
    {
        get
        {
            var count = 0;
            for (var i = 0; i < ReplayPermitCensus.Length; i++)
            {
                if (Volatile.Read(ref ReplayPermitCensus[i]) is not null)
                    count++;
            }

            return count;
        }
    }

    /// <summary>
    /// Who physically holds one acquired replay permit: the frame that acquired
    /// it, or nobody while a nested call has it out. One per acquisition, on the
    /// replay path only.
    /// </summary>
    private sealed class ReplayPermitLease(SemaphoreSlim gate)
    {
        /// <summary>The frame holds the permit.</summary>
        internal const int Held = 0;

        /// <summary>The permit is back in the gate for a nested call.</summary>
        internal const int Out = 1;

        /// <summary>The owning frame has ended; a resume releases what it re-takes.</summary>
        internal const int Ended = 2;

        internal readonly SemaphoreSlim Gate = gate;

        internal int State;
    }

    /// <summary>
    /// Marks <paramref name="permit"/> as held by the current replay and records
    /// this activation in the census. Must be called from the frame that owns the
    /// permit, so the flow-local value covers exactly that frame's awaits.
    /// </summary>
    /// <returns>The census slot, or -1 when the census is full.</returns>
    private int EnterReplayPermitScope(SemaphoreSlim permit)
    {
        HeldReplayPermit.Value = new ReplayPermitLease(permit);
        for (var i = 0; i < ReplayPermitCensus.Length; i++)
        {
            if (Interlocked.CompareExchange(ref ReplayPermitCensus[i], this, null) is null)
            {
                Volatile.Write(ref ReplayPermitCensusSince[i], Stopwatch.GetTimestamp());
                return i;
            }
        }

        Interlocked.Increment(ref _replayPermitCensusOverflow);
        return -1;
    }

    /// <summary>
    /// Ends the scope <see cref="EnterReplayPermitScope"/> opened. Called from the
    /// owning frame's <c>finally</c>, before the permit is released.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when the frame still holds the permit and must
    /// release it; <see langword="false"/> when a nested call has it out, in which
    /// case the resume releases what it re-takes. Only a frame that abandons its
    /// replay mid-call (the starvation drive's budget) can see the second.
    /// </returns>
    private static bool ExitReplayPermitScope(int slot)
    {
        if (slot == NoReplayPermitScope)
            return true;

        var lease = HeldReplayPermit.Value;
        HeldReplayPermit.Value = null;
        if (slot >= 0)
            Volatile.Write(ref ReplayPermitCensus[slot], null);
        else
            Interlocked.Decrement(ref _replayPermitCensusOverflow);

        return lease is null
            || Interlocked.Exchange(ref lease.State, ReplayPermitLease.Ended) == ReplayPermitLease.Held;
    }

    /// <summary>
    /// Returns the current replay's permit to the gate before a grain call that may
    /// need one. A no-op, returning <see langword="null"/>, outside a permit-holding
    /// replay or while the permit is already out. The caller must pass a non-null
    /// result to <see cref="ResumeReplayPermitAsync"/> in a <c>finally</c>.
    /// </summary>
    private object? SuspendReplayPermit()
    {
        var lease = HeldReplayPermit.Value;
        if (lease is null
            || Interlocked.CompareExchange(ref lease.State, ReplayPermitLease.Out, ReplayPermitLease.Held) != ReplayPermitLease.Held)
        {
            return null;
        }

        Interlocked.Increment(ref _replayPermitsSuspended);
        ReplayPermitSuspendedForTest?.Invoke(this);
        lease.Gate.Release();
        return lease;
    }

    /// <summary>
    /// Takes the permit <see cref="SuspendReplayPermit"/> returned back from the
    /// gate. The wait is FIFO behind any waiter already queued, and it is not
    /// cancellable: the replay must hold its permit again before control returns
    /// to the frame that releases it. It strands nothing, because the replay holds
    /// no permit while it waits. If that frame has ended meanwhile, the permit
    /// just taken is released at once.
    /// <para>
    /// Nothing between the wait and the hand-back can throw: the lease changes
    /// state first, and the bookkeeping after it reads no grain state. The
    /// activation may have been invalidated while it waited, and a state read
    /// would then throw with the permit taken but the lease still out, so the
    /// frame would release the permit it no longer held and the re-take would
    /// never be returned.
    /// </para>
    /// </summary>
    private async Task ResumeReplayPermitAsync(object suspended)
    {
        var lease = (ReplayPermitLease)suspended;
        var queuedAt = Stopwatch.GetTimestamp();
        NoteReplayPermitArrival();
        try
        {
            var permitWaitScope = LatticeMetrics.EnterWalReplayPermitWait(CensusTreeId);
            try
            {
                await lease.Gate.WaitAsync().ConfigureAwait(true);
            }
            finally
            {
                permitWaitScope.Dispose();
            }
        }
        finally
        {
            NoteReplayPermitDeparture();
            Interlocked.Decrement(ref _replayPermitsSuspended);
        }

        if (Interlocked.CompareExchange(ref lease.State, ReplayPermitLease.Held, ReplayPermitLease.Out) != ReplayPermitLease.Out)
            lease.Gate.Release();

        // Recorded as an ordinary acquisition, so the queue's progress stamp and
        // smoothed wait see a re-take exactly as they see a first take; otherwise
        // sustained re-takes would read as a queue that is not draining.
        RecordReplayPermitQueueWait(queuedAt, LatticeMetrics.PermitQueueWaitAcquired);
    }
    /// <summary>
    /// Describes the activations recorded as holding a permit, for the
    /// no-progress refusal. Allocates only when a refusal is raised.
    /// </summary>
    internal static string DescribeReplayPermitHolders()
    {
        var now = Stopwatch.GetTimestamp();
        var text = new StringBuilder(" Permit holders:");
        var listed = 0;
        for (var i = 0; i < ReplayPermitCensus.Length; i++)
        {
            var holder = Volatile.Read(ref ReplayPermitCensus[i]);
            if (holder is null)
                continue;

            listed++;
            if (listed > 8)
                continue;

            text.Append(' ')
                .Append(holder.CensusGrainId)
                .Append(" (tree ")
                .Append(holder.CensusTreeId)
                .Append(", held ")
                .Append(Stopwatch.GetElapsedTime(Volatile.Read(ref ReplayPermitCensusSince[i]), now).TotalSeconds.ToString("F0", System.Globalization.CultureInfo.InvariantCulture))
                .Append(" s")
                .Append(Volatile.Read(ref holder._replayPermitsSuspended) > 0 ? ", permit out on a nested call" : string.Empty)
                .Append(");");
        }

        if (listed == 0)
            text.Append(" none recorded;");
        else if (listed > 8)
            text.Append(' ').Append(listed - 8).Append(" more;");

        var overflow = Volatile.Read(ref _replayPermitCensusOverflow);
        if (overflow > 0)
            text.Append(' ').Append(overflow).Append(" beyond the census bound;");

        text.Append(" free permits ")
            .Append(Volatile.Read(ref _replayConcurrencyGate)?.CurrentCount ?? 0)
            .Append(", queued waiters ")
            .Append(QueuedReplayPermitWaiters)
            .Append('.');

        return text.ToString();
    }

    /// <summary>Clears the census. Test-only, alongside the gate reset.</summary>
    internal static void ResetReplayPermitCensusForTest()
    {
        for (var i = 0; i < ReplayPermitCensus.Length; i++)
            Volatile.Write(ref ReplayPermitCensus[i], null);
        Volatile.Write(ref _replayPermitCensusOverflow, 0);
        ReplayPermitSuspendedForTest = null;
    }
}
