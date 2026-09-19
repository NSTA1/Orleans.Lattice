using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The replay barrier (issue #2871). WAL replay no longer runs <i>on</i> the leaf's
/// activation critical path; it runs behind a once-only guarded barrier that
/// <b>data operations</b> await, while the activation itself completes
/// immediately.
/// <para>
/// <b>Why the activation cannot own the replay.</b> Replay runs behind a
/// per-silo concurrency permit, so losing the race for a permit used to destroy
/// the whole activation. That one fact produced three separate symptoms: a
/// cancelled activation banks no snapshot, so its durable materialiser pin stays
/// at Zero and blocks its tree's WAL cursor floor; the remedy for a blocked pin
/// (the WAL GC blocked-leaf reactivation sweep, issues #2768 / #2870) has to make
/// a grain call into the very activation that cannot complete, so its
/// <c>GetTreeIdAsync()</c> probe times out; and no setting of the permit ceiling
/// avoids it, because too many permits exhausts the heap and too few queues past
/// the activation timeout, and both ends of the dial cancel activations.
/// </para>
/// <para>
/// <b>The barrier is STARTED by the activation, not by the first data operation, and
/// that distinction is load-bearing.</b> A purely lazy barrier - replay runs only
/// when a data operation arrives - satisfies every acceptance criterion #2871
/// wrote down and still leaves WAL GC blocked: the sweep's remedy works by
/// causing the leaf to <i>activate and repair itself</i>, and the probe is only
/// the trigger. Under a lazy barrier the probe would return instantly, the sweep's
/// <c>undelivered</c> arm would fall to zero and <c>completed</c> would rise,
/// while <c>healed</c> stayed at zero forever - the metric would read fixed while
/// nothing was reclaimed. It is the main case rather than an edge case, because a
/// quiesced tree (issue #2692) is precisely one where the data operation that
/// would trigger a lazy replay is the thing that never arrives. So the activation
/// arms the barrier and returns; the repair still runs on a touch alone.
/// </para>
/// <para>
/// <b>Failure is re-armable, never terminal.</b> A barrier task that faults or is
/// cancelled clears itself, so the next data operation - or the next WAL GC touch
/// - starts a fresh replay. A permanently faulted barrier would recreate the wedge
/// this issue exists to remove, with extra steps. The failing request sees the
/// fault; the activation stays usable.
/// </para>
/// <para>
/// <b>Failure is also observable.</b> Moving replay off the activation path
/// trades a loud failure (a destroyed activation) for a quiet one (a live,
/// healthy-looking activation whose data operations hang), so every terminal
/// state of the barrier is counted on
/// <see cref="LatticeMetrics.LeafReplayBarrierOutcomes"/>, zero-primed when the barrier
/// is armed so a zero on <c>faulted</c> is a measured zero rather than an
/// unpublished series.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// The in-flight (or completed) replay for this activation, or
    /// <see langword="null"/> when no replay is armed - which is the state after
    /// a fault, a cancellation, or a retirement.
    /// </summary>
    private Task? _replayBarrier;

    /// <summary>
    /// Set once the barrier has completed successfully, so the overwhelmingly common
    /// case (every data operation after the first replay) costs one field read and
    /// allocates nothing.
    /// </summary>
    private bool _replayBarrierSatisfied;

    /// <summary>
    /// Cancels the in-flight replay when the activation goes away, or when an
    /// operation supersedes the replay outright
    /// (<see cref="ClearGrainStateAsync"/>, <see cref="RebuildProjectionFromWalAsync"/>).
    /// </summary>
    private CancellationTokenSource? _replayBarrierCts;

    /// <summary>
    /// Set when the replay has been deliberately retired for the remainder of this
    /// activation, because the state it would rebuild has been discarded. Distinct
    /// from <see cref="_replayBarrierSatisfied"/>: nothing replayed, and nothing
    /// should.
    /// </summary>
    private bool _replayBarrierRetired;

    /// <summary>
    /// Set by <see cref="EnsureReplayStarted"/> and never cleared, recording that
    /// <b>this activation requires a replay</b> - as distinct from
    /// <see cref="_replayBarrier"/>, which is merely the attempt currently in
    /// flight and is nulled whenever an attempt fails.
    /// </summary>
    /// <remarks>
    /// <para>
    /// It is what lets a failed replay be re-armed by the next request without
    /// letting a request <i>invent</i> a replay that was never armed. Those two
    /// states are otherwise indistinguishable: both present as
    /// <c>_replayBarrier is null</c>, and conflating them is a bug in whichever
    /// direction it is resolved. Treat a null barrier as "nothing to wait for" and
    /// a request arriving after a faulted replay reads an unreplayed projection -
    /// the silent wrong-data hole this issue's acceptance criteria are built
    /// around. Treat it as "arm one" and a grain whose activation hook never ran
    /// starts a replay nobody asked for.
    /// </para>
    /// <para>
    /// The second case is not hypothetical, and it is not only a test artefact.
    /// Orleans guarantees the activation hook runs before any request reaches the
    /// grain, so in a silo the flag is always set by the time a data operation
    /// arrives and this condition never fires. Off that path - a directly
    /// constructed grain - the old code ran no replay either, because the replay
    /// WAS the activation hook. Keying on the flag preserves that exactly, rather
    /// than making every construction of the type start WAL traffic.
    /// </para>
    /// </remarks>
    private bool _replayBarrierArmed;

    /// <summary>
    /// Test seam: <see langword="true"/> while a replay is armed and has not yet
    /// completed. Exists so a test asserting that a metadata getter answers
    /// "while a replay is pending" can establish that the replay really <i>is</i>
    /// pending at the moment of the call, rather than passing against a leaf with
    /// nothing to replay.
    /// </summary>
    internal bool IsReplayPending
        => !_replayBarrierSatisfied && !_replayBarrierRetired && _replayBarrier is { IsCompleted: false };

    /// <summary>
    /// Test seam: the armed replay task, so a test can wait for the background
    /// replay to settle without racing it through a data operation (which would
    /// await the barrier and so could not observe the pending state it is asserting).
    /// </summary>
    internal Task? ReplayBarrierForTest => _replayBarrier;

    /// <summary>
    /// Arms the replay if it is not already armed, and returns <b>without waiting
    /// for it</b>. Never throws.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Called from the activation hook, and from <see cref="GetTreeIdAsync"/> -
    /// the WAL GC sweep's probe. The second call site is what makes the sweep's
    /// remedy work on a leaf whose previous replay faulted: the touch re-arms it.
    /// It is a deliberate side effect on a getter, and it is the narrow one that
    /// matters, because a touch whose only purpose is to cause the repair would
    /// otherwise cause nothing at all.
    /// </para>
    /// <para>
    /// The task is started on the grain's own scheduler, so its continuations are
    /// ordinary grain turns that interleave with requests at await points rather
    /// than running concurrently with them. The leaf stays single-threaded.
    /// </para>
    /// </remarks>
    private void EnsureReplayStarted()
    {
        _replayBarrierArmed = true;

        if (_replayBarrierSatisfied || _replayBarrierRetired || _replayBarrier is not null)
            return;

        var cts = new CancellationTokenSource();
        _replayBarrierCts = cts;

        LatticeMetrics.PrimeReplayBarrierOutcomes(state.State.TreeId);

        // Started, deliberately not awaited: this is the whole point of the
        // change. The fault is observed twice over - by whoever awaits the barrier,
        // and by the continuation below - so a replay nobody is waiting on cannot
        // become an unobserved task exception.
        var barrier = RunReplayBarrierAsync(cts.Token);
        _replayBarrier = barrier;

        _ = barrier.ContinueWith(
            static t => _ = t.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <summary>
    /// Awaited by every data-path entry point before it reads or writes the
    /// projection. Completes immediately once the replay has been applied.
    /// </summary>
    /// <remarks>
    /// A <see cref="ValueTask"/> rather than a <see cref="Task"/> so the satisfied
    /// path - which is every call after the first replay - allocates nothing.
    /// </remarks>
    private ValueTask AwaitReplayBarrierAsync()
    {
        // _replayBarrierArmed, NOT _replayBarrier: a faulted replay nulls the
        // latter, and the next request must re-arm rather than sail past it.
        if (_replayBarrierSatisfied || _replayBarrierRetired || !_replayBarrierArmed)
            return ValueTask.CompletedTask;

        return new ValueTask(JoinReplayBarrierAsync());
    }

    /// <summary>
    /// The slow half of <see cref="AwaitReplayBarrierAsync"/>: arms the replay if it
    /// is not armed, waits for it, and - on a failure - disarms it so the next
    /// request re-attempts rather than inheriting a dead barrier.
    /// </summary>
    private async Task JoinReplayBarrierAsync()
    {
        EnsureReplayStarted();

        var barrier = _replayBarrier;
        if (barrier is null)
            return;

        // Wait on the barrier's OWN cancellation token rather than on the replay
        // task alone. Awaiting the task by itself makes acceptance criterion 3 -
        // "a cancelled wait fails the request and leaves the activation usable" -
        // conditional on every await inside the replay being prompt about its
        // token, because a replay parked in an await that ignores cancellation
        // keeps its waiters parked with it however long that await runs. That is
        // the same shape of wedge this issue exists to remove, merely moved from
        // the activation to the request, and the awaits below reach host-supplied
        // storage whose cancellation behaviour is not ours to assume. WaitAsync
        // fails the waiter at the moment of cancellation and leaves the replay
        // task itself untouched, so the guarantee is structural rather than
        // inherited from the least cooperative call on the replay path.
        CancellationToken cancellation;
        try
        {
            cancellation = _replayBarrierCts?.Token ?? CancellationToken.None;
        }
        catch (ObjectDisposedException)
        {
            // The source was cancelled and disposed between the two reads, which
            // means cancellation has already happened.
            cancellation = new CancellationToken(canceled: true);
        }

        try
        {
            await barrier.WaitAsync(cancellation);
        }
        catch
        {
            // Disarm, so the NEXT request re-arms a fresh replay. The alternative
            // - leaving the barrier permanently faulted - would wedge every data
            // operation on this activation for as long as it lived, which is the
            // condition this issue exists to remove rather than to relocate.
            //
            // Guarded on identity so a request that lost a race and is observing
            // an already-superseded barrier cannot disarm its replacement.
            //
            // On the cancellation path this can disarm a replay that is still
            // running, so a later request arms a second one alongside it. That is
            // deliberate and bounded: cancellation comes only from deactivation or
            // from retirement, and retirement short-circuits the wait entirely, so
            // in production nothing re-arms afterwards. Where it can happen, replay
            // application is idempotent - entries are merged, not appended - so the
            // cost is duplicated work rather than a wrong projection, and it is the
            // cheaper side of the trade against leaving the activation unusable.
            if (ReferenceEquals(_replayBarrier, barrier))
                _replayBarrier = null;

            // Fails THIS REQUEST and leaves the activation usable (#2871
            // acceptance criterion 3).
            throw;
        }
    }

    /// <summary>
    /// Runs the replay and records its terminal state on
    /// <see cref="LatticeMetrics.LeafReplayBarrierOutcomes"/>.
    /// </summary>
    private async Task RunReplayBarrierAsync(CancellationToken cancellationToken)
    {
        // Read before the first await. The tree id is what the outcome is tagged
        // by, and a birth seam (SetTreeIdAsync / InitializeSiblingAsync) can now
        // interleave with this task, so sampling it later would tag the same
        // replay under two different trees.
        var treeId = state.State.TreeId;

        // Yield before any replay work, unconditionally. Without this the replay
        // runs as far as its first genuine await INSIDE the caller's turn, so a
        // leaf whose WAL slices happen to be served synchronously - from a
        // co-located cache, or a coordinator that answers from memory - would
        // still block the activation hook for the whole replay. That is precisely
        // the failure #2871 exists to remove, and leaving it to depend on whether
        // some seam below happens to be asynchronous would make the guarantee
        // incidental rather than structural. One yield buys an unconditional one:
        // OnActivateAsync and GetTreeIdAsync return having executed no replay
        // work at all.
        await Task.Yield();

        try
        {
            await ExecuteActivationReplayAsync(cancellationToken);

            _replayBarrierSatisfied = true;
            RecordReplayBarrierOutcome(treeId, LatticeMetrics.ReplayBarrierCompleted);
        }
        catch (OperationCanceledException)
        {
            RecordReplayBarrierOutcome(treeId, LatticeMetrics.ReplayBarrierCanceled);
            throw;
        }
        catch (Exception ex)
        {
            RecordReplayBarrierOutcome(treeId, LatticeMetrics.ReplayBarrierFaulted);

            // Rate-limited alongside the counter, reusing the activation path's
            // existing per-silo token so a reactivation storm cannot self-amplify
            // into a log flood.
            //
            // Guarded for the same reason RecordReplayBarrierOutcome is: this runs
            // on the terminal path of a background task, and resolving a logger is
            // not free of failure - the factory is host-supplied and CreateLogger
            // can throw. An unguarded throw here would propagate out of the catch
            // block and REPLACE `ex`, so a logging-sink fault would present as the
            // replay fault and the real cause would never be seen.
            try
            {
                if (ShouldLogCursorPublishFailure())
                {
                    ResolveLogger()?.LogWarning(
                        ex,
                        "Deferred WAL replay failed for leaf {GrainId} on tree {TreeId}. The activation is still alive and "
                        + "metadata getters continue to answer, so this failure is NOT visible as an activation failure; the "
                        + "symptom is that data operations on this leaf fail until a replay succeeds. The barrier is re-armed on "
                        + "the next data operation or the next WAL GC touch.",
                        context.GrainId,
                        treeId);
                }
            }
            catch
            {
                // Intentionally swallowed. Losing the diagnostic is a bounded loss;
                // losing the exception it describes is not.
            }

            throw;
        }
    }

    /// <summary>
    /// Adds one to the replay-barrier outcome counter, never throwing: this runs on
    /// the terminal path of a background task, where an incidental failure in the
    /// observation would replace the fault the caller needs to see.
    /// </summary>
    private static void RecordReplayBarrierOutcome(string? treeId, KeyValuePair<string, object?> outcome)
    {
        try
        {
            LatticeMetrics.LeafReplayBarrierOutcomes.Add(
                1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId ?? string.Empty),
                outcome,
                LatticeTenantLabel.ForTree(treeId ?? string.Empty));
        }
        catch
        {
            // Intentionally swallowed. Losing the diagnostic is a bounded loss;
            // losing the exception it describes is not.
        }
    }

    /// <summary>
    /// Retires the replay for the remainder of this activation, because the state
    /// it would rebuild has just been discarded or is about to be rebuilt from
    /// scratch. Cancels any in-flight replay so it cannot repopulate the cache
    /// behind the caller.
    /// </summary>
    private void RetireReplayBarrier()
    {
        _replayBarrierRetired = true;
        _replayBarrier = null;
        CancelReplayBarrier();
    }

    /// <summary>
    /// Test seam: cancels the armed replay by exactly the production mechanism
    /// deactivation uses.
    /// </summary>
    /// <remarks>
    /// Before issue #2871 the replay ran inside the activation hook, so the
    /// activation's own <see cref="CancellationToken"/> bounded it and a fixture
    /// could drive a mid-replay cancellation simply by cancelling that token. It no
    /// longer does, and that is the point of the change rather than an oversight:
    /// Orleans documents that token as signalling "activation should abort
    /// promptly", and binding the replay to it is precisely what let a replay be
    /// destroyed by the activation deadline. The replay is now bounded by the
    /// activation's LIFETIME instead - the source cancelled in the deactivation
    /// hook - so a fixture that wants a mid-replay cancellation must ask for one
    /// here.
    /// </remarks>
    internal void CancelReplayBarrierForTest() => CancelReplayBarrier();

    /// <summary>
    /// Cancels and disposes the barrier's cancellation source. Called from the
    /// deactivation hook so a replay cannot outlive the activation that armed it.
    /// </summary>
    private void CancelReplayBarrier()
    {
        var cts = _replayBarrierCts;
        if (cts is null)
            return;

        _replayBarrierCts = null;
        try
        {
            cts.Cancel();
        }
        catch
        {
            // A cancellation callback that throws must not break teardown.
        }
        finally
        {
            cts.Dispose();
        }
    }
}
