using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Drives a grain's activation hook <b>and</b> waits for the deferred WAL replay
/// that hook now merely arms (issue #2871), so a fixture written against the old
/// coupling still observes the replay's outcome at the point it expects to.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists.</b> Before #2871, <c>OnActivateAsync</c> <i>was</i> the WAL
/// replay: it ran the whole materialiser inline, so awaiting the hook meant the
/// projection was up to the WAL head, and a replay that threw left activation by
/// throwing. That coupling is the defect. A replay runs behind a per-silo
/// concurrency permit, so losing the race for one destroyed the activation; a
/// destroyed activation banks no snapshot, pins its tree's WAL cursor floor at
/// zero, and cannot answer the very <c>GetTreeIdAsync()</c> probe the WAL GC
/// reactivation sweep uses to repair it.
/// </para>
/// <para>
/// After #2871 the hook arms the replay and returns, and the replay is awaited by
/// data operations instead. So <c>await OnActivateAsync(...)</c> no longer means
/// "the replay finished" - it means "the replay started". Roughly a hundred
/// fixtures in this directory are about REPLAY behaviour (permit accounting,
/// progress banking, cancellation attribution, stale-projection detection) and
/// reached it through the activation hook only because that was the sole way in.
/// Routing them through this helper keeps each assertion testing exactly what it
/// tested before, against the new structure.
/// </para>
/// <para>
/// <b>Naming caveat, deliberately not papered over.</b> Several fixtures reached
/// through here are named <c>..._activation_...</c> and assert on an exception
/// that now surfaces from the replay rather than from the activation. Their
/// subject is unchanged - the replay - but their names now describe the old
/// coupling. They are left named as they are so this change stays reviewable;
/// the drift is recorded in the pull request rather than hidden by a rename.
/// </para>
/// <para>
/// A fixture that means to assert the NEW behaviour - that the activation
/// completes while a replay is pending, or fails a request without destroying the
/// activation - must NOT use this helper, because waiting for the replay is
/// precisely what it needs not to do. Those assertions live in
/// <c>BPlusLeafGrainTests.ReplayBarrier.cs</c> and call the hook directly.
/// </para>
/// </remarks>
internal static class LeafActivationHarness
{
    /// <summary>
    /// Runs <see cref="IGrainBase.OnActivateAsync"/> and then awaits the replay it
    /// armed, so any fault or cancellation the replay raises is observed here -
    /// the position an <c>Assert.ThrowsAsync</c> around the old hook expected it.
    /// </summary>
    internal static async Task ActivateAsync(IGrainBase grain, CancellationToken cancellationToken)
    {
        if (grain is not BPlusLeafGrain leaf)
        {
            await grain.OnActivateAsync(cancellationToken);
            return;
        }

        // Re-establish, for the duration of this call only, the binding #2871
        // removed from production: that cancelling the ACTIVATION token cancels the
        // replay. Fixtures drive mid-replay cancellation that way - most sharply
        // the ones that cancel from inside a persist callback to tear down after an
        // exact number of slices, and the ones that drain the permit gate and then
        // cancel to prove a queued-for-permit abort is attributed to its own
        // reason. Without this the first would replay to completion and the second
        // would wait forever on a permit nothing will release.
        //
        // Registered BEFORE the hook, not after. Against NSubstitute every seam
        // completes synchronously, so the whole replay usually runs to completion
        // inside the hook call - which is exactly where those fixtures fire their
        // cancellation. A registration that waited for the hook to return would
        // always be too late. The callback reads the barrier's cancellation source
        // when it fires, and that source is assigned before the replay body starts,
        // so a callback firing mid-hook still finds it.
        //
        // This belongs here and not in the grain. In production the replay is
        // deliberately NOT bounded by that token, because being bounded by it is
        // what let the activation deadline destroy replays in the first place.
        using (cancellationToken.CanBeCanceled
            ? cancellationToken.Register(leaf.CancelReplayBarrierForTest)
            : default)
        {
            await grain.OnActivateAsync(cancellationToken);

            // Covers the opposite ordering: a token already cancelled on entry
            // fires the callback above before any barrier exists, so the hook then
            // arms an un-cancelled one. Cancelling is idempotent.
            if (cancellationToken.IsCancellationRequested)
            {
                leaf.CancelReplayBarrierForTest();
            }

            // Null on a leaf whose replay was retired, or one that armed nothing.
            if (leaf.ReplayBarrierForTest is { } replay)
            {
                await replay;
            }
        }
    }
}
