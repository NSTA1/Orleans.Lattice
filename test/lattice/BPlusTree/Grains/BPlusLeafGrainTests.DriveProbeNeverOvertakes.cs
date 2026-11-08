using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// #5992's G1 in production (issue #6006): a background drive's admission probe
/// never overtakes a replay waiter parked on the gate, including in the instant
/// after a holder releases, and the parked waiter is the one granted. This is
/// the premise of A1's work-available re-check: a positive
/// <see cref="SemaphoreSlim.CurrentCount"/> is a permit nobody is queued for
/// only while every acquire of the gate is a queued wait or a zero-timeout
/// probe. Refinement: <c>DriveAdmission</c> and <c>ClassPriority</c> of
/// <c>spec/scheduling/ReplayPermits</c>, mapped to the semaphore's direct
/// hand-off.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly TimeSpan DriveProbeObservationWindow = TimeSpan.FromMilliseconds(250);

    [Test]
    [NonParallelizable]
    public async Task A_drive_probe_is_refused_while_a_replay_waiter_is_parked_even_just_after_a_release()
    {
        const int Ceiling = 2;
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        // The parked replay below is counted as production counts one, so the
        // gate's own accounting knows a waiter is parked while the probes ask.
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(Ceiling, queued: 1);
        using var gate = new SemaphoreSlim(Ceiling, Ceiling);
        Task? parked = null;
        var held = 0;
        try
        {
            // Two interactive holders take every permit, and a third interactive
            // replay parks behind them - the replay path's queued wait.
            for (; held < Ceiling; held++)
                Assert.That(gate.Wait(0), Is.True, "precondition: an interactive holder takes a permit");
            parked = gate.WaitAsync();
            Assert.That(parked.IsCompleted, Is.False, "precondition: the third replay is parked");

            Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, BPlusLeafGrain.StarvationDriveOrigin.WalGcSweep),
                Is.False, "a drive probe with every permit held is refused");

            // The drive asks while the waiter is still parked, and a holder then
            // releases while that request may still be outstanding. Only an
            // acquire that bypassed the semaphore could be admitted ahead of the
            // parked replay here.
            var probe = Task.Run(() => BPlusLeafGrain.TryAcquireStarvationReplayPermit(
                gate, BPlusLeafGrain.StarvationDriveOrigin.WalGcSweep));
            await Task.WhenAny(probe, Task.Delay(DriveProbeObservationWindow));
            gate.Release();
            held--;
            var probeAdmitted = await probe;

            // Immediately after the release, before any continuation of the
            // parked waiter has run, a second probe asks.
            var probeAfterRelease = BPlusLeafGrain.TryAcquireStarvationReplayPermit(
                gate, BPlusLeafGrain.StarvationDriveOrigin.WalGcSweep);
            var countAfterRelease = gate.CurrentCount;
            var waiterGranted = await Task.WhenAny(parked, Task.Delay(DriveProbeObservationWindow)) == parked;

            Assert.Multiple(() =>
            {
                Assert.That(probeAdmitted, Is.False,
                    "a drive probe issued while a replay waiter is parked must not be granted the next released permit");
                Assert.That(probeAfterRelease, Is.False,
                    "the released permit belongs to the parked waiter, so a probe just after the release is refused");
                Assert.That(countAfterRelease, Is.Zero,
                    "the release hands the permit straight to the parked waiter rather than raising the free count, "
                    + "which is what makes A1's free-count re-check mean no waiter is ahead");
                Assert.That(waiterGranted, Is.True, "the parked replay is the one granted the released permit");
            });

            if (probeAdmitted)
                BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
            if (probeAfterRelease)
                BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
        }
        finally
        {
            if (parked is not null && await Task.WhenAny(parked, Task.Delay(DriveProbeObservationWindow)) == parked)
                gate.Release();
            if (held > 0)
                gate.Release(held);
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}
