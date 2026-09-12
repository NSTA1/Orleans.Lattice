using System.Collections.Concurrent;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the memory-adaptive backpressure on the per-silo WAL replay
/// concurrency gate (issue #2781).
/// <para>
/// The defect is a closed loop. A tree whose WAL entries are large makes every
/// cold activation replay an enormous window while holding large buffers; the
/// gate admits <see cref="Environment.ProcessorCount"/> such replays at once;
/// the heap is exhausted; the replays then fail <b>slowly, while holding
/// permits</b>; other leaves time out queued for a permit and never replay at
/// all; a cancelled activation banks no snapshot, so the durable materialiser
/// pin never advances and WAL garbage collection reclaims nothing - which makes
/// the next replay window larger still.
/// </para>
/// <para>
/// The gate has no memory dimension, so nothing in that loop ever reduces
/// concurrency. This fixture covers the reduction, its floor, its recovery, and
/// - most importantly - the invariant that recovery can never put more permits
/// into circulation than the operator configured.
/// </para>
/// <para>
/// <b>Why that invariant is the load-bearing one.</b> Issues #2278/#2279 settled
/// that the core library must not silently defeat an operator's
/// <c>WalMaterialiserMaxConcurrentReplays</c> or <c>DOTNET_PROCESSOR_COUNT</c>,
/// and a reviewer meeting this change will reach for that objection on sight.
/// The answer is structural rather than rhetorical: the mechanism works by
/// <i>declining to return</i> a permit that was already taken, so it is
/// incapable of adding one. These tests are what make that claim checkable
/// rather than asserted.
/// </para>
/// <para>
/// Every test here mutates process-wide statics, so each restores the gate to
/// the state it found it in and is marked <see cref="NonParallelizableAttribute"/>.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Sizes the process-wide gate if it is not sized yet and returns it,
    /// asserting it is quiescent - every permit in circulation and nothing
    /// withheld. The assertions are instrument validation, not the subject: a
    /// test that withheld against an already-depressed baseline would compare
    /// against the wrong number and could pass for the wrong reason.
    /// </summary>
    private static async Task<SemaphoreSlim> QuiescentReplayGateAsync()
    {
        var (warmGrain, warmState, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null,
            persistedCheckpoint: 0,
            walHead: 0);
        warmState.State.TreeId = UniqueReplayPermitTree();
        await ((IGrainBase)warmGrain).OnActivateAsync(CancellationToken.None);

        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest;
        Assert.That(gate, Is.Not.Null,
            "a completed activation with a tree id must have sized the process-wide replay gate");

        Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.Zero,
            "the gate must start this test with nothing withheld - a leaked withholding from an "
            + "earlier test would silently move every baseline below");
        Assert.That(gate!.CurrentCount, Is.EqualTo(BPlusLeafGrain.ReplayConcurrencyCeilingForTest),
            "the gate must start this test with every permit in circulation, so the ceiling read "
            + "below is the real one");

        return gate;
    }

    [Test]
    [NonParallelizable]
    public async Task Withholding_never_takes_the_last_replay_permit_out_of_circulation()
    {
        var gate = await QuiescentReplayGateAsync();
        var ceiling = BPlusLeafGrain.ReplayConcurrencyCeilingForTest;
        Assert.That(ceiling, Is.GreaterThan(1),
            "this test needs a ceiling above one for a floor to be distinguishable from it");

        // Model the real discipline: a permit is only ever withheld by a replay
        // that already holds it, so each successful withholding is matched by a
        // permit taken from the gate and not returned.
        var withheld = 0;
        while (withheld <= ceiling + 4 && BPlusLeafGrain.TryWithholdReplayPermitOnPressure())
        {
            Assert.That(gate.Wait(0), Is.True,
                "withholding claimed a permit the gate could not supply - the accounting and the "
                + "semaphore have diverged");
            withheld++;
        }

        try
        {
            Assert.Multiple(() =>
            {
                Assert.That(withheld, Is.EqualTo(ceiling - 1),
                    "backpressure must stop one permit short of the ceiling. Withholding the last "
                    + "permit converts a memory stall into a total stall, and a gate that admits "
                    + "nothing can never observe the clean replay that recovers it - the mechanism "
                    + "would latch permanently, which is the defect issue #2783 reports elsewhere");

                Assert.That(gate.CurrentCount, Is.EqualTo(1),
                    "exactly one permit must remain in circulation at the floor");
            });

            Assert.That(gate.Wait(0), Is.True,
                "the surviving permit must be acquirable - a floor that is only an accounting "
                + "figure, with no permit actually behind it, would not let replay make progress");
            gate.Release();
        }
        finally
        {
            for (var i = 0; i < withheld; i++)
            {
                Assert.That(BPlusLeafGrain.TryRestoreWithheldReplayPermit(), Is.True);
                gate.Release();
            }
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Restoring_withheld_permits_never_exceeds_the_configured_replay_ceiling()
    {
        var gate = await QuiescentReplayGateAsync();
        var ceiling = BPlusLeafGrain.ReplayConcurrencyCeilingForTest;
        Assert.That(ceiling, Is.GreaterThan(3),
            "this test withholds three permits, so it needs a ceiling that admits them below the floor");

        const int ToWithhold = 3;
        var withheld = 0;
        for (var i = 0; i < ToWithhold; i++)
        {
            Assert.That(BPlusLeafGrain.TryWithholdReplayPermitOnPressure(), Is.True);
            Assert.That(gate.Wait(0), Is.True);
            withheld++;
        }

        // Instrument validation: proves the gate really was reduced, so the
        // restoration assertions below are about permits that were actually
        // taken out of circulation.
        Assert.That(gate.CurrentCount, Is.EqualTo(ceiling - withheld),
            "the withheld permits must be absent from the gate before restoration is tested");

        // Attempt far more restorations than were withheld. This is the arm the
        // invariant is about: a recovery term that is not bounded by what was
        // withheld would put permits into circulation that the operator never
        // granted.
        var restored = 0;
        for (var attempt = 0; attempt < withheld + 5; attempt++)
        {
            if (!BPlusLeafGrain.TryRestoreWithheldReplayPermit())
                continue;

            // Deliberately unguarded. SemaphoreSlim was constructed with a
            // maximum, so an over-restoration raises SemaphoreFullException here
            // rather than silently over-admitting - the failure is loud, and
            // this test is the thing that makes it observable.
            gate.Release();
            restored++;
        }

        Assert.Multiple(() =>
        {
            Assert.That(restored, Is.EqualTo(withheld),
                "recovery must return exactly the permits backpressure withheld and no more. More "
                + "would raise the gate above the ceiling the operator configured, which is the "
                + "#2278/#2279 objection this mechanism must not be vulnerable to");

            Assert.That(gate.CurrentCount, Is.EqualTo(ceiling),
                "the gate must return to - and stop at - the configured ceiling");

            Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.Zero,
                "the withheld count must settle at zero rather than going negative, or the next "
                + "pressure event would be measured from the wrong base");
        });
    }

    [Test]
    [NonParallelizable]
    public async Task Activation_withholds_a_replay_permit_when_the_replay_fails_for_memory_pressure()
    {
        var gate = await QuiescentReplayGateAsync();
        var baseline = gate.CurrentCount;

        var (grain, state) = CreateGrainWithLoggerFactory(
            new ThrowingProbeLoggerFactory(
                () => throw new OutOfMemoryException("replay-gate-backpressure-probe")));
        state.State.TreeId = UniqueReplayPermitTree();

        Assert.ThrowsAsync<OutOfMemoryException>(
            async () => await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None),
            "the injected memory fault must still propagate - backpressure observes the failure, it "
            + "does not absorb it");

        try
        {
            Assert.Multiple(() =>
            {
                Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.EqualTo(1),
                    "a replay that failed for memory pressure must reduce the gate. Without this the "
                    + "loop is closed: the same concurrency is re-admitted, the heap is exhausted "
                    + "again, and nothing in the system ever lowers the pressure");

                Assert.That(gate.CurrentCount, Is.EqualTo(baseline - 1),
                    "the permit must be withheld rather than returned - the reduction is the permit "
                    + "not going back, not a bookkeeping figure kept alongside an unchanged gate");
            });
        }
        finally
        {
            if (BPlusLeafGrain.TryRestoreWithheldReplayPermit())
                gate.Release();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Activation_does_not_withhold_a_replay_permit_when_the_replay_fails_for_a_non_memory_fault()
    {
        var gate = await QuiescentReplayGateAsync();
        var baseline = gate.CurrentCount;

        var (grain, state) = CreateGrainWithLoggerFactory(
            new ThrowingProbeLoggerFactory(
                () => throw new InvalidOperationException("replay-gate-non-memory-probe")));
        state.State.TreeId = UniqueReplayPermitTree();

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.Zero,
                "only memory pressure is evidence the heap is short. Withholding on any failure "
                + "would let an unrelated fault - a logging-sink throw, a cancelled activation - "
                + "ratchet leaf-activation concurrency down with nothing to recover it");

            Assert.That(gate.CurrentCount, Is.EqualTo(baseline),
                "a non-memory fault must return its permit exactly as it did before this change "
                + "(issue #2256)");
        });
    }

    [Test]
    [NonParallelizable]
    public async Task Clean_activation_restores_one_withheld_replay_permit()
    {
        var gate = await QuiescentReplayGateAsync();

        Assert.That(BPlusLeafGrain.TryWithholdReplayPermitOnPressure(), Is.True);
        Assert.That(gate.Wait(0), Is.True);
        var depressed = gate.CurrentCount;

        var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null,
            persistedCheckpoint: 0,
            walHead: 0);
        state.State.TreeId = UniqueReplayPermitTree();
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.Zero,
                "a replay that completed without memory pressure is the evidence the heap can "
                + "afford more concurrency again, so it must return one withheld permit. Without "
                + "recovery the reduction is permanent and the silo never regains its throughput");

            Assert.That(gate.CurrentCount, Is.EqualTo(depressed + 1),
                "recovery must be a real permit returning to the gate, not only a decrement of the "
                + "withheld count");
        });
    }

    [Test]
    [NonParallelizable]
    public async Task Failed_activation_does_not_restore_a_withheld_replay_permit()
    {
        var gate = await QuiescentReplayGateAsync();

        Assert.That(BPlusLeafGrain.TryWithholdReplayPermitOnPressure(), Is.True);
        Assert.That(gate.Wait(0), Is.True);
        var depressed = gate.CurrentCount;

        var (grain, state) = CreateGrainWithLoggerFactory(
            new ThrowingProbeLoggerFactory(
                () => throw new InvalidOperationException("replay-gate-failed-recovery-probe")));
        state.State.TreeId = UniqueReplayPermitTree();

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None));

        try
        {
            Assert.Multiple(() =>
            {
                Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.EqualTo(1),
                    "an activation that did not complete is not evidence the heap recovered. "
                    + "Restoring on a failure path would let a silo whose replays all fail fast "
                    + "for unrelated reasons walk the gate straight back up to the ceiling it was "
                    + "reduced from");

                Assert.That(gate.CurrentCount, Is.EqualTo(depressed),
                    "the gate must be left where the withholding put it");
            });
        }
        finally
        {
            if (BPlusLeafGrain.TryRestoreWithheldReplayPermit())
                gate.Release();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Sizing_the_replay_gate_zero_primes_both_backpressure_arms()
    {
        // The gate is sized once per process, so it must be returned to its
        // unsized state for the priming to be observable at all.
        await QuiescentReplayGateAsync();
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();

        var records = new ConcurrentBag<(long Value, KeyValuePair<string, object?>[] Tags)>();
        using (MeterListening.StartForInstrument(
            LatticeMetrics.WalReplayPermitAdaptations,
            l => l.SetMeasurementEventCallback<long>(
                (_, value, tags, _) => records.Add((value, tags.ToArray())))))
        {
            var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                preloadedSnapshot: null,
                persistedCheckpoint: 0,
                walHead: 0);
            state.State.TreeId = UniqueReplayPermitTree();
            await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        }

        var outcomes = records
            .Select(r => (Value: r.Value, Outcome: r.Tags.Single(t => t.Key == LatticeMetrics.TagOutcome).Value))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(outcomes.Any(o => Equals(o.Outcome, "withheld") && o.Value == 0), Is.True,
                "the withheld arm must be primed at zero when the gate is sized. Unprimed, an "
                + "absent series cannot distinguish 'backpressure is present and has never "
                + "engaged' from 'this build does not have backpressure' - and on this epic that "
                + "exact ambiguity has already been read as evidence of a failed deploy");

            Assert.That(outcomes.Any(o => Equals(o.Outcome, "restored") && o.Value == 0), Is.True,
                "the restored arm must be primed at zero for the same reason. The effective "
                + "ceiling is read as withheld minus restored, so a missing restored series makes "
                + "the withheld series unreadable rather than merely incomplete");
        });
    }
}
