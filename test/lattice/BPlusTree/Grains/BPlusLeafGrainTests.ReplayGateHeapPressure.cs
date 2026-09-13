using System.Collections.Concurrent;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the <b>proactive</b> heap dimension of the per-silo WAL replay
/// concurrency gate (issue #2862).
/// <para>
/// <b>What this fixture exists to stop recurring.</b> Issue #2781 gave the gate a
/// memory dimension whose only trigger was a fault: a permit was withheld when a
/// replay escaped its guarded region with an <c>IsReadMemoryPressure</c>
/// exception. Acceptance run 10 measured that trigger firing <b>zero</b> times
/// while the silo threw 625 <see cref="OutOfMemoryException"/>s, escalated 129 of
/// them fatally, and died and restarted twice. Both arms of
/// <c>orleans.lattice.wal.replay.permit_adaptations</c> are zero-primed when the
/// gate is sized, so that was a measured zero and not an absent series - the
/// build landed and the mechanism simply never engaged.
/// </para>
/// <para>
/// <b>Why it never engaged, which is what these tests encode.</b> The replay's own
/// slice-budget narrowing (issue #2742) catches exactly
/// <c>IsReadMemoryPressure</c> inside the partition loop and retries the same
/// range at a quarter width. A recovered read leaves the guarded region
/// <i>clean</i>, so the gate observes a healthy replay and runs its
/// <i>recovery</i> arm - handing permits back into a heap that is already at its
/// ceiling. The snapshot rehydrate faults earlier still, before any permit is
/// held. And a fault-driven trigger is late by construction: its precondition is
/// that the heap has already been exhausted, so no threshold on it could ever
/// satisfy "withhold before the process reaches its GC hard limit".
/// </para>
/// <para>
/// The tests that drive a real activation therefore all use a <b>clean</b> replay.
/// A fixture that injected a memory fault would be exercising the #2781 trigger
/// that already worked, and would pass unchanged against the defective build.
/// </para>
/// <para>
/// Every test here mutates process-wide statics, so each restores the gate and
/// the simulated heap to the state it found them in, and is marked
/// <see cref="NonParallelizableAttribute"/>.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>The run-10 heap hard limit: 75% of a 12 GiB container grant.</summary>
    private const long RunTenHeapCeilingBytes = 9L * 1024 * 1024 * 1024;

    /// <summary>The run-10 container memory grant, which is <b>not</b> the limit that throws.</summary>
    private const long RunTenContainerGrantBytes = 12L * 1024 * 1024 * 1024;

    /// <summary>
    /// Installs a simulated heap reading for the duration of the returned scope
    /// and clears it afterwards, whatever the test does.
    /// </summary>
    /// <remarks>
    /// The reading is simulated rather than provoked because no unit test can
    /// drive a real process to its heap hard limit reproducibly, and one that
    /// tried would destabilise every other test sharing the runner. What is under
    /// test is the gate's <i>response</i> to occupancy, not the runtime's
    /// reporting of it - the latter is covered separately by
    /// <see cref="Reading_the_real_heap_reports_a_positive_occupancy"/>.
    /// </remarks>
    private static IDisposable SimulatedHeap(long inUseBytes, long ceilingBytes)
    {
        ReplayHeapPressure.ReaderForTest = () => new ReplayHeapReading(inUseBytes, ceilingBytes);
        return new SimulatedHeapScope();
    }

    private sealed class SimulatedHeapScope : IDisposable
    {
        public void Dispose() => ReplayHeapPressure.ReaderForTest = null;
    }

    /// <summary>
    /// Occupancy in the middle of the hysteresis band - above the restore
    /// threshold and below the withholding one - derived from the thresholds
    /// themselves so it stays inside the band if either is ever retuned.
    /// </summary>
    private static long MidHysteresisBandBytes(long ceilingBytes) =>
        ceilingBytes
        / 100
        * ((ReplayHeapPressure.WithholdOccupancyPercent + ReplayHeapPressure.RestoreOccupancyPercent) / 2);

    /// <summary>
    /// Runs one activation that replays cleanly - no injected fault of any kind -
    /// and returns once it has completed.
    /// </summary>
    private static async Task ActivateWithCleanReplayAsync()
    {
        var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null,
            persistedCheckpoint: 0,
            walHead: 0);
        state.State.TreeId = UniqueReplayPermitTree();
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
    }

    /// <summary>
    /// Returns every withheld permit to the gate, so a failed assertion cannot
    /// leave the process-wide gate depressed for the rest of the run.
    /// </summary>
    private static void DrainWithheldReplayPermits(SemaphoreSlim gate)
    {
        while (BPlusLeafGrain.TryRestoreWithheldReplayPermit())
            gate.Release();
    }

    [Test]
    [NonParallelizable]
    public async Task Activation_withholds_a_replay_permit_when_heap_occupancy_reaches_the_withholding_band()
    {
        // THE HEADLINE REGRESSION. Every input here is benign to the issue #2781
        // trigger: the replay completes, nothing throws, and no exception with a
        // memory verdict is ever constructed. That is precisely the shape run 10
        // produced 625 times over - the slice-narrowing retry absorbed each OOM
        // and handed the gate a clean replay - and against the defective build
        // this test observes zero withholds, which is the measured zero the issue
        // reports.
        var gate = await QuiescentReplayGateAsync();
        var baseline = gate.CurrentCount;

        try
        {
            using (SimulatedHeap(
                inUseBytes: RunTenHeapCeilingBytes / 10 * 9,
                ceilingBytes: RunTenHeapCeilingBytes))
            {
                await ActivateWithCleanReplayAsync();
            }

            Assert.Multiple(() =>
            {
                Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.EqualTo(1),
                    "a replay completing against a heap at 90% of the limit that throws must reduce "
                    + "the gate. Without this the loop is closed exactly as run 10 measured it: the "
                    + "narrowing retry absorbs the OOM, the region reports clean, the gate sees no "
                    + "pressure at any point, and the same concurrency is re-admitted into the same "
                    + "exhausted heap until the process dies");

                Assert.That(gate.CurrentCount, Is.EqualTo(baseline - 1),
                    "the reduction must be the permit not going back, not a bookkeeping figure kept "
                    + "alongside an unchanged gate");
            });
        }
        finally
        {
            DrainWithheldReplayPermits(gate);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Activation_withholds_a_replay_permit_before_occupancy_reaches_the_heap_ceiling()
    {
        // The definition-of-done boundary, and it is part of the finding rather
        // than a refinement of it. A mechanism that only engages once occupancy
        // has reached the ceiling engages after the allocation that throws, which
        // is the #2781 trigger's defect restated. Occupancy here is strictly
        // below the ceiling and by a wide margin - a quarter of the heap is still
        // unallocated - so a threshold that had been written against the
        // container GRANT (12 GiB, 33% above the 9 GiB that actually throws)
        // would read this as comfortable and withhold nothing.
        var gate = await QuiescentReplayGateAsync();

        try
        {
            var occupancy = RunTenHeapCeilingBytes / 100 * ReplayHeapPressure.WithholdOccupancyPercent;
            Assert.That(occupancy, Is.LessThan(RunTenHeapCeilingBytes),
                "instrument validation: the occupancy this test drives must itself be below the "
                + "ceiling, or the test would prove nothing about withholding before the wall");

            using (SimulatedHeap(occupancy, RunTenHeapCeilingBytes))
            {
                await ActivateWithCleanReplayAsync();
            }

            Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.EqualTo(1),
                "the gate must withhold while the process still has heap left to complete the "
                + "replays it has already admitted. Withholding only at the ceiling is withholding "
                + "after the allocation that throws");
        }
        finally
        {
            DrainWithheldReplayPermits(gate);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Clean_activation_does_not_restore_a_withheld_permit_while_occupancy_stays_above_the_restore_band()
    {
        // The other half of the closed loop, and the half that made run 10 worse
        // rather than merely un-helped. Under issue #2781 alone a clean replay
        // was sufficient evidence to hand a permit back - and under the #2742
        // narrowing retry a replay riddled with absorbed OOMs is indistinguishable
        // from a healthy one at this seam. So the recovery arm stood ready to
        // raise concurrency against a heap that was already at its limit.
        //
        // Occupancy is driven to the middle of the hysteresis band rather than to
        // the withholding band, and that choice is what makes this test able to
        // fail. Above the withholding threshold the replay withholds its own
        // permit and never reaches the restore branch at all, so the clause would
        // hold for a build whose restore condition had been deleted outright.
        // Inside the band the restore branch is reached and declines, which is
        // the behaviour actually under test.
        var gate = await QuiescentReplayGateAsync();

        Assert.That(
            BPlusLeafGrain.TryWithholdReplayPermitOnPressure(
                LatticeMetrics.PermitAdaptationTriggerOccupancy),
            Is.True);
        Assert.That(gate.Wait(0), Is.True,
            "instrument validation: the withheld permit must really leave the gate, or the "
            + "restoration assertion below would be about a permit that was never removed");
        var depressed = gate.CurrentCount;

        try
        {
            var midBand = MidHysteresisBandBytes(RunTenHeapCeilingBytes);
            var reading = new ReplayHeapReading(midBand, RunTenHeapCeilingBytes);

            Assert.Multiple(() =>
            {
                Assert.That(ReplayHeapPressure.IsPressured(reading), Is.False,
                    "instrument validation: occupancy must sit below the withholding threshold, or "
                    + "the replay would withhold and the restore branch would never be reached");
                Assert.That(ReplayHeapPressure.IsRelieved(reading), Is.False,
                    "instrument validation: and above the restore threshold, or there would be "
                    + "nothing for the assertion below to discriminate");
            });

            using (SimulatedHeap(midBand, RunTenHeapCeilingBytes))
            {
                await ActivateWithCleanReplayAsync();
            }

            Assert.Multiple(() =>
            {
                Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.EqualTo(1),
                    "a clean replay is not on its own evidence the heap recovered. Restoring on it "
                    + "while occupancy has not receded walks the gate straight back up to the "
                    + "ceiling it was reduced from - which is what the build measured in run 10 "
                    + "would have done on every one of the replays whose OOM the narrowing retry "
                    + "absorbed");

                Assert.That(gate.CurrentCount, Is.EqualTo(depressed),
                    "the gate must be left where the withholding put it");
            });
        }
        finally
        {
            DrainWithheldReplayPermits(gate);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Clean_activation_restores_a_withheld_permit_once_heap_occupancy_recedes()
    {
        // The complement of the test above, and it is what keeps the reduction
        // from being permanent. A silo that withheld under a transient spike and
        // never handed the permits back would have traded an exhaustion for a
        // throughput collapse, which is a worse trade than it looks because
        // nothing in the system would ever report it.
        var gate = await QuiescentReplayGateAsync();

        Assert.That(
            BPlusLeafGrain.TryWithholdReplayPermitOnPressure(
                LatticeMetrics.PermitAdaptationTriggerOccupancy),
            Is.True);
        Assert.That(gate.Wait(0), Is.True);
        var depressed = gate.CurrentCount;

        try
        {
            using (SimulatedHeap(
                inUseBytes: RunTenHeapCeilingBytes / 10,
                ceilingBytes: RunTenHeapCeilingBytes))
            {
                await ActivateWithCleanReplayAsync();
            }

            Assert.Multiple(() =>
            {
                Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.Zero,
                    "a replay that completed against a heap at a tenth of its ceiling is the "
                    + "evidence the process can afford more concurrency again, so one withheld "
                    + "permit must return");

                Assert.That(gate.CurrentCount, Is.EqualTo(depressed + 1),
                    "recovery must be a real permit returning to the gate, not only a decrement of "
                    + "the withheld count");
            });
        }
        finally
        {
            DrainWithheldReplayPermits(gate);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Activation_does_not_withhold_a_replay_permit_when_no_heap_ceiling_is_known()
    {
        // Keeps the change additive. TotalAvailableMemoryBytes reports host
        // physical memory rather than zero when no heap hard limit is configured,
        // and the cgroup limit is absent outside a container, so an unknown
        // ceiling is the ordinary condition on a developer machine and on any
        // unquotaed host. Withholding there would be a reduction taken on no
        // evidence at all.
        var gate = await QuiescentReplayGateAsync();
        var baseline = gate.CurrentCount;

        try
        {
            using (SimulatedHeap(inUseBytes: RunTenHeapCeilingBytes, ceilingBytes: 0))
            {
                await ActivateWithCleanReplayAsync();
            }

            Assert.Multiple(() =>
            {
                Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.Zero,
                    "an unreadable ceiling is not evidence of pressure, however large the occupancy "
                    + "figure beside it happens to be");

                Assert.That(gate.CurrentCount, Is.EqualTo(baseline),
                    "the gate must be left exactly as it behaved before this change on a host with "
                    + "no heap hard limit");
            });
        }
        finally
        {
            DrainWithheldReplayPermits(gate);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Sustained_heap_pressure_never_starves_the_gate_of_its_last_replay_permit()
    {
        // THE STARVATION DOOR. Withholding under heap pressure lengthens permit
        // queues by construction, and a cold leaf that queues past the activation
        // request timeout is cancelled - which banks no snapshot, leaves the
        // durable materialiser pin unusable, and blocks WAL GC exactly as an OOM
        // cancellation does. The two failures sit at opposite ends of one dial,
        // so a proactive trigger that could drive availability to zero would open
        // the far door while closing the near one.
        //
        // The floor lives in TryWithholdReplayPermitOnPressure and is shared by
        // both triggers, but sharing it is an implementation fact, not an
        // observable. What this pins is that the PROACTIVE path routes through it
        // rather than decrementing the gate itself: pressure is held continuously
        // across more activations than the ceiling admits, and the gate is
        // required to saturate one permit short rather than empty.
        var gate = await QuiescentReplayGateAsync();
        var ceiling = BPlusLeafGrain.ReplayConcurrencyCeilingForTest;
        Assert.That(ceiling, Is.GreaterThan(1),
            "this test needs a ceiling above one for a floor to be distinguishable from an "
            + "exhausted gate");

        var activations = ceiling + 3;
        var lowestObservedCount = int.MaxValue;

        try
        {
            using (SimulatedHeap(
                inUseBytes: RunTenHeapCeilingBytes / 10 * 9,
                ceilingBytes: RunTenHeapCeilingBytes))
            {
                for (var i = 0; i < activations; i++)
                {
                    // Starvation does not present as a wrong number, it presents
                    // as an activation that never returns - so it is bounded here
                    // rather than left to deadlock. Against a build with no floor
                    // this is the clause that reddens, and it reddens the way the
                    // production failure reads: the replay waits on a permit that
                    // will never be released, which in the silo is what the
                    // activation request timeout eventually cancels.
                    var activation = ActivateWithCleanReplayAsync();
                    var completed = await Task.WhenAny(activation, Task.Delay(TimeSpan.FromSeconds(30)));

                    Assert.That(completed, Is.SameAs(activation),
                        $"activation {i + 1} of {activations} never acquired a replay permit. "
                        + "Withholding has taken every permit out of circulation, so no replay can "
                        + "run, nothing can observe the clean replay that would restore a permit, "
                        + "and the gate has latched shut");

                    await activation;
                    lowestObservedCount = Math.Min(lowestObservedCount, gate.CurrentCount);
                }
            }

            Assert.Multiple(() =>
            {
                Assert.That(activations, Is.GreaterThan(ceiling),
                    "the run must outnumber the ceiling, or saturation is never attempted and a "
                    + "build with no floor at all would pass this unchanged");

                Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.EqualTo(ceiling - 1),
                    "sustained pressure must saturate withholding exactly one permit short of the "
                    + "ceiling. More than that is starvation; fewer means the proactive trigger "
                    + "stopped reducing before the floor and is not the mechanism it claims to be");

                Assert.That(lowestObservedCount, Is.EqualTo(1),
                    "at least one permit must remain in circulation at every point of the run. A "
                    + "gate that admits nothing can never observe the clean replay that recovers "
                    + "it, so the reduction would latch permanently (issue #2783), and every cold "
                    + "leaf behind it would queue until the request timeout cancelled it");
            });

            Assert.That(gate.Wait(0), Is.True,
                "the surviving permit must be acquirable rather than an accounting figure with no "
                + "permit behind it");
            gate.Release();
        }
        finally
        {
            DrainWithheldReplayPermits(gate);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Withholding_under_heap_pressure_records_the_withheld_arm_of_the_adaptation_counter()
    {
        // The issue is scored on this counter, so the counter is part of the
        // fix rather than a by-product of it. `withheld` at zero against a
        // zero-primed series was the whole of run 10's evidence; a build whose
        // gate reduced concurrency without saying so would be unfalsifiable in
        // exactly the same way.
        var gate = await QuiescentReplayGateAsync();
        var records = new ConcurrentBag<(long Value, KeyValuePair<string, object?>[] Tags)>();

        try
        {
            using (MeterListening.StartForInstrument(
                LatticeMetrics.WalReplayPermitAdaptations,
                l => l.SetMeasurementEventCallback<long>(
                    (_, value, tags, _) => records.Add((value, tags.ToArray())))))
            using (SimulatedHeap(
                inUseBytes: RunTenHeapCeilingBytes / 10 * 9,
                ceilingBytes: RunTenHeapCeilingBytes))
            {
                await ActivateWithCleanReplayAsync();
            }

            Assert.That(records, Is.Not.Empty,
                "instrument validation: the listener must have observed at least one measurement, "
                + "or the assertion below would pass by observing nothing");

            var withheld = records
                .Where(r => Equals(
                    r.Tags.Single(t => t.Key == LatticeMetrics.TagOutcome).Value, "withheld"))
                .Sum(r => r.Value);

            Assert.That(withheld, Is.EqualTo(1),
                "the withheld arm must record the reduction. This is the exact series that read "
                + "zero through 625 OutOfMemoryExceptions in acceptance run 10");
        }
        finally
        {
            DrainWithheldReplayPermits(gate);
        }
    }

    [Test]
    public void Heap_ceiling_prefers_the_smaller_known_limit()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                ReplayHeapPressure.ResolveCeilingBytes(
                    RunTenHeapCeilingBytes, RunTenContainerGrantBytes),
                Is.EqualTo(RunTenHeapCeilingBytes),
                "the runtime's hard limit is the smaller figure on the deployment this issue was "
                + "raised from, and it is the one that throws");

            Assert.That(
                ReplayHeapPressure.ResolveCeilingBytes(
                    RunTenContainerGrantBytes, RunTenHeapCeilingBytes),
                Is.EqualTo(RunTenHeapCeilingBytes),
                "the container grant constrains the ceiling when it is the smaller figure - which "
                + "is the case whenever no heap hard limit is configured, because "
                + "TotalAvailableMemoryBytes then reports host physical memory (issue #2788)");
        });
    }

    [Test]
    public void Heap_ceiling_is_unknown_when_neither_limit_is_known()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ReplayHeapPressure.ResolveCeilingBytes(0, 0), Is.LessThanOrEqualTo(0),
                "neither figure known must resolve to unknown, not to a ceiling of zero - which "
                + "would judge every process catastrophically over its limit");

            Assert.That(
                ReplayHeapPressure.ResolveCeilingBytes(RunTenHeapCeilingBytes, 0),
                Is.EqualTo(RunTenHeapCeilingBytes),
                "one known figure is a ceiling; the unknown side must not be read as a bound of zero");

            Assert.That(
                ReplayHeapPressure.ResolveCeilingBytes(0, RunTenContainerGrantBytes),
                Is.EqualTo(RunTenContainerGrantBytes),
                "and symmetrically on the other side");
        });
    }

    [Test]
    public void Heap_ceiling_reads_the_cgroup_unlimited_saturation_as_unknown()
    {
        // cgroup v1 spells "unlimited" as a page-aligned saturation of the page
        // counter near long.MaxValue. It is a well-formed positive number, so a
        // resolver that believed it would pin the ceiling at exabytes and make
        // this mechanism permanently inert on cgroup v1 - silently, and only
        // there.
        //
        // The discriminating input is the sentinel ALONE. Paired with a real
        // runtime figure the minimum would discard it regardless, so a clause
        // written that way asserts something true of a resolver that has no
        // sentinel handling at all, and cannot fail.
        var sentinel = long.MaxValue / 4096 * 4096;

        Assert.Multiple(() =>
        {
            Assert.That(ReplayHeapPressure.ResolveCeilingBytes(0, sentinel), Is.LessThanOrEqualTo(0),
                "an unlimited container limit is the absence of a bound, not a bound of eight "
                + "exabytes. Believed, it would resolve to a ceiling no occupancy could ever reach "
                + "and the gate would read as protective while being unreachable by construction");

            Assert.That(ReplayHeapPressure.ResolveCeilingBytes(sentinel, 0), Is.LessThanOrEqualTo(0),
                "and symmetrically, should the runtime figure ever saturate the same way");

            Assert.That(
                ReplayHeapPressure.IsPressured(
                    new ReplayHeapReading(
                        RunTenHeapCeilingBytes,
                        ReplayHeapPressure.ResolveCeilingBytes(0, sentinel))),
                Is.False,
                "and the resolved ceiling must carry that through to the verdict, rather than "
                + "being a number that is merely reported correctly");
        });
    }

    [Test]
    public void Withholding_is_measured_against_the_heap_hard_limit_not_the_container_grant()
    {
        // Hypothesis 3 of the issue, pinned. Run 10's ceiling was 9 GiB - .NET's
        // default GCHeapHardLimitPercent of 75% applied to a 12 GiB grant - and
        // observed RSS reached 9.293 GiB. A threshold expressed against the grant
        // would sit 33% above the limit that actually bites, so it would look
        // correct, be measurable, and be unreachable.
        var occupancy = RunTenHeapCeilingBytes / 100 * 80;

        Assert.Multiple(() =>
        {
            Assert.That(
                ReplayHeapPressure.IsPressured(
                    new ReplayHeapReading(occupancy, RunTenHeapCeilingBytes)),
                Is.True,
                "80% of the 9 GiB the runtime throws against is pressure");

            Assert.That(
                ReplayHeapPressure.IsPressured(
                    new ReplayHeapReading(occupancy, RunTenContainerGrantBytes)),
                Is.False,
                "the same occupancy judged against the 12 GiB grant is not - which is precisely "
                + "why the grant must not be the denominator. This clause is the control: without "
                + "it the test above would pass for a threshold written against either figure");
        });
    }

    [Test]
    public void Withholding_threshold_is_crossable_strictly_below_the_heap_ceiling()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ReplayHeapPressure.WithholdOccupancyPercent, Is.LessThan(100),
                "a threshold at or above the ceiling could only be crossed by a process that had "
                + "already reached the limit it is meant to stay under");

            Assert.That(ReplayHeapPressure.WithholdOccupancyPercent, Is.GreaterThan(0),
                "and a threshold at or below zero would withhold on an empty heap, converting the "
                + "gate into a permanent single-permit stall");

            Assert.That(
                ReplayHeapPressure.IsPressured(
                    new ReplayHeapReading(RunTenHeapCeilingBytes - 1, RunTenHeapCeilingBytes)),
                Is.True,
                "occupancy one byte below the ceiling must be pressure");

            Assert.That(
                ReplayHeapPressure.IsPressured(
                    new ReplayHeapReading(RunTenHeapCeilingBytes / 100, RunTenHeapCeilingBytes)),
                Is.False,
                "and a percent of the ceiling must not be, or the gate would be reduced to one "
                + "permit on every host at all times");
        });
    }

    [Test]
    public void Restore_band_sits_strictly_below_the_withholding_band()
    {
        // Hysteresis, and it is not decoration. With one threshold a process
        // sitting on the boundary would withhold on one replay and restore on the
        // next for as long as the condition lasted - both arms of the counter
        // rising together while the effective ceiling never moved, which the
        // dashboard reads as "the gate oscillating around the concurrency the
        // heap can afford" and would therefore mistake for the mechanism working.
        Assert.That(
            ReplayHeapPressure.RestoreOccupancyPercent,
            Is.LessThan(ReplayHeapPressure.WithholdOccupancyPercent),
            "the restore band must sit below the withholding band, or a permit withheld at the "
            + "boundary is immediately eligible to be returned");

        var band = MidHysteresisBandBytes(RunTenHeapCeilingBytes);
        var reading = new ReplayHeapReading(band, RunTenHeapCeilingBytes);

        Assert.Multiple(() =>
        {
            Assert.That(ReplayHeapPressure.IsPressured(reading), Is.False,
                "inside the band the gate must not withhold further");

            Assert.That(ReplayHeapPressure.IsRelieved(reading), Is.False,
                "and it must not restore either - the band is where a reduction is held rather "
                + "than adjusted");
        });
    }

    [Test]
    public void An_unknown_heap_ceiling_permits_recovery_but_never_withholding()
    {
        // The asymmetry that keeps the change additive. An unreadable ceiling
        // must not disable the recovery half of issue #2781's mechanism, which is
        // reached by every clean replay on every host - including the many with no
        // heap hard limit, where this type has nothing to say.
        var reading = new ReplayHeapReading(long.MaxValue / 2, 0);

        Assert.Multiple(() =>
        {
            Assert.That(ReplayHeapPressure.IsRelieved(reading), Is.True,
                "recovery must keep working where no ceiling is known");

            Assert.That(ReplayHeapPressure.IsPressured(reading), Is.False,
                "withholding must not - a reduction taken on no evidence is not backpressure");
        });
    }

    [Test]
    public void Reading_the_real_heap_reports_a_positive_occupancy()
    {
        // The simulated readings above test the gate's response to occupancy;
        // this tests that the runtime reading behind them is live at all. Without
        // it every test in this fixture could pass against a reader that returned
        // default(ReplayHeapReading) in production.
        Assert.That((object?)ReplayHeapPressure.ReaderForTest, Is.Null,
            "instrument validation: a simulated reader leaked from an earlier test would make "
            + "this assertion about the fixture rather than about the runtime");

        var reading = ReplayHeapPressure.Read();

        Assert.Multiple(() =>
        {
            Assert.That(reading.InUseBytes, Is.GreaterThan(0),
                "a running process holds managed memory, so a non-positive occupancy means the "
                + "reader is not reading anything");

            Assert.That(reading.CeilingBytes, Is.GreaterThanOrEqualTo(0),
                "the ceiling is either a real bound or the unknown sentinel; a negative figure "
                + "would be neither");
        });
    }
}
