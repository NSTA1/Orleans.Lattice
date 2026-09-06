using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Convergence, oscillation, and resumability coverage for automatic
/// over-split healing.
/// <para>
/// These are the tests that decide whether the feature is safe to ship
/// default-on. Convergence proves an already-damaged tree really does come back
/// down; the oscillation test proves the healer and the splitter do not fight,
/// which is the one failure mode that would otherwise appear only in
/// production; and resumability proves an interrupted heal finishes rather than
/// leaving a half-repaired tree.
/// </para>
/// </summary>
public partial class ShardHealingOrchestratorGrainTests
{
    /// <summary>
    /// Models the consolidation coordinator committing the fold the
    /// orchestrator most recently admitted: the donor's virtual slots move to
    /// the survivor and the donor leaves the routing map, exactly as the real
    /// swap phase does.
    /// </summary>
    private static async Task CommitPendingFoldAsync(Harness h)
    {
        var tracked = h.State.State.InFlightDonorShardIndices;
        if (tracked.Count == 0) return;

        var donor = tracked[^1];
        var survivor = (await h.ConsolidationOf(donor).GetProgressAsync()).SurvivorShardIndex;
        if (survivor < 0)
        {
            // The orchestrator admitted this sweep, so the survivor is whatever
            // the planner paired it with; recover it from the call the
            // orchestrator made.
            survivor = donor - 1;
        }

        var map = h.CurrentMap();
        var slots = new int[map.Slots.Length];
        for (var i = 0; i < slots.Length; i++)
            slots[i] = map.Slots[i] == donor ? survivor : map.Slots[i];
        h.SetMap(new ShardMap { Slots = slots, Version = map.Version + 1 });

        h.ConsolidationOf(donor).GetProgressAsync().Returns(new ShardConsolidationProgress
        {
            InProgress = false,
            Complete = true,
            DonorShardIndex = donor,
            SurvivorShardIndex = survivor,
        });
    }

    /// <summary>
    /// Captures which survivor the orchestrator aimed a fold at, so
    /// <see cref="CommitPendingFoldAsync"/> can model the real swap. Substitutes
    /// record the call, so the aim is read back from the received arguments.
    /// </summary>
    private static void RecordAim(Harness h, int donor)
    {
        var survivor = h.ConsolidationOf(donor).ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(ITreeShardConsolidationGrain.StartAsync))
            .Select(c => (int)c.GetArguments()[0]!)
            .LastOrDefault(-1);

        if (survivor < 0) return;

        h.ConsolidationOf(donor).GetProgressAsync().Returns(new ShardConsolidationProgress
        {
            InProgress = true,
            DonorShardIndex = donor,
            SurvivorShardIndex = survivor,
            Phase = ShardConsolidationPhase.Drain,
        });
    }

    /// <summary>
    /// Runs healing sweeps, committing each admitted fold, until the tree stops
    /// shrinking or <paramref name="maxSweeps"/> is reached. Returns the
    /// physical shard count observed after every sweep.
    /// <para>
    /// Admission is detected from the sweep's own decision rather than from a
    /// change in the tracked-fold count: a sweep that finishes one fold and
    /// starts another leaves the count identical, so counting would silently
    /// stall the simulation at the first fold.
    /// </para>
    /// </summary>
    private static async Task<List<int>> HealToSettlementAsync(Harness h, int maxSweeps = 4_000)
    {
        var counts = new List<int>();
        for (var sweep = 0; sweep < maxSweeps; sweep++)
        {
            await h.Grain.RunHealingPassAsync();
            await CompleteAdmittedFoldAsync(h);

            counts.Add(h.CurrentMap().GetPhysicalShardIndices().Count);
            if (h.State.State.LastDecision == ShardHealingDecision.NotOverSplit) break;
        }
        return counts;
    }

    /// <summary>
    /// If the sweep just admitted a fold, models its coordinator running the
    /// fold to a durable commit: the donor's slots move to the survivor and the
    /// donor leaves the routing map.
    /// </summary>
    private static async Task<bool> CompleteAdmittedFoldAsync(Harness h)
    {
        if (h.State.State.LastDecision != ShardHealingDecision.Admitted) return false;
        var tracked = h.State.State.InFlightDonorShardIndices;
        if (tracked.Count == 0) return false;

        RecordAim(h, tracked[^1]);
        await CommitPendingFoldAsync(h);
        return true;
    }

    // --- Convergence ------------------------------------------------------

    [Test]
    public async Task An_over_split_tree_converges_to_its_base_shard_count()
    {
        var h = CreateGrain(physicalShardCount: 16, baseShardCount: 2, virtualShardCount: 64);

        var counts = await HealToSettlementAsync(h);

        Assert.Multiple(() =>
        {
            Assert.That(counts[^1], Is.EqualTo(2), "healing must bring the tree back to its pinned base count");
            Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.NotOverSplit));
            Assert.That(h.State.State.LastBacklog, Is.Zero);
        });
    }

    [Test]
    public async Task Convergence_is_monotonic_and_never_overshoots()
    {
        var h = CreateGrain(physicalShardCount: 16, baseShardCount: 4, virtualShardCount: 64);

        var counts = await HealToSettlementAsync(h);

        Assert.Multiple(() =>
        {
            for (var i = 1; i < counts.Count; i++)
            {
                Assert.That(counts[i], Is.LessThanOrEqualTo(counts[i - 1]),
                    $"the physical shard count grew from {counts[i - 1]} to {counts[i]} at sweep {i}; "
                    + "healing must never add a shard");
            }
            Assert.That(counts[^1], Is.EqualTo(4),
                "healing must stop exactly at the base count, never fold past it");
        });
    }

    [Test]
    public async Task A_tree_in_a_severely_over_split_shape_converges()
    {
        // A severely over-split tree - an order of magnitude past its base -
        // driven through the real planner and the real decision core, to show
        // convergence holds at scale rather than only for a handful of folds.
        //
        // This was originally described as "the measured damage shape" after the
        // figure that founded the epic. That figure counts LEAF GRAINS, not
        // physical shards, and S14 measured the real deployment at exactly its
        // base 64 physical shards on every tree - so no such damage was ever
        // observed in production. The scale here is a deliberate stress case.
        var h = CreateGrain(physicalShardCount: 64, baseShardCount: 8, virtualShardCount: 256);

        var counts = await HealToSettlementAsync(h);

        Assert.Multiple(() =>
        {
            Assert.That(counts[^1], Is.EqualTo(8));
            Assert.That(counts, Has.Count.LessThan(4_000), "convergence must terminate, not merely trend");
        });
    }

    [Test]
    public async Task Convergence_preserves_every_key_route()
    {
        // A reader must observe no missing key while healing runs. Routing is
        // decided entirely by the shard map, so the invariant is that every
        // virtual slot always resolves to a live physical shard - a slot
        // pointing at a retired shard is precisely a key that has gone missing.
        var h = CreateGrain(physicalShardCount: 16, baseShardCount: 2, virtualShardCount: 64);

        var violations = new List<string>();
        for (var sweep = 0; sweep < 500; sweep++)
        {
            await h.Grain.RunHealingPassAsync();
            await CompleteAdmittedFoldAsync(h);

            var map = h.CurrentMap();
            var live = map.GetPhysicalShardIndices();
            for (var slot = 0; slot < map.Slots.Length; slot++)
            {
                if (!live.Contains(map.Slots[slot]))
                    violations.Add($"sweep {sweep}: slot {slot} routes to retired shard {map.Slots[slot]}");
            }

            if (h.State.State.LastDecision == ShardHealingDecision.NotOverSplit) break;
        }

        Assert.That(violations, Is.Empty, string.Join("; ", violations.Take(5)));
    }

    // --- Oscillation ------------------------------------------------------

    [Test]
    public async Task Sustained_mixed_load_reaches_a_stable_shard_count_without_cycling()
    {
        // The control-loop test. Both halves are driven for real: the splitter's
        // own admission core decides whether the map grows, and the healing
        // orchestrator decides whether it shrinks, against one shared tree under
        // sustained mixed read and write load.
        //
        // Backpressure is deliberately disabled so both loops are genuinely
        // armed at once and the ONLY thing keeping them apart is the skew dead
        // band. With the shipped defaults the two are additionally separated in
        // the load domain (see
        // Backpressure_and_the_split_threshold_separate_the_loops_in_the_load_domain),
        // which would mask an overlapping skew band rather than test it.
        const int baseShardCount = 4;
        var h = CreateGrain(physicalShardCount: 16, baseShardCount: baseShardCount, virtualShardCount: 64,
            options: new LatticeOptions { ShardHealingBackpressureOpsPerSecond = 0d });
        var splitPolicy = ShardSplitAdmissionPolicy.FromOptions(h.Options);

        var counts = new List<int>();
        var splits = 0;
        var folds = 0;

        for (var round = 0; round < 200; round++)
        {
            var map = h.CurrentMap();
            var live = map.GetPhysicalShardIndices();

            // Sustained mixed load, well above the splitter's ops/sec threshold:
            // every live shard serves reads and writes at a comparable rate,
            // with a small deterministic jitter so the tree is never
            // artificially, exactly uniform.
            var rates = new double[live.Count];
            for (var i = 0; i < live.Count; i++)
            {
                var reads = 900 + (round + i) % 7;
                var writes = 400 + (round * 3 + i) % 5;
                h.ShardOf(live[i]).GetHotnessAsync().Returns(new ShardHotness
                {
                    Reads = reads, Writes = writes, Window = TimeSpan.FromSeconds(1),
                });
                rates[i] = reads + writes;
            }

            var maxRate = rates.Max();
            var scratch = (double[])rates.Clone();
            var skew = ShardSplitAdmissionCore.ComputeSkewRatio(
                maxRate, ShardSplitAdmissionCore.ComputeMedianRate(scratch));

            // The splitter's real admission rule against the hottest shard.
            var hottest = new ShardSplitSample
            {
                Rate = maxRate,
                Entries = 100_000,
                OwnedSlots = map.Slots.Length / Math.Max(live.Count, 1),
                IsSplitting = false,
                InCooldown = false,
            };
            if (ShardSplitAdmissionCore.Evaluate(in hottest, in splitPolicy, skew, live.Count)
                == ShardSplitAdmissionOutcome.Admitted)
            {
                splits++;
                var newIndex = live[^1] + 1;
                var slots = (int[])map.Slots.Clone();
                var moved = 0;
                for (var s = 0; s < slots.Length && moved < 2; s++)
                {
                    if (slots[s] == live[0]) { slots[s] = newIndex; moved++; }
                }
                h.SetMap(new ShardMap { Slots = slots, Version = map.Version + 1 });
            }

            await h.Grain.RunHealingPassAsync();
            if (await CompleteAdmittedFoldAsync(h)) folds++;

            counts.Add(h.CurrentMap().GetPhysicalShardIndices().Count);
        }

        // Settled: the last quarter of the run shows no movement at all.
        var tail = counts.Skip(counts.Count - 50).Distinct().ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(splits, Is.Zero,
                "a uniformly loaded tree must never be split: rate alone cannot tell a hot shard from a hot tree");
            Assert.That(tail, Has.Length.EqualTo(1),
                "the shard count must settle rather than cycle; observed tail: " + string.Join(", ", tail));
            Assert.That(tail[0], Is.EqualTo(baseShardCount));
            Assert.That(folds, Is.EqualTo(16 - baseShardCount),
                "exactly one fold per excess shard, with no fold undone and re-done");
        });
    }

    [Test]
    public void Backpressure_and_the_split_threshold_separate_the_loops_in_the_load_domain()
    {
        // A second, independent separation the shipped defaults happen to give
        // for free, worth pinning because it is load-bearing for the control
        // loop's safety and would be easy to break by "tuning" one knob.
        //
        // Healing yields at or above a median shard rate of
        // ShardHealingBackpressureOpsPerSecond; the splitter only considers a
        // shard at or above HotShardOpsPerSecondThreshold. The two defaults are
        // the same number, so at any load where a split is even conceivable,
        // healing has already stood down - the loops are separated by load as
        // well as by skew, and neither separation alone is relied upon.
        var options = new LatticeOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.ShardHealingBackpressureOpsPerSecond,
                Is.LessThanOrEqualTo(options.HotShardOpsPerSecondThreshold),
                "healing must yield at or before the load at which the splitter starts considering a shard");
            Assert.That(
                ShardHealingDecisionCore.IsUnderBackpressure(
                    options.HotShardOpsPerSecondThreshold, options.ShardHealingBackpressureOpsPerSecond),
                Is.True,
                "at exactly the splitter's rate threshold, healing must already have yielded");
        });
    }

    [Test]
    public async Task A_settled_tree_is_left_completely_alone()
    {
        // Once healed, repeated sweeps must be inert: no fold started, no
        // coordinator touched, nothing written. A healer that kept nudging a
        // healthy tree would be indistinguishable from one that oscillates.
        var h = CreateGrain(physicalShardCount: 4, baseShardCount: 4);

        for (var i = 0; i < 20; i++) await h.Grain.RunHealingPassAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.NotOverSplit));
            Assert.That(h.State.State.InFlightDonorShardIndices, Is.Empty);
            Assert.That(h.State.WriteCount, Is.LessThanOrEqualTo(1),
                "a settled tree must not cost a storage write per sweep");
        });
        await h.ConsolidationOf(3).DidNotReceive().StartAsync(Arg.Any<int>());
    }

    // --- Resumability -----------------------------------------------------

    [Test]
    public async Task A_reactivated_orchestrator_resumes_from_persisted_state()
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);
        await h.Grain.RunHealingPassAsync();
        var donor = h.State.State.InFlightDonorShardIndices.Single();
        RecordAim(h, donor);

        // Model a silo restart: a brand-new grain over the same persisted row.
        var restarted = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            existingState: h.State.State);
        MarkInFlight(restarted, donor, donor - 1);

        await restarted.Grain.RunHealingPassAsync();

        Assert.Multiple(() =>
        {
            Assert.That(restarted.State.State.InFlightDonorShardIndices, Is.EqualTo(new[] { donor }),
                "the restarted orchestrator must resume tracking the fold it left in flight");
            Assert.That(restarted.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.AtCapacity));
        });
        await restarted.ConsolidationOf(donor).Received().RunConsolidationPassAsync();
    }

    [Test]
    public async Task Repeated_interruption_still_converges()
    {
        // Healing is interrupted on every single sweep - a fresh grain each
        // time, as if the silo restarted constantly - and must still reach the
        // base shard count, because every fact it needs is either persisted or
        // recoverable from the coordinator that owns it.
        var state = new ShardHealingOrchestratorState();
        var map = ShardMap.CreateDefault(64, 16);
        var count = 16;

        for (var sweep = 0; sweep < 400 && count > 2; sweep++)
        {
            var h = CreateGrain(physicalShardCount: 16, baseShardCount: 2, existingState: state);
            h.SetMap(map);

            await h.Grain.RunHealingPassAsync();
            if (await CompleteAdmittedFoldAsync(h)) map = h.CurrentMap();

            count = map.GetPhysicalShardIndices().Count;
            state = h.State.State;
        }

        Assert.That(count, Is.EqualTo(2),
            "convergence must survive an interruption on every sweep");
    }

    [Test]
    public async Task A_completed_fold_is_dropped_from_tracking()
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            existingState: new ShardHealingOrchestratorState { InFlightDonorShardIndices = [7] });
        h.ConsolidationOf(7).GetProgressAsync().Returns(new ShardConsolidationProgress
        {
            InProgress = false, Complete = true, DonorShardIndex = 7, SurvivorShardIndex = 6,
        });
        h.SetMap(MapOver(64, 0, 1, 2, 3, 4, 5, 6));

        await h.Grain.RunHealingPassAsync();

        Assert.That(h.State.State.InFlightDonorShardIndices, Does.Not.Contain(7),
            "a finished fold must free its admission slot");
    }

    [Test]
    public async Task An_abandoned_fold_is_dropped_from_tracking()
    {
        // A cancel honoured at a pre-swap boundary leaves the tree exactly as it
        // was. The orchestrator must treat that as a freed slot, not as a fold
        // still running, or healing would stall on a tree nothing is repairing.
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            existingState: new ShardHealingOrchestratorState { InFlightDonorShardIndices = [7] });
        h.ConsolidationOf(7).GetProgressAsync().Returns(new ShardConsolidationProgress
        {
            InProgress = false, Cancelled = true, DonorShardIndex = 7, SurvivorShardIndex = 6,
        });

        await h.Grain.RunHealingPassAsync();

        Assert.That(h.State.State.InFlightDonorShardIndices, Does.Not.Contain(7));
    }

    [Test]
    public async Task Re_issuing_the_plan_every_sweep_never_double_starts_a_fold()
    {
        // The orchestrator relies on the coordinator being idempotent for the
        // same survivor, so a sweep that re-plans the same pair must be a no-op
        // rather than a second fold.
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            options: new LatticeOptions { MaxConcurrentShardConsolidations = 4 });

        await h.Grain.RunHealingPassAsync();
        var donor = h.State.State.InFlightDonorShardIndices.Single();
        RecordAim(h, donor);
        await h.Grain.RunHealingPassAsync();

        Assert.That(h.State.State.InFlightDonorShardIndices.Count(d => d == donor), Is.EqualTo(1),
            "the same donor must never be tracked twice");
    }

    [Test]
    public async Task Healing_survives_a_storage_write_failure()
    {
        // Healing is best-effort background repair. A failed persist must not
        // fault the sweep: the next sweep re-derives everything it needs, and
        // the durable record of the fold lives in its own coordinator.
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);
        h.State.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        Assert.That(async () => await h.Grain.RunHealingPassAsync(), Throws.Nothing);
        await Task.CompletedTask;
    }

    [Test]
    public async Task Admission_records_its_intent_before_starting_the_fold()
    {
        // Write-ahead intent. A crash between persisting and starting must be
        // recoverable in the safe direction: the orchestrator may believe in a
        // fold that never started - which the next reconcile clears - but must
        // never start one it has not recorded, because it would then admit a
        // second fold and exceed the concurrency cap.
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);
        var trackedWhenStarted = -1;
        h.ConsolidationOf(1).StartAsync(Arg.Any<int>()).Returns(_ =>
        {
            trackedWhenStarted = h.State.State.InFlightDonorShardIndices.Count;
            return Task.CompletedTask;
        });

        await h.Grain.RunHealingPassAsync();

        Assert.That(trackedWhenStarted, Is.EqualTo(1),
            "the donor must already be tracked, and persisted, by the time StartAsync is issued");
    }

    [Test]
    public async Task A_forgotten_fold_that_never_started_is_cleared_by_the_next_reconcile()
    {
        // The recovery half of write-ahead intent: a donor recorded but never
        // started leaves its coordinator idle, so the next sweep drops it and
        // frees the admission slot rather than stalling healing forever.
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            existingState: new ShardHealingOrchestratorState { InFlightDonorShardIndices = [5] });

        await h.Grain.RunHealingPassAsync();

        Assert.That(h.State.State.InFlightDonorShardIndices, Does.Not.Contain(5),
            "a recorded fold whose coordinator is idle must be cleared, not waited on");
    }

    [Test]
    public async Task A_disabled_orchestrator_still_reports_the_folds_it_is_holding()
    {
        // Switching healing off must not make an in-flight fold invisible: an
        // operator needs to know a drain is still finishing before they act on
        // the tree.
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            options: new LatticeOptions { ShardHealingEnabled = false },
            existingState: new ShardHealingOrchestratorState { InFlightDonorShardIndices = [7] });

        await h.Grain.RunHealingPassAsync();
        var report = await h.Grain.GetHealingReportAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.Decision, Is.EqualTo(ShardHealingDecision.Disabled));
            Assert.That(report.InFlightConsolidations, Is.EqualTo(1),
                "the in-flight count is read from durable state, so it stays accurate with no RPC");
        });
    }

    [Test]
    public async Task Healing_still_folds_a_map_whose_indices_are_no_longer_contiguous()
    {
        // A partially-healed tree has gaps where retired shards used to be.
        // Adjacency ignores those gaps, so folding must remain possible all the
        // way down rather than stalling at the first hole.
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);
        h.SetMap(MapOver(64, 0, 3, 9, 40));

        await h.Grain.RunHealingPassAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.Admitted));
            Assert.That(h.State.State.InFlightDonorShardIndices, Has.Count.EqualTo(1));
        });
    }
}
