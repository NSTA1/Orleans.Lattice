using System.Text;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for <see cref="LeafResidentWorkingSet"/>, the per-silo bound on
/// resident leaf hydration (issue #2767).
/// <para>
/// The measurement this type answers: attaching a leaf snapshot retains the
/// whole encoded frame for the life of the activation, so resident heap grew at
/// 1.008x frame size per retained activation with zero rows materialised and did
/// not fall under a forced blocking compacting gen2 collection, while the same
/// hydrations completed and released cost 0.2% of that. The exhaustion is
/// resident, so a gate that bounds concurrent hydration cannot see it at any
/// budget. These tests pin the shedding policy that does.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafResidentWorkingSetTests
{
    private const long KiB = 1024L;

    private sealed record Shed(string TreeId, bool Banked);

    private static LeafResidencyRegistration RegisterLeaf(
        LeafResidentWorkingSet workingSet,
        List<Shed> shed,
        string treeId,
        long bytes,
        bool banked,
        Func<bool>? isPinned = null)
        => workingSet.Register(
            treeId, bytes, banked, () => shed.Add(new Shed(treeId, banked)), isPinned);

    // ---------------------------------------------------------------
    // Budget derivation. The bound must come from the runtime, never
    // from configuration: the defect is a process already exhausting its
    // heap, and a bound that only binds once an operator sets it does
    // not fix one.
    // ---------------------------------------------------------------

    [Test]
    public void ResolveBudgetBytes_takes_a_fraction_of_the_heap_hard_limit()
    {
        const long heapLimit = 9L * 1024 * 1024 * 1024;

        var budget = LeafResidentWorkingSet.ResolveBudgetBytes(heapLimit);

        Assert.That(budget, Is.EqualTo(heapLimit / LeafResidentWorkingSet.HeapBudgetDivisor));
        Assert.That(
            budget,
            Is.LessThan(heapLimit),
            "a resident budget at or above the heap limit bounds nothing");
    }

    [Test]
    public void ResolveBudgetBytes_never_falls_below_the_floor()
    {
        // A heap so small that the derived share would shed every leaf the
        // moment it activated, converting an exhaustion into a livelock.
        var budget = LeafResidentWorkingSet.ResolveBudgetBytes(1024L);

        Assert.That(budget, Is.EqualTo(LeafResidentWorkingSet.MinimumBudgetBytes));
    }

    [Test]
    public void ResolveBudgetBytes_stays_bounded_when_the_runtime_reports_no_heap_limit()
    {
        // Asserts the PROPERTY (boundedness), not the constant. An earlier
        // revision compared the result against
        // LeafResidentWorkingSet.UnknownHeapLimitBudgetBytes, which is a
        // tautology: flipping the constant flips the expectation with it, so the
        // assertion held for every possible value including long.MaxValue - the
        // exact value the test's own name says must not be used. A perturbation
        // arm that set the constant to long.MaxValue reddened nothing, which is
        // the signature of a vacuous assertion rather than a dead clause.
        //
        // The ceiling below is an independent quantity chosen to be far above
        // any defensible fallback and far below "unbounded", so it discriminates
        // without re-encoding the implementation.
        const long unboundedFloor = 16L * 1024 * 1024 * 1024;

        Assert.Multiple(() =>
        {
            Assert.That(
                LeafResidentWorkingSet.ResolveBudgetBytes(0L),
                Is.GreaterThan(0L).And.LessThan(unboundedFloor),
                "an unknown ceiling is not licence to be unbounded");
            Assert.That(
                LeafResidentWorkingSet.ResolveBudgetBytes(-1L),
                Is.GreaterThan(0L).And.LessThan(unboundedFloor),
                "a negative reported limit is also an unknown ceiling, not an unbounded one");
        });
    }

    // ---------------------------------------------------------------
    // Accounting.
    // ---------------------------------------------------------------

    [Test]
    public void Registering_within_budget_sheds_nothing()
    {
        var shed = new List<Shed>();
        var workingSet = new LeafResidentWorkingSet(10 * KiB);

        RegisterLeaf(workingSet, shed, "t", 3 * KiB, banked: true);
        RegisterLeaf(workingSet, shed, "t", 3 * KiB, banked: true);

        Assert.That(shed, Is.Empty);
        Assert.That(workingSet.ResidentBytes, Is.EqualTo(6 * KiB));
        Assert.That(workingSet.RegisteredCount, Is.EqualTo(2));
    }

    [Test]
    public void Releasing_a_registration_returns_its_bytes()
    {
        var shed = new List<Shed>();
        var workingSet = new LeafResidentWorkingSet(10 * KiB);

        var first = RegisterLeaf(workingSet, shed, "t", 4 * KiB, banked: true);
        RegisterLeaf(workingSet, shed, "t", 4 * KiB, banked: true);

        first.Dispose();

        Assert.That(workingSet.ResidentBytes, Is.EqualTo(4 * KiB));
        Assert.That(workingSet.RegisteredCount, Is.EqualTo(1));
    }

    [Test]
    public void Releasing_twice_returns_the_bytes_once()
    {
        var shed = new List<Shed>();
        var workingSet = new LeafResidentWorkingSet(10 * KiB);

        var registration = RegisterLeaf(workingSet, shed, "t", 4 * KiB, banked: true);

        registration.Dispose();
        registration.Dispose();

        Assert.That(
            workingSet.ResidentBytes,
            Is.Zero,
            "a double release that drives the total negative silently raises the effective budget");
    }

    [Test]
    public void Exceeding_the_budget_sheds_until_it_fits()
    {
        var shed = new List<Shed>();
        var workingSet = new LeafResidentWorkingSet(10 * KiB);

        RegisterLeaf(workingSet, shed, "t", 4 * KiB, banked: true);
        RegisterLeaf(workingSet, shed, "t", 4 * KiB, banked: true);
        RegisterLeaf(workingSet, shed, "t", 4 * KiB, banked: true);

        Assert.That(shed, Has.Count.EqualTo(1), "one shed brings 12 KiB back under a 10 KiB budget");
        Assert.That(workingSet.ResidentBytes, Is.EqualTo(8 * KiB));
    }

    [Test]
    public void A_condemned_registration_does_not_return_its_bytes_a_second_time()
    {
        var shed = new List<Shed>();
        var workingSet = new LeafResidentWorkingSet(10 * KiB);

        var first = RegisterLeaf(workingSet, shed, "t", 6 * KiB, banked: true);
        RegisterLeaf(workingSet, shed, "t", 6 * KiB, banked: true);

        Assert.That(shed, Has.Count.EqualTo(1), "precondition: the first leaf was condemned");

        // The shed leaf now finishes deactivating and releases.
        first.Dispose();

        Assert.That(
            workingSet.ResidentBytes,
            Is.EqualTo(6 * KiB),
            "condemnation already deducted these bytes; deducting again would understate the resident set");
    }

    // ---------------------------------------------------------------
    // Shedding order. This is the load-bearing policy.
    // ---------------------------------------------------------------

    [Test]
    public void A_snapshot_banked_leaf_is_shed_in_preference_to_an_unbanked_one()
    {
        var shed = new List<Shed>();
        var workingSet = new LeafResidentWorkingSet(10 * KiB);

        // The unbanked leaf is registered FIRST, so pure recency would select
        // it. It must not be selected: it has no snapshot to come back on, so
        // shedding it buys a cold whole-window replay queued behind the replay
        // permit that issue #2768 measured as failing to drain.
        RegisterLeaf(workingSet, shed, "t", 4 * KiB, banked: false);
        RegisterLeaf(workingSet, shed, "t", 4 * KiB, banked: true);
        RegisterLeaf(workingSet, shed, "t", 4 * KiB, banked: true);

        Assert.That(shed, Has.Count.EqualTo(1));
        Assert.That(
            shed[0].Banked,
            Is.True,
            "the banked leaf must be shed even though the unbanked one is older");
    }

    [Test]
    public void The_oldest_leaf_within_a_class_is_shed_first()
    {
        var shed = new List<Shed>();
        var workingSet = new LeafResidentWorkingSet(10 * KiB);

        RegisterLeaf(workingSet, shed, "oldest", 4 * KiB, banked: true);
        RegisterLeaf(workingSet, shed, "middle", 4 * KiB, banked: true);
        RegisterLeaf(workingSet, shed, "newest", 4 * KiB, banked: true);

        Assert.That(shed, Has.Count.EqualTo(1));
        Assert.That(shed[0].TreeId, Is.EqualTo("oldest"));
    }

    [Test]
    public void An_unbanked_leaf_is_shed_once_no_banked_candidate_remains()
    {
        var shed = new List<Shed>();
        var workingSet = new LeafResidentWorkingSet(10 * KiB);

        RegisterLeaf(workingSet, shed, "u1", 4 * KiB, banked: false);
        RegisterLeaf(workingSet, shed, "u2", 4 * KiB, banked: false);
        RegisterLeaf(workingSet, shed, "u3", 4 * KiB, banked: false);

        Assert.That(
            shed.Select(s => s.TreeId).ToArray(),
            Is.EqualTo(new[] { "u1" }),
            "the preference orders the classes; it must not exempt either, or a working set of "
            + "only unbanked leaves would sit permanently over budget");
    }

    [Test]
    public void Banked_candidates_are_exhausted_before_any_unbanked_one_is_touched()
    {
        var shed = new List<Shed>();
        var workingSet = new LeafResidentWorkingSet(10 * KiB);

        RegisterLeaf(workingSet, shed, "u1", 2 * KiB, banked: false);
        RegisterLeaf(workingSet, shed, "b1", 2 * KiB, banked: true);
        RegisterLeaf(workingSet, shed, "b2", 2 * KiB, banked: true);

        // 6 KiB resident; this admission takes it to 15 KiB, so the sweep must
        // free at least 5 KiB and no single candidate can supply it. Both banked
        // leaves must go before the unbanked one is considered, even though the
        // unbanked one is the oldest registration of the three.
        RegisterLeaf(workingSet, shed, "big", 9 * KiB, banked: true);

        Assert.That(shed.Select(s => s.TreeId).ToArray(), Is.EqualTo(new[] { "b1", "b2", "u1" }));
    }

    // ---------------------------------------------------------------
    // Forward progress. A bound that can stall the leaf it is admitting
    // is worse than no bound.
    // ---------------------------------------------------------------

    [Test]
    public void The_admitting_leaf_is_never_shed_by_its_own_admission()
    {
        var shed = new List<Shed>();
        var workingSet = new LeafResidentWorkingSet(4 * KiB);

        // On its own, and already over budget.
        RegisterLeaf(workingSet, shed, "sole", 9 * KiB, banked: true);

        Assert.That(
            shed,
            Is.Empty,
            "a leaf that can never come online can never be divided back under bound");
        Assert.That(workingSet.RegisteredCount, Is.EqualTo(1));
    }

    [Test]
    public void A_leaf_is_never_shed_by_an_admission_that_followed_it_into_the_same_sweep()
    {
        var shed = new List<Shed>();
        var workingSet = new LeafResidentWorkingSet(4 * KiB);

        RegisterLeaf(workingSet, shed, "first", 3 * KiB, banked: true);
        RegisterLeaf(workingSet, shed, "second", 3 * KiB, banked: true);

        Assert.That(shed.Select(s => s.TreeId).ToArray(), Is.EqualTo(new[] { "first" }));
        Assert.That(
            shed.Select(s => s.TreeId),
            Does.Not.Contain("second"),
            "an activation must outlive the sweep it triggers, or it is shed before taking any "
            + "benefit from the frame it just attached");
    }

    [Test]
    public void Shedding_stops_when_no_older_candidate_remains_rather_than_looping()
    {
        var shed = new List<Shed>();
        var workingSet = new LeafResidentWorkingSet(KiB);

        RegisterLeaf(workingSet, shed, "a", 8 * KiB, banked: true);
        RegisterLeaf(workingSet, shed, "b", 8 * KiB, banked: true);

        Assert.That(shed.Select(s => s.TreeId).ToArray(), Is.EqualTo(new[] { "a" }));
        Assert.That(
            workingSet.ResidentBytes,
            Is.EqualTo(8 * KiB),
            "the admitting leaf stays resident over budget rather than being shed for its own admission");
    }

    [Test]
    public void A_pinned_leaf_is_never_shed_even_when_it_is_the_only_candidate()
    {
        // A split is persisted state, so it spans turns; Orleans deferring a
        // requested deactivation to the end of the current turn protects a
        // turn-local operation and does nothing for a multi-turn one. Without
        // this exclusion a sweep landing between transfer batches would
        // interrupt a partial split.
        var shed = new List<Shed>();
        var workingSet = new LeafResidentWorkingSet(KiB);

        RegisterLeaf(workingSet, shed, "splitting", 8 * KiB, banked: true, isPinned: () => true);
        RegisterLeaf(workingSet, shed, "admitting", 8 * KiB, banked: true);

        Assert.Multiple(() =>
        {
            Assert.That(shed, Is.Empty, "a mid-split leaf must not be shed");
            Assert.That(
                workingSet.ResidentBytes,
                Is.EqualTo(16 * KiB),
                "the budget is knowingly exceeded rather than interrupting a multi-turn split");
        });
    }

    [Test]
    public void An_unpinned_leaf_is_still_shed_when_a_pinned_one_is_present()
    {
        // The control for the test above. Without it, an implementation that
        // treated every leaf as pinned would pass that one and shed nothing
        // ever, which is the same observable as having no bound at all.
        var shed = new List<Shed>();
        var workingSet = new LeafResidentWorkingSet(10 * KiB);

        RegisterLeaf(workingSet, shed, "splitting", 4 * KiB, banked: true, isPinned: () => true);
        RegisterLeaf(workingSet, shed, "idle", 4 * KiB, banked: true);
        RegisterLeaf(workingSet, shed, "admitting", 4 * KiB, banked: true);

        Assert.That(
            shed.Select(s => s.TreeId).ToArray(),
            Is.EqualTo(new[] { "idle" }),
            "the pin excludes one leaf from selection; it does not disable shedding");
    }

    [Test]
    public void A_leaf_larger_than_the_entire_budget_is_admitted_and_the_bound_is_knowingly_exceeded()
    {
        // The terminal behaviour, named deliberately rather than left to be
        // discovered. A bound that is silently violated is worse than one that
        // documents its own escape hatch, because the next reader relies on it.
        //
        // Refusing the admission instead would be far worse than exceeding the
        // budget: a leaf that cannot stay activated can never be captured,
        // split, or repaired, so refusal converts a bounded overshoot into a
        // permanent stall - the self-reinforcing shape this epic exists to
        // remove. The overshoot is bounded by one leaf and is repaid as soon as
        // the oversized leaf divides.
        var shed = new List<Shed>();
        var workingSet = new LeafResidentWorkingSet(4 * KiB);

        var registration = RegisterLeaf(workingSet, shed, "huge", 64 * KiB, banked: false);

        Assert.Multiple(() =>
        {
            Assert.That(shed, Is.Empty, "there is nothing older to shed");
            Assert.That(registration.IsCondemned, Is.False, "the sole occupant is admitted, not shed");
            Assert.That(
                workingSet.ResidentBytes,
                Is.EqualTo(64 * KiB),
                "a single leaf exceeding the whole budget is admitted anyway and the bound is exceeded");
            Assert.That(
                workingSet.ResidentBytes,
                Is.GreaterThan(workingSet.BudgetBytes),
                "stated explicitly so a future reader cannot mistake this for a hard bound");
        });
    }

    [Test]
    public void An_over_budget_leaf_does_not_shed_itself_into_a_reactivation_loop()
    {
        // The livelock the terminal case must not become: if admission could
        // select the leaf it has just admitted, an oversized leaf would
        // deactivate, reactivate, re-register and repeat, presenting as a leaf
        // that can never stay activated long enough to be repaired.
        var shed = new List<Shed>();
        var workingSet = new LeafResidentWorkingSet(4 * KiB);

        for (var i = 0; i < 5; i++)
        {
            using var registration = RegisterLeaf(workingSet, shed, "huge", 64 * KiB, banked: false);
            Assert.That(
                registration.IsCondemned,
                Is.False,
                $"cycle {i}: the admitting leaf must never be shed by its own admission");
        }

        Assert.That(
            shed,
            Is.Empty,
            "a sole oversized occupant re-activating repeatedly must never shed itself");
    }

    [Test]
    public void A_failing_shed_callback_does_not_fail_the_admitting_activation()
    {
        var workingSet = new LeafResidentWorkingSet(10 * KiB);

        workingSet.Register("t", 6 * KiB, snapshotBanked: true, () => throw new InvalidOperationException("boom"));

        Assert.DoesNotThrow(
            () => workingSet.Register("t", 6 * KiB, snapshotBanked: true, () => { }),
            "shedding runs on an unrelated leaf's activation turn; a failure to deactivate one "
            + "leaf must not fail the activation that triggered the sweep");
    }

    [Test]
    public void A_registration_over_no_working_set_is_inert()
    {
        Assert.DoesNotThrow(() => LeafResidencyRegistration.None.Dispose());
        Assert.That(LeafResidencyRegistration.None.Bytes, Is.Zero);
    }

    // ---------------------------------------------------------------
    // The quantity being bounded.
    // ---------------------------------------------------------------

    [Test]
    public void ResidentFootprintBytes_counts_the_retained_frame_that_StateBytes_cannot_see()
    {
        var rows = new LeafSnapshotRow[64];
        for (var i = 0; i < rows.Length; i++)
        {
            var payload = new byte[512];
            Array.Fill(payload, (byte)i);
            rows[i] = new LeafSnapshotRow(
                $"k{i:D5}",
                new LwwValue<byte[]>
                {
                    Value = payload,
                    Timestamp = new HybridLogicalClock { WallClockTicks = 100L + i, Counter = i },
                });
        }

        var frame = LeafSnapshotCodec.Encode(rows);
        var cache = new LeafEntryCache(new SortedDictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal));
        Assert.That(cache.TryAttachSnapshot(frame, long.MaxValue), Is.True);

        Assert.That(cache.HydratedRowCount, Is.Zero, "precondition: nothing has been read");
        Assert.That(
            cache.ResidentFootprintBytes,
            Is.GreaterThanOrEqualTo(frame.Length),
            "a leaf that attaches a snapshot and is never read still holds the whole frame; "
            + "a footprint that excludes it reports that leaf as costing nothing");

        // And the frame term is not merely present, it dominates: the rows this
        // leaf has taken the benefit of number zero.
        Assert.That(cache.SnapshotBytesRead, Is.Zero);
    }

    [Test]
    public void ResidentFootprintBytes_falls_back_to_decoded_rows_when_no_snapshot_is_attached()
    {
        var cache = new LeafEntryCache(new SortedDictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal));
        cache.StoreRow(
            "k",
            new LwwValue<byte[]>
            {
                Value = Encoding.UTF8.GetBytes(new string('x', 4096)),
                Timestamp = new HybridLogicalClock { WallClockTicks = 1L },
            });

        Assert.That(cache.HasPendingHydration, Is.False);
        Assert.That(cache.ResidentFootprintBytes, Is.EqualTo(cache.StateBytes));
        Assert.That(
            cache.ResidentFootprintBytes,
            Is.GreaterThan(0L),
            "a cold activation holds decoded rows and no frame; counting only the frame would "
            + "leave that class unbounded while reporting the silo as within budget");
    }
}
