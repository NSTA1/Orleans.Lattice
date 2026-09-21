using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Tests for the durable-offset refusal arm of the WAL GC trim entitlement
/// clause (issue #3300).
/// <para>
/// Issue #3172 made the entitlement clause a DISJUNCTION over two axes, the HLC
/// consumer cursor and the durable materialiser offset floor, so that a leaf
/// making durable progress in offset space could earn trim rights its flat HLC
/// frontier never would. The offset axis was wired as an admit-only input: it
/// was consulted only once the cursor had already refused, so it could add
/// entitlement and never subtract it. The clause's own comment justified that by
/// calling the offset floor a stronger proof of application than the HLC
/// frontier - and then let the WEAKER proof destroy the only copy of an entry on
/// its own.
/// </para>
/// <para>
/// That asymmetry is what this arm closes. Where an admission is supplied it may
/// now REFUSE an entry the cursor would have admitted, reported as
/// <see cref="WalGcTrimEligibility.DurableOffsetRefusal"/> so the hold is
/// nameable in a scrape rather than silent. Two carve-outs are deliberate and
/// are pinned below: a null admission leaves the predicate byte-identical to its
/// pre-#3172 self, and a configured retention TTL still admits independently, so
/// an operator who has declared that data past a window may be dropped still
/// gets that honoured against a floor that has stalled.
/// </para>
/// <para>
/// <b>Read the composition section at the foot of this fixture before treating
/// this arm as a live loss fix.</b> Under the composition <c>LatticeWalGc</c>
/// builds today the refusal is unreachable, by two invariants that are pinned
/// here as executable facts rather than assumed. The durable floor is enforced
/// entirely OUTSIDE this predicate - by the <c>OffsetFloor</c> gate when a floor
/// exists and by the <c>DurabilityHold</c> arm when one does not - so the
/// disjunction inside the entitlement clause is not where the floor is applied
/// at all. The arm is defence in depth: it is what turns a future break of
/// either invariant into a named, observable hold instead of a silent trim.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeWalGcDurableOffsetRefusalTests
{
    private const string Tree = "tree";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    // The entitlement clause in its pre-#3300 form, written out independently of
    // the production source. The control test below compares the shipped
    // predicate against THIS rather than against a golden list of verdicts, so
    // the "null admission changes nothing" claim is checked against the rule it
    // is a claim about.
    private static WalGcTrimEligibility PreFixEntitlement(
        HybridLogicalClock entryTimestamp,
        HybridLogicalClock? minCursor,
        HybridLogicalClock? ttlCeiling)
    {
        var accepted = (minCursor is { } mc && mc > HybridLogicalClock.Zero && entryTimestamp <= mc)
            || (ttlCeiling is { } ceiling && entryTimestamp <= ceiling);
        return accepted ? WalGcTrimEligibility.Eligible : WalGcTrimEligibility.CursorFloor;
    }

    [Test]
    public void ClassifyEntry_refuses_when_the_admission_overrules_a_cursor_that_would_admit()
    {
        // The defect shape, stated at the decision core. The consumer cursor has
        // passed the entry (HLC 10 <= 20), so the pre-fix clause admitted it
        // outright and never consulted the offset axis at all. The admission is
        // supplied and refuses: an uncovered consumer sitting at HLC 5 has not
        // reached the entry, so no durable evidence covers it.
        //
        // The cursor tracks what a leaf folded into its CACHE, so it advances
        // the moment a write lands. Trimming on that alone destroys the only
        // durable copy of an entry that nothing has yet made durable.
        var admission = new WalGcOffsetAdmission(Floor: 100, UncoveredCursor: Hlc(5));

        var verdict = WalGcTrimCore.ClassifyEntry(
            entryTimestamp: Hlc(10),
            entryVectorClock: null,
            entryOffset: 0,
            minCursor: Hlc(20),
            ttlCeiling: null,
            causalStable: null,
            blockedFloor: null,
            offsetAdmission: admission);

        Assert.That(verdict, Is.EqualTo(WalGcTrimEligibility.DurableOffsetRefusal),
            "A supplied durable offset admission must be able to refuse an entry the consumer cursor would "
            + "have admitted, not merely to admit one the cursor refused.");
    }

    [Test]
    public void ClassifyEntry_refuses_an_entry_above_the_floor_that_the_cursor_would_admit()
    {
        // The other ground on which an admission refuses. Offset 5 is above the
        // floor of 3, so no leaf has durably applied through it, while the
        // cursor at HLC 20 has passed the entry's HLC 10 and would release it.
        //
        // LatticeWalGc stops such an entry on its own offset-floor gate before
        // the predicate is reached, so through that caller this verdict is
        // shadowed by the `offset_floor` arm. It is asserted here because
        // WalGcTrimCore is a standalone decision core with other drivers - the
        // Coyote trim-floor model among them - and must be sound on its own
        // terms rather than only in composition with one caller's gate ordering.
        var admission = new WalGcOffsetAdmission(Floor: 3, UncoveredCursor: null);

        var verdict = WalGcTrimCore.ClassifyEntry(
            entryTimestamp: Hlc(10),
            entryVectorClock: null,
            entryOffset: 5,
            minCursor: Hlc(20),
            ttlCeiling: null,
            causalStable: null,
            blockedFloor: null,
            offsetAdmission: admission);

        Assert.That(verdict, Is.EqualTo(WalGcTrimEligibility.DurableOffsetRefusal),
            "An entry above the durable offset floor has not been applied by every reporting leaf, so a "
            + "cursor that has passed it must not be sufficient on its own.");
    }

    [Test]
    public void ClassifyEntry_retention_ttl_still_admits_against_a_refusing_admission()
    {
        // The deliberate carve-out, and the one that keeps this change from
        // reintroducing issue #3094. A retention TTL is an operator statement
        // that data older than a window may be dropped; it is not evidence about
        // durability and is not meant to be. Letting the offset axis overrule it
        // would leave a retention-configured tree growing without bound whenever
        // its durable floor stalled, which is the unbounded-hold defect this
        // epic exists to prevent.
        //
        // Same inputs as the first test, plus a TTL ceiling the entry clears.
        var admission = new WalGcOffsetAdmission(Floor: 100, UncoveredCursor: Hlc(5));

        var verdict = WalGcTrimCore.ClassifyEntry(
            entryTimestamp: Hlc(10),
            entryVectorClock: null,
            entryOffset: 0,
            minCursor: Hlc(20),
            ttlCeiling: Hlc(50),
            causalStable: null,
            blockedFloor: null,
            offsetAdmission: admission);

        Assert.That(verdict, Is.EqualTo(WalGcTrimEligibility.Eligible),
            "A configured retention TTL must remain an independent admit, so a stalled durable floor cannot "
            + "hold a retention-configured tree without bound.");
    }

    [Test]
    public void ClassifyEntry_offset_admission_still_admits_what_the_cursor_refuses()
    {
        // Issue #3172 preserved, unchanged. The cursor is flat at tick 1 and
        // refuses the entry; the leaf has durably applied through offset 3 and
        // there is no uncovered consumer, so the floor is a complete proof and
        // the offset axis must still carry the pass. The whole point of scoping
        // the refusal to the cursor arm is that this direction keeps working.
        var admission = new WalGcOffsetAdmission(Floor: 3, UncoveredCursor: null);

        var verdict = WalGcTrimCore.ClassifyEntry(
            entryTimestamp: Hlc(40),
            entryVectorClock: null,
            entryOffset: 3,
            minCursor: Hlc(1),
            ttlCeiling: null,
            causalStable: null,
            blockedFloor: null,
            offsetAdmission: admission);

        Assert.That(verdict, Is.EqualTo(WalGcTrimEligibility.Eligible),
            "The offset axis must still grant entitlement the HLC axis refuses; closing the inverse hole must "
            + "not close the one issue #3172 opened deliberately.");
    }

    // The truth table of the pre-#3300 entitlement clause: every combination of
    // "cursor admits", "cursor is unset", "cursor is Zero" and "TTL admits",
    // driven at offsets both below and above where a floor would sit.
    [Test]
    [TestCase(10L, 20L, null, 0L, TestName = "cursor_admits")]
    [TestCase(30L, 20L, null, 0L, TestName = "cursor_refuses")]
    [TestCase(10L, null, null, 0L, TestName = "cursor_unset")]
    [TestCase(10L, 0L, null, 0L, TestName = "cursor_zero")]
    [TestCase(30L, 20L, 50L, 0L, TestName = "ttl_admits_what_cursor_refuses")]
    [TestCase(30L, 20L, 5L, 0L, TestName = "ttl_refuses_too")]
    [TestCase(10L, 20L, 50L, 0L, TestName = "both_admit")]
    [TestCase(10L, 20L, null, 99L, TestName = "cursor_admits_high_offset")]
    [TestCase(30L, 20L, null, 99L, TestName = "cursor_refuses_high_offset")]
    [TestCase(10L, null, 50L, 99L, TestName = "ttl_only_high_offset")]
    public void ClassifyEntry_with_a_null_admission_is_byte_for_byte_the_pre_fix_predicate(
        long entryTicks,
        long? cursorTicks,
        long? ttlTicks,
        long entryOffset)
    {
        // The fail-closed contract, and the constraint that makes this change
        // safe to ship to trees that report no durable offsets at all. A null
        // admission is what every caller supplies when no floor could be
        // established - an unreachable pin store, a host that never wired the
        // offset contract, an all-"-1" pin set - and for those trees the
        // predicate must be indistinguishable from its pre-#3172 self.
        //
        // Compared against an independently written statement of the old rule
        // rather than against recorded verdicts, so a change to both the
        // predicate and a golden list cannot pass.
        var entryTimestamp = Hlc(entryTicks);
        var minCursor = cursorTicks is { } c ? Hlc(c) : (HybridLogicalClock?)null;
        var ttlCeiling = ttlTicks is { } t ? Hlc(t) : (HybridLogicalClock?)null;

        var verdict = WalGcTrimCore.ClassifyEntry(
            entryTimestamp,
            entryVectorClock: null,
            entryOffset,
            minCursor,
            ttlCeiling,
            causalStable: null,
            blockedFloor: null,
            offsetAdmission: null);

        Assert.Multiple(() =>
        {
            Assert.That(verdict, Is.EqualTo(PreFixEntitlement(entryTimestamp, minCursor, ttlCeiling)),
                "With no admission supplied the entitlement clause must be exactly its pre-fix self.");
            Assert.That(verdict, Is.Not.EqualTo(WalGcTrimEligibility.DurableOffsetRefusal),
                "The refusal arm must be unreachable without an admission, or a tree that reports no durable "
                + "offsets would acquire a hold it has no way to clear.");
        });
    }

    [Test]
    public void IsEntryEligible_agrees_with_ClassifyEntry_on_the_refusal_arm()
    {
        // WalGcTrimCore's standing contract is that IsEntryEligible is exactly
        // `ClassifyEntry(...) == Eligible`. A new verdict is the one change that
        // can break it - the Coyote trim-floor model drives IsEntryEligible while
        // the production scan drives ClassifyEntry, so a divergence would make
        // the model verify a predicate the collector does not run.
        var admission = new WalGcOffsetAdmission(Floor: 100, UncoveredCursor: Hlc(5));

        var eligible = WalGcTrimCore.IsEntryEligible(
            entryTimestamp: Hlc(10),
            entryVectorClock: null,
            entryOffset: 0,
            minCursor: Hlc(20),
            ttlCeiling: null,
            causalStable: null,
            blockedFloor: null,
            offsetAdmission: admission);

        Assert.That(eligible, Is.False,
            "IsEntryEligible must remain exactly ClassifyEntry(...) == Eligible across the new verdict.");
    }

    // ---------------------------------------------------------------------
    // Composition. Why the arm above is defence in depth rather than a live
    // loss fix, pinned as executable facts.
    //
    // MEASURED, by source read of LatticeWalGc at the time of writing: the
    // refusal cannot fire through that caller, because reaching it needs
    // `cursorAccepts && !admission.Admits(...)` and two invariants make those
    // mutually exclusive.
    //
    //   1. EXACT COMPLEMENT. TrimShardAsync stops the scan on
    //      `offsetFloor is { } floor && walEntry.Offset > floor` BEFORE calling
    //      the predicate, and its own invariant comment requires both sides to
    //      read the same PartitionOffsetFloor(partition). So every entry that
    //      reaches the predicate already satisfies `entryOffset <= Floor`, and
    //      Admits cannot refuse on the offset ground.
    //
    //   2. SUBSET MINIMUM. minCursor is folded over EVERY registry consumer,
    //      covered ones included, while UncoveredCursor is folded over the
    //      strict subset the offset floor does not speak for. A minimum over a
    //      subset is at least the minimum over the superset, so
    //      `minCursor <= UncoveredCursor` and Admits cannot refuse on the
    //      cursor ground either.
    //
    // Neither invariant is self-evident and neither is enforced by a type, so
    // both are pinned below. If either is ever broken the refusal arm becomes
    // load-bearing: the outcome is a named hold on a published series rather
    // than an entry trimmed with no durable evidence behind it.
    //
    // FOURTH FACT, and the one that sets how much comfort to take from the
    // three above. The durable floor is enforced ENTIRELY OUTSIDE this
    // predicate, by two arms that are exact complements on whether the floor is
    // null: the OffsetFloor gate fires only when it is non-null, the
    // DurabilityHold arm only when it is null. Measured on the live estate at
    // the time of writing, with all eight arms present and zero-primed so every
    // zero is a measured one: `durability_unverified` and `durability_hold` are
    // BOTH ZERO on every tree. Neither is exercised in production.
    //
    // So the guards below pin an invariant nothing is currently leaning on, and
    // the arms that would catch a regression in it are cold. That is a reason to
    // keep these tests rather than to relax them - a cold arm gives no warning
    // when it starts being wrong - but it is not evidence that the composition
    // has been exercised and held.
    // ---------------------------------------------------------------------

    [Test]
    public async Task GetMinCursorAsync_folds_covered_consumers_so_the_tree_minimum_dominates_the_uncovered_one()
    {
        // Invariant 2, at its source. The leaf materialiser is a COVERED
        // consumer - it reports a durable offset, so the offset floor speaks for
        // it and it is excluded from the uncovered fold. It is NOT excluded from
        // this one, which is what makes the tree-wide minimum dominate.
        //
        // Were the covered consumer skipped here, the tree minimum would rise to
        // the uncovered shipper's cursor and an entry between the two could be
        // admitted by the cursor while the admission refused it - the refusal arm
        // above would start firing, and before that arm existed such an entry was
        // trimmed on cursor evidence alone.
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "_lattice_materialiser_tree_leaf-1", Hlc(5));
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(50));

        var min = await registry.GetMinCursorAsync(Tree);

        Assert.That(min, Is.EqualTo(Hlc(5)),
            "The tree-wide cursor minimum must fold every consumer, covered ones included, so it can never "
            + "exceed the minimum over the uncovered subset.");
    }

    [Test]
    public void ClassifyEntry_cannot_refuse_while_both_composition_invariants_hold()
    {
        // Invariants 1 and 2 together, asserted as the property they buy: over
        // the whole space LatticeWalGc can actually present, the refusal arm is
        // unreachable. This is what makes it honest to describe the arm as
        // defence in depth rather than as a fix for a live trim.
        //
        // The space is every combination of entry HLC, cursor and uncovered
        // cursor, filtered to the pairs the composition admits - entry at or
        // below the floor (invariant 1) and cursor no greater than uncovered
        // (invariant 2). A build that breaks either invariant will start
        // producing refusals here and this test will say so.
        long[] ticks = [1, 5, 10, 20, 50];
        const long Floor = 100;
        var refusals = 0;

        foreach (var entryTicks in ticks)
        {
            foreach (var cursorTicks in ticks)
            {
                foreach (var uncoveredTicks in ticks)
                {
                    if (cursorTicks > uncoveredTicks)
                    {
                        // Excluded by invariant 2: a minimum over a subset is
                        // never below the minimum over the superset.
                        continue;
                    }

                    var verdict = WalGcTrimCore.ClassifyEntry(
                        entryTimestamp: Hlc(entryTicks),
                        entryVectorClock: null,
                        // Excluded by invariant 1: an entry above the floor is
                        // stopped by the offset-floor gate before the predicate.
                        entryOffset: Floor,
                        minCursor: Hlc(cursorTicks),
                        ttlCeiling: null,
                        causalStable: null,
                        blockedFloor: null,
                        offsetAdmission: new WalGcOffsetAdmission(Floor, Hlc(uncoveredTicks)));

                    if (verdict == WalGcTrimEligibility.DurableOffsetRefusal)
                    {
                        refusals++;
                    }
                }
            }
        }

        Assert.That(refusals, Is.Zero,
            "While the exact-complement and subset-minimum invariants hold the refusal arm is unreachable. A "
            + "refusal here means one of them has been broken, and the arm has just prevented a trim that "
            + "would otherwise have released an entry on cursor evidence alone.");
    }
}
