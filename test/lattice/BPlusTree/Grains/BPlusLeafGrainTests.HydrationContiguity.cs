using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the contiguity clause of the cold-activation hydration
/// admission gate (issue #2844).
/// <para>
/// The gate these extend already bounded <b>aggregate</b> in-flight bytes
/// (issue #2765) and was entirely silent about <b>contiguity</b>. In production
/// that silence read as coverage: a claim reserving 32.6% of a 1,207,959,552
/// byte budget was admitted with 776 MiB of headroom, and threw
/// <see cref="OutOfMemoryException"/> anyway inside the storage provider's blob
/// read, 41 times in a 22-minute run. Nothing about that outcome is visible to
/// byte accounting, because what ran out was not total memory but one unbroken
/// run of it on the large object heap.
/// </para>
/// <para>
/// Every test here drives the gate directly with stored-byte <i>figures</i> and
/// allocates nothing, so the property under test is the admission arithmetic
/// rather than the host's actual heap - which is the only way to assert it
/// deterministically, since whether a given contiguous allocation succeeds
/// depends on fragmentation the test cannot arrange.
/// </para>
/// <para>
/// The two-sidedness matters and is deliberate. A test that only showed a large
/// claim being serialised would be satisfied by a gate that serialised
/// <i>everything</i>, which is a far worse defect than the one being fixed - so
/// each exclusion test is paired with a claim of comparable aggregate weight
/// that must still be admitted concurrently. In every exclusion test the
/// precondition - that the claim fits by the rules the gate had before this
/// change - is asserted through the pre-existing byte accounting
/// (<see cref="LeafSnapshotHydrationAdmission.BudgetBytes"/>,
/// <see cref="LeafSnapshotHydrationAdmission.InFlightBytes"/> and
/// <see cref="LeafSnapshotHydrationAdmission.ToHeapCostBytes(long)"/>), never
/// through the contiguity predicate itself. Establishing the precondition with
/// the mechanism under test would be the instrument agreeing with itself.
/// </para>
/// </summary>
[TestFixture]
public class BPlusLeafGrainHydrationContiguityTests
{
    // Stored sizes, in the units every caller of the gate speaks. The gate
    // performs both conversions itself, which is what stops a call site
    // comparing a stored figure against a heap-denominated budget.

    // Exactly the configured leaf size bound. A leaf this size is by definition
    // healthy, so it must keep hydrating alongside its peers.
    private const long BoundSizedStoredBytes = LatticeOptions.DefaultMaxLeafBytes;

    // Comfortably oversized: 200 MiB stored needs a 400 MiB contiguous buffer,
    // three times the ceiling. This is the shape the reported corpus carried.
    private const long OversizedStoredBytes = 200L * 1024 * 1024;

    private const long TinyStoredBytes = 1_000L;

    // Four times the heap cost of the oversized claim, so that claim reserves a
    // quarter of the budget - close to the 32.6% the production incident
    // recorded, and the whole point: it fits, with room to spare.
    private static long SpaciousBudgetBytes
        => LeafSnapshotHydrationAdmission.ToHeapCostBytes(OversizedStoredBytes) * 4;

    private static async Task SpinUntilAsync(Func<bool> condition, string because)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                Assert.Fail($"Timed out waiting for {because}.");
            }

            await Task.Delay(10);
        }
    }

    // Every claim below is taken through a bounded token. A clause that has been
    // reverted must redden a test rather than park a task on the gate forever,
    // because an orphaned claim hangs the host at shutdown and names nothing.
    private static CancellationTokenSource ClaimDeadline()
        => new(TimeSpan.FromSeconds(15));

    /// <summary>
    /// Asserts, using only the accounting the gate had before this change, that
    /// a claim of <paramref name="storedBytes"/> genuinely fits the remaining
    /// aggregate budget. Every exclusion test calls this first, so that when it
    /// then shows the claim being held back, the only thing that can be holding
    /// it back is the new predicate.
    /// </summary>
    private static void AssertFitsByByteAccounting(
        LeafSnapshotHydrationAdmission admission,
        long storedBytes)
    {
        var cost = LeafSnapshotHydrationAdmission.ToHeapCostBytes(storedBytes);
        Assert.That(
            admission.InFlightBytes + cost,
            Is.LessThanOrEqualTo(admission.BudgetBytes),
            "PRECONDITION: by the aggregate byte accounting that predates issue #2844 this claim fits, "
            + "so anything that holds it back is the contiguity rule and not the budget");
    }

    // ---------------------------------------------------------------------
    // The conversions and the predicate.
    // ---------------------------------------------------------------------

    [Test]
    public void ToContiguousBytes_is_the_legacy_json_ratio_not_the_heap_amplification()
    {
        // Two multiples of the same stored figure, both denominated in bytes,
        // are trivially swapped at a call site and the mistake compiles.
        //
        // The ratio is 8/3 and it describes the LEGACY JSON read path, which is
        // the worse of the two the gate may get and the one it must therefore
        // size against: the provider hands back the whole document as a single
        // contiguous UTF-16 string, base64 inflating the frame by 4/3 and each
        // character costing 2 bytes. The binary path added by issues #2516 and
        // #2833 returns a byte[] of about the frame length instead, roughly 1x.
        // Reads route on the stored payload's LGB1 magic, not on the type, so a
        // blob written by an older build still takes the JSON path today and
        // the gate cannot know which it will get before it sizes the claim.
        //
        // The remaining heap amplification is the parsed graph, which is many
        // small objects and imposes no contiguity requirement at all.
        Assert.Multiple(() =>
        {
            Assert.That(
                LeafSnapshotHydrationAdmission.ToContiguousBytes(3_000L),
                Is.EqualTo(8_000L),
                "4/3 for base64 inflation, times 2 bytes per UTF-16 character");
            Assert.That(
                LeafSnapshotHydrationAdmission.ToContiguousBytes(3_000L),
                Is.Not.EqualTo(LeafSnapshotHydrationAdmission.ToHeapCostBytes(3_000L)),
                "the contiguous requirement and the heap cost are different quantities");
            Assert.That(
                LeafSnapshotHydrationAdmission.ToContiguousBytes(3_000L),
                Is.GreaterThan(3_000L * 2),
                "REGRESSION GUARD: an earlier revision used 2x, derived from a JSON serializer "
                + "shape that no longer describes this path, and understated the live legacy "
                + "requirement by a third");
            Assert.That(
                LeafSnapshotHydrationAdmission.ToContiguousBytes(0L),
                Is.Zero,
                "a leaf with no recorded size claims nothing");
            Assert.That(
                LeafSnapshotHydrationAdmission.ToContiguousBytes(long.MaxValue),
                Is.EqualTo(long.MaxValue),
                "the conversion saturates rather than overflowing to a negative figure, which would "
                + "read as a tiny claim and admit the largest snapshot in the corpus unconditionally");
        });
    }

    [Test]
    public void The_contiguous_ratio_cancels_out_of_the_sole_occupancy_predicate()
    {
        // The ceiling is the SAME conversion applied to a bound-sized leaf, so
        // the ratio appears on both sides of RequiresSoleOccupancy and cancels:
        // the effective rule is "stored frame larger than the default leaf size
        // bound". That is the whole justification for the number, so the
        // boundary is pinned on both sides. Were it off by one in the
        // permissive direction the population it governs would be unchanged;
        // off by one the other way, every healthy leaf in a default-configured
        // tree would serialise and cold start would become sequential.
        //
        // The cancellation is also what makes the ratio safe to correct:
        // choosing 2x, 8/3 or 3x moves only the figure reported in the
        // exception and in logs, never which claims are serialised. Pinned
        // because without it a future correction of the ratio would look like a
        // behaviour change and invite someone to "compensate" by moving the
        // ceiling as well, which WOULD be a behaviour change, disguised as
        // bookkeeping.
        Assert.Multiple(() =>
        {
            Assert.That(
                LeafSnapshotHydrationAdmission.RequiresSoleOccupancy(BoundSizedStoredBytes),
                Is.False,
                "a leaf exactly at its size bound is healthy and must hydrate concurrently");
            Assert.That(
                LeafSnapshotHydrationAdmission.RequiresSoleOccupancy(BoundSizedStoredBytes + 1),
                Is.True,
                "and one byte past it is over bound, which is the population this rule governs");
            Assert.That(
                LeafSnapshotHydrationAdmission.ConcurrentContiguousCeilingBytes,
                Is.EqualTo(
                    LeafSnapshotHydrationAdmission.ToContiguousBytes(LatticeOptions.DefaultMaxLeafBytes)),
                "the ceiling is the conversion of a bound-sized leaf, which is both what makes the "
                + "ratio cancel and what keeps the ceiling derived from the leaf size bound rather "
                + "than from the memory grant - a ceiling that rose with the grant would relax "
                + "exactly as the population it governs grew");
        });
    }

    // ---------------------------------------------------------------------
    // The headline regression, both sides.
    // ---------------------------------------------------------------------

    [Test]
    public async Task A_claim_whose_contiguous_requirement_exceeds_the_ceiling_waits_even_though_its_bytes_fit()
    {
        // The reported defect exactly. A small hydration is already in flight;
        // an oversized one arrives whose reservation fits the remaining budget
        // with room to spare. Before this change it was admitted, and then threw
        // OutOfMemoryException while materialising one contiguous buffer.
        var admission = new LeafSnapshotHydrationAdmission(SpaciousBudgetBytes);
        using var deadline = ClaimDeadline();

        using var resident = await admission.AcquireAsync(TinyStoredBytes, deadline.Token);
        Assert.That(admission.AdmittedCount, Is.EqualTo(1));

        AssertFitsByByteAccounting(admission, OversizedStoredBytes);

        var oversized = admission.AcquireAsync(OversizedStoredBytes, deadline.Token);

        // Spin until the claim has REACHED a decision either way, rather than
        // until it has queued. Waiting for the queue would turn a regression
        // into a 15-second timeout whose message names the wait and not the
        // property; this resolves immediately in both worlds, so a reverted
        // clause fails on the assertion below and says what it expected.
        await SpinUntilAsync(
            () => oversized.IsCompleted || admission.QueuedCount == 1,
            "the oversized claim to reach the gate");

        Assert.Multiple(() =>
        {
            Assert.That(oversized.IsCompleted, Is.False,
                "the oversized claim is held back despite fitting the budget, because the buffer it needs "
                + "must come from one unbroken run of memory and aggregate headroom does not supply that");
            Assert.That(admission.QueuedCount, Is.EqualTo(1),
                "and it waits rather than being dropped");
            Assert.That(admission.AdmittedCount, Is.EqualTo(1),
                "only the resident hydration is running");
            Assert.That(admission.ExclusiveCount, Is.Zero,
                "and nothing is holding the gate exclusively yet");
        });

        // Forward progress: releasing the resident claim empties the gate, which
        // is the condition the oversized claim was waiting for. It is serialised,
        // never refused - a refusal would leave the leaf unable to activate and
        // therefore unable to divide back under bound, which is strictly worse
        // than the crash it replaces.
        resident.Dispose();

        using var admitted = await oversized.WaitAsync(TimeSpan.FromSeconds(20));

        Assert.Multiple(() =>
        {
            Assert.That(admitted.Exclusive, Is.True,
                "it ran as sole occupant");
            Assert.That(
                admitted.ContiguousBytes,
                Is.EqualTo(LeafSnapshotHydrationAdmission.ToContiguousBytes(OversizedStoredBytes)),
                "and reports the contiguous figure, so a failure downstream is attributable to the "
                + "quantity that actually ran out rather than to the reservation that comfortably fitted");
            Assert.That(admission.ExclusiveCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_bound_sized_claim_of_similar_weight_is_still_admitted_concurrently()
    {
        // The other side, and the reason the test above proves anything. A gate
        // that serialised every large claim would satisfy it while turning every
        // cold start sequential. The claim here costs 335,544,320 heap bytes -
        // the same order as the oversized one - and is admitted alongside a
        // resident hydration, because its CONTIGUOUS requirement is within the
        // ceiling. The rule discriminates on contiguity, not on size.
        var admission = new LeafSnapshotHydrationAdmission(SpaciousBudgetBytes);
        using var deadline = ClaimDeadline();

        using var resident = await admission.AcquireAsync(TinyStoredBytes, deadline.Token);

        var boundSized = admission.AcquireAsync(BoundSizedStoredBytes, deadline.Token);
        using var lease = await boundSized.WaitAsync(TimeSpan.FromSeconds(20));

        Assert.Multiple(() =>
        {
            Assert.That(lease.Queued, Is.False,
                "a leaf within its configured size bound never waits on the contiguity rule");
            Assert.That(lease.Exclusive, Is.False,
                "and is not promoted to sole occupant");
            Assert.That(admission.AdmittedCount, Is.EqualTo(2),
                "both hydrations run together, so the fix does not sequentialise cold start");
            Assert.That(
                LeafSnapshotHydrationAdmission.ToHeapCostBytes(BoundSizedStoredBytes),
                Is.GreaterThan(LeafSnapshotHydrationAdmission.ToHeapCostBytes(TinyStoredBytes) * 1_000),
                "and it is genuinely a heavy claim, not a token one that would pass any gate");
        });
    }

    [Test]
    public async Task While_a_sole_occupant_hydration_is_held_no_other_claim_joins_it()
    {
        // The exclusion has to be MUTUAL or it is worth nothing. Admitting an
        // oversized claim only into an empty gate, while still letting later
        // claims join once it is admitted, would leave its allocation racing
        // exactly the concurrent large-object churn the rule exists to remove -
        // and would do so by construction, because that claim reserves only a
        // quarter of the budget and so leaves ample room for others.
        var admission = new LeafSnapshotHydrationAdmission(SpaciousBudgetBytes);
        using var deadline = ClaimDeadline();

        var exclusive = await admission.AcquireAsync(OversizedStoredBytes, deadline.Token);
        Assert.That(admission.ExclusiveCount, Is.EqualTo(1));

        AssertFitsByByteAccounting(admission, TinyStoredBytes);

        var joiner = admission.AcquireAsync(TinyStoredBytes, deadline.Token);

        await SpinUntilAsync(
            () => joiner.IsCompleted || admission.QueuedCount == 1,
            "the joining claim to reach the gate");

        Assert.Multiple(() =>
        {
            Assert.That(joiner.IsCompleted, Is.False,
                "a trivially small claim waits behind the sole occupant, because the guarantee the sole "
                + "occupant was given is exactly that nothing else allocates alongside it");
            Assert.That(admission.QueuedCount, Is.EqualTo(1));
            Assert.That(admission.AdmittedCount, Is.EqualTo(1));
        });

        exclusive.Dispose();

        using var joined = await joiner.WaitAsync(TimeSpan.FromSeconds(20));

        Assert.Multiple(() =>
        {
            Assert.That(joined.Queued, Is.True, "it was held, then admitted");
            Assert.That(admission.ExclusiveCount, Is.Zero,
                "the exclusive hold is released with its lease, so one oversized leaf cannot wedge the "
                + "gate for the rest of the process");
            Assert.That(admission.AdmittedCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Reconcile_does_not_promote_or_demote_an_existing_claim()
    {
        // Exclusivity is fixed at admission and deliberately never revisited. A
        // measurement only arrives after the blob has been read, so the
        // contiguous allocation this rule governs has already either succeeded
        // or thrown: promoting here would exclude other claims to protect an
        // allocation that is over, and demoting would release a guarantee that
        // was already spent. Only the aggregate figure is still live, so only it
        // is corrected.
        var admission = new LeafSnapshotHydrationAdmission(SpaciousBudgetBytes);
        using var deadline = ClaimDeadline();

        using var lease = await admission.AcquireAsync(TinyStoredBytes, deadline.Token);
        Assert.That(lease.Exclusive, Is.False);

        lease.Reconcile(OversizedStoredBytes);

        Assert.Multiple(() =>
        {
            Assert.That(lease.Exclusive, Is.False,
                "a claim that turned out to be oversized is not promoted after the fact");
            Assert.That(admission.ExclusiveCount, Is.Zero);
            Assert.That(
                lease.HeldBytes,
                Is.EqualTo(LeafSnapshotHydrationAdmission.ToHeapCostBytes(OversizedStoredBytes)),
                "but the aggregate reservation IS corrected, which tightens admission for everyone still "
                + "queued rather than only for the next activation");
            Assert.That(
                lease.ContiguousBytes,
                Is.EqualTo(LeafSnapshotHydrationAdmission.ToContiguousBytes(OversizedStoredBytes)),
                "and the reported contiguous figure follows the measurement, so the failure path quotes "
                + "what was actually attempted rather than what was estimated");
        });
    }

    // ---------------------------------------------------------------------
    // Attribution.
    // ---------------------------------------------------------------------

    [Test]
    public void The_unaffordable_exception_names_contiguity_only_when_the_hydration_ran_alone()
    {
        // The message this replaces quoted reserved-against-budget
        // unconditionally, so on the arm where the claim fitted it read as "the
        // budget was too small" and sent a reader straight to enlarging the
        // memory grant - the single worst available action, because every
        // byte-denominated bound here is derived from that grant and each admits
        // MORE concurrent work as it rises.
        //
        // Naming contiguity is only supportable when concurrent demand was zero
        // by construction, which is what sole occupancy records. See the paired
        // test below for the other side.
        var error = new LeafSnapshotUnaffordableException(
            "tree-a",
            reservedBytes: 394_270_800L,
            budgetBytes: 1_207_959_552L,
            contiguousBytes: 157_708_320L,
            soleOccupant: true,
            innerException: new OutOfMemoryException());

        Assert.Multiple(() =>
        {
            Assert.That(error.ContiguousBytes, Is.EqualTo(157_708_320L),
                "the contiguous requirement is carried, not merely described");
            Assert.That(error.SoleOccupant, Is.True,
                "and so is the admission fact the diagnosis rests on");
            Assert.That(error.Message, Does.Contain("SOLE OCCUPANT"),
                "the message states plainly that nothing else was hydrating alongside it, which is what "
                + "rules out aggregate demand");
            Assert.That(error.Message, Does.Contain("CONTIGUITY"),
                "and names the predicate that actually failed");
            Assert.That(error.Message, Does.Contain("157708320"),
                "quoting the buffer that could not be found");
            Assert.That(error.Message, Does.Contain("394270800"),
                "and the reservation, which is the figure that makes 'the budget was too small' untenable");
            Assert.That(error.Message, Does.Contain("will not fix"),
                "the message must actively steer away from raising the memory limit rather than merely "
                + "omitting the advice");
        });
    }

    [Test]
    public void The_unaffordable_exception_withholds_the_contiguity_verdict_when_the_hydration_ran_concurrently()
    {
        // The other side of the pair, and the one that caught a real regression.
        //
        // The first draft branched this message on reserved <= budget, which is
        // true of very nearly every claim - the budget is a fraction of the heap
        // and an ordinary leaf reserves a fraction of the budget. So it asserted
        // a CONTIGUITY failure, in capitals, for the entire ordinary
        // out-of-memory population, including claims that ran alongside others
        // and therefore had concurrent aggregate demand as a live explanation.
        //
        // Identical figures to the test above. Only the admission fact differs,
        // which is the point: the numbers cannot carry this verdict on their own.
        var error = new LeafSnapshotUnaffordableException(
            "tree-a",
            reservedBytes: 394_270_800L,
            budgetBytes: 1_207_959_552L,
            contiguousBytes: 157_708_320L,
            soleOccupant: false,
            innerException: new OutOfMemoryException());

        Assert.Multiple(() =>
        {
            Assert.That(error.SoleOccupant, Is.False);
            Assert.That(error.Message, Does.Contain("FITTED"),
                "the claim did fit, and saying so is still the useful half - it is the verdict that has "
                + "to be withheld, not the arithmetic");
            Assert.That(error.Message, Does.Not.Contain("CONTIGUITY"),
                "a claim that ran concurrently cannot be attributed to contiguity on these figures alone, "
                + "and a confident wrong attribution costs more than none");
            Assert.That(error.Message, Does.Contain("does not choose between them"),
                "the message says explicitly that it is leaving the question open, rather than leaving a "
                + "reader to notice an absence");
            Assert.That(error.Message, Does.Contain("157708320"),
                "the contiguous figure is still quoted, because it is what a reader needs in order to "
                + "decide the question the message declines to decide");
        });
    }

    [Test]
    public void The_unaffordable_exception_keeps_the_overran_framing_when_the_claim_overran()
    {
        // The third arm: a snapshot larger than the whole budget is admitted
        // only because nothing else is in flight, and may still fail. That one
        // genuinely is a shortage of total memory relative to the corpus, so it
        // must not inherit the contiguity wording either.
        //
        // Reachable without sole occupancy: on a small heap the budget floors at
        // 32 MiB, so a claim can overrun it while its contiguous requirement
        // stays under the ceiling.
        var error = new LeafSnapshotUnaffordableException(
            "tree-a",
            reservedBytes: 4_000L,
            budgetBytes: 1_000L,
            contiguousBytes: 1_600L,
            soleOccupant: false,
            innerException: new OutOfMemoryException());

        Assert.Multiple(() =>
        {
            Assert.That(error.Message, Does.Contain("exceeded the gate's budget"));
            Assert.That(error.Message, Does.Not.Contain("FITTED"),
                "a claim that overran the budget did not fit, and saying so on both arms would make the "
                + "distinction worthless");
            Assert.That(error.Message, Does.Not.Contain("CONTIGUITY"),
                "nor did the gate serialise it, so the contiguity verdict is unsupportable here too");
            Assert.That(error.Message, Does.Contain("nothing else was in flight"));
        });
    }
}
