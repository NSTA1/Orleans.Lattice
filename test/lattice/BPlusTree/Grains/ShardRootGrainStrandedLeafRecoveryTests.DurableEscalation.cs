using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3016, second half: whether the stranded-leaf recovery <b>took</b>.
/// <para>
/// The sibling file asserts the classification and the recovery, and both are
/// correctly scoped to one shard-root activation: the run counts how many times
/// this activation attached to one parked coalesced read and got nowhere, and
/// the remedy it selects drops that activation's own map entry. What neither
/// can observe is the question that remedy immediately raises. A freshly
/// activated shard root holds no coalesced reads at all, so its eviction is a
/// no-op <em>by construction</em>, and its first stall is byte-identical to a
/// first-ever stall no matter how many days the leaf has been unreadable.
/// </para>
/// <para>
/// That is not a hypothetical. It is the exact shape of the deployed corpus in
/// the issue - 307 attempts, byte-identical message, three days - and it is why
/// the count of recovery applications is the one quantity here that is
/// persisted. Every test in this file therefore builds a <b>second grain over
/// the same storage row</b>, which is what a recycled shard root is, and every
/// one of them would pass vacuously against activation-scoped state if it did
/// not.
/// </para>
/// </summary>
public partial class ShardRootGrainStrandedLeafRecoveryTests
{
    /// <summary>
    /// Drives exactly enough zero-progress ceiling fires to classify the wedged
    /// leaf unreadable, and hands back the fault raised by the one that crossed
    /// the threshold.
    /// </summary>
    private static ScanPageStalledException StrandOnce(RecoveryHarness harness)
    {
        ScanPageStalledException? last = null;
        for (var attempt = 1; attempt <= ShardRootGrain.StrandedLeafStallThreshold; attempt++)
        {
            last = Stall(harness);
        }

        return last!;
    }

    /// <summary>
    /// The first stranding reports one application and says nothing about the
    /// remedy having failed, because at that point it has not: the eviction has
    /// just happened and the next attempt genuinely will issue a fresh read.
    /// <para>
    /// Paired with the arm below rather than standing alone. On its own it is
    /// satisfied by a field hard-wired to one, and the two together are not.
    /// </para>
    /// </summary>
    [Test]
    public void A_first_stranding_reports_one_application_and_does_not_localise_the_fault()
    {
        var harness = CreateHarness();

        var stall = StrandOnce(harness);

        Assert.Multiple(() =>
        {
            Assert.That(stall.LeafStranded, Is.True);
            Assert.That(stall.StrandedRecoveryApplications, Is.EqualTo(1),
                "the recovery has been applied once and may yet take");
            Assert.That(stall.Message, Does.Not.Contain("fault is inside that leaf activation"),
                "a first application must not be reported as a proven failure of the remedy, or "
                + "every slow leaf would be blamed on its own activation the first time it stalls");
        });

        harness.Drain();
    }

    /// <summary>
    /// <b>The arm this file exists for.</b> A shard root that is recycled and
    /// strands the same leaf again must report that the recovery has been
    /// applied twice - which is the only available evidence that a fresh read
    /// was already issued and the leaf still did not answer, and therefore the
    /// only thing that separates "the shard root was parked on a dead read" from
    /// "the leaf itself cannot be read".
    /// <para>
    /// The second activation's <em>own</em> run is asserted to have restarted
    /// from scratch, and that assertion is doing real work: it establishes that
    /// the escalation cannot have come from the in-memory run, so the only thing
    /// it can have come from is the storage row. Without it, an implementation
    /// that simply carried the activation-scoped counter forward would satisfy
    /// the rest of this test.
    /// </para>
    /// </summary>
    [Test]
    public void A_recycled_shard_root_reports_that_the_recovery_was_already_applied()
    {
        var first = CreateHarness();
        var firstStall = StrandOnce(first);
        first.Drain();

        // A recycled shard root: same storage row, same leaf identities, an
        // entirely fresh activation with an empty coalescing map.
        var second = CreateHarness(carriedState: first.State);
        var secondStall = StrandOnce(second);

        Assert.Multiple(() =>
        {
            Assert.That(firstStall.StrandedRecoveryApplications, Is.EqualTo(1));
            Assert.That(secondStall.StrandedRecoveryApplications, Is.EqualTo(2),
                "the recycled activation must be able to tell that the eviction it just performed "
                + "had already been performed once before and did not make the leaf readable");

            Assert.That(secondStall.ConsecutiveZeroProgressStalls,
                Is.EqualTo(ShardRootGrain.StrandedLeafStallThreshold),
                "the second activation's own run must have restarted from zero - if it had not, "
                + "the escalation could be coming from activation-scoped state and this test "
                + "would prove nothing about durability");

            Assert.That(secondStall.Message, Does.Contain("fault is inside that leaf activation"),
                "the escalated reading must reach an operator through the message, not only "
                + "through a typed slot nothing logs");
            Assert.That(secondStall.Message, Does.Contain("2 times"));
        });

        second.Drain();
    }

    /// <summary>
    /// The escalation is a property of the leaf, not of the shard, so stranding
    /// a different leaf restarts the count rather than continuing it. Without
    /// this the field would become a cumulative shard-level tally, which reads
    /// as "this leaf has failed twice" on a shard that has in fact failed two
    /// different leaves once each - the opposite condition, and one with an
    /// entirely different remedy.
    /// </summary>
    [Test]
    public void Stranding_a_different_leaf_restarts_the_durable_count()
    {
        var first = CreateHarness();
        StrandOnce(first);
        first.Drain();

        var second = CreateHarness(carriedState: first.State);
        second.MoveWedgeToSecondLeaf();
        var stall = StrandOnce(second);

        Assert.Multiple(() =>
        {
            Assert.That(stall.LeafInFlight, Is.EqualTo(second.LeafIds[1].ToString()),
                "the fixture must actually have moved the wedge, or this arm is measuring the "
                + "first leaf a second time and would pass on a cumulative counter");
            Assert.That(stall.StrandedRecoveryApplications, Is.EqualTo(1),
                "a newly stranded leaf has had the recovery applied once, whatever happened to "
                + "any other leaf on this shard");
            Assert.That(second.State.State.StrandedScanLeafId,
                Is.EqualTo(second.LeafIds[1].ToString()));
        });

        second.Drain();
    }

    /// <summary>
    /// The record is committed to storage before the fault that reports it is
    /// observed by the caller, not merely mutated in memory. A record that is
    /// only in memory is exactly the thing this file exists to replace, so
    /// asserting the in-memory value alone would restate the defect as the fix.
    /// </summary>
    [Test]
    public void The_escalation_record_is_persisted_before_the_fault_is_observed()
    {
        var harness = CreateHarness();
        var writesBefore = harness.State.WriteCount;

        StrandOnce(harness);

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.WriteCount, Is.GreaterThan(writesBefore),
                "the durable record must reach storage, or it cannot survive the recycling it "
                + "exists to survive");
            Assert.That(harness.State.State.StrandedScanLeafId,
                Is.EqualTo(harness.WedgedLeafId.ToString()));
            Assert.That(harness.State.State.StrandedScanRecoveries, Is.EqualTo(1));
        });

        harness.Drain();
    }

    /// <summary>
    /// Stalls short of the threshold must not write. The write is affordable
    /// only because it happens once per stranding - a value reached after
    /// <see cref="ShardRootGrain.StrandedLeafStallThreshold"/> whole ceilings of
    /// wall clock - and a shard that wrote on every ordinary stall would be
    /// putting storage traffic on the path of a tree that is already failing.
    /// </summary>
    [Test]
    public void A_stall_short_of_stranding_writes_nothing()
    {
        var harness = CreateHarness();
        var writesBefore = harness.State.WriteCount;

        for (var attempt = 1; attempt < ShardRootGrain.StrandedLeafStallThreshold; attempt++)
        {
            var stall = Stall(harness);
            Assert.That(stall.LeafStranded, Is.False,
                "the loop must stop short of the classification, or this arm is not measuring "
                + "the sub-threshold path at all");
            Assert.That(stall.StrandedRecoveryApplications, Is.Zero,
                "a stall that did not reach the classification has applied no recovery");
        }

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.WriteCount, Is.EqualTo(writesBefore));
            Assert.That(harness.State.State.StrandedScanLeafId, Is.Null);
        });

        harness.Drain();
    }

    /// <summary>
    /// A storage failure while recording the escalation must leave the caller
    /// holding the <see cref="ScanPageStalledException"/> that names the wedge,
    /// not a storage fault that names its bookkeeping. The wedge is what the
    /// caller retries on and what an operator diagnoses from; replacing it with
    /// a write failure would lose the incident behind its own audit trail.
    /// <para>
    /// The count is still asserted correct on the returned fault, because the
    /// in-memory advance is what the fault is built from and it survives the
    /// failed write for the rest of the activation's life.
    /// </para>
    /// </summary>
    [Test]
    public void A_failed_write_still_raises_the_stall_and_still_reports_the_count()
    {
        var harness = CreateHarness();
        harness.State.ThrowOnWrite = new InvalidOperationException("storage is unavailable");

        var stall = StrandOnce(harness);

        Assert.Multiple(() =>
        {
            Assert.That(stall.LeafStranded, Is.True);
            Assert.That(stall.StrandedRecoveryApplications, Is.EqualTo(1),
                "the fault is built from the in-memory advance, which a failed write does not "
                + "roll back");
            Assert.That(stall.LeafInFlight, Is.EqualTo(harness.WedgedLeafId.ToString()));
        });

        harness.Drain();
    }
}
