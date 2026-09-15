using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #2703: partition 0's checkpoint lives in the scalar
/// <c>LeafNodeState.ProjectionCheckpointOffset</c>, which has no initializer and
/// is therefore born <c>0</c> rather than at the <c>-1</c> "nothing applied"
/// sentinel every other partition uses.
/// <para>
/// The ambiguity is not theoretical. A census of 2,353 leaf rows on the
/// reference deployment found the scalar persisted as neither <c>0</c> nor
/// <c>-1</c> anywhere - the serializer omits default-valued members - so 370
/// leaves carry no explicit partition-0 value at all and read back as <c>0</c>
/// by omission, indistinguishable from a leaf genuinely checkpointed at
/// offset 0.
/// </para>
/// <para>
/// The consequence is a self-perpetuating skip in the activation replay's final
/// reconciliation. It advances a partition only when
/// <c>maxApplied &gt; GetPersistedCheckpointForPartition(partition)</c>, so a
/// partition-0 replay that reaches exactly offset <c>0</c> evaluates
/// <c>0 &gt; 0</c>, declines to advance, and never calls
/// <c>SetCheckpointOffsetAsync</c>. The partition stays uncheckpointed, which is
/// precisely the state that makes <c>ResolveDurablePinForPartition</c> return
/// the Zero block pin and disables cursor trim for the leaf's entire tree
/// (issue #2692). With the sentinel reachable the same comparison reads
/// <c>0 &gt; -1</c> and advances.
/// </para>
/// <para>
/// The remedy is an ADDITIVE nullable presence marker, not a re-defaulting of
/// the scalar to <c>-1</c>. Because the encoding omits defaults, a deployed row
/// that omitted the member would change meaning on read, silently converting
/// real progress into "nothing applied" - the failure mode runs the wrong way
/// and is unobservable. A new slot instead leaves every legacy row reading
/// exactly as it did, and the remaining ambiguity is resolved conservatively in
/// ONE place, <c>GetPersistedCheckpointForPartition</c>, so every consumer of a
/// per-partition checkpoint is honest by construction rather than by carrying
/// its own guard.
/// </para>
/// <para>
/// Why one place and not two. An earlier revision of this work carried a
/// compensating <c>&gt; 0</c> clamp inside
/// <c>IsPartitionProvenCheckpointed</c> as well. Two independent mechanisms
/// enforcing one invariant means neither can be shown to be load-bearing:
/// mutating either leaves the suite green, so the tests silently stop covering
/// the property they were written for. The clamp was removed rather than kept as
/// defence in depth, and it would additionally have been WRONG once the marker
/// exists, rejecting a leaf legitimately assigned offset 0 - which is what
/// <see cref="Partition_zero_checkpointed_at_offset_zero_is_reported_at_face_value"/>
/// exists to catch.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// The #2703 regression. A leaf that has never checkpointed partition 0 must
    /// report the "nothing applied" sentinel, not the type default it was born
    /// holding.
    /// <para>
    /// RED pre-fix: <c>GetPersistedCheckpointForPartition</c> returned the raw
    /// scalar, so this read <c>0</c> - the value that makes the replay
    /// reconciliation decline to advance a partition whose replay reached
    /// offset 0, leaving it permanently uncheckpointed.
    /// </para>
    /// <para>
    /// This is asserted on <c>GetCurrentCheckpointForPartition</c> deliberately.
    /// That is the exact accessor the reconciliation's inner guard consults, so
    /// a fix applied only to the outer guard - which reads the persisted value -
    /// would leave the skip intact and this test would still fail. Asserting the
    /// value both guards derive from covers the mechanism rather than one half
    /// of it.
    /// </para>
    /// </summary>
    [Test]
    public void Never_checkpointed_partition_zero_reports_the_nothing_applied_sentinel()
    {
        var (grain, state, _, _) = CreateLeafForCoverageRepair(persistedCheckpoint: 0L);

        // The birth shape as production actually produces it: the scalar holds
        // the CLR default and no presence marker was ever written. Asserted
        // rather than assumed, so the test cannot pass by having been set up
        // into some other state.
        Assert.That(state.State.ProjectionCheckpointOffset, Is.Zero);
        Assert.That(state.State.ProjectionCheckpointOffsetAssigned, Is.Null);

        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(-1L));
    }

    /// <summary>
    /// The over-correction guard, and the reason the disambiguation is scoped to
    /// an UNASSIGNED zero rather than to zero as such.
    /// <para>
    /// Once a leaf has genuinely checkpointed partition 0 at offset 0 the value
    /// is real progress and must be reported at face value. A fix that mapped
    /// every <c>0</c> to the sentinel would under-report it forever: the
    /// partition would re-read WAL offset 0 on every activation, never satisfy
    /// the proven-checkpoint predicate, and so never earn the coverage stamp
    /// that authorises a trim - converting the #2703 skip into a permanent
    /// version of the very starvation #2692 is about.
    /// </para>
    /// <para>
    /// This test has a real failure mode and is not a restatement of the
    /// implementation: mutating the accessor to return the sentinel for any
    /// <c>0</c> turns it RED, while the sibling test above stays green. The two
    /// together bracket the fix from opposite sides.
    /// </para>
    /// </summary>
    [Test]
    public async Task Partition_zero_checkpointed_at_offset_zero_is_reported_at_face_value()
    {
        var (grain, state, _, _) = CreateLeafForCoverageRepair(persistedCheckpoint: 0L);

        // Precondition: it reads as the sentinel BEFORE the checkpoint lands.
        // Without this the post-flush assertion could be satisfied by a leaf
        // that had simply never been ambiguous.
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(-1L));

        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(0, CancellationToken.None);
        await ((ILeafProjection)grain).FlushCheckpointAsync(CancellationToken.None);

        Assert.That(state.State.ProjectionCheckpointOffsetAssigned, Is.True);
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.Zero);
    }

    /// <summary>
    /// The upgrade direction. A row written by a version that never knew about
    /// the presence marker carries real partition-0 progress and no marker, and
    /// must keep reading at face value.
    /// <para>
    /// This is the assertion that makes the remedy safe to deploy. The rejected
    /// alternative - re-defaulting the scalar to <c>-1</c> - passes the sibling
    /// tests above and fails here, because it cannot distinguish "omitted
    /// because unset" from "omitted because default" on a row it did not write.
    /// Mutating the accessor to ignore the marker and always report the sentinel
    /// for an unmarked row turns this RED.
    /// </para>
    /// </summary>
    [Test]
    public void Legacy_partition_zero_checkpoint_is_read_at_face_value_without_a_presence_marker()
    {
        var (grain, state, _, _) = CreateLeafForCoverageRepair(persistedCheckpoint: 42L);

        Assert.That(state.State.ProjectionCheckpointOffsetAssigned, Is.Null);
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(42L));
    }

    /// <summary>
    /// ACCEPTANCE CRITERION 2, end to end. The criteria ask for a leaf whose
    /// partition-0 replay reaches exactly offset <c>0</c> to ADVANCE its
    /// checkpoint, observed through the activation replay rather than through
    /// the accessor the fix edits.
    /// <para>
    /// The sibling tests above assert the accessor's return value, which is a
    /// proxy: they would stay green against an accessor that was correct while
    /// the reconciliation that consumes it was not. This test drives the real
    /// activation path and asserts the consequence, so it covers the mechanism
    /// the issue is actually about.
    /// </para>
    /// <para>
    /// It also states the defect in its strongest form, which reading the
    /// accessor cannot. Replay resumes strictly after the reported checkpoint.
    /// A birth leaf reporting the ambiguous <c>0</c> resumes at offset <c>1</c>
    /// and so NEVER READS OFFSET 0 AT ALL: the entry is not merely left
    /// uncheckpointed, its mutation is never applied to the projection. The
    /// <c>k0</c> assertion below is therefore a committed-data-loss assertion,
    /// not a bookkeeping one. Post-fix the leaf reports the sentinel, resumes at
    /// offset 0 inclusive, applies the entry, and the final reconciliation
    /// advances because <c>0 &gt; -1</c>.
    /// </para>
    /// <para>
    /// Constructed the way production makes the state - the scalar left at the
    /// CLR default with no presence marker - and NOT by writing an explicit
    /// <c>-1</c>. That distinction is the whole point of acceptance criterion 8:
    /// the pre-existing replay coverage
    /// (<c>Materialiser_replays_offset_zero_when_checkpoint_is_nothing_applied_sentinel</c>)
    /// seeds an explicit <c>-1</c>, which only the operator-driven projection
    /// rebuild ever writes, so its green says nothing about the birth path.
    /// </para>
    /// <para>
    /// RED pre-fix: with the accessor reporting the raw scalar, the head-versus-
    /// checkpoint guard sees <c>0 &lt;= 0</c>, no replay runs, <c>k0</c> reads
    /// back null and the presence marker is never written.
    /// </para>
    /// </summary>
    [Test]
    public async Task Birth_leaf_whose_replay_reaches_offset_zero_applies_it_and_advances_the_checkpoint()
    {
        var entry = new CommitLogSliceEntry(0, BuildCommittedSet("k0", Encoding.UTF8.GetBytes("v0")));

        // head is an EXCLUSIVE bound - a leaf that has read the whole partition
        // sits at head - 1 - so a WAL holding exactly one entry, at offset 0,
        // has head 1. Passing 0 here would describe an EMPTY WAL, and the replay
        // would decline on the newest-entry check for a reason that has nothing
        // to do with this issue, making the test vacuous in the GREEN direction.
        var coord = BuildCoordinator(head: 1, entry);

        // persistedCheckpoint defaults to 0 and the helper never writes the
        // presence marker, which is exactly the production birth shape.
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ProjectionCheckpointOffset, Is.Zero,
                "precondition: partition 0 carries the CLR default, the value the encoding "
                + "produces by omitting the member");
            Assert.That(state.State.ProjectionCheckpointOffsetAssigned, Is.Null,
                "precondition: no presence marker, so this leaf has never assigned partition 0");
        });

        await ActivateAsync(grain);

        Assert.That(
            await grain.GetAsync("k0"),
            Is.Not.Null,
            "WAL offset 0 must be REPLAYED. A birth leaf that reports the ambiguous 0 resumes at "
            + "offset 1 and never reads offset 0, silently dropping its mutation from the "
            + "projection - this is the committed-data-loss face of issue #2703");
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("k0"))!), Is.EqualTo("v0"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ProjectionCheckpointOffsetAssigned, Is.True,
                "the replay advance must record the checkpoint, which is what makes partition 0 "
                + "unambiguous from here on and lets it ever satisfy the proven-checkpoint predicate");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.Zero,
                "and the recorded value is the real offset 0, now reported at face value rather "
                + "than deferred to the sentinel");
        });
    }
}
