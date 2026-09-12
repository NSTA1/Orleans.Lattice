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
}
