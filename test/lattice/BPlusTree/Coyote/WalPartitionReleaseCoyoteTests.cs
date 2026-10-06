using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote tests for <see cref="WalPartitionReleaseModel"/>: the empty
/// releases of <see cref="Orleans.Lattice.BPlusTree.LeafDurablePinCore"/> on a
/// leaf whose writes span two WAL partitions (issue #4433, review finding F08),
/// under the GC's cursor arm, its offset admission and a retention TTL. Tagged
/// <c>[Category("Coyote")]</c> so the dev loop and the deterministic CI tier skip
/// them; the <c>coyote</c> tier runs them.
/// <para>
/// Every guard removes one rule and requires the violation to be reported by
/// <c>[AckedWriteDurable]</c>, the only assertion the model makes.
/// </para>
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class WalPartitionReleaseCoyoteTests
{
    private const string Tag = "[AckedWriteDurable]";

    /// <summary>
    /// The intended design: an empty release only for a partition this
    /// activation has replayed or whose WAL is proven empty, and a TTL ceiling
    /// capped at the lowest frontier of a partition's uncovered pins (issue
    /// #4622), with override-stamped writes held by issue #4641's override hold:
    /// raised before the append of a record stamped below the clock or not ticked by
    /// the leaf (saturated merges included), once
    /// per partition per activation, dropped by the pin store only with the
    /// consumer's first real offset, and read by a GC pass after its head bound
    /// and before its census, each read a separate step. No acknowledged write
    /// leaves durable state.
    /// </summary>
    [TestCase(false)]
    [TestCase(true)]
    public void Empty_releases_lose_no_acknowledged_write_under_the_replay_barrier_and_the_frontier_capped_ttl(bool afterWalReset)
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new WalPartitionReleaseModel(
                releaseOnlyAfterReplay: true, stopBudget: 2, ttl: true, afterWalReset: afterWalReset,
                overrideStampedWrites: true, saturatedMerges: true),
            iterations: 20000);
    }

    /// <summary>
    /// The guard for issue #4622's cap: with the TTL ceiling yielding only to a
    /// Zero pin, an empty release published before the leaf writes to that
    /// partition (or, after a WAL reset, the proven-empty release of issue #3103)
    /// leaves an uncovered pin whose frontier no Zero hold sees, and the TTL arm
    /// trims the write before it is checkpointed or captured.
    /// </summary>
    /// <remarks>
    /// The measured per-run detection rate is p ~ 0.15 from a normal start (40
    /// explorations, 265 paths), so 5000 runs leave no realistic chance of a miss
    /// from either start.
    /// </remarks>
    [TestCase(false)]
    [TestCase(true)]
    public void A_ttl_that_yields_only_to_zero_pins_trims_a_write_made_after_an_empty_release(bool afterWalReset)
    {
        var result = CoyoteModelHarness.Explore(
            new WalPartitionReleaseModel(
                releaseOnlyAfterReplay: true, ttl: true, ttlCappedAtUncoveredFrontier: false, afterWalReset: afterWalReset),
            iterations: 5000);

        AssertCaughtBy(result, "a TTL ceiling yielding only to Zero pins");
    }

    /// <summary>
    /// The guard for the replay barrier: an empty release published for a
    /// partition the activation has not yet replayed carries a frontier above
    /// the leaf's unread writes there, so the cursor arm trims them. This is the
    /// F08 hazard. Production's checkpoint flush tail did publish such a release
    /// for the partition a cold leaf swept first (issue #4669); since #4677 every
    /// partition counts as data-bearing until the replay barrier latches
    /// (<c>BPlusLeafGrain.WithUnreplayedPartitionsLive</c>), and
    /// <c>LeafEmptyReleaseBeforeReplayTests</c> is red with that gate disabled.
    /// </summary>
    /// <remarks>
    /// The measured per-run detection rate is p ~ 1.0e-2 (40 explorations, 3864
    /// paths), so 5000 runs miss it with probability ~ e^-50.
    /// </remarks>
    [Test]
    public void An_empty_release_published_before_the_partition_is_replayed_trims_an_unread_write()
    {
        var result = CoyoteModelHarness.Explore(
            new WalPartitionReleaseModel(releaseOnlyAfterReplay: false),
            iterations: 5000);

        AssertCaughtBy(result, "an empty release published before the replay");
    }

    /// <summary>
    /// The guard for issue #4641's override hold: a write stamped with a source
    /// HLC below the leaf's clock (<c>LatticeHlcOverrideContext</c>) can land at or
    /// below the frontier of the empty release its partition already published,
    /// and the pin store cannot lower that frontier. Without the hold the offset
    /// admission (its uncovered cursor is that frontier) trims the write before it
    /// is checkpointed or captured, with no TTL involved.
    /// </summary>
    /// <remarks>
    /// The measured per-run detection rate is p ~ 3.2e-3 (20 explorations, 6275
    /// paths), so 10000 runs miss it with probability ~ e^-32.
    /// </remarks>
    [Test]
    public void Skipping_the_override_hold_lets_the_gc_trim_an_override_stamped_write_issue_4641()
    {
        var result = CoyoteModelHarness.Explore(
            new WalPartitionReleaseModel(releaseOnlyAfterReplay: true, overrideStampedWrites: true, overrideHold: false),
            iterations: 10000);

        AssertCaughtBy(result, "skipping the override hold");
    }
    /// <summary>
    /// The guard for who clears the override hold (issue #4641). The hold records
    /// no offsets, so a leaf that clears it on a persisted checkpoint clears a pin
    /// no snapshot covers, and the next GC pass trims the override write. In the
    /// landed design the leaf never clears a hold; only the pin store does, with a
    /// real offset.
    /// </summary>
    [Test]
    public void Clearing_the_override_hold_on_a_persisted_checkpoint_lets_the_gc_trim_an_override_stamped_write_issue_4641()
    {
        var result = CoyoteModelHarness.Explore(
            new WalPartitionReleaseModel(
                releaseOnlyAfterReplay: true, stopBudget: 2, ttl: true, overrideStampedWrites: true,
                overrideHoldClear: WalPartitionReleaseModel.OverrideHoldClear.OnPersistedCheckpoint),
            iterations: 20000);

        AssertCaughtBy(result, "clearing the override hold on a persisted checkpoint");
    }
    /// <summary>
    /// The guard for the pin store's prune (issue #4641): a hold dropped by any
    /// publish - the empty release that carries a frontier and no offset - leaves
    /// the override write below that frontier, and the cursor arm trims it.
    /// </summary>
    [Test]
    public void Dropping_the_override_hold_without_a_real_offset_lets_the_gc_trim_an_override_stamped_write_issue_4641()
    {
        var result = CoyoteModelHarness.Explore(
            new WalPartitionReleaseModel(
                releaseOnlyAfterReplay: true, stopBudget: 2, ttl: true, overrideStampedWrites: true,
                overrideHoldClear: WalPartitionReleaseModel.OverrideHoldClear.StoreOnAnyPublish),
            iterations: 20000);

        AssertCaughtBy(result, "dropping the hold on any publish");
    }

    /// <summary>
    /// The guard for the GC's read order (issue #4641): a pass that reads the
    /// holds before its head bound misses a hold raised between the two reads,
    /// yet its bound covers the write that hold guards, so it trims the write.
    /// </summary>
    [Test]
    public void Reading_the_holds_before_the_head_bound_lets_the_gc_trim_an_override_stamped_write_issue_4641()
    {
        var result = CoyoteModelHarness.Explore(
            new WalPartitionReleaseModel(
                releaseOnlyAfterReplay: true, overrideStampedWrites: true,
                gcReadOrder: WalPartitionReleaseModel.GcReadOrder.HoldsHeadCensus),
            iterations: 20000);

        AssertCaughtBy(result, "reading the holds before the head bound");
    }

    /// <summary>
    /// The guard for the trigger (issue #4641). At the HLC counter ceiling
    /// <c>HybridLogicalClock.Merge</c> saturates, so the merged clock can equal the
    /// carried stamp and a trigger of "stamped strictly below the clock" alone does
    /// not fire. The landed trigger (<c>BPlusLeafGrain.NeedsOverrideHold</c>) also
    /// fires on every record the leaf did not tick, and the design run above
    /// includes saturated merges.
    /// </summary>
    [Test]
    public void A_trigger_on_a_stamp_below_the_clock_alone_misses_a_saturated_merge_issue_4641()
    {
        var result = CoyoteModelHarness.Explore(
            new WalPartitionReleaseModel(
                releaseOnlyAfterReplay: true, ttl: true, overrideStampedWrites: true, saturatedMerges: true,
                overrideHoldTrigger: WalPartitionReleaseModel.OverrideHoldTrigger.StampBelowClockOnly),
            iterations: 20000);

        AssertCaughtBy(result, "a trigger on a stamp below the clock alone");
    }

    private static void AssertCaughtBy(CoyoteExplorationResult result, string removed)
    {
        Assert.That(
            result.BugsFound,
            Is.GreaterThan(0),
            $"{removed} must produce a violation in {result.Iterations} explored runs; none was found.");
        Assert.That(
            string.Join("\n", result.BugReports),
            Does.Contain(Tag),
            $"{removed} was caught, but not by {Tag}.");
    }
}
