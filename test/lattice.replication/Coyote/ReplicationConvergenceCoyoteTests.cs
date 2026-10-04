using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Replication.Tests.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for replication convergence: the receiver's
/// dedup and merge (<see cref="ReplicationDedupConvergenceModel"/>), the cycle-break over a
/// multi-hop topology (<see cref="ReplicationCycleBreakModel"/>), and the shipper's scalar
/// cursor filter (<see cref="ReplicationShipCursorModel"/>). Each fixed-design arm has a
/// companion guard arm that removes exactly one fix and must find a violation, and an
/// anti-vacuity witness proving the exploration reaches the ordering the fix is for. Tagged
/// <c>[Category("Coyote")]</c>; see the "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>. The TLA+ module these models execute
/// the cores of is <c>spec/replication/Replication.tla</c>.
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class ReplicationConvergenceCoyoteTests
{
    /// <summary>
    /// The fix: deduplicating only on exact identity and the idempotent merge never drops a new
    /// write, and every replica value converges, under reordering, loss and duplication.
    /// </summary>
    [Test]
    public void Identity_and_merge_dedup_never_drops_a_new_write_and_converges()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new ReplicationDedupConvergenceModel(ReplicationDedupMode.IdentityAndMerge));
    }

    /// <summary>
    /// The guard, reproducing the receiver half of #1060: deduplicating on the incremental
    /// per-origin high-water mark drops a new write whose leaf clock is behind another leaf's.
    /// </summary>
    [Test]
    public void Incremental_diagonal_dedup_drops_a_new_write()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new ReplicationDedupConvergenceModel(ReplicationDedupMode.IncrementalDiagonal));
    }

    /// <summary>
    /// The anti-vacuity witness: the exploration does deliver an entry below its origin's
    /// high-water mark, so the passing arm above is not passing for want of that ordering.
    /// </summary>
    [Test]
    public void Exploration_reaches_a_delivery_below_the_origin_high_water_mark()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new ReplicationDedupConvergenceModel(ReplicationDedupMode.MonotonicDeliveryProbe));
    }

    /// <summary>
    /// The no-regression arm: with no transport faults the fixed design still converges, so the
    /// fix does not depend on duplication or loss to look correct.
    /// </summary>
    [Test]
    public void Identity_and_merge_dedup_converges_over_a_reliable_transport()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new ReplicationDedupConvergenceModel(ReplicationDedupMode.IdentityAndMerge, drops: 0, duplicates: 0));
    }

    /// <summary>
    /// The fix: with both cycle-breaks, no cluster relays a peer's write to a third cluster and
    /// no cluster applies its own write received back from a peer.
    /// </summary>
    [Test]
    public void Both_cycle_breaks_prevent_relay_and_reflection()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new ReplicationCycleBreakModel(ReplicationCycleBreakMode.BothGuards));
    }

    /// <summary>The guard: without the shipper's local-origin filter, b relays a's write to c.</summary>
    [Test]
    public void Without_the_ship_filter_a_cluster_relays_a_peers_write()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new ReplicationCycleBreakModel(ReplicationCycleBreakMode.NoShipFilter));
    }

    /// <summary>
    /// The guard: without the receiver-side cycle-break, an echoed own write is applied. The
    /// shipper's filter alone does not protect a cluster from a peer on another build.
    /// </summary>
    [Test]
    public void Without_the_receiver_guard_an_echoed_own_write_is_applied()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new ReplicationCycleBreakModel(ReplicationCycleBreakMode.NoReceiverGuard));
    }

    /// <summary>
    /// The anti-vacuity witness: some shipper does drain a foreign-origin entry, so the filter
    /// arm above is exercised on the multi-hop state rather than passing on an empty one.
    /// </summary>
    [Test]
    public void Exploration_reaches_a_shipper_draining_a_foreign_entry()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new ReplicationCycleBreakModel(ReplicationCycleBreakMode.ForeignEntryProbe));
    }

    /// <summary>
    /// The fix: with the partition cursor authoritative, every entry ships, including new
    /// writes whose leaf clock is below the scalar cursor, across lost acknowledgements.
    /// </summary>
    [Test]
    public void Partition_cursor_ships_every_entry()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new ReplicationShipCursorModel(ReplicationShipCursorMode.PartitionCursorAuthoritative));
    }

    /// <summary>
    /// The guard, reproducing the shipper half of #1060: skipping on the scalar HLC cursor on
    /// every tick consumes a new below-cursor write without shipping it.
    /// </summary>
    [Test]
    public void Scalar_cursor_filter_skips_an_unshipped_write()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new ReplicationShipCursorModel(ReplicationShipCursorMode.ScalarCursorAlwaysFilters));
    }

    /// <summary>
    /// The anti-vacuity witness: the merge does consume an entry at or below the scalar
    /// cursor, so the passing arm is exercised on the ordering #1060 turned on.
    /// </summary>
    [Test]
    public void Exploration_reaches_a_write_below_the_scalar_cursor()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new ReplicationShipCursorModel(ReplicationShipCursorMode.BelowCursorProbe));
    }
}
