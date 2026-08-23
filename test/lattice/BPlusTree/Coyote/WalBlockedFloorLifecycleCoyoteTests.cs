using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for the WAL cursor registry's
/// blocked-floor lifecycle racing GC floor reads. They are tagged
/// <c>[Category("Coyote")]</c> so the fast dev loop and the per-package
/// deterministic CI step skip them; a dedicated CI step runs this category. See
/// the "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>.
/// <para>
/// The safety test drives the real production meet
/// (<see cref="Orleans.Lattice.WalBlockedFloorCore.Meet"/>) that
/// <c>InMemoryWalCursorRegistry</c> folds over every consumer's buffer pin, and
/// the guard test joins at the maximum live pin instead so Coyote re-finds a
/// buffering consumer whose live pin the GC trimmed past - a vacuous model that
/// still passed with the guard removed would be rejected.
/// </para>
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class WalBlockedFloorLifecycleCoyoteTests
{
    /// <summary>
    /// The fix: meeting the blocked floor at the minimum live buffer pin keeps the
    /// floor at or below every buffering consumer's pin through every interleaving
    /// of pin-take, pin-raise, and pin-clear, so the GC never trims an entry a live
    /// buffer still needs.
    /// </summary>
    [Test]
    public void Min_pin_meet_never_trims_past_a_live_buffer_pin([Values(2, 3)] int consumerCount)
    {
        CoyoteModelHarness.AssertNoInterleavingViolation(
            new WalBlockedFloorLifecycleModel(consumerCount, WalBlockedFloorMode.MinPinMeet));
    }

    /// <summary>
    /// The guard: joining the floor at the maximum live pin (as if the
    /// lowest-pinned consumer were dropped from the meet) lets the floor rise above
    /// a slower consumer's live pin, and Coyote must find the schedule where the GC
    /// trims an entry that consumer is still buffering. This proves the min meet in
    /// the safety test above is load-bearing rather than decorative. (Needs at
    /// least two buffering consumers so their pins can diverge.)
    /// </summary>
    [Test]
    public void Max_pin_join_trims_past_a_lagging_buffer_pin([Values(2, 3)] int consumerCount)
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new WalBlockedFloorLifecycleModel(consumerCount, WalBlockedFloorMode.MaxPinJoinNoLaggard));
    }
}
