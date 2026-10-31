using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for the WAL GC's orphaned
/// materialiser pin retirement (issue #5358), the executable companion of
/// <c>spec/wal/WalPinRetirement.tla</c>. Tagged <c>[Category("Coyote")]</c>;
/// see the "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>.
/// <para>
/// The safety test drives the production decision core and removal fence
/// (<see cref="Orleans.Lattice.WalOrphanPinRetirementCore"/>); each guard test
/// removes one half of the fix and Coyote must re-find the retired pin of a
/// re-created leaf.
/// </para>
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class WalPinRetirementCoyoteTests
{
    /// <summary>
    /// The fix: a token taken before the leaf read and a fenced removal admit
    /// no interleaving with a re-created leaf in which the leaf's un-checkpointed
    /// write is left without a pin.
    /// </summary>
    [Test]
    public void Fenced_retirement_never_retires_the_pin_of_a_recreated_leaf()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(new WalPinRetirementModel(WalPinRetirementMode.Fenced));
    }

    /// <summary>
    /// The guard: an unconditional removal by consumer id - production before
    /// issue #5358 - erases the re-created leaf's pin.
    /// </summary>
    [Test]
    public void Unfenced_retirement_retires_the_pin_of_a_recreated_leaf()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(new WalPinRetirementModel(WalPinRetirementMode.Unfenced));
    }

    /// <summary>
    /// The second guard: the fence only works if the token precedes the read
    /// the removal is decided on.
    /// </summary>
    [Test]
    public void A_token_taken_after_the_read_does_not_fence_the_recreation()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(new WalPinRetirementModel(WalPinRetirementMode.TokenAfterRead));
    }
}
