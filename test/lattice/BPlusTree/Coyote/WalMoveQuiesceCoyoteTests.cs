using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for the WAL placement-move quiesce
/// fence racing concurrent writers. They are tagged <c>[Category("Coyote")]</c>
/// so the fast dev loop and the per-package deterministic CI step skip them; a
/// dedicated CI step runs this category. See the "Coyote concurrency tier"
/// section of <c>.github/instructions/testing.instructions.md</c>.
/// <para>
/// The safety test drives the real production admission rule
/// (<see cref="Orleans.Lattice.BPlusTree.WalMoveFenceCore.IsAppendAdmitted"/>)
/// that <c>WalShardGrain</c> applies under its state gate, and the guard test
/// splits the atomic check-and-assign so Coyote re-finds an offset committed
/// after the fence - a vacuous model that still passed with the guard removed
/// would be rejected.
/// </para>
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class WalMoveQuiesceCoyoteTests
{
    /// <summary>
    /// The fix: checking the move fence and assigning the offset atomically
    /// admits no interleaving where a writer commits an offset after the quiesce
    /// raised the fence - for one writer and wider - so the coordinator's captured
    /// tail is always complete.
    /// </summary>
    [Test]
    public void Atomic_fence_check_never_assigns_after_the_fence([Values(1, 2, 3)] int writerCount)
    {
        CoyoteModelHarness.AssertNoInterleavingViolation(
            new WalMoveQuiesceModel(writerCount, WalMoveQuiesceMode.AtomicFenceCheck));
    }

    /// <summary>
    /// The guard: splitting the check from the assignment lets a writer that
    /// observed the fence down commit its offset after the quiesce fenced and
    /// captured the tail, and Coyote must find the stranded offset. This proves
    /// the atomic check-and-assign in the safety test above is load-bearing
    /// rather than decorative.
    /// </summary>
    [Test]
    public void Split_fence_check_strands_an_offset_past_the_fence([Values(1, 2)] int writerCount)
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new WalMoveQuiesceModel(writerCount, WalMoveQuiesceMode.NonAtomicFenceCheck));
    }
}
