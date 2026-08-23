using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for the WAL per-shard offset
/// allocation racing concurrent appends. They are tagged <c>[Category("Coyote")]</c>
/// so the fast dev loop and the per-package deterministic CI step skip them; a
/// dedicated CI step runs this category. See the "Coyote concurrency tier"
/// section of <c>.github/instructions/testing.instructions.md</c>.
/// <para>
/// The safety test drives the real production allocation step
/// (<see cref="Orleans.Lattice.BPlusTree.WalOffsetAllocationCore.Assign"/>) that
/// <c>WalShardGrain</c> performs under its state gate, and the guard test splits
/// the atomic read-and-advance so Coyote re-finds two appends handed the same
/// offset - a vacuous model that still passed with the guard removed would be
/// rejected.
/// </para>
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class WalOffsetContiguityCoyoteTests
{
    /// <summary>
    /// The fix: reading and advancing the offset counter atomically admits no
    /// interleaving where two appends share an offset - for one appender and
    /// wider - so the assigned sequence is always dense and duplicate-free.
    /// </summary>
    [Test]
    public void Atomic_assign_keeps_every_offset_unique_and_dense([Values(1, 2, 3)] int writerCount)
    {
        CoyoteModelHarness.AssertNoInterleavingViolation(
            new WalOffsetContiguityModel(writerCount, WalOffsetContiguityMode.AtomicAssign));
    }

    /// <summary>
    /// The guard: splitting the read from the advance lets two appenders observe
    /// the same counter value and stamp the same offset, and Coyote must find the
    /// duplicate. This proves the atomic read-and-advance in the safety test above
    /// is load-bearing rather than decorative. (Needs at least two appenders - a
    /// lone appender cannot collide with itself.)
    /// </summary>
    [Test]
    public void Split_read_advance_hands_two_appends_the_same_offset([Values(2, 3)] int writerCount)
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new WalOffsetContiguityModel(writerCount, WalOffsetContiguityMode.SplitReadAdvance));
    }
}
