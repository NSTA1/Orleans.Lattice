using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for the WAL placement move's
/// resumable tail copy under a coordinator that crashes and re-drives mid-copy.
/// They are tagged <c>[Category("Coyote")]</c> so the fast dev loop and the
/// per-package deterministic CI step skip them; a dedicated CI step runs this
/// category. See the "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>.
/// <para>
/// The safety test drives the real production resume arithmetic
/// (<see cref="Orleans.Lattice.WalMoveResumeCore.ResumeCursor"/>) that
/// <c>LatticeAdminGrain.RunMoveCopyPhasesAsync</c> applies before flipping the
/// placement pin, and the guard test always resumes from the source floor so
/// Coyote re-finds an offset copied to the target twice - a vacuous model that
/// still passed with the guard removed would be rejected.
/// </para>
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class WalMoveRedriveCoyoteTests
{
    /// <summary>
    /// The fix: resuming each re-drive just past the target's current highest
    /// offset copies every source offset to the target exactly once and leaves no
    /// gap, however many times and at whatever offset boundary the copy is
    /// interrupted and retried.
    /// </summary>
    [Test]
    public void Resume_past_target_copies_each_offset_exactly_once([Values(1, 2, 3)] int tailLength)
    {
        CoyoteModelHarness.AssertNoInterleavingViolation(
            new WalMoveRedriveModel(tailLength, WalMoveRedriveMode.ResumePastTarget));
    }

    /// <summary>
    /// The guard: resuming every re-drive from the source floor regardless of the
    /// prefix a prior attempt already landed re-appends offsets the target already
    /// holds, and Coyote must find the duplicate. This proves the resume-past-target
    /// cursor in the safety test above is load-bearing rather than decorative.
    /// (Needs a tail of at least two offsets so a partial copy can be re-driven.)
    /// </summary>
    [Test]
    public void Resume_from_floor_always_duplicates_an_offset([Values(2, 3)] int tailLength)
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new WalMoveRedriveModel(tailLength, WalMoveRedriveMode.ResumeFromFloorAlways));
    }
}
