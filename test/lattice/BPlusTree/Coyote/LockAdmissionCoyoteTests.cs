using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for the distributed lock's fencing
/// admission core (<see cref="Orleans.Lattice.BPlusTree.LockAdmissionCore"/>,
/// issue #1608). They are tagged <c>[Category("Coyote")]</c> so the fast dev loop
/// and the per-package deterministic CI step skip them; a dedicated CI step runs
/// this category. See the "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>.
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class LockAdmissionCoyoteTests
{
    /// <summary>
    /// The fix: the proven core rejects a stale-token release / renew on every
    /// explored order of the reclaim-and-regrant race, so once B holds the lock no
    /// stale operation from the presumed-dead holder A can dislodge it and the
    /// fencing token strictly increases across the grant.
    /// </summary>
    [Test]
    public void Stale_token_never_dislodges_current_holder_on_any_order()
    {
        CoyoteModelHarness.AssertNoInterleavingViolation(
            new LockAdmissionModel(useBrokenTokenCheck: false));
    }

    /// <summary>
    /// The guard: a release that frees the lock without checking the presented
    /// token reintroduces the fencing regression, and Coyote must find a delivery
    /// order (reclaim-and-grant B, then A's stale release) that trips it. This
    /// proves the model genuinely exercises the race, so the passing test above is
    /// meaningful rather than vacuous.
    /// </summary>
    [Test]
    public void Release_ignoring_the_fencing_token_is_caught()
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new LockAdmissionModel(useBrokenTokenCheck: true));
    }
}
