using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for the <b>forward window</b> of the
/// online-reshard split path (#3117). They are tagged <c>[Category("Coyote")]</c> so
/// the fast dev loop and the per-package deterministic CI step skip them; a
/// dedicated CI step runs this category. See the "Coyote concurrency tier" section
/// of <c>.github/instructions/testing.instructions.md</c>.
/// <para>
/// <see cref="ReshardMigrationCoyoteTests"/> covers the phase <b>after</b> migration
/// has converged, where a late orphan prepare can shadow a newer value (#1584).
/// This fixture covers the phase <b>during</b> migration, where the destination
/// already holds a drain-migrated pre-saga value but does not yet carry the shadow
/// marker, so the read gate is never consulted and a reader falls through onto the
/// stale value. The two are different phases of the same protocol, and the earlier
/// model's state space does not contain this one.
/// </para>
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class ReshardForwardWindowCoyoteTests
{
    /// <summary>
    /// The fix direction: installing the destination shadow marker in the same step
    /// that forwards the drain-migrated value leaves no schedule in which a key is
    /// observable as migrated-but-unmarked, so every reader fan-out observes a single
    /// saga round. This is the oracle a candidate fix for #3117 must satisfy.
    /// </summary>
    [Test]
    public void Marker_installed_with_the_forward_never_splits_a_reader([Values(2, 3, 4)] int keyCount)
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new ReshardForwardWindowModel(keyCount, ForwardMarkerMode.InstalledWithForward));
    }

    /// <summary>
    /// The guard: forwarding the value and installing the shadow marker as two
    /// separately scheduled steps - the shipping split path's structure - lets a
    /// reader resolve one key through the gate and another through the ungated
    /// fall-through, observing two saga rounds in one fan-out. Coyote must find the
    /// split. This proves the model is non-vacuous and that the single-step marker
    /// install above is load-bearing rather than incidentally true.
    /// </summary>
    [Test]
    public void Marker_installed_in_a_separate_call_splits_a_reader([Values(2, 3)] int keyCount)
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new ReshardForwardWindowModel(keyCount, ForwardMarkerMode.InstalledSeparately));
    }
}
