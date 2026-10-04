using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Backup.Tests.Coyote;

/// <summary>
/// Coyote tests for the cross-tree backup set's capture window
/// (<see cref="CrossTreeFenceWindow"/>, issue #4440): the fixed-design arm, a
/// no-regression arm, an anti-vacuity witness, and one companion guard per rule
/// the window depends on. Opt-in: <c>[Category("Coyote")]</c>.
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class CrossTreeFenceCaptureCoyoteTests
{
    /// <summary>
    /// The production window never accepts a set that holds a cross-tree saga on
    /// one member and not the other, on any explored schedule.
    /// </summary>
    [Test]
    public void Production_window_never_accepts_a_torn_set() =>
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new CrossTreeFenceCaptureModel(CrossTreeFenceGuard.Production));

    /// <summary>No-regression arm: with no saga in flight the window accepts every time.</summary>
    [Test]
    public void Production_window_accepts_a_quiet_set() =>
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new CrossTreeFenceCaptureModel(CrossTreeFenceGuard.Production, sagaRuns: false));

    /// <summary>
    /// Anti-vacuity witness: some explored schedule accepts a set holding the
    /// committed saga, so the fixed-design arm is not passing by never capturing
    /// the saga at all.
    /// </summary>
    [Test]
    public void Exploration_reaches_an_accepted_set_holding_the_committed_saga() =>
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new CrossTreeFenceCaptureModel(CrossTreeFenceGuard.WitnessCommittedCaptured));

    /// <summary>
    /// Guard: a re-observation that ignores the registration epoch accepts a
    /// saga that registered and finalized on one member inside the window.
    /// </summary>
    [Test]
    public void Reobservation_ignoring_the_epoch_accepts_a_torn_set() =>
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new CrossTreeFenceCaptureModel(CrossTreeFenceGuard.IgnoreEpoch));

    /// <summary>
    /// Guard: a drain gate that does not wait lets a saga registered before the
    /// window finalize on one member before its capture and the other after.
    /// </summary>
    [Test]
    public void Skipping_the_drain_gate_accepts_a_torn_set() =>
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new CrossTreeFenceCaptureModel(CrossTreeFenceGuard.SkipDrain));
}
