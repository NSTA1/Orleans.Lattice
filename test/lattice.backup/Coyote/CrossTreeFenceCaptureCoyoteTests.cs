using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Backup.Tests.Coyote;

/// <summary>
/// Coyote tests for the cross-tree backup set's capture window
/// (<see cref="CrossTreeFenceWindow"/>, issues #4440 and #4441): the
/// fixed-design arm, a no-regression arm, an anti-vacuity witness, one arm per
/// rule in which that rule is the only defence left standing, and two guards
/// that must find a torn set once every defence of a window is removed.
/// <para>
/// Under the shipped design the window's rules are mutually redundant (see
/// <see cref="CrossTreeFenceCaptureModel"/>), so no rule has a guard of its own:
/// removing any one of them alone is correctly clean. Each rule is instead
/// pinned by its single-defence arm, which routes that rule - and only that
/// rule - through the production <see cref="CrossTreeFenceWindow"/>, so a
/// perturbation of the rule in the core turns its arm red. Opt-in:
/// <c>[Category("Coyote")]</c>.
/// </para>
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class CrossTreeFenceCaptureCoyoteTests
{
    private const CrossTreeFenceRulesRemoved ReobserveBoth =
        CrossTreeFenceRulesRemoved.ReobserveEpoch | CrossTreeFenceRulesRemoved.ReobserveInFlight;

    /// <summary>
    /// The production window never accepts a set that holds a cross-tree saga on
    /// one member and not the other, on any explored schedule, a lost fence
    /// included.
    /// </summary>
    [Test]
    public void Production_window_never_accepts_a_torn_set() =>
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new CrossTreeFenceCaptureModel(CrossTreeFenceRulesRemoved.None));

    /// <summary>No-regression arm: with no saga in flight the window accepts every time.</summary>
    [Test]
    public void Production_window_accepts_a_quiet_set() =>
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new CrossTreeFenceCaptureModel(CrossTreeFenceRulesRemoved.None, sagaRuns: false));

    /// <summary>
    /// Anti-vacuity witness: some explored schedule accepts a set holding the
    /// committed saga, so the fixed-design arm is not passing by never capturing
    /// the saga at all.
    /// </summary>
    [Test]
    public void Exploration_reaches_an_accepted_set_holding_the_committed_saga() =>
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new CrossTreeFenceCaptureModel(CrossTreeFenceRulesRemoved.None, witnessCommittedCaptured: true));

    /// <summary>
    /// The gated re-check alone refuses a delegation a lost fence admitted: it is
    /// still live at the gate, which refuses its finalize.
    /// </summary>
    [Test]
    public void The_gated_recheck_alone_refuses_a_registration_a_lost_fence_admitted() =>
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new CrossTreeFenceCaptureModel(ReobserveBoth));

    /// <summary>
    /// The re-observation's epoch clause alone refuses a registration a lost
    /// fence admitted, however far that saga got before the gate.
    /// </summary>
    [Test]
    public void The_reobserved_epoch_alone_refuses_a_registration_a_lost_fence_admitted() =>
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new CrossTreeFenceCaptureModel(CrossTreeFenceRulesRemoved.Recheck | CrossTreeFenceRulesRemoved.ReobserveInFlight));

    /// <summary>
    /// The re-observation's in-flight clause alone refuses a registration a lost
    /// fence admitted and the gate then held unfinalized.
    /// </summary>
    [Test]
    public void The_reobserved_in_flight_count_alone_refuses_a_registration_a_lost_fence_admitted() =>
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new CrossTreeFenceCaptureModel(CrossTreeFenceRulesRemoved.Recheck | CrossTreeFenceRulesRemoved.ReobserveEpoch));

    /// <summary>
    /// While the fence holds, the drain gate alone keeps a saga registered before
    /// the capture from straddling it.
    /// </summary>
    [Test]
    public void The_drain_gate_alone_keeps_a_saga_registered_before_the_capture_whole() =>
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new CrossTreeFenceCaptureModel(CrossTreeFenceRulesRemoved.Recheck | ReobserveBoth, lapse: false));

    /// <summary>
    /// Iteration budget of <see cref="Without_the_recheck_and_reobservation_a_lost_fence_admits_a_torn_set"/>.
    /// </summary>
    /// <remarks>
    /// Sized from the measured per-run detection rate p: runs to the first
    /// violation over 40 seeded explorations found it 40 times in 12,984 runs,
    /// p ~ 0.0031. The torn set needs the lapse at the gate and then four saga
    /// steps (both registrations, the commit decision and one finalize) to win
    /// the race against the gate's acquire, so it is rare. At the default 1000
    /// runs it is missed with probability (1 - p)^1000 ~ 4.6%, which is the CI
    /// flake this budget removes: at 10000 runs the miss probability is
    /// ~ e^-31, and ~ 6e-10 even at the lower 95% bound of p (~ 0.0021).
    /// Exploration stops at the first violation, so the expected cost is
    /// ~ 1/p ~ 325 runs. The other two violation-seeking cases are safe at the
    /// default 1000: the drain guard's p ~ 0.031 (miss ~ 2e-14) and the
    /// witness's p ~ 0.16 (miss ~ 2e-76).
    /// </remarks>
    private const int LapseGuardIterations = 10_000;

    /// <summary>
    /// Guard: with the re-check and both re-observation clauses removed, a
    /// registration a lost fence admitted is captured torn.
    /// </summary>
    [Test]
    public void Without_the_recheck_and_reobservation_a_lost_fence_admits_a_torn_set() =>
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new CrossTreeFenceCaptureModel(CrossTreeFenceRulesRemoved.Recheck | ReobserveBoth),
            LapseGuardIterations);

    /// <summary>
    /// Guard: with every rule removed, even a held fence lets a saga registered
    /// before the capture finalize on one member before its capture and on the
    /// other after it.
    /// </summary>
    [Test]
    public void Without_the_drain_a_saga_registered_before_the_capture_is_captured_torn() =>
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new CrossTreeFenceCaptureModel(
                CrossTreeFenceRulesRemoved.Drain | CrossTreeFenceRulesRemoved.Recheck | ReobserveBoth, lapse: false));
}