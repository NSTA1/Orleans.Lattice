using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for the <c>repocontext_health</c> probe: it reports reachability AND
/// whether retrieval can actually serve, read from the same
/// <see cref="RepoContextRetrievalReadinessState"/> the HTTP readiness endpoint
/// reads.
/// <para>
/// The defect this fixture pins is that the handler used to return a
/// <c>static readonly</c> singleton. It made no observation at all, so
/// <c>available: true</c> was a compile-time literal and the probe was
/// <b>structurally incapable</b> of reporting a degraded host - no input could
/// have changed its answer. An agent is instructed to call this probe first, so a
/// green that cannot go red is worse than no probe: it converts "I did not look"
/// into "I looked and it was fine".
/// </para>
/// <para>
/// Every readiness arm below is driven from a population derived by reflection
/// over the phase enum rather than from a hand-written list, so a phase added
/// later is enrolled the moment it exists. See
/// <see cref="Health_covers_every_declared_readiness_phase"/> for the arm that
/// keeps the driver table honest.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextToolHandlersTests
{
    /// <summary>
    /// Drives a fresh readiness state into each declared phase. This table is a
    /// hand-written mapping and is therefore NOT trusted on its own: the companion
    /// test asserts its key set equals the enum's member set exactly, so the table
    /// is a report checked against the scan rather than a substitute for it.
    /// </summary>
    private static readonly Dictionary<RepoContextRetrievalReadinessPhase, Action<RepoContextRetrievalReadinessState>> PhaseDrivers = new()
    {
        [RepoContextRetrievalReadinessPhase.Serving] = s => s.MarkServing(),
        [RepoContextRetrievalReadinessPhase.KeywordOnly] = s => s.MarkKeywordOnly(),
        [RepoContextRetrievalReadinessPhase.NothingRegistered] = s => s.MarkNothingRegistered(),

        // A fresh state is NothingRegistered, and a real query proving the plane
        // unavailable falsifies that phase's premise, so this drops straight to
        // Building with no hold-down to wait out.
        [RepoContextRetrievalReadinessPhase.Building] = s => s.MarkUnavailable(),

        // The saturation declaration applies from any not-yet-serving phase, so a
        // fresh state reaches it directly (issue #3286).
        [RepoContextRetrievalReadinessPhase.SaturatedUnavailable] = s => s.MarkSaturationUnavailable(),
    };

    private static RepoContextRetrievalReadinessState NewState(SettableTimeProvider clock)
        => new(clock, TimeSpan.FromSeconds(30));

    private static async Task<RepoContextHealthResult> HealthWithAsync(RepoContextRetrievalReadinessState readiness)
    {
        var services = new ServiceCollection()
            .AddSingleton(readiness)
            .BuildServiceProvider();
        await using (services.ConfigureAwait(false))
        {
            var context = await RepoContextRequestContexts.CreateAsync(services).ConfigureAwait(false);
            return RepoContextToolHandlers.Health(context);
        }
    }

    // ---- the defect ---------------------------------------------------------

    /// <summary>
    /// The reported live failure: the vector plane is up, a search resolves to
    /// <see cref="RepoContextRetrievalPath.KeywordExactFallbackSuppressed"/>, the
    /// fault persists past the hold-down - and the probe must now say so.
    /// </summary>
    [Test]
    public async Task Health_reports_not_ready_when_retrieval_has_suppressed_the_exact_fallback()
    {
        var clock = new SettableTimeProvider();
        using var readiness = NewState(clock);
        readiness.MarkServing();

        readiness.Observe(RepoContextRetrievalPath.KeywordExactFallbackSuppressed);
        clock.Advance(TimeSpan.FromSeconds(31));

        var health = await HealthWithAsync(readiness);

        Assert.Multiple(() =>
        {
            Assert.That(health.RetrievalReady, Is.False,
                "A plane answering with the exact fallback suppressed is not serving its headline capability, "
                + "so the probe an agent calls first must not report it ready.");
            Assert.That(health.RetrievalPhase,
                Is.EqualTo(RepoContextRetrievalReadinessState.PhaseBuildingTag));
            Assert.That(health.Status, Does.Contain("NOT serving"),
                "The prose is what a reader acts on, so the degradation has to be legible in it too.");
        });
    }

    /// <summary>
    /// Positive control for the test above. Same harness, same state type, same
    /// call - only the observation differs. Without this, a false
    /// <c>RetrievalReady</c> could equally mean the harness never wires the state
    /// up at all, and the red would prove nothing.
    /// </summary>
    [Test]
    public async Task Health_reports_ready_while_retrieval_is_serving()
    {
        var clock = new SettableTimeProvider();
        using var readiness = NewState(clock);
        readiness.MarkServing();

        var health = await HealthWithAsync(readiness);

        Assert.Multiple(() =>
        {
            Assert.That(health.RetrievalReady, Is.True);
            Assert.That(health.RetrievalPhase,
                Is.EqualTo(RepoContextRetrievalReadinessState.PhaseServingTag));
        });
    }

    /// <summary>
    /// The discrimination that keeps the fix from over-reporting. A host with no
    /// embedding provider bound answers by keyword as its INTENDED steady state; it
    /// is fully ready and must not be reported degraded just because it is not
    /// semantic. Both this phase and <c>building</c> answer keyword recall, so a fix
    /// that keyed on "answering by keyword" would redden every keyword-only
    /// deployment on the fleet.
    /// </summary>
    [Test]
    public async Task Health_reports_ready_on_a_keyword_only_host()
    {
        var clock = new SettableTimeProvider();
        using var readiness = NewState(clock);
        readiness.MarkKeywordOnly();

        var health = await HealthWithAsync(readiness);

        Assert.Multiple(() =>
        {
            Assert.That(health.RetrievalReady, Is.True,
                "Keyword-only is a deployment choice, not a degradation.");
            Assert.That(health.RetrievalPhase,
                Is.EqualTo(RepoContextRetrievalReadinessState.PhaseKeywordOnlyTag));
        });
    }

    /// <summary>
    /// The shape of the defect, independent of any particular phase: the handler
    /// must be a function of observed state. Two calls that differ only in what the
    /// host observed must differ in what they report. The superseded
    /// implementation returned one shared instance and failed this by construction.
    /// </summary>
    [Test]
    public async Task Health_is_a_function_of_observed_state_not_a_constant()
    {
        var clock = new SettableTimeProvider();
        using var serving = NewState(clock);
        serving.MarkServing();
        using var degraded = NewState(clock);
        degraded.MarkUnavailable();

        var first = await HealthWithAsync(serving);
        var second = await HealthWithAsync(degraded);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Not.SameAs(second),
                "A cached singleton cannot report a degraded host; that was the defect.");
            Assert.That(first.RetrievalPhase, Is.Not.EqualTo(second.RetrievalPhase));
            Assert.That(first.Status, Is.Not.EqualTo(second.Status));
        });
    }

    // ---- structural coverage ------------------------------------------------

    /// <summary>
    /// Anti-vacuity plus enrolment: the driver table's keys must equal the phase
    /// enum's members exactly. A phase added to the enum and forgotten here fails
    /// this arm rather than silently leaving the new phase untested, and an empty
    /// or shrunken enum fails the count floor rather than passing on nothing.
    /// </summary>
    [Test]
    public void Health_covers_every_declared_readiness_phase()
    {
        var declared = Enum.GetValues<RepoContextRetrievalReadinessPhase>();

        Assert.Multiple(() =>
        {
            Assert.That(declared, Has.Length.GreaterThanOrEqualTo(4),
                "Anti-vacuity: this gate must never pass because the population it quantifies over emptied out.");
            Assert.That(PhaseDrivers.Keys, Is.EquivalentTo(declared),
                "The driver table is a report of the enum, not a substitute for reading it. "
                + "Add the new phase's driver rather than deleting this assertion.");
        });
    }

    /// <summary>
    /// For every declared phase: the probe reports that phase's tag, reports ready
    /// for exactly the non-<c>building</c> phases, and carries a status line that
    /// is distinct from every other phase's. The last clause is what stops a future
    /// phase being added with a copy-pasted line that reads healthy while the host
    /// is not.
    /// </summary>
    [Test]
    public async Task Health_reports_a_distinct_status_line_and_the_right_verdict_for_every_phase()
    {
        var lines = new Dictionary<string, RepoContextRetrievalReadinessPhase>(StringComparer.Ordinal);

        foreach (var phase in Enum.GetValues<RepoContextRetrievalReadinessPhase>())
        {
            var clock = new SettableTimeProvider();
            using var readiness = NewState(clock);
            PhaseDrivers[phase](readiness);

            Assert.That(readiness.Phase, Is.EqualTo(phase),
                $"The driver for {phase} did not actually reach it, so this row would test the wrong phase.");

            var health = await HealthWithAsync(readiness);

            Assert.Multiple(() =>
            {
                Assert.That(health.Available, Is.True,
                    "Reachability is a separate fact from readiness and stays true on a degraded host: "
                    + "capture, recall and scan still work.");
                Assert.That(health.RetrievalPhase,
                    Is.EqualTo(RepoContextRetrievalReadinessState.PhaseTag(phase)));
                Assert.That(health.RetrievalReady,
                    Is.EqualTo(RepoContextRetrievalReadinessState.IsReadyPhase(phase)),
                    $"Readiness for {phase} disagrees with the state's own IsReady contract. "
                    + "This reads the POSITIVE ready-set predicate rather than restating the old "
                    + "'anything that is not Building' form: that negative form silently defaulted "
                    + "each new phase to ready, which is how a plane declared unavailable-saturated "
                    + "would have been reported as serving (issue #3286).");
                Assert.That(health.Status, Is.Not.Null.And.Not.Empty);
            });

            Assert.That(lines.TryAdd(health.Status, phase), Is.True,
                $"The status line for {phase} duplicates another phase's. "
                + "Each phase must name its own consequence.");
        }
    }

    // ---- unchanged contract -------------------------------------------------

    [Test]
    public async Task Health_reports_the_surface_available_under_the_stable_group_name()
    {
        var clock = new SettableTimeProvider();
        using var readiness = NewState(clock);
        readiness.MarkServing();

        var health = await HealthWithAsync(readiness);

        Assert.Multiple(() =>
        {
            Assert.That(health.Available, Is.True);
            Assert.That(health.Group, Is.EqualTo("repocontext"));
            Assert.That(health.Status, Is.Not.Null.And.Not.Empty);
        });
    }
}
