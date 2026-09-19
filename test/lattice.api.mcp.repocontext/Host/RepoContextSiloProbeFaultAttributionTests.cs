using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers the fault attribution the grain-liveness check now carries, and the seam
/// that takes it from the check to the <c>/metrics</c> scrape.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2868. A wedged <c>sys-auth-policy</c> tree left MCP returning 500 for 43
/// minutes while <c>/metrics</c> answered 200. The check detected it correctly
/// throughout - it reads that very tree - but reported only <i>that</i> the probe
/// failed, to a surface nothing was wired to. These fixtures defend the two halves of
/// the fix: the check now says <i>why</i> it failed, and the publisher carries the
/// answer onto the endpoint that is already scraped.
/// </para>
/// <para>
/// The causes are separated because their remedies differ. A hung tree has only ever
/// been cleared by a restart; a refused grant is a configuration defect on a perfectly
/// healthy tree and a restart does nothing for it. Collapsing the two is how a healthy
/// container gets restarted and a wedged one gets left alone.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextSiloProbeFaultAttributionTests
{
    private static readonly TimeSpan ShortTimeout = TimeSpan.FromMilliseconds(200);

    private static HealthCheckContext NewContext() => new()
    {
        Registration = new HealthCheckRegistration(
            RepoContextSiloHealthCheck.Name,
            new RepoContextLivenessHealthCheck(),
            HealthStatus.Unhealthy,
            tags: null),
    };

    private sealed class FakeProbe(Func<CancellationToken, Task> behaviour) : IRepoContextSiloProbe
    {
        public static FakeProbe Succeeds() => new(_ => Task.CompletedTask);

        public static FakeProbe Throws(Exception ex) => new(_ => Task.FromException(ex));

        public static FakeProbe Hangs() => new(async ct =>
            await Task.Delay(Timeout.Infinite, ct).ConfigureAwait(false));

        public Task ProbeAsync(CancellationToken cancellationToken) => behaviour(cancellationToken);
    }

    private sealed class MutableClock(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _now = start;

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now += by;
    }

    private static RepoContextSiloProbeFaultCause CauseOf(HealthCheckResult result)
    {
        Assert.That(
            result.Data,
            Does.ContainKey(RepoContextSiloHealthCheck.CauseDataKey),
            "every outcome must be classified, successes included. A result that carries no cause key "
            + "at all is the silence this change exists to remove.");
        return (RepoContextSiloProbeFaultCause)result.Data[RepoContextSiloHealthCheck.CauseDataKey];
    }

    /// <summary>
    /// The wedge of issue #2868: the grain call hangs rather than returning or
    /// throwing, and the check's own deadline fires.
    /// </summary>
    [Test]
    public async Task A_hanging_grain_call_is_attributed_to_the_probes_own_deadline()
    {
        var readiness = new RepoContextReadinessState();
        readiness.MarkReady();
        var check = new RepoContextSiloHealthCheck(FakeProbe.Hangs(), readiness, ShortTimeout);

        var result = await check.CheckHealthAsync(NewContext());

        Assert.Multiple(() =>
        {
            Assert.That(
                result.Status,
                Is.EqualTo(HealthStatus.Unhealthy),
                "control: the verdict is red, so the cause below is attributing a failure that really "
                + "occurred.");
            Assert.That(
                CauseOf(result),
                Is.EqualTo(RepoContextSiloProbeFaultCause.ProbeDeadline),
                "a hang is not a timeout thrown by the callee. Only this arm carries the remedy that "
                + "has actually been observed to work on a wedged authorization tree.");
        });
    }

    /// <summary>
    /// Adversarial arm. A working silo must carry no fault at all, or the attribution
    /// is a constant dressed up as a classification.
    /// </summary>
    [Test]
    public async Task A_successful_probe_is_attributed_no_fault()
    {
        var readiness = new RepoContextReadinessState();
        readiness.MarkReady();
        var check = new RepoContextSiloHealthCheck(FakeProbe.Succeeds(), readiness, ShortTimeout);

        var result = await check.CheckHealthAsync(NewContext());

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Healthy), "control: the probe succeeded.");
            Assert.That(
                CauseOf(result),
                Is.EqualTo(RepoContextSiloProbeFaultCause.None),
                "adversarial: an attribution that always reported the same cause would satisfy every "
                + "positive arm in this fixture and be worthless.");
        });
    }

    private static IEnumerable<TestCaseData> ThrownFaults()
    {
        yield return new TestCaseData(
            new TimeoutException("the shard did not answer"),
            RepoContextSiloProbeFaultCause.GrainTimeout)
            .SetName("A_downstream_timeout_is_attributed_to_the_grain_call");
        yield return new TestCaseData(
            new LatticeTenantAccessDeniedException("no active tenant for the probe"),
            RepoContextSiloProbeFaultCause.AccessDenied)
            .SetName("A_tenant_access_denial_is_attributed_to_the_grant");
        yield return new TestCaseData(
            new UnauthorizedAccessException("no grant"),
            RepoContextSiloProbeFaultCause.AccessDenied)
            .SetName("An_unauthorized_access_failure_is_attributed_to_the_grant");
        yield return new TestCaseData(
            new InvalidOperationException("something else entirely"),
            RepoContextSiloProbeFaultCause.Unexpected)
            .SetName("An_unnamed_failure_fails_open_onto_unexpected");
    }

    /// <summary>
    /// Each thrown failure lands on the arm it names. A table, so a classifier that
    /// routed two shapes to one arm fails rather than passing on the one asserted.
    /// </summary>
    /// <param name="thrown">The exception the probe throws.</param>
    /// <param name="expected">The cause the check must record.</param>
    [TestCaseSource(nameof(ThrownFaults))]
    public async Task A_thrown_failure_is_attributed_to_its_own_cause(
        Exception thrown,
        RepoContextSiloProbeFaultCause expected)
    {
        var readiness = new RepoContextReadinessState();
        readiness.MarkReady();
        var check = new RepoContextSiloHealthCheck(FakeProbe.Throws(thrown), readiness, ShortTimeout);

        var result = await check.CheckHealthAsync(NewContext());

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Unhealthy), "control: the verdict is red.");
            Assert.That(CauseOf(result), Is.EqualTo(expected));
        });
    }

    /// <summary>
    /// A drain that outruns its stop-grace window is a hung shutdown, not a wedged
    /// tree. It gets its own arm because restarting a container that is already trying
    /// to stop is a different action entirely.
    /// </summary>
    [Test]
    public async Task A_drain_past_its_grace_window_is_attributed_to_a_hung_drain()
    {
        var clock = new MutableClock(DateTimeOffset.UnixEpoch);
        var readiness = new RepoContextReadinessState(clock);
        readiness.MarkReady();
        readiness.BeginDrain();
        var check = new RepoContextSiloHealthCheck(
            FakeProbe.Succeeds(),
            readiness,
            TimeSpan.FromSeconds(30),
            clock);

        var inside = await check.CheckHealthAsync(NewContext());
        clock.Advance(TimeSpan.FromSeconds(31));
        var outside = await check.CheckHealthAsync(NewContext());

        Assert.Multiple(() =>
        {
            Assert.That(
                inside.Status,
                Is.EqualTo(HealthStatus.Healthy),
                "control: inside the window a stopping container is not a fault.");
            Assert.That(
                CauseOf(inside),
                Is.EqualTo(RepoContextSiloProbeFaultCause.None),
                "adversarial: the same code path with the clock not advanced must report no cause, so "
                + "the arm below is driven by the elapsed drain and not by the drain phase alone.");
            Assert.That(outside.Status, Is.EqualTo(HealthStatus.Unhealthy));
            Assert.That(CauseOf(outside), Is.EqualTo(RepoContextSiloProbeFaultCause.DrainHung));
        });
    }

    /// <summary>
    /// The verdict grades the failure and the cause says what it was, so a silo that
    /// is failing every probe while joining stays distinguishable from one joining
    /// quietly. Grading alone cannot tell those apart.
    /// </summary>
    [Test]
    public async Task A_failing_probe_during_startup_is_graded_degraded_but_still_carries_its_cause()
    {
        var starting = new RepoContextReadinessState();
        var check = new RepoContextSiloHealthCheck(FakeProbe.Hangs(), starting, ShortTimeout);

        var result = await check.CheckHealthAsync(NewContext());

        Assert.Multiple(() =>
        {
            Assert.That(
                result.Status,
                Is.EqualTo(HealthStatus.Degraded),
                "control: a silo that has never joined is starting, not broken. The grading is "
                + "unchanged by this work.");
            Assert.That(
                CauseOf(result),
                Is.EqualTo(RepoContextSiloProbeFaultCause.ProbeDeadline),
                "but the cause rides on the degraded arm too. Attaching it only to Unhealthy would "
                + "make a container wedged from the first second look like one that is merely slow to "
                + "join, which is exactly the 43 minutes issue #2868 records.");
        });
    }

    /// <summary>
    /// The classifier is a pure function and is asserted directly, so a future arm
    /// added above another cannot shadow it unnoticed.
    /// </summary>
    [Test]
    public void ClassifyProbeFault_maps_each_shape_and_fails_open()
        => Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextSiloHealthCheck.ClassifyProbeFault(new TimeoutException()),
                Is.EqualTo(RepoContextSiloProbeFaultCause.GrainTimeout));
            Assert.That(
                RepoContextSiloHealthCheck.ClassifyProbeFault(
                    new LatticeTenantAccessDeniedException()),
                Is.EqualTo(RepoContextSiloProbeFaultCause.AccessDenied));
            Assert.That(
                RepoContextSiloHealthCheck.ClassifyProbeFault(new UnauthorizedAccessException()),
                Is.EqualTo(RepoContextSiloProbeFaultCause.AccessDenied));
            Assert.That(
                RepoContextSiloHealthCheck.ClassifyProbeFault(new Exception()),
                Is.EqualTo(RepoContextSiloProbeFaultCause.Unexpected),
                "failing open, so a failure this taxonomy does not name still moves a series.");
            Assert.That(
                () => RepoContextSiloHealthCheck.ClassifyProbeFault(null!),
                Throws.ArgumentNullException);
        });

    /// <summary>
    /// The whole seam, end to end: a wedged probe becomes a red verdict and an
    /// attributed cause on the surface an operator actually reads.
    /// </summary>
    [Test]
    public async Task A_wedged_probe_reaches_the_signal_through_the_publisher()
    {
        var readiness = new RepoContextReadinessState();
        readiness.MarkReady();
        var check = new RepoContextSiloHealthCheck(FakeProbe.Hangs(), readiness, ShortTimeout);
        var signal = new RepoContextHealthSignal([RepoContextSiloHealthCheck.Name]);
        var publisher = new RepoContextHealthPublisher(signal);

        var before = signal.Read(RepoContextSiloHealthCheck.Name);
        var result = await check.CheckHealthAsync(NewContext());
        await publisher.PublishAsync(
            HealthReports.Report((RepoContextSiloHealthCheck.Name, HealthReports.From(result))),
            CancellationToken.None);
        var after = signal.Read(RepoContextSiloHealthCheck.Name);

        Assert.Multiple(() =>
        {
            Assert.That(
                before.Status,
                Is.Null,
                "control: nothing had been published, so the verdict below arrived through the "
                + "publisher rather than having been there all along.");
            Assert.That(after.Status, Is.EqualTo(HealthStatus.Unhealthy));
            Assert.That(after.Evaluations, Is.EqualTo(1));
            Assert.That(
                signal.ReadSiloFaults().ProbeDeadline,
                Is.EqualTo(1),
                "the cause survives the hop from the check's Data bag into the signal, which is the "
                + "only path by which it can reach the scrape.");
            Assert.That(signal.ReadSiloFaults().ArmSum, Is.EqualTo(signal.ReadSiloFaults().Total));
        });
    }

    /// <summary>
    /// Adversarial counterpart to the seam test: a healthy probe travels the same path
    /// and must arrive as a healthy verdict with no cause.
    /// </summary>
    [Test]
    public async Task A_healthy_probe_reaches_the_signal_with_no_cause()
    {
        var readiness = new RepoContextReadinessState();
        readiness.MarkReady();
        var check = new RepoContextSiloHealthCheck(FakeProbe.Succeeds(), readiness, ShortTimeout);
        var signal = new RepoContextHealthSignal([RepoContextSiloHealthCheck.Name]);
        var publisher = new RepoContextHealthPublisher(signal);

        var result = await check.CheckHealthAsync(NewContext());
        await publisher.PublishAsync(
            HealthReports.Report((RepoContextSiloHealthCheck.Name, HealthReports.From(result))),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                signal.Read(RepoContextSiloHealthCheck.Name).Evaluations,
                Is.EqualTo(1),
                "control: the report was recorded, so the zero below is measured.");
            Assert.That(signal.Read(RepoContextSiloHealthCheck.Name).Status, Is.EqualTo(HealthStatus.Healthy));
            Assert.That(signal.ReadSiloFaults().Total, Is.Zero);
        });
    }

    /// <summary>
    /// A publisher invoked with an already-cancelled token records nothing rather than
    /// booking a half-collected report as an evaluation.
    /// </summary>
    [Test]
    public async Task A_cancelled_publication_records_nothing()
    {
        var signal = new RepoContextHealthSignal([RepoContextSiloHealthCheck.Name]);
        var publisher = new RepoContextHealthPublisher(signal);
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        await publisher.PublishAsync(
            HealthReports.Report((RepoContextSiloHealthCheck.Name, HealthReports.Entry(
                HealthStatus.Unhealthy,
                RepoContextSiloProbeFaultCause.ProbeDeadline))),
            cancelled.Token);
        var afterCancelled = signal.Read(RepoContextSiloHealthCheck.Name).Evaluations;

        await publisher.PublishAsync(
            HealthReports.Report((RepoContextSiloHealthCheck.Name, HealthReports.Entry(
                HealthStatus.Unhealthy,
                RepoContextSiloProbeFaultCause.ProbeDeadline))),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(afterCancelled, Is.Zero);
            Assert.That(
                signal.Read(RepoContextSiloHealthCheck.Name).Evaluations,
                Is.EqualTo(1),
                "control: the same publisher and the same report do record when the token is live, so "
                + "the zero above is cancellation and not a publisher that never works.");
        });
    }

    /// <summary>Parameter validation on the publisher.</summary>
    [Test]
    public void The_publisher_rejects_null_arguments()
        => Assert.Multiple(() =>
        {
            Assert.That(() => new RepoContextHealthPublisher(null!), Throws.ArgumentNullException);
            Assert.That(
                async () => await new RepoContextHealthPublisher(new RepoContextHealthSignal(["silo"]))
                    .PublishAsync(null!, CancellationToken.None),
                Throws.ArgumentNullException);
        });

    /// <summary>
    /// The registration itself. A publisher that is written but never registered
    /// produces exactly the failure this work removes - an absent series that reads as
    /// a healthy zero - so the wiring is the part most worth asserting.
    /// </summary>
    [Test]
    public void The_host_registration_wires_the_publisher_the_meter_and_every_registered_component()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHealthChecks()
            .AddCheck<RepoContextLivenessHealthCheck>("self")
            .AddCheck<RepoContextLivenessHealthCheck>(RepoContextSiloHealthCheck.Name);
        services.AddRepoContextHealthPublication();

        using var provider = services.BuildServiceProvider();
        var signal = provider.GetRequiredService<RepoContextHealthSignal>();
        var publishers = provider.GetServices<IHealthCheckPublisher>().ToArray();
        var options = provider.GetRequiredService<IOptions<HealthCheckPublisherOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(
                publishers.OfType<RepoContextHealthPublisher>().Count(),
                Is.EqualTo(1),
                "registered exactly once. Zero means the verdict never reaches the scrape; more than "
                + "one would double every evaluation count and make the fault ratio wrong.");
            Assert.That(
                signal.Components,
                Is.EquivalentTo(new[] { "self", RepoContextSiloHealthCheck.Name }),
                "the component set is read from the health-check registrations rather than listed at "
                + "the registration site, so a component added later - or one registered only on a "
                + "deployment-conditional branch - cannot be silently unreported.");
            Assert.That(
                provider.GetRequiredService<RepoContextHealthMeter>(),
                Is.Not.Null,
                "the meter is resolvable, and the host resolves it eagerly: an observable instrument "
                + "nobody resolves is never published, so a lazily-registered meter would leave the "
                + "health series absent from the very scrape this change adds them to.");
            Assert.That(
                provider.GetRequiredService<RepoContextHealthSignal>(),
                Is.SameAs(signal),
                "a singleton. A per-resolution signal would give the meter a holder nobody publishes "
                + "into, which reads on the scrape as a container that is never evaluated.");
            Assert.That(
                options.Period,
                Is.EqualTo(RepoContextHealthPublication.PublishPeriod),
                "the cadence is explicit. Health components are evaluated on demand, so without a "
                + "timer the only component that touches the authorization seam runs solely when "
                + "something probes it over HTTP.");
            Assert.That(options.Delay, Is.EqualTo(RepoContextHealthPublication.PublishDelay));
            Assert.That(
                RepoContextHealthPublication.PublishPeriod,
                Is.GreaterThan(TimeSpan.Zero),
                "control: a zero period would disable the cadence while both assertions above still "
                + "passed against it.");
        });
    }

    /// <summary>The registration extension validates its argument.</summary>
    [Test]
    public void The_host_registration_rejects_a_null_collection()
        => Assert.That(
            () => RepoContextHealthPublication.AddRepoContextHealthPublication(null!),
            Throws.ArgumentNullException);
}
