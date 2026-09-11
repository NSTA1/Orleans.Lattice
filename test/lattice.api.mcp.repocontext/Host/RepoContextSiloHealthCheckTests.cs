using Microsoft.Extensions.Diagnostics.HealthChecks;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for the grain-liveness health check (<c>/health/silo</c>), which is
/// the probe the container's Docker healthcheck targets. It exercises all four
/// states issue #2666 requires be demonstrated - a stopped silo and a failing grain
/// layer both report unhealthy once the host has reached readiness, a silo still
/// joining reports the distinct "starting" (Degraded) state rather than a fault, and
/// a working silo reports healthy and stays healthy across consecutive checks.
/// </summary>
/// <remarks>
/// The critical distinction under test is that the check re-exercises the grain
/// layer on <b>every</b> call: a fake probe stands in for the trivial grain read, so
/// a silo that has reached readiness and then dies (probe now throws) flips the
/// verdict red, which the always-green liveness check and the latched readiness
/// check structurally cannot do.
/// </remarks>
[TestFixture]
public sealed class RepoContextSiloHealthCheckTests
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

    /// <summary>A controllable stand-in for the trivial grain call.</summary>
    private sealed class FakeSiloProbe : IRepoContextSiloProbe
    {
        private readonly Func<CancellationToken, Task> _behaviour;

        public FakeSiloProbe(Func<CancellationToken, Task> behaviour) => _behaviour = behaviour;

        public int Calls { get; private set; }

        public static FakeSiloProbe Succeeds() => new(_ => Task.CompletedTask);

        public static FakeSiloProbe Throws(Exception ex) => new(_ => Task.FromException(ex));

        public static FakeSiloProbe Hangs() => new(async ct =>
            await Task.Delay(Timeout.Infinite, ct).ConfigureAwait(false));

        public Task ProbeAsync(CancellationToken cancellationToken)
        {
            Calls++;
            return _behaviour(cancellationToken);
        }
    }

    // State 4: a working silo. Healthy, and stable across consecutive checks.
    [Test]
    public async Task Healthy_when_the_grain_probe_succeeds_after_readiness()
    {
        var probe = FakeSiloProbe.Succeeds();
        var state = new RepoContextReadinessState();
        state.MarkReady();
        var check = new RepoContextSiloHealthCheck(probe, state);

        for (var i = 0; i < 5; i++)
        {
            var result = await check.CheckHealthAsync(NewContext());
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Healthy),
                $"A working silo flapped on consecutive check {i}.");
        }

        Assert.That(probe.Calls, Is.EqualTo(5),
            "The check must re-exercise the grain layer on every call, not cache a first verdict.");
    }

    // State 1: silo stopped -> the grain call cannot connect -> UNHEALTHY (once ready).
    [Test]
    public async Task Unhealthy_when_the_silo_is_stopped_after_readiness()
    {
        var probe = FakeSiloProbe.Throws(
            new InvalidOperationException("no active silo membership: connection refused"));
        var state = new RepoContextReadinessState();
        state.MarkReady();
        var check = new RepoContextSiloHealthCheck(probe, state);

        var result = await check.CheckHealthAsync(NewContext());

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Unhealthy));
            Assert.That(result.Description, Does.Contain("connection refused"),
                "The failure text must reach the description so an operator sees WHY, not a bare verdict.");
        });
    }

    // State 2: silo up, grain layer failing -> the grain call throws -> UNHEALTHY.
    // This is the state that matters most and is the one a bare "is Kestrel
    // listening" probe cannot produce: the process is up and answering HTTP, yet the
    // grain layer beneath it is broken.
    [Test]
    public async Task Unhealthy_when_the_grain_layer_fails_though_the_silo_is_up()
    {
        var probe = FakeSiloProbe.Throws(
            new TimeoutException("grain call failed: activation could not be created"));
        var state = new RepoContextReadinessState();
        state.MarkReady();
        var check = new RepoContextSiloHealthCheck(probe, state);

        var result = await check.CheckHealthAsync(NewContext());

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Unhealthy),
                "A silo whose grain layer cannot answer is unhealthy even though the web host is up.");
            Assert.That(result.Description, Does.Contain("activation could not be created"));
        });
    }

    // State 3: silo still starting -> STARTING (Degraded), NOT unhealthy. Conflating
    // this with unhealthy under restart: unless-stopped is what crash-loops normal
    // boot, so it is a distinct, load-bearing state.
    [Test]
    public async Task Degraded_when_the_probe_fails_but_the_host_has_not_reached_readiness()
    {
        var probe = FakeSiloProbe.Throws(
            new InvalidOperationException("silo is still joining the cluster"));
        var state = new RepoContextReadinessState(); // never MarkReady -> phase Starting

        var check = new RepoContextSiloHealthCheck(probe, state);

        var result = await check.CheckHealthAsync(NewContext());

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Degraded),
                "A silo that has not yet joined is starting, not faulted.");
            Assert.That(result.Description, Does.StartWith("Starting"));
        });
    }

    // A wedged silo answers by HANGING rather than throwing; the probe's own deadline
    // must convert that into a fault. After readiness -> Unhealthy.
    [Test]
    public async Task Unhealthy_when_a_wedged_silo_hangs_past_the_timeout_after_readiness()
    {
        var probe = FakeSiloProbe.Hangs();
        var state = new RepoContextReadinessState();
        state.MarkReady();
        var check = new RepoContextSiloHealthCheck(probe, state, ShortTimeout);

        var result = await check.CheckHealthAsync(NewContext());

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Unhealthy));
            Assert.That(result.Description, Does.Contain("wedged"));
        });
    }

    // The same hang while still starting is starting, not a fault.
    [Test]
    public async Task Degraded_when_a_hanging_silo_times_out_before_readiness()
    {
        var probe = FakeSiloProbe.Hangs();
        var state = new RepoContextReadinessState();
        var check = new RepoContextSiloHealthCheck(probe, state, ShortTimeout);

        var result = await check.CheckHealthAsync(NewContext());

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Degraded));
    }

    // A draining container is stopping on purpose: grain calls will fail as
    // activations deactivate, and marking it unhealthy in its last seconds is wrong.
    // The probe must not even be invoked.
    [Test]
    public async Task Healthy_and_probe_not_invoked_while_draining()
    {
        var probe = FakeSiloProbe.Throws(new InvalidOperationException("should not be called"));
        var state = new RepoContextReadinessState();
        state.MarkReady();
        state.BeginDrain();
        var check = new RepoContextSiloHealthCheck(probe, state);

        var result = await check.CheckHealthAsync(NewContext());

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Healthy));
            Assert.That(result.Description, Does.Contain("Draining"));
            Assert.That(probe.Calls, Is.EqualTo(0),
                "A draining container must not be probed; a failing grain call there is expected.");
        });
    }

    [Test]
    public void Check_rejects_a_null_probe()
        => Assert.That(
            () => new RepoContextSiloHealthCheck(null!, new RepoContextReadinessState()),
            Throws.ArgumentNullException);

    [Test]
    public void Check_rejects_a_null_readiness_state()
        => Assert.That(
            () => new RepoContextSiloHealthCheck(FakeSiloProbe.Succeeds(), null!),
            Throws.ArgumentNullException);

    [Test]
    public void Check_rejects_a_non_positive_timeout()
        => Assert.That(
            () => new RepoContextSiloHealthCheck(
                FakeSiloProbe.Succeeds(), new RepoContextReadinessState(), TimeSpan.Zero),
            Throws.TypeOf<ArgumentOutOfRangeException>());

    [Test]
    public void Silo_probe_rejects_a_null_policy_store()
        => Assert.That(() => new RepoContextSiloProbe(null!), Throws.ArgumentNullException);

    [Test]
    public void The_silo_check_name_is_distinct_from_the_other_probes()
        => Assert.That(
            new[]
            {
                RepoContextLivenessHealthCheck.Name,
                RepoContextReadinessHealthCheck.Name,
                RepoContextRetrievalReadinessHealthCheck.Name,
                RepoContextBackupHealthCheck.Name,
                RepoContextSiloHealthCheck.Name,
            },
            Is.Unique,
            "Two health checks sharing a registration name would collide in the probe endpoint.");
}
