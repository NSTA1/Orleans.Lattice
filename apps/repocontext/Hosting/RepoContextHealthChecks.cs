using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Liveness probe: reports healthy while the process and silo host are alive. It
/// deliberately does not consult the readiness phase, so a draining or
/// still-replaying container is reported live (the process is up) even though it
/// is not yet, or no longer, ready to serve. An orchestrator uses this to decide
/// whether to restart the container, not whether to route traffic to it.
/// </summary>
public sealed class RepoContextLivenessHealthCheck : IHealthCheck
{
    /// <summary>The health-check registration name.</summary>
    public const string Name = "self";

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
        => Task.FromResult(HealthCheckResult.Healthy("Process and silo host are alive."));
}

/// <summary>
/// Readiness probe: reports healthy only once the host has reached
/// <see cref="RepoContextLifecyclePhase.Ready"/> - the silo has joined, the
/// activation-time WAL replay / cold rebuild warmup has completed, the durable
/// stores were proven reachable, and the MCP surface is serving. It reports
/// not-ready during startup replay and again during drain, so an orchestrator
/// stops routing MCP traffic before the silo begins to stop.
/// </summary>
/// <param name="state">The shared lifecycle-phase holder.</param>
public sealed class RepoContextReadinessHealthCheck(RepoContextReadinessState state) : IHealthCheck
{
    /// <summary>The health-check registration name.</summary>
    public const string Name = "ready";

    private readonly RepoContextReadinessState _state = state
        ?? throw new ArgumentNullException(nameof(state));

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var phase = _state.Phase;
        var result = phase == RepoContextLifecyclePhase.Ready
            ? HealthCheckResult.Healthy("Silo joined, stores reachable, MCP serving.")
            : HealthCheckResult.Unhealthy($"Not ready: lifecycle phase is {phase}.");

        return Task.FromResult(result);
    }
}

/// <summary>
/// Readiness probe component for the <b>vector plane</b>: reports healthy only once
/// the host can serve the retrieval it is configured for. It exists because the
/// lifecycle-phase component alone reports fully ready as soon as the silo has joined
/// and its durable stores are writable - which a box happily does while its vector
/// plane is still replaying and cannot answer a single semantic query. Registered
/// under the readiness tag alongside
/// <see cref="RepoContextReadinessHealthCheck"/>, so <c>/health/ready</c> is the
/// conjunction of both and an orchestrator holds traffic back until semantic retrieval
/// actually works.
/// <para>
/// <b>Liveness is deliberately untouched.</b> A still-replaying box is alive and must
/// not be restarted; only readiness reflects the vector plane.
/// </para>
/// <para>
/// <b>It never deadlocks.</b> A host with no embedding provider bound reports
/// <see cref="RepoContextRetrievalReadinessPhase.KeywordOnly"/>, which is healthy:
/// keyword recall is that deployment's intended steady state, not a degradation. A
/// host that has onboarded no repository yet reports
/// <see cref="RepoContextRetrievalReadinessPhase.NothingRegistered"/>, which is also
/// healthy: there is nothing the vector plane could be asked to serve, and holding
/// traffic back would stop the very calls that onboard the first repository.
/// </para>
/// <para>
/// <b>It never flaps.</b> The check is a pure reader of
/// <see cref="RepoContextRetrievalReadinessState"/>, whose fault hold-down keeps a
/// proven-serving plane ready across a transient fault.
/// </para>
/// </summary>
/// <param name="state">The shared vector-plane readiness state.</param>
public sealed class RepoContextRetrievalReadinessHealthCheck(RepoContextRetrievalReadinessState state) : IHealthCheck
{
    /// <summary>The health-check registration name.</summary>
    public const string Name = "retrieval";

    // Cached results: the probe runs on every orchestrator poll, so the steady-state
    // path allocates neither a result nor a Task.
    //
    // Serving is reported in three forms rather than one. All three are Healthy and
    // the verdict is identical: an unarmed plane answers by exhaustive scan with
    // complete recall, so it is genuinely ready, and a corpus below the training
    // threshold can never partition and must never be failed for it. What differs is
    // what the line SAYS, because "the vector plane is serving" was true of an armed
    // and an unarmed plane alike, which made the distinction issue #2441 exists to
    // expose invisible on the one surface an operator actually reads. Do not collapse
    // these back into one message, and do not turn the unarmed arm Unhealthy.
    private static readonly Task<HealthCheckResult> ServingArmed = Task.FromResult(
        HealthCheckResult.Healthy(
            "Vector plane is serving semantic retrieval from its trained partitioning (arming: armed)."));

    private static readonly Task<HealthCheckResult> ServingUnarmed = Task.FromResult(
        HealthCheckResult.Healthy(
            "Vector plane is serving semantic retrieval by exhaustive scan of the vectors it holds, not from a "
            + "trained partitioning (arming: unarmed). Recall is complete and this is healthy; it means the "
            + "approximate index has not armed, because its corpus is below the training threshold or training "
            + "has not run."));

    private static readonly Task<HealthCheckResult> ServingArmingUnknown = Task.FromResult(
        HealthCheckResult.Healthy(
            "Vector plane is serving semantic retrieval; which path inside the plane answered has not been "
            + "observed yet (arming: unknown). This is not the same as observing an unarmed plane."));

    private static readonly Task<HealthCheckResult> KeywordOnly = Task.FromResult(
        HealthCheckResult.Healthy(
            "Keyword-only: no embedding provider is bound, so there is no vector plane to wait for."));

    private static readonly Task<HealthCheckResult> NothingRegistered = Task.FromResult(
        HealthCheckResult.Healthy(
            "Nothing registered: no repository is onboarded, so there is nothing the vector plane could be asked to serve."));

    // Named for the readiness phase, not for a cause: what the phase records is
    // that the plane has never served, and nothing here observes why. Asserting a
    // build was in progress - which this message did until issue #2362 - turned an
    // absence of evidence into a claim, and on a deployment whose sweep had
    // scheduled no build at all it was simply false. The machine-readable form of
    // the same fact is the search response's retrieval path, which this text
    // deliberately quotes so an operator reading either one is told the same thing.
    // It names both keyword causes rather than one, because since issue #2720 a
    // plane that holds nothing and an exact fallback a guard is holding shut report
    // differently, and this phase is reached by both - so pinning a single value
    // here would make the line wrong for whichever box is in the other state.
    private static readonly Task<HealthCheckResult> Building = Task.FromResult(
        HealthCheckResult.Unhealthy(
            "Not ready: the vector plane has not served semantic retrieval, so searches are answering as "
            + "keyword.vector_plane_unavailable, or as keyword.exact_fallback_suppressed when a stalled gather "
            + "has left the exact fallback withheld. Run a search and read its retrievalPath to tell which. "
            + "Whether a build is in progress is not known here; check the "
            + "index build's own status rather than inferring it from this line."));

    private readonly RepoContextRetrievalReadinessState _state = state
        ?? throw new ArgumentNullException(nameof(state));

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
        => _state.Phase switch
        {
            RepoContextRetrievalReadinessPhase.Serving => _state.Arming switch
            {
                RepoContextRetrievalArming.Armed => ServingArmed,
                RepoContextRetrievalArming.Unarmed => ServingUnarmed,
                _ => ServingArmingUnknown,
            },
            RepoContextRetrievalReadinessPhase.KeywordOnly => KeywordOnly,
            RepoContextRetrievalReadinessPhase.NothingRegistered => NothingRegistered,
            _ => Building,
        };
}

/// <summary>
/// Reports whether the durable agent-memory tree is actually being captured. It is
/// the missing machine-readable surface recorded by issue #2640: the backup wiring
/// added by #2602 kept a complete status object and printed it to the log, and no
/// health component ever read it, so a container on which every single capture threw
/// (issue #2621, 14 failures out of 14 attempts) answered every probe green.
/// </summary>
/// <remarks>
/// <para>
/// <b>Never-captured is Degraded, not Healthy.</b> That is the entire point. A
/// component that reports healthy until something fails cannot distinguish a
/// container capturing hourly from one that has never captured at all, which is the
/// exact confusion that let an unprotected deployment look fine. So the verdicts are
/// three-valued: Healthy states a capture demonstrably happened, Degraded states
/// that nothing is known to be protected yet, and Unhealthy states an attempt
/// failed.
/// </para>
/// <para>
/// <b>Disabled is Healthy, deliberately.</b> A host is required to boot with no
/// backup sink configured, and failing an optional durability feature would make it
/// mandatory. The verdict is healthy and the message says plainly that the tree is
/// not captured anywhere, so the fact is reported rather than implied by silence.
/// </para>
/// <para>
/// <b>It is deliberately not tagged for readiness or liveness.</b> A failing backup
/// is not a reason to restart the container or to stop routing MCP traffic to it -
/// draining a box because its backup sink is unreachable would turn a durability
/// fault into an availability outage, and would stop the very traffic that keeps the
/// memory worth backing up. It is registered on its own tag and served on its own
/// path so it can be probed and alerted on without touching orchestration.
/// </para>
/// <para>
/// Unlike its siblings here, no result is cached: every field of the message is live
/// state, and a cached result would report a stale verdict.
/// </para>
/// </remarks>
/// <param name="status">The shared backup status.</param>
public sealed class RepoContextBackupHealthCheck(RepoContextBackupStatus status) : IHealthCheck
{
    /// <summary>The health-check registration name.</summary>
    public const string Name = "backup";

    private readonly RepoContextBackupStatus _status = status
        ?? throw new ArgumentNullException(nameof(status));

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        // One read of the derived state, then one rendering of the evidence behind
        // it. The state is derived on the status object itself so this check and the
        // metric series can never disagree about the same container.
        var state = _status.State;
        var description = _status.Describe();

        var result = state switch
        {
            RepoContextBackupState.Disabled => HealthCheckResult.Healthy(description),

            RepoContextBackupState.Protected => HealthCheckResult.Healthy(description),

            RepoContextBackupState.NeverCaptured => HealthCheckResult.Degraded(
                "Nothing has been captured yet, so nothing produced by this container is recoverable. "
                + description),

            RepoContextBackupState.CapturedNothing => HealthCheckResult.Degraded(
                "Captures are completing but the last full capture described ZERO entries, so it "
                + "protects nothing. " + description),

            RepoContextBackupState.FailingAfterCapture => HealthCheckResult.Unhealthy(
                "The most recent capture attempt FAILED; earlier captures from this container are still "
                + "recoverable but the configured cadence is broken. " + description),

            // Nothing this container produced is recoverable and attempts are
            // failing. This is the state issue #2621 reported as healthy.
            RepoContextBackupState.FailingUnprotected => HealthCheckResult.Unhealthy(
                "Capture is FAILING and this container has never captured anything, so the tree is "
                + "unprotected by it. " + description),

            // Fail closed. Every state above is enumerated, so this arm is reached
            // only by a state added later without a verdict, and the safe reading of
            // an unclassified backup condition is "not known to be protected" rather
            // than a green probe. It deliberately claims nothing specific about the
            // cause, because it does not know one.
            _ => HealthCheckResult.Unhealthy(
                $"Backup is in an unrecognised state ({state}), so protection cannot be confirmed. "
                + description),
        };

        return Task.FromResult(result);
    }
}
