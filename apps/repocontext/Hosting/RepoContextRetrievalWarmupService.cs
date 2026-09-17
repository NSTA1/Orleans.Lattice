using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Drives the vector plane to a provably-serving state at startup, so the retrieval
/// readiness probe reports <b>demonstrated</b> capability rather than configuration.
/// It owns the container-side concerns - lifetime anchoring, the retry cadence, and the
/// trusted local-agent credential - and delegates the pass itself to
/// <see cref="IRepoContextRetrievalWarmup"/>, which drives the ordinary search path.
/// <para>
/// <b>It cannot wedge the box.</b> A host with no embedding provider bound is marked
/// keyword-only immediately and never issues a query, so a keyword-only deployment is
/// ready at once. A host with an embedder bound retries with backoff until the plane
/// answers or shutdown begins.
/// </para>
/// <para>
/// <b>It supervises for the life of the host; it is not a one-shot.</b> Readiness is
/// revocable by design - <see cref="RepoContextRetrievalReadinessState"/> falls back to
/// <see cref="RepoContextRetrievalReadinessPhase.Building"/> once an observed fault has
/// outlived the hold-down - and this service is the only thing that can restore it
/// without client traffic. So reaching ready moves the loop to a supervision cadence
/// rather than ending it, and a later revocation re-drives the warmup pass. See the
/// remarks on <see cref="WarmupAsync"/> for what returning on first-ready cost.
/// </para>
/// <para>
/// <b>It never blocks startup and never throws into the host.</b> The loop starts on
/// <see cref="IHostApplicationLifetime.ApplicationStarted"/> and every pass is
/// fail-closed.
/// </para>
/// </summary>
public sealed class RepoContextRetrievalWarmupService : IHostedService
{
    private static readonly TimeSpan MinRetryDelay = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How often a ready plane is re-checked so a revocation is noticed and re-driven.
    /// It is a poll of an in-memory phase, not a query: the pass itself runs only once
    /// the poll observes that readiness has actually been lost, so a steady-state host
    /// issues no warmup search at all.
    /// </summary>
    internal static readonly TimeSpan SupervisionInterval = TimeSpan.FromSeconds(30);

    private readonly IRepoContextRetrievalWarmup _warmupPass;
    private readonly RepoContextRetrievalReadinessState _readiness;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly ILogger<RepoContextRetrievalWarmupService> _logger;
    private readonly IEmbeddingProvider? _embeddingProvider;
    private readonly TimeProvider _timeProvider;
    private readonly CancellationTokenSource _stopping = new();
    private Task? _warmup;

    /// <summary>Initializes the warmup coordinator.</summary>
    /// <param name="warmupPass">The seam that runs one warmup pass. Must not be <see langword="null"/>.</param>
    /// <param name="readiness">The shared vector-plane readiness state. Must not be <see langword="null"/>.</param>
    /// <param name="lifetime">The host application lifetime the warmup is anchored to. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger. Must not be <see langword="null"/>.</param>
    /// <param name="embeddingProvider">The embedding provider, or <see langword="null"/> when the host bound none (the box is then keyword-only and needs no warmup).</param>
    /// <param name="timeProvider">The clock the retry and supervision waits are measured on; defaults to <see cref="TimeProvider.System"/>. Injected so a test can drive the supervision loop without waiting on a real timer.</param>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    public RepoContextRetrievalWarmupService(
        IRepoContextRetrievalWarmup warmupPass,
        RepoContextRetrievalReadinessState readiness,
        IHostApplicationLifetime lifetime,
        ILogger<RepoContextRetrievalWarmupService> logger,
        IEmbeddingProvider? embeddingProvider = null,
        TimeProvider? timeProvider = null)
    {
        _warmupPass = warmupPass ?? throw new ArgumentNullException(nameof(warmupPass));
        _readiness = readiness ?? throw new ArgumentNullException(nameof(readiness));
        _lifetime = lifetime ?? throw new ArgumentNullException(nameof(lifetime));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _embeddingProvider = embeddingProvider;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _lifetime.ApplicationStarted.Register(() => _warmup = WarmupAsync(_stopping.Token));
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stopping.CancelAsync().ConfigureAwait(false);

        if (_warmup is not null)
        {
            try
            {
                await _warmup.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected when shutdown interrupts a warmup retry.
            }
        }
    }

    /// <summary>
    /// Drives warmup passes until the retrieval plane reports ready, then supervises it
    /// for the life of the host, re-driving a pass whenever readiness is lost. Never
    /// throws.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Returning on first-ready was the defect.</b> This loop used to exit the moment
    /// a pass reported ready, which reads as correct only if readiness were a latch. It
    /// is not: <see cref="RepoContextRetrievalReadinessState"/> revokes readiness once an
    /// observed fault outlives its hold-down, and the only inputs that can restore it are
    /// a real client query and this pass. On an idle container there are no client
    /// queries, so once the loop had returned nothing was left to observe the plane at
    /// all.
    /// </para>
    /// <para>
    /// That is not hypothetical. A container reached ready 224 s after start, took a
    /// single <c>keyword.vector_plane_unavailable</c> fault episode about 28 minutes
    /// later, and then reported <c>/health/ready</c> as 503 continuously for the next
    /// five and a half hours - through the vector plane finishing its build and logging
    /// that it was serving 133,713 vectors, which the readiness check does not consult.
    /// One manually-issued search flipped it green immediately. An orchestrator would
    /// never have routed traffic to that replica, and a rolling deploy would have
    /// stalled on it.
    /// </para>
    /// <para>
    /// The steady-state cost of supervising is one volatile read of an in-memory phase
    /// every <see cref="SupervisionInterval"/>. A pass - which embeds a query and
    /// searches - runs only when that read observes readiness actually lost, so a
    /// healthy host issues no warmup search after the first.
    /// </para>
    /// </remarks>
    /// <param name="cancellationToken">Cancelled when the host begins to stop.</param>
    internal async Task WarmupAsync(CancellationToken cancellationToken)
    {
        if (_embeddingProvider is null)
        {
            // No vector plane exists to wait for: keyword recall is this deployment's
            // intended steady state, so readiness must not block on it.
            _readiness.MarkKeywordOnly();
            _logger.LogInformation(
                "RepoContext retrieval warmup skipped: no embedding provider is bound, so the host is ready in keyword-only mode.");
            return;
        }

        var delay = MinRetryDelay;
        var converged = false;
        var wasReady = false;

        while (!cancellationToken.IsCancellationRequested)
        {
            if (_readiness.IsReady)
            {
                if (!converged)
                {
                    converged = true;
                    _logger.LogInformation(
                        "RepoContext retrieval warmup complete after {Elapsed}: the retrieval plane is ready in phase {Phase}. The warmup now supervises readiness every {SupervisionInterval} and re-drives a pass if it is lost.",
                        _readiness.TimeToReady,
                        _readiness.Phase,
                        SupervisionInterval);
                }
                else if (!wasReady)
                {
                    _logger.LogInformation(
                        "RepoContext retrieval readiness restored by a warmup pass: the retrieval plane is serving again in phase {Phase}.",
                        _readiness.Phase);
                }

                wasReady = true;

                // A fresh episode gets the full backoff ramp again, so a revocation
                // hours into a run is retried as promptly as one at startup.
                delay = MinRetryDelay;

                if (!await TryDelayAsync(SupervisionInterval, cancellationToken).ConfigureAwait(false))
                {
                    return;
                }

                continue;
            }

            if (wasReady)
            {
                wasReady = false;
                _logger.LogWarning(
                    "RepoContext retrieval readiness was REVOKED after the plane had been serving; re-driving the warmup pass. Until it succeeds /health/ready reports not-ready and an orchestrator will hold traffic back.");
            }

            bool ready;
            try
            {
                ready = await RunPassAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            if (ready)
            {
                // Let the ready branch above do the logging and move to supervision,
                // so first-convergence and restoration are reported in one place.
                continue;
            }

            if (!await TryDelayAsync(delay, cancellationToken).ConfigureAwait(false))
            {
                return;
            }

            delay = delay >= MaxRetryDelay ? MaxRetryDelay : delay + delay;
        }
    }

    /// <summary>
    /// Waits <paramref name="delay"/>, reporting whether the wait completed rather than
    /// throwing when shutdown interrupts it.
    /// </summary>
    /// <param name="delay">How long to wait.</param>
    /// <param name="cancellationToken">Cancelled when the host begins to stop.</param>
    /// <returns><see langword="false"/> when the wait was cancelled and the caller should return.</returns>
    private async Task<bool> TryDelayAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, _timeProvider, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    /// <summary>
    /// Runs one warmup pass under the trusted local-agent credential, so its reads pass
    /// the default-deny access gate exactly as an inbound tool call would. This is a
    /// fixed container identity, not a per-request credential, so scoping it here
    /// re-globalises no caller state.
    /// </summary>
    /// <param name="cancellationToken">Cancelled when the host begins to stop.</param>
    /// <returns><see langword="true"/> once the retrieval plane reports ready.</returns>
    internal async Task<bool> RunPassAsync(CancellationToken cancellationToken)
    {
        using (LatticeCredentialContext.Use(
            LocalTrustedAgent.SubjectId,
            scheme: LocalTrustedAgent.Scheme))
        {
            return await _warmupPass.TryWarmAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
