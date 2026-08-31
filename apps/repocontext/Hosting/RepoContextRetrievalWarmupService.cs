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
/// <b>It never blocks startup and never throws into the host.</b> The loop starts on
/// <see cref="IHostApplicationLifetime.ApplicationStarted"/> and every pass is
/// fail-closed.
/// </para>
/// </summary>
public sealed class RepoContextRetrievalWarmupService : IHostedService
{
    private static readonly TimeSpan MinRetryDelay = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(30);

    private readonly IRepoContextRetrievalWarmup _warmupPass;
    private readonly RepoContextRetrievalReadinessState _readiness;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly ILogger<RepoContextRetrievalWarmupService> _logger;
    private readonly IEmbeddingProvider? _embeddingProvider;
    private readonly CancellationTokenSource _stopping = new();
    private Task? _warmup;

    /// <summary>Initializes the warmup coordinator.</summary>
    /// <param name="warmupPass">The seam that runs one warmup pass. Must not be <see langword="null"/>.</param>
    /// <param name="readiness">The shared vector-plane readiness state. Must not be <see langword="null"/>.</param>
    /// <param name="lifetime">The host application lifetime the warmup is anchored to. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger. Must not be <see langword="null"/>.</param>
    /// <param name="embeddingProvider">The embedding provider, or <see langword="null"/> when the host bound none (the box is then keyword-only and needs no warmup).</param>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    public RepoContextRetrievalWarmupService(
        IRepoContextRetrievalWarmup warmupPass,
        RepoContextRetrievalReadinessState readiness,
        IHostApplicationLifetime lifetime,
        ILogger<RepoContextRetrievalWarmupService> logger,
        IEmbeddingProvider? embeddingProvider = null)
    {
        _warmupPass = warmupPass ?? throw new ArgumentNullException(nameof(warmupPass));
        _readiness = readiness ?? throw new ArgumentNullException(nameof(readiness));
        _lifetime = lifetime ?? throw new ArgumentNullException(nameof(lifetime));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _embeddingProvider = embeddingProvider;
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
    /// Runs warmup passes until the retrieval plane reports ready or shutdown is
    /// requested, backing off between attempts. Never throws.
    /// </summary>
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
        while (!cancellationToken.IsCancellationRequested && !_readiness.IsReady)
        {
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
                _logger.LogInformation(
                    "RepoContext retrieval warmup complete: the vector plane served a semantic query after {Elapsed}.",
                    _readiness.TimeToReady);
                return;
            }

            try
            {
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            delay = delay >= MaxRetryDelay ? MaxRetryDelay : delay + delay;
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
