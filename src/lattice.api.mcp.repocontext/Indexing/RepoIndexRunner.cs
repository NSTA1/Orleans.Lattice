using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The default <see cref="IRepoIndexRunner"/>: runs each indexing pass on a
/// background task bound to the host's <see cref="IHostApplicationLifetime"/>, not
/// to any client request, and reports progress back into the job grain. It keeps
/// one live run per repository id (single-flight), so a duplicate start or an
/// overlapping resume reminder never launches a second concurrent run over the
/// same tree.
/// </summary>
internal sealed class RepoIndexRunner : IRepoIndexRunner
{
    private readonly RepoContextBootstrapService _bootstrap;
    private readonly IGrainFactory _grainFactory;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly IRepoIndexRunAuthority _runAuthority;
    private readonly ILogger<RepoIndexRunner> _logger;
    private readonly RepoContextIndexingPacer? _pacer;
    private readonly RepoContextIngestReporter? _ingest;

    /// <summary>Live runs keyed by repository id. The value carries the run's cancellation source and a completion signal.</summary>
    private readonly ConcurrentDictionary<string, RunHandle> _runs =
        new(StringComparer.Ordinal);

    /// <summary>Creates the runner.</summary>
    /// <param name="bootstrap">The bootstrap coordinator that performs the pass. Must not be <see langword="null"/>.</param>
    /// <param name="grainFactory">The grain factory used to report progress back to the job grain. Must not be <see langword="null"/>.</param>
    /// <param name="lifetime">The host lifetime whose stopping token bounds every run. Must not be <see langword="null"/>.</param>
    /// <param name="runAuthority">Resolves the fixed credential every run assumes so a reminder-driven resume writes under the same subject as the original pass. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger. Must not be <see langword="null"/>.</param>
    /// <param name="pacer">The silo's indexing pacer, whose reading is overlaid on the progress of a running job, or <see langword="null"/> when the host registers none.</param>
    /// <param name="ingestReporter">The ingest instrument family every pass is accounted against, or <see langword="null"/> when the host registers none.</param>
    /// <exception cref="ArgumentNullException">Any argument other than <paramref name="pacer"/> or <paramref name="ingestReporter"/> is null.</exception>
    public RepoIndexRunner(
        RepoContextBootstrapService bootstrap,
        IGrainFactory grainFactory,
        IHostApplicationLifetime lifetime,
        IRepoIndexRunAuthority runAuthority,
        ILogger<RepoIndexRunner> logger,
        RepoContextIndexingPacer? pacer = null,
        RepoContextIngestReporter? ingestReporter = null)
    {
        ArgumentNullException.ThrowIfNull(bootstrap);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(lifetime);
        ArgumentNullException.ThrowIfNull(runAuthority);
        ArgumentNullException.ThrowIfNull(logger);
        _bootstrap = bootstrap;
        _grainFactory = grainFactory;
        _lifetime = lifetime;
        _runAuthority = runAuthority;
        _logger = logger;
        _pacer = pacer;
        _ingest = ingestReporter;
    }

    /// <inheritdoc />
    public Task<RepoIndexProgress> StartIndexAsync(RepoIndexJobRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        return _grainFactory.GetGrain<IRepoIndexJobGrain>(request.RepoId).StartAsync(request);
    }

    /// <inheritdoc />
    public async Task<RepoIndexProgress> GetProgressAsync(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        var progress = await _grainFactory.GetGrain<IRepoIndexJobGrain>(repoId)
            .GetProgressAsync()
            .ConfigureAwait(false);

        // The pacer is silo-local, live state, so it is read here at query time
        // rather than persisted on the job grain: a stored reading would be stale by
        // the time anyone read it. It is attached only to a running job, since a
        // finished or failed job is not being paced (issue #3447).
        return _pacer is not null && progress.Status == RepoIndexStatus.Running
            ? progress with { Pacing = _pacer.Snapshot() }
            : progress;
    }

    /// <inheritdoc />
    public void Enqueue(RepoIndexJobRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        var cts = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.ApplicationStopping);
        var handle = new RunHandle(cts);
        if (!_runs.TryAdd(request.RepoId, handle))
        {
            // A run for this repository is already in flight: single-flight no-op.
            cts.Dispose();
            return;
        }

        _ = Task.Run(() => RunAsync(request, handle), CancellationToken.None);
    }

    /// <inheritdoc />
    public bool Cancel(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        if (!_runs.TryGetValue(repoId, out var handle))
        {
            return false;
        }

        try
        {
            handle.Cts.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // The run finished between the lookup and the cancel; nothing to do.
        }

        return true;
    }

    /// <inheritdoc />
    public async Task CancelAndWaitAsync(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        if (!_runs.TryGetValue(repoId, out var handle))
        {
            // No run in flight. If a run removed itself from the map it has already
            // passed its last write and reached its finally, so there is nothing to
            // drain either way.
            return;
        }

        try
        {
            handle.Cts.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // The run finished between the lookup and the cancel; its writes are done.
        }

        // Wait for the run to observe cancellation and reach its finally, at which
        // point no further structural write can be issued by it.
        await handle.Finished.Task.ConfigureAwait(false);
    }

    private async Task RunAsync(RepoIndexJobRequest request, RunHandle handle)
    {
        var repoId = request.RepoId;
        var cts = handle.Cts;
        var grain = _grainFactory.GetGrain<IRepoIndexJobGrain>(repoId);

        // Stamp the run's credential onto the ambient context for the whole pass so
        // every structural and vector write - and every progress report - carries a
        // subject the access gate can authorize. This is the single point all runs
        // funnel through, so a reminder-driven resume (a system-origin grain call
        // that carries no ambient credential) writes under the same subject as the
        // original request-initiated pass, instead of failing closed as anonymous.
        // A null authority result leaves the ambient credential untouched.
        var runCredential = _runAuthority.Resolve();
        using var credentialScope = runCredential is null
            ? NullDisposable.Instance
            : LatticeCredentialContext.With(runCredential);

        // Begun immediately before the try, so every path out of it settles the pass
        // exactly once: completed, failed or cancelled (issue #3151).
        var pass = _ingest?.BeginPass(repoId);
        var sink = new GrainProgressSink(grain, _logger, repoId, pass);

        try
        {
            var bootstrapRequest = new RepoContextBootstrapRequest
            {
                RepoRoot = request.RepoRoot,
                RepoId = repoId,
                IncludeGlobs = request.IncludeGlobs,
                ExcludeGlobs = request.ExcludeGlobs,
                RespectGitignore = request.RespectGitignore,
                ExcludeBinary = request.ExcludeBinary,
                AllowPrune = request.AllowPrune,
                ForceEmbeddingGapScan = request.ForceEmbeddingGapScan,
                CommitSha = request.CommitSha,
            };

            var result = await _bootstrap
                .RunAsync(bootstrapRequest, sink, cts.Token)
                .ConfigureAwait(false);

            await grain.CompleteAsync(
                new RepoIndexProgressUpdate
                {
                    FilesScanned = result.FilesScanned,
                    FilesAdded = result.FilesAdded,
                    FilesUpdated = result.FilesUpdated,
                    FilesRemoved = result.FilesRemoved,
                    FilesUnchanged = result.FilesUnchanged,
                },
                result.ElapsedMilliseconds).ConfigureAwait(false);

            // Settled only once the grain has durably recorded completion, so the
            // metric never reports a pass completed that index_status reports failed.
            // The result lands the plan outcomes a no-change pass never reports as
            // progress.
            pass?.Observe(result);
            pass?.Complete();
        }
        catch (OperationCanceledException)
        {
            // Cancelled by host shutdown or an explicit repository removal. Do not
            // settle the grain: on shutdown the resume reminder restarts the run on
            // the next activation; on removal the grain state is being cleared by
            // the removal path.
            pass?.Cancel();
            _logger.LogInformation(
                "Repo {RepoId}: indexing run cancelled; it will resume on the next start.", repoId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Repo {RepoId}: indexing run failed.", repoId);
            pass?.Fail();
            try
            {
                await grain.FailAsync(Describe(ex)).ConfigureAwait(false);
            }
            catch (Exception reportEx)
            {
                _logger.LogWarning(
                    reportEx, "Repo {RepoId}: failed to record the indexing job failure.", repoId);
            }
        }
        finally
        {
            // Remove only if the map still holds this exact run's handle, so a
            // freshly enqueued successor run is never evicted.
            if (_runs.TryRemove(new KeyValuePair<string, RunHandle>(repoId, handle)))
            {
                cts.Dispose();
            }

            // Always release any drainer awaiting this run's termination, even on
            // an unexpected fault, so CancelAndWaitAsync can never hang.
            handle.Finished.TrySetResult();
        }
    }

    private static string Describe(Exception ex) =>
        $"{ex.GetType().Name}: {ex.Message}";

    /// <summary>
    /// A no-op <see cref="IDisposable"/> used when the run authority resolves no
    /// credential, so the credential-scope <c>using</c> can leave the ambient
    /// credential untouched without allocating a real scope.
    /// </summary>
    private sealed class NullDisposable : IDisposable
    {
        public static readonly NullDisposable Instance = new();

        public void Dispose()
        {
        }
    }

    /// <summary>
    /// The per-run bookkeeping the runner keeps in its live-runs map: the run's
    /// linked cancellation source and a completion signal that
    /// <see cref="CancelAndWaitAsync"/> awaits so a drain observes the run's true
    /// termination rather than merely its cancellation request.
    /// </summary>
    private sealed class RunHandle(CancellationTokenSource cts)
    {
        public CancellationTokenSource Cts { get; } = cts;

        public TaskCompletionSource Finished { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    /// <summary>
    /// Forwards progress deltas to the job grain, swallowing a transient report
    /// failure so advisory progress never fails the durable run - but letting a
    /// cancellation propagate so the run stops promptly. Each delta is accounted
    /// against the pass's ingest instruments first, so a report the grain drops
    /// still advances <c>/metrics</c>.
    /// </summary>
    private sealed class GrainProgressSink(
        IRepoIndexJobGrain grain, ILogger logger, string repoId, RepoContextIngestPass? pass)
        : IRepoIndexProgressSink
    {
        public async ValueTask ReportAsync(
            RepoIndexProgressUpdate update, CancellationToken cancellationToken)
        {
            pass?.Observe(update);
            try
            {
                await grain.ReportProgressAsync(update).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogDebug(
                    ex, "Repo {RepoId}: a progress report was dropped (non-fatal).", repoId);
            }
        }
    }
}
