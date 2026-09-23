using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Runs a <c>repocontext_reset_index</c> sweep on a background task bound to the
/// host's <see cref="IHostApplicationLifetime"/> rather than to the calling request,
/// so a caller that is cancelled or loses its connection part-way through abandons
/// only its wait - the sweep keeps running and reports its own completion through
/// <c>repocontext_index_status</c> (issue #2642).
/// </summary>
/// <remarks>
/// <para>
/// Before this seam existed the tool handler passed the request's cancellation
/// token straight into the sweep, so a dropped response cancelled the teardown
/// mid-sweep and left the job surface reporting a running reset that nothing would
/// ever finish - exactly the "still working or wedged?" ambiguity the pollable
/// status was added to remove.
/// </para>
/// <para>
/// The detached sweep runs under the <em>calling</em> principal: the Orleans
/// <see cref="Orleans.Runtime.RequestContext"/> that carries <see cref="LatticeCredentialContext"/>
/// is async-local and flows into the background task with the execution context,
/// so the fail-closed per-tree access gate authorizes every write exactly as it did
/// when the sweep ran inline. No run authority is substituted and no caller shares
/// another caller's run: each call launches its own sweep, as concurrent calls
/// always did.
/// </para>
/// <para>
/// A fault the sweep did not already record is stamped onto the job as
/// <see cref="RepoIndexStatus.Failed"/>, so even a caller that stopped waiting sees
/// a terminal outcome. A host shutdown is deliberately not recorded as a failure:
/// the reset is left <see cref="RepoIndexStatus.Running"/> in
/// <see cref="RepoIndexPhase.Resetting"/>, never marked complete, and re-running the
/// reset (which is idempotent) finishes it.
/// </para>
/// </remarks>
internal sealed class RepoIndexResetLauncher
{
    private readonly Func<string, CancellationToken, Task<RepoContextIndexResetResult>> _reset;
    private readonly IGrainFactory _grainFactory;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly ILogger<RepoIndexResetLauncher> _logger;

    /// <summary>Creates the launcher over the store's reset sweep.</summary>
    /// <param name="store">The store whose <see cref="RepoContextStore.ResetIndexAsync"/> performs the sweep. Must not be <see langword="null"/>.</param>
    /// <param name="grainFactory">The grain factory used to record an unrecorded fault on the job grain. Must not be <see langword="null"/>.</param>
    /// <param name="lifetime">The host lifetime whose stopping token bounds every sweep. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Any argument is null.</exception>
    public RepoIndexResetLauncher(
        RepoContextStore store,
        IGrainFactory grainFactory,
        IHostApplicationLifetime lifetime,
        ILogger<RepoIndexResetLauncher> logger)
        : this(ResetOf(store), grainFactory, lifetime, logger)
    {
    }

    /// <summary>Creates the launcher over an arbitrary reset sweep (the unit-test seam).</summary>
    /// <param name="reset">The sweep to run for a repository id under the host-stopping token. Must not be <see langword="null"/>.</param>
    /// <param name="grainFactory">The grain factory used to record an unrecorded fault on the job grain. Must not be <see langword="null"/>.</param>
    /// <param name="lifetime">The host lifetime whose stopping token bounds every sweep. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Any argument is null.</exception>
    internal RepoIndexResetLauncher(
        Func<string, CancellationToken, Task<RepoContextIndexResetResult>> reset,
        IGrainFactory grainFactory,
        IHostApplicationLifetime lifetime,
        ILogger<RepoIndexResetLauncher> logger)
    {
        ArgumentNullException.ThrowIfNull(reset);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(lifetime);
        ArgumentNullException.ThrowIfNull(logger);
        _reset = reset;
        _grainFactory = grainFactory;
        _lifetime = lifetime;
        _logger = logger;
    }

    /// <summary>
    /// Launches the reset sweep for <paramref name="repoId"/> on a host-lifetime
    /// background task and returns a task that completes with its result.
    /// Cancelling <paramref name="waitCancellationToken"/> abandons only the wait:
    /// the sweep itself is cancelled solely by host shutdown.
    /// </summary>
    /// <param name="repoId">The repository whose code index to reset. Must not be <see langword="null"/>.</param>
    /// <param name="waitCancellationToken">Cancels the caller's wait for the result, never the sweep.</param>
    /// <returns>The reset result, or a cancelled task when the caller stopped waiting.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public Task<RepoContextIndexResetResult> ResetAsync(string repoId, CancellationToken waitCancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);

        var stopping = _lifetime.ApplicationStopping;
        var sweep = Task.Run(() => RunAsync(repoId, stopping), CancellationToken.None);

        // A caller that stops waiting leaves nobody to observe the sweep's fault,
        // which RunAsync has already logged and recorded; observe it here so it is
        // not re-raised as an unobserved task exception at finalization.
        _ = sweep.ContinueWith(
            static t => _ = t.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        return waitCancellationToken.CanBeCanceled ? sweep.WaitAsync(waitCancellationToken) : sweep;
    }

    private async Task<RepoContextIndexResetResult> RunAsync(string repoId, CancellationToken stopping)
    {
        try
        {
            return await _reset(repoId, stopping).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested)
        {
            _logger.LogWarning(
                "Repo {RepoId}: index reset interrupted by host shutdown; it stays Running/Resetting and re-running the reset finishes it.",
                repoId);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Repo {RepoId}: index reset failed.", repoId);
            await RecordFailureAsync(repoId, ex).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Stamps <paramref name="fault"/> onto the job only while it still reports this
    /// reset in flight: a sweep that already recorded its own failure, or a job a
    /// later onboarding has since taken over, is left untouched.
    /// </summary>
    private async Task RecordFailureAsync(string repoId, Exception fault)
    {
        try
        {
            var job = _grainFactory.GetGrain<IRepoIndexJobGrain>(repoId);
            var progress = await job.GetProgressAsync().ConfigureAwait(false);
            if (progress.Status == RepoIndexStatus.Running && progress.Phase == RepoIndexPhase.Resetting)
            {
                await job.FailAsync($"Index reset failed: {fault.Message}").ConfigureAwait(false);
            }
        }
        catch (Exception recordFault)
        {
            _logger.LogError(recordFault, "Repo {RepoId}: could not record the index reset failure on the job.", repoId);
        }
    }

    private static Func<string, CancellationToken, Task<RepoContextIndexResetResult>> ResetOf(RepoContextStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        return store.ResetIndexAsync;
    }
}
