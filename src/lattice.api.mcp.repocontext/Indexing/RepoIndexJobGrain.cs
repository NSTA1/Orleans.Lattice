using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The durable, reminder-anchored coordinator for one repository's indexing job.
/// <para>
/// <b>Decoupled from the request.</b> <see cref="StartAsync"/> records the job,
/// arms a resume reminder, hands the work to the background
/// <see cref="IRepoIndexRunner"/>, and returns at once - so the run outlives the
/// client call that triggered it and a dropped MCP stream never aborts an index.
/// </para>
/// <para>
/// <b>Resumable across restart.</b> The resume reminder is the durable trigger:
/// while a run is in flight it beats as a harmless single-flight heartbeat, and
/// after a host restart it re-fires, reactivates this grain, and re-enqueues the
/// persisted request. The bootstrap pass is idempotent, so the resumed run skips
/// the chunks that already committed and finishes the remainder.
/// </para>
/// </summary>
internal sealed class RepoIndexJobGrain(
    IGrainContext grainContext,
    [PersistentState("repoIndexJob", global::Orleans.Lattice.LatticeOptions.StorageProviderName)]
    IPersistentState<RepoIndexJobState> state,
    IReminderRegistry reminderRegistry,
    IRepoIndexRunner runner,
    TimeProvider timeProvider,
    ILogger<RepoIndexJobGrain> logger) : IRepoIndexJobGrain, IRemindable, IGrainBase
{
    /// <summary>
    /// The stable name of the resume reminder. Never rename it: a rename would
    /// orphan the reminders already registered for in-flight jobs.
    /// </summary>
    private const string ResumeReminderName = "repo-index-resume";

    /// <summary>
    /// The reminder cadence. Orleans reminder granularity is one minute, so the
    /// resume backstop fires at most once a minute; between ticks the run is driven
    /// by the runner, not the reminder.
    /// </summary>
    private static readonly TimeSpan ResumePeriod = TimeSpan.FromMinutes(1);

    IGrainContext IGrainBase.GrainContext => grainContext;

    private string RepoId => this.GetPrimaryKeyString();

    /// <inheritdoc />
    public async Task<RepoIndexProgress> StartAsync(RepoIndexJobRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        // A run is already in flight: re-attach idempotently. Re-enqueue as a
        // belt-and-braces resume (the runner is single-flight, so this is a no-op
        // when the task is genuinely still running) and report current progress.
        if (state.State.Status == RepoIndexStatus.Running)
        {
            runner.Enqueue(state.State.Request ?? request);
            return state.State.ToProgress(RepoId);
        }

        var now = timeProvider.GetUtcNow();
        state.State.Request = request;
        state.State.Status = RepoIndexStatus.Running;
        state.State.Phase = RepoIndexPhase.Pending;
        state.State.FilesScanned = 0;
        state.State.FilesAdded = 0;
        state.State.FilesUpdated = 0;
        state.State.FilesRemoved = 0;
        state.State.FilesUnchanged = 0;
        state.State.ChunksTotal = 0;
        state.State.ChunksCommitted = 0;
        state.State.FilesEmbedded = 0;
        state.State.FilesContentProjected = 0;
        state.State.Attempt += 1;
        state.State.StartedAt = now;
        state.State.UpdatedAt = now;
        state.State.CompletedAt = null;
        state.State.ElapsedMilliseconds = null;
        state.State.Error = null;
        await state.WriteStateAsync().ConfigureAwait(true);

        await RegisterResumeReminderAsync().ConfigureAwait(true);
        logger.LogInformation(
            "Repo {RepoId}: indexing job started (attempt {Attempt}); running asynchronously.",
            RepoId, state.State.Attempt);

        runner.Enqueue(request);
        return state.State.ToProgress(RepoId);
    }

    /// <inheritdoc />
    public async Task<bool> EnsureIndexedAsync()
    {
        // Never bootstrapped: there is no persisted request to re-drive, so the
        // self-heal sweep has nothing to do for this repository.
        if (state.State.Request is null)
        {
            return false;
        }

        // Already running: a run is in flight (or resuming), so the back-fill it
        // performs will close any embedding gap. Do not start a duplicate.
        if (state.State.Status == RepoIndexStatus.Running)
        {
            return false;
        }

        // A background reconcile or gap back-fill re-drives the persisted request
        // with pruning allowed: these run continuously, so the cheaper pruned walk
        // is the intended behaviour and its periodic full-sweep backstop catches the
        // in-place edits pruning cannot see. An explicit onboarding or re-bootstrap
        // comes through StartAsync with the request as built at the tool seam
        // (AllowPrune left false), so it stays a full, exact walk.
        await StartAsync(state.State.Request with { AllowPrune = true }).ConfigureAwait(true);
        return true;
    }

    /// <inheritdoc />
    public Task<RepoIndexProgress> GetProgressAsync() =>
        Task.FromResult(state.State.ToProgress(RepoId));

    /// <inheritdoc />
    public async Task ReportProgressAsync(RepoIndexProgressUpdate update)
    {
        // A late report for a job that was cleared or already settled is ignored,
        // so a straggling runner callback cannot revive a removed repository.
        if (state.State.Status != RepoIndexStatus.Running)
        {
            return;
        }

        Merge(update);
        state.State.UpdatedAt = timeProvider.GetUtcNow();
        await state.WriteStateAsync().ConfigureAwait(true);
    }

    /// <inheritdoc />
    public async Task CompleteAsync(RepoIndexProgressUpdate finalCounts, long elapsedMilliseconds)
    {
        Merge(finalCounts);
        var now = timeProvider.GetUtcNow();
        state.State.Status = RepoIndexStatus.Completed;
        state.State.Phase = RepoIndexPhase.Done;
        state.State.UpdatedAt = now;
        state.State.CompletedAt = now;
        state.State.ElapsedMilliseconds = elapsedMilliseconds;
        state.State.Error = null;
        await state.WriteStateAsync().ConfigureAwait(true);

        await UnregisterResumeReminderAsync().ConfigureAwait(true);
        logger.LogInformation(
            "Repo {RepoId}: indexing job completed in {Elapsed} ms ({Added} added, {Updated} updated, {Removed} removed, {Unchanged} unchanged, {Embedded} embedded, {ContentProjected} content projected).",
            RepoId, elapsedMilliseconds, state.State.FilesAdded, state.State.FilesUpdated,
            state.State.FilesRemoved, state.State.FilesUnchanged, state.State.FilesEmbedded, state.State.FilesContentProjected);
    }

    /// <inheritdoc />
    public async Task FailAsync(string error)
    {
        ArgumentNullException.ThrowIfNull(error);

        state.State.Status = RepoIndexStatus.Failed;
        state.State.UpdatedAt = timeProvider.GetUtcNow();
        state.State.Error = error;
        await state.WriteStateAsync().ConfigureAwait(true);

        // Clear the reminder so a logic failure does not retry forever; a fresh
        // client request restarts the job. (A host restart is a different case:
        // the runner never calls FailAsync for a shutdown cancellation, so the
        // reminder survives and resumes the run.)
        await UnregisterResumeReminderAsync().ConfigureAwait(true);
        logger.LogWarning("Repo {RepoId}: indexing job failed: {Error}", RepoId, error);
    }

    /// <inheritdoc />
    public async Task CancelAndClearAsync()
    {
        runner.Cancel(RepoId);
        await UnregisterResumeReminderAsync().ConfigureAwait(true);
        await state.ClearStateAsync().ConfigureAwait(true);
        state.State = new RepoIndexJobState();
        logger.LogInformation("Repo {RepoId}: indexing job cancelled and cleared.", RepoId);
        this.DeactivateOnIdle();
    }

    /// <inheritdoc />
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName != ResumeReminderName)
        {
            return;
        }

        if (state.State.Status != RepoIndexStatus.Running || state.State.Request is null)
        {
            // Nothing to resume: the job settled or was cleared. Drop the reminder.
            await UnregisterResumeReminderAsync().ConfigureAwait(true);
            return;
        }

        logger.LogInformation(
            "Repo {RepoId}: resume reminder fired; ensuring the index runner is active.", RepoId);
        runner.Enqueue(state.State.Request);
    }

    private void Merge(RepoIndexProgressUpdate update)
    {
        if (update.Phase is { } phase) state.State.Phase = phase;
        if (update.FilesScanned is { } scanned) state.State.FilesScanned = scanned;
        if (update.FilesAdded is { } added) state.State.FilesAdded = added;
        if (update.FilesUpdated is { } updated) state.State.FilesUpdated = updated;
        if (update.FilesRemoved is { } removed) state.State.FilesRemoved = removed;
        if (update.FilesUnchanged is { } unchanged) state.State.FilesUnchanged = unchanged;
        if (update.ChunksTotal is { } chunksTotal) state.State.ChunksTotal = chunksTotal;
        if (update.ChunksCommitted is { } chunksCommitted) state.State.ChunksCommitted = chunksCommitted;
        if (update.FilesEmbedded is { } embedded) state.State.FilesEmbedded = embedded;
        if (update.FilesContentProjected is { } contentProjected) state.State.FilesContentProjected = contentProjected;
    }

    private async Task RegisterResumeReminderAsync()
    {
        try
        {
            await reminderRegistry.RegisterOrUpdateReminder(
                callingGrainId: grainContext.GrainId,
                reminderName: ResumeReminderName,
                dueTime: ResumePeriod,
                period: ResumePeriod).ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            // A reminder hiccup must not fail the start; the run still proceeds via
            // the runner, only the restart-resume backstop is degraded.
            logger.LogWarning(ex,
                "Repo {RepoId}: failed to register the resume reminder (non-fatal).", RepoId);
        }
    }

    private async Task UnregisterResumeReminderAsync()
    {
        try
        {
            var reminder = await reminderRegistry
                .GetReminder(grainContext.GrainId, ResumeReminderName).ConfigureAwait(true);
            if (reminder is not null)
            {
                await reminderRegistry
                    .UnregisterReminder(grainContext.GrainId, reminder).ConfigureAwait(true);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Repo {RepoId}: failed to unregister the resume reminder (non-fatal).", RepoId);
        }
    }
}
