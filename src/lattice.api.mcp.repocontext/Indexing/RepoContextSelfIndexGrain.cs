using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The per-repository, reminder-anchored self-index grain. It is the single owner
/// of one repository's "reach and stay fully indexed" guarantee: it drives the
/// initial index on onboarding, and then runs a continuous, paged, low-cost
/// background scan of its own repository's structural file range that re-drives the
/// idempotent index whenever it finds an unembedded file or a failed prior run -
/// all without a client call.
/// <para>
/// <b>Cheap and bounded.</b> A grain-local timer ticks one bounded page of a
/// keys-only structural scan per tick, probing embedded presence in memory against
/// the repository's add-wins membership set (source identifiers only, never the
/// embeddings). The scan stops at the first missing file, so a repository with a
/// gap is detected without reading the rest of it. Between full scans the grain
/// idles behind a jittered cooldown, and the first tick after activation is itself
/// jittered, so many repositories' grains never all scan at the same instant.
/// </para>
/// <para>
/// <b>Durable and self-reactivating.</b> A one-minute keep-alive reminder keeps the
/// grain activated and re-fires it after a host restart, at which point the grain
/// re-arms its timer and resumes from the persisted checkpoint. The grain is armed
/// on repository add and torn down on removal, so exactly the live repositories
/// have a self-index scan. Arming the reminder is the onboarding commit point, so
/// a first pass interrupted before it settles is still healed here.
/// </para>
/// </summary>
internal sealed class RepoContextSelfIndexGrain(
    IGrainContext grainContext,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IRepoIndexRunner runner,
    RepoContextEmbeddingGapScanner gapScanner,
    IRepoIndexRunAuthority runAuthority,
    TimeProvider timeProvider,
    RepoContextIndexingOptions options,
    RepoContextAnnIndexScheduler annIndexScheduler,
    RepoContextIndexSourceGate sourceGate,
    ILogger<RepoContextSelfIndexGrain> logger,
    [PersistentState("repoContextSelfIndex", global::Orleans.Lattice.LatticeOptions.StorageProviderName)]
    IPersistentState<RepoContextSelfIndexState> state) : IRepoContextSelfIndexGrain, IRemindable, IGrainBase
{
    /// <summary>
    /// The keep-alive reminder name. Never rename it: a rename would orphan the
    /// reminders already registered for live repositories.
    /// </summary>
    private const string KeepaliveReminderName = "repo-context-self-index-keepalive";

    /// <summary>The base cooldown between completed scans of a clean (or just re-driven) repository.</summary>
    private static readonly TimeSpan ScanCooldown = TimeSpan.FromMinutes(5);

    /// <summary>The maximum extra random cooldown added on top of <see cref="ScanCooldown"/> to desync repositories.</summary>
    private static readonly TimeSpan ScanCooldownJitter = TimeSpan.FromMinutes(1);

    /// <summary>The maximum number of structural file keys inspected per tick.</summary>
    private const int PageSize = 512;

    IGrainContext IGrainBase.GrainContext => grainContext;

    private string RepoId => this.GetPrimaryKeyString();

    private IGrainTimer? _timer;

    /// <inheritdoc />
    public async Task<RepoIndexProgress> EnsureRunningAsync(RepoIndexJobRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        // Hub-and-spoke gate: a spoke serves the replicated index records for reads
        // but must never walk, reconcile, prune, or re-embed source-derived index
        // state, so two clusters can never race to mutate it. Its self-index pass is
        // inert - no keep-alive reminder, no scan timer, no runner pass - and it
        // returns a benign "nothing indexed here" snapshot. Only the hub indexes.
        if (!options.IndexingEnabled)
        {
            logger.LogInformation(
                "Repo {RepoId}: self-index is inert (spoke role); serving replicated reads only.", RepoId);
            return SpokeIdleProgress();
        }

        // Arm the durable backstop before starting the run so an interrupted first
        // pass is still healed by this grain's own scan: the keep-alive reactivates
        // the grain after a restart, and the timer drives the gap scan for the life
        // of this activation.
        await RegisterKeepaliveAsync().ConfigureAwait(true);
        ArmTimer();

        // Drive the initial (idempotent, single-flight) indexing pass and return its
        // snapshot. This is the single onboarding entry point: the runner funnels
        // every run - onboarding, resume, and self-index recovery - through one
        // credential-stamped, single-flight background pass, so this call is exactly
        // the run the self-index scan would otherwise re-drive. The source strategy
        // decides what content that pass sees: the mounted tree as configured, or -
        // for a git-sourced repository - the staged commit the configured ref
        // resolves to.
        var progress = await StartFromSourceAsync(request).ConfigureAwait(true);

        // Arm the approximate index's durable build coordinator for this repository
        // as soon as it has vectors worth indexing. The startup sweep would pick it
        // up on its next pass anyway; doing it here makes a freshly onboarded
        // repository converge on the pass that produced its vectors rather than on
        // the following sweep interval. Idempotent and non-fatal - a failure here
        // must never fail onboarding, and the sweep is the backstop.
        await TryArmAnnIndexAsync().ConfigureAwait(true);

        // Space the first periodic reconcile one interval past this onboarding pass:
        // onboarding already reconciled the whole tree, so an immediate reconcile
        // would be redundant. Persist the deadline so a restart mid-interval keeps
        // it rather than reconciling again on the first tick after reactivation.
        ScheduleNextReconcile(timeProvider.GetUtcNow().UtcTicks);
        await state.WriteStateAsync().ConfigureAwait(true);
        return progress;
    }

    /// <summary>
    /// The benign progress snapshot a spoke returns instead of driving an index
    /// pass: it asserts no indexing job runs on this cluster (source-derived index
    /// state arrives only by replication from the hub) without arming any timer,
    /// reminder, or runner.
    /// </summary>
    private RepoIndexProgress SpokeIdleProgress() => new()
    {
        RepoId = RepoId,
        Status = RepoIndexStatus.None,
        Phase = RepoIndexPhase.Pending,
    };

    /// <inheritdoc />
    public async Task StopAsync()
    {
        _timer?.Dispose();
        _timer = null;

        await UnregisterKeepaliveAsync().ConfigureAwait(true);
        await state.ClearStateAsync().ConfigureAwait(true);
        state.State = new RepoContextSelfIndexState();
        logger.LogInformation("Repo {RepoId}: self-index stopped and cleared.", RepoId);
        this.DeactivateOnIdle();
    }

    /// <inheritdoc />
    public Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName == KeepaliveReminderName)
        {
            // The keep-alive re-fired (typically the first beat after a host
            // restart reactivated this grain): make sure the scan timer is armed.
            ArmTimer();
        }

        return Task.CompletedTask;
    }

    private void ArmTimer()
    {
        // Defensive spoke gate: a spoke never arms the scan timer even if some other
        // path reaches here (e.g. a stray reminder), so its reconcile/prune/gap-scan
        // pass can never run. EnsureRunningAsync already short-circuits, and a spoke
        // never registers the keep-alive reminder, but the guard keeps the invariant
        // local to the one method that starts the timer.
        if (!options.IndexingEnabled)
        {
            return;
        }

        if (_timer is not null)
        {
            return;
        }

        // Jitter the first tick within one interval so a fleet of repositories
        // reactivated together by their keep-alive reminders does not all fire
        // their first scan at the same instant. The fixed period then keeps them
        // phase-shifted.
        var jitterMs = Random.Shared.Next(0, (int)options.TickInterval.TotalMilliseconds);
        var dueTime = options.TickInterval + TimeSpan.FromMilliseconds(jitterMs);
        _timer = this.RegisterGrainTimer(
            OnTickAsync, new GrainTimerCreationOptions(dueTime, options.TickInterval));
    }

    private async Task OnTickAsync(CancellationToken cancellationToken)
    {
        // Defensive spoke gate: the timer is never armed for a spoke, so a tick can
        // only reach here on a hub. The guard keeps the "a spoke mutates no index
        // state" invariant true even if a timer somehow survived a role change.
        if (!options.IndexingEnabled)
        {
            return;
        }

        try
        {
            // Stamp the same fixed run credential the background indexer uses onto
            // the ambient context for the scan, so the keys-only structural scan and
            // the membership read carry a subject the access gate can authorize. A
            // null authority leaves the ambient credential untouched (the in-process
            // default whose gate is not enabled).
            var credential = runAuthority.Resolve();
            if (credential is null)
            {
                await ScanStepAsync(cancellationToken).ConfigureAwait(true);
            }
            else
            {
                using var scope = LatticeCredentialContext.With(credential);
                await ScanStepAsync(cancellationToken).ConfigureAwait(true);
            }
        }
        catch (OperationCanceledException)
        {
            // The host is stopping; the keep-alive resumes the scan on the next
            // activation from the persisted checkpoint.
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Repo {RepoId}: self-index scan step failed (non-fatal); it will retry on the next tick.",
                RepoId);
        }
    }

    private async Task ScanStepAsync(CancellationToken cancellationToken)
    {
        var nowTicks = timeProvider.GetUtcNow().UtcTicks;
        var continuing = state.State.ResumeKey is not null;

        // The periodic content reconcile is checked before the gap-scan cooldown gate,
        // so a short reconcile interval yields near-continuous reconciles bounded only by
        // the tick cadence rather than being delayed behind the longer presence-scan
        // cooldown. A reconcile re-drives the whole idempotent index so on-disk edits and
        // deletions are picked up automatically; the gap scan only detects missing
        // embeddings. It is idempotent and single-flight, so a reconcile already in flight
        // is a no-op, and because each tick is a fresh grain turn re-driving on completion
        // polls for the prior run rather than recursing.
        if (!continuing && nowTicks >= state.State.NextReconcileAfterTicks)
        {
            var triggered = await TriggerReconcileAsync(cancellationToken).ConfigureAwait(true);
            if (triggered)
            {
                logger.LogInformation(
                    "Repo {RepoId}: self-index re-drove a periodic reconcile to pick up edits and deletions.",
                    RepoId);
            }

            ScheduleNextReconcile(nowTicks);
            EndScan(nowTicks);
            await state.WriteStateAsync().ConfigureAwait(true);
            return;
        }

        if (!continuing && nowTicks < state.State.NextSweepAfterTicks)
        {
            // Between scans and still cooling down: a cheap no-op tick.
            return;
        }

        // At the start of a fresh cycle, re-drive a repository whose last run
        // outright failed. A failure before any structural record was written leaves
        // nothing for the file scan to detect, so this status check - not the gap
        // scan - is what keeps a failed onboarding from being abandoned until the
        // repository is removed. The re-drive is idempotent and single-flight.
        if (!continuing)
        {
            var progress = await grainFactory
                .GetGrain<IRepoIndexJobGrain>(RepoId).GetProgressAsync().ConfigureAwait(true);
            if (progress.Status == RepoIndexStatus.Failed)
            {
                var triggered = await grainFactory
                    .GetGrain<IRepoIndexJobGrain>(RepoId).EnsureIndexedAsync().ConfigureAwait(true);
                if (triggered)
                {
                    logger.LogInformation(
                        "Repo {RepoId}: self-index re-drove a failed prior run.", RepoId);
                }

                EndScan(nowTicks);
                await state.WriteStateAsync().ConfigureAwait(true);
                return;
            }
        }

        // Probe coverage for exactly this page's files with a bounded point-read, so
        // no read in the sweep scales with the membership tree size (issue #1556).
        var page = await gapScanner
            .ScanFilePageAsync(RepoId, state.State.ResumeKey, PageSize, cancellationToken)
            .ConfigureAwait(true);

        if (page.GapFound)
        {
            // A file has no live embedding: re-drive the whole repository index. The
            // back-fill re-embeds every missing file, so one trigger heals all of
            // this repository's gaps. EnsureIndexedAsync is a no-op when a run is
            // already in flight.
            var triggered = await grainFactory
                .GetGrain<IRepoIndexJobGrain>(RepoId).EnsureIndexedAsync().ConfigureAwait(true);
            if (triggered)
            {
                logger.LogInformation(
                    "Repo {RepoId}: self-index found an unembedded file and re-drove indexing to back-fill it.",
                    RepoId);
            }

            EndScan(nowTicks);
        }
        else if (page.HasMore)
        {
            // No gap in this page but more files remain: checkpoint the resume key
            // so the next tick continues where this one stopped.
            state.State.ResumeKey = page.NextResumeKey;
        }
        else
        {
            // The whole file range is clean: nothing to heal this cycle.
            EndScan(nowTicks);
        }

        await state.WriteStateAsync().ConfigureAwait(true);
    }

    private void EndScan(long nowTicks)
    {
        // Space the next scan by the base cooldown plus a random jitter so many
        // repositories' scans stay desynchronised in steady state.
        var jitterTicks = (long)(Random.Shared.NextDouble() * ScanCooldownJitter.Ticks);
        state.State.ResumeKey = null;
        state.State.NextSweepAfterTicks = nowTicks + ScanCooldown.Ticks + jitterTicks;
    }

    private void ScheduleNextReconcile(long nowTicks)
    {
        // Space the next content reconcile by the base interval plus a random jitter
        // so many repositories' reconciles stay desynchronised in steady state. A
        // git-sourced repository uses its own refresh cadence: its reconcile is an
        // outbound fetch against a git host, so it is paced by the source's
        // configured interval rather than the shared mounted-walk interval.
        var interval = sourceGate.RefreshIntervalFor(RepoId, options.ReconcileInterval);
        var jitterTicks = (long)(Random.Shared.NextDouble() * options.ReconcileIntervalJitter.Ticks);
        state.State.NextReconcileAfterTicks = nowTicks + interval.Ticks + jitterTicks;
    }

    /// <summary>
    /// Re-drives one idempotent reconcile through whichever source strategy owns the
    /// repository. A mounted repository re-drives from its persisted request exactly
    /// as before; a git-sourced repository first re-fetches its configured ref, and
    /// only starts a run when the ref resolved to a commit it has not already
    /// indexed. A fetch that fails, or a ref that has not moved, triggers nothing, so
    /// the last-good index keeps serving untouched.
    /// </summary>
    private async Task<bool> TriggerReconcileAsync(CancellationToken cancellationToken)
    {
        var job = grainFactory.GetGrain<IRepoIndexJobGrain>(RepoId);
        if (!sourceGate.IsGitSourced(RepoId))
        {
            return await job.EnsureIndexedAsync().ConfigureAwait(true);
        }

        var persisted = await job.GetRequestAsync().ConfigureAwait(true);
        var seed = persisted ?? sourceGate.SeedRequestFor(RepoId);
        if (seed is null)
        {
            return false;
        }

        var preparation = await PrepareAsync(job, seed, cancellationToken).ConfigureAwait(true);
        if (preparation.Outcome != RepoContextSourceOutcome.Proceed)
        {
            return false;
        }

        await runner.StartIndexAsync(preparation.Request!).ConfigureAwait(true);
        return true;
    }

    /// <summary>
    /// Starts an index pass for <paramref name="request"/> through its source
    /// strategy. A preparation that is up to date or fails closed starts nothing and
    /// returns the repository's current snapshot, so the last-good index keeps
    /// serving and the caller sees the truth rather than a fabricated success.
    /// </summary>
    private async Task<RepoIndexProgress> StartFromSourceAsync(RepoIndexJobRequest request)
    {
        var job = grainFactory.GetGrain<IRepoIndexJobGrain>(RepoId);
        var preparation = await PrepareAsync(job, request, CancellationToken.None).ConfigureAwait(true);
        if (preparation.Outcome == RepoContextSourceOutcome.Proceed)
        {
            return await runner.StartIndexAsync(preparation.Request!).ConfigureAwait(true);
        }

        logger.LogInformation(
            "Repo {RepoId}: source preparation returned {Outcome} ({Reason}); no index pass was started.",
            RepoId, preparation.Outcome, preparation.FailureReason ?? preparation.CommitSha ?? "none");
        return await job.GetProgressAsync().ConfigureAwait(true);
    }

    /// <summary>
    /// Asks the source gate to prepare content for the next generation, supplying the
    /// commit SHA of the last generation that actually completed. Reading the anchor
    /// from a completed run - rather than from a separate persisted counter - is what
    /// makes a partial or failed run safe to repeat: its commit is never treated as
    /// indexed, so the next refresh re-stages and re-applies it.
    /// </summary>
    private async Task<RepoContextSourcePreparation> PrepareAsync(
        IRepoIndexJobGrain job, RepoIndexJobRequest request, CancellationToken cancellationToken)
    {
        string? lastIndexedCommitSha = null;
        if (sourceGate.IsGitSourced(RepoId))
        {
            var progress = await job.GetProgressAsync().ConfigureAwait(true);
            if (progress.Status == RepoIndexStatus.Completed)
            {
                var persisted = await job.GetRequestAsync().ConfigureAwait(true);
                lastIndexedCommitSha = persisted?.CommitSha;
            }
        }

        return await sourceGate.PrepareAsync(request, lastIndexedCommitSha, cancellationToken).ConfigureAwait(true);
    }

    /// <summary>
    /// Arms the approximate index's build coordinator for this repository, if the
    /// plane is scheduled at all. Never throws: indexing must not fail because a
    /// derived index could not be scheduled, and the periodic sweep re-arms it.
    /// </summary>
    private async Task TryArmAnnIndexAsync()
    {
        try
        {
            await annIndexScheduler.TryArmAsync(RepoId, CancellationToken.None).ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Repo {RepoId}: failed to arm the approximate-index build coordinator (non-fatal); the periodic "
                + "sweep will retry.",
                RepoId);
        }
    }

    private async Task RegisterKeepaliveAsync()
    {
        try
        {
            await reminderRegistry.RegisterOrUpdateReminder(
                callingGrainId: grainContext.GrainId,
                reminderName: KeepaliveReminderName,
                dueTime: TimeSpan.FromMinutes(1),
                period: TimeSpan.FromMinutes(1)).ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            // A reminder hiccup must not fail onboarding; the timer still drives the
            // scan for the life of this activation, only the restart-reactivation
            // backstop is degraded.
            logger.LogWarning(
                ex, "Repo {RepoId}: failed to register the self-index keep-alive reminder (non-fatal).", RepoId);
        }
    }

    private async Task UnregisterKeepaliveAsync()
    {
        try
        {
            var reminder = await reminderRegistry
                .GetReminder(grainContext.GrainId, KeepaliveReminderName).ConfigureAwait(true);
            if (reminder is not null)
            {
                await reminderRegistry
                    .UnregisterReminder(grainContext.GrainId, reminder).ConfigureAwait(true);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex, "Repo {RepoId}: failed to unregister the self-index keep-alive reminder (non-fatal).", RepoId);
        }
    }
}
