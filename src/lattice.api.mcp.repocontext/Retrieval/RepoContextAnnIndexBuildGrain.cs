using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Vector.Persistence;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The reminder-anchored coordinator that drives one <c>(repository, embedding
/// space)</c> approximate index to <c>Ready</c> and then retires the sibling
/// spaces the repository has abandoned.
/// <para>
/// <b>Why a coordinator rather than a task.</b> The build was previously armed by
/// a declining query through a fire-and-forget <c>Task.Run</c>, which put the work
/// that makes queries fast behind a query: it died with the process with nothing
/// to resume it, it left the first query after a restart both paying the
/// un-indexed cost and being the trigger, it sat outside the Orleans lifecycle,
/// and a repository nobody queried never indexed itself at all. Deriving
/// <see cref="CoordinatorGrain{TSelf}"/> replaces every one of those: the
/// keep-alive reminder reactivates the grain after a silo restart while work
/// remains, the phase timer is re-armed from the activation hook rather than from
/// whichever call happened to activate the grain, and the single-threaded
/// activation is what keeps two builds off one index - so the registry needs no
/// dedupe flag and no in-place retry loop, because the reminder <i>is</i> the
/// retry and a durable one.
/// </para>
/// <para>
/// <b>One bounded step per tick.</b>
/// <see cref="RepoContextAnnIndexRegistry.BuildStepAsync"/> already does exactly
/// one bounded slice and reports where it got to, so the phase pump needs nothing
/// but to call it; the turn is released between slices, so a query arriving
/// mid-build is answered by the exact scan immediately rather than queueing behind
/// the build.
/// </para>
/// </summary>
internal sealed class RepoContextAnnIndexBuildGrain(
    IGrainContext grainContext,
    IReminderRegistry reminderRegistry,
    RepoContextAnnIndexRegistry registry,
    IRepoContextAnnBackingFactory backing,
    RepoContextIndexingOptions options,
    ILogger<RepoContextAnnIndexBuildGrain> logger,
    [PersistentState("repoContextAnnIndexBuild", global::Orleans.Lattice.LatticeOptions.StorageProviderName)]
    IPersistentState<RepoContextAnnIndexBuildState> state)
    : CoordinatorGrain<RepoContextAnnIndexBuildGrain>(grainContext, reminderRegistry, logger),
      IRepoContextAnnIndexBuildGrain
{
    /// <summary>
    /// The keep-alive reminder name. Never rename it: a rename would orphan the
    /// reminders already registered for live repositories, and an orphaned
    /// keep-alive is a coordinator that reactivates forever with nothing to do.
    /// </summary>
    private const string KeepaliveReminder = "repo-context-ann-index-build-keepalive";

    /// <summary>
    /// Whether this activation has completed at least one build step. It is what
    /// makes a converged coordinator still do a single pass when it is reactivated:
    /// the durable index is shared, but the in-memory index the registry serves
    /// from is per process, so an activation that skipped the step would leave the
    /// registry with no open handle and hand the next query the very cost this
    /// grain exists to have already paid.
    /// </summary>
    private bool _advancedThisActivation;

    /// <summary>
    /// The repository this coordinator builds for, parsed once from the grain key.
    /// The key is immutable for the life of the activation, so re-splitting it on
    /// every phase tick would allocate three strings a tick for the whole build and
    /// tell us nothing new.
    /// </summary>
    private string? _repoId;

    /// <inheritdoc />
    protected override string KeepaliveReminderName => KeepaliveReminder;

    /// <inheritdoc />
    protected override bool InProgress =>
        options.AnnIndexSchedulingEnabled
        && state.State.Space.IsSpecified
        && (!state.State.Converged || !_advancedThisActivation);

    private string GrainKey => Context.GrainId.Key.ToString() ?? string.Empty;

    /// <summary>
    /// The repository this coordinator builds for, resolved once and cached. A key
    /// that does not parse is used verbatim, so a coordinator addressed by an
    /// unexpected key still names a repository rather than silently building for an
    /// empty one.
    /// </summary>
    private string RepoId => _repoId ??=
        RepoContextAnnIndexKeys.TryParseBuildGrainKey(GrainKey, out var parsed, out _) ? parsed : GrainKey;

    /// <inheritdoc />
    public async Task EnsureBuildingAsync(EmbeddingSpaceTag space)
    {
        if (!space.IsSpecified)
        {
            throw new ArgumentException(
                "The embedding space to build an approximate index for must carry a model id and a positive "
                + "dimension.",
                nameof(space));
        }

        if (!RepoContextAnnIndexKeys.TryParseBuildGrainKey(GrainKey, out _, out var fingerprint)
            || !string.Equals(fingerprint, RepoContextAnnIndexKeys.SpaceFingerprint(space), StringComparison.Ordinal))
        {
            // The key is the identity. A caller that addressed one pair and asked
            // for another's space would have this coordinator build an index under
            // a prefix it does not own, which the index's own recovery path would
            // then be free to range-delete.
            throw new ArgumentException(
                $"The embedding space does not match the coordinator key '{GrainKey}'. A build coordinator is "
                + "addressed by RepoContextAnnIndexKeys.BuildGrainKey for exactly the pair it builds.",
                nameof(space));
        }

        if (!options.AnnIndexSchedulingEnabled)
        {
            // The switch is off, so nothing is scheduled and nothing is torn down
            // here: an already-registered keep-alive is unregistered by the base
            // class the next time it fires and finds no work outstanding.
            return;
        }

        if (!state.State.Space.IsSpecified)
        {
            state.State.Space = space;
            await state.WriteStateAsync().ConfigureAwait(true);
        }

        // Idempotent: RegisterOrUpdateReminder replaces the existing registration
        // and the phase timer is created at most once, so the startup sweep may call
        // this for every repository on every start.
        await StartCoordinatorAsync().ConfigureAwait(true);
    }

    /// <inheritdoc />
    public Task<bool> IsConvergedAsync() => Task.FromResult(state.State.Converged);

    /// <inheritdoc />
    protected override Task OnActivateCoreAsync(CancellationToken cancellationToken)
    {
        // Perpetual-coordinator override: re-arm the pump from the activation hook
        // so steady-state processing is decoupled from whichever call activated the
        // grain - a keep-alive reminder after a silo restart, most of the time -
        // and can never be starved by that call.
        if (InProgress)
        {
            StartPhaseTimer();
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    protected internal override async Task ProcessNextPhaseAsync()
    {
        if (!InProgress)
        {
            await CompleteCoordinatorAsync().ConfigureAwait(true);
            return;
        }

        var repoId = RepoId;
        var space = state.State.Space;

        // Exactly one bounded slice. An exception propagates to the base class,
        // which logs it and leaves the timer running - and the keep-alive reminder
        // survives a process death - so a transient store fault costs one slice and
        // the build resumes from its checkpoint rather than being abandoned until
        // some query happens to re-arm it.
        var progress = await registry
            .BuildStepAsync(repoId, space, CancellationToken.None)
            .ConfigureAwait(true);

        _advancedThisActivation = true;

        if (progress.Phase != VectorIndexBuildPhase.Ready)
        {
            return;
        }

        if (!state.State.Converged)
        {
            state.State.Converged = true;
            state.State.VectorsIndexed = progress.VectorsIndexed;
            await state.WriteStateAsync().ConfigureAwait(true);

            Logger.LogInformation(
                "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} reached Ready "
                + "holding {VectorsIndexed} vectors (restored from durable state: {Restored}); the build "
                + "coordinator is standing down.",
                repoId,
                space.ModelId,
                space.Dimension,
                progress.VectorsIndexed,
                progress.RestoredFromDurableState);
        }

        // STRICTLY AFTER Ready. Until the replacement index can answer, the space it
        // replaces is the only thing a failed re-embed could fall back to, so
        // retiring it any earlier would trade a bounded storage cost for a window
        // with no usable index at all.
        await ReclaimSupersededSpacesAsync(repoId, space).ConfigureAwait(true);

        await CompleteCoordinatorAsync().ConfigureAwait(true);
    }

    /// <summary>
    /// Retires the sibling index prefixes of this repository whose embedding-space
    /// fingerprint is not the live one. A fault is logged and swallowed with the
    /// persisted flag left unset, so the next pass retries: the reclamation is pure
    /// housekeeping and must never be able to stop a converged index standing down.
    /// </summary>
    private async Task ReclaimSupersededSpacesAsync(string repoId, EmbeddingSpaceTag space)
    {
        if (!options.AnnIndexReclamation || state.State.Reclaimed)
        {
            return;
        }

        try
        {
            var retired = await backing
                .ReclaimSupersededSpacesAsync(repoId, space, CancellationToken.None)
                .ConfigureAwait(true);

            state.State.Reclaimed = true;
            await state.WriteStateAsync().ConfigureAwait(true);

            if (retired > 0)
            {
                Logger.LogInformation(
                    "Repository-context approximate index for {RepoId} retired {Retired} superseded embedding-space "
                    + "index prefix(es) now that the live space {ModelId}/{Dimension} is Ready.",
                    repoId,
                    retired,
                    space.ModelId,
                    space.Dimension);
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(
                ex,
                "Repository-context approximate index for {RepoId} could not retire its superseded embedding-space "
                + "index prefixes; the live index is unaffected and the sweep will be retried.",
                repoId);
        }
    }
}
