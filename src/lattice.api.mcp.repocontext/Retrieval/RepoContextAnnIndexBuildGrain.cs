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
/// <see cref="RepoContextAnnIndexRegistry.BuildStepAsync"/> does exactly one
/// bounded slice and reports where it got to, so the phase pump needs nothing but
/// to call it, and the turn is released between slices.
/// </para>
/// <para>
/// <b>What "bounded" has to mean here.</b> A slice used to be bounded only by a
/// vector count, and issue #2483 measured what that is worth against a source
/// that streams over grain calls: a 4,096-vector slice ran for twenty minutes
/// thirteen seconds, and the keep-alive reminder and every arming call queued
/// behind it for the whole of it. Releasing the turn between slices buys the
/// caller nothing when a slice is unbounded in time, so the slice now carries a
/// wall-clock budget as well
/// (<see cref="RepoContextAnnOptions.IngestSliceBudget"/>) and yields on
/// whichever bound it reaches first. The lesson generalises past this grain: a
/// work-count bound is a time bound only where the per-item cost is small and
/// predictable, which is precisely what a remote store of record does not
/// promise.
/// </para>
/// </summary>
internal sealed class RepoContextAnnIndexBuildGrain(
    IGrainContext grainContext,
    IReminderRegistry reminderRegistry,
    RepoContextAnnIndexRegistry registry,
    IRepoContextAnnBackingFactory backing,
    RepoContextIndexingOptions options,
    IRepoIndexRunAuthority runAuthority,
    IRepoContextCorpusGateProbe corpusGateProbe,
    RepoContextAnnBuildCorpusReporter corpusReporter,
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
    /// Consecutive uninterpretable corpus reads after which the coordinator
    /// concludes the host is refusing it, emits
    /// <see cref="RepoContextAnnBuildCorpusReporter.TerminalDenialInstrumentName"/>
    /// once, and parks on the capped retry interval.
    /// <para>
    /// Deliberately small. The states this separates are "a grant seeded slightly
    /// late" and "this deployment will never build an index", and five attempts
    /// spread over roughly a minute of backoff is far more than the first needs
    /// and far less than the second is worth waiting for.
    /// </para>
    /// </summary>
    internal const int TerminalDenialThreshold = 5;

    /// <summary>
    /// The ceiling on skipped phase ticks between retries of a refused corpus read.
    /// At the two-second phase period this parks a permanently-denied coordinator
    /// at one attempt every five minutes.
    /// <para>
    /// <b>Why a bound at all.</b> Refusing to converge means the coordinator stays
    /// alive, and staying alive on the phase cadence would be a retry every two
    /// seconds - a busy failure that never ends, because a denial is not a
    /// condition retrying can clear. A permanently refused host would spin at that
    /// rate indefinitely while banking nothing, and the container the approximate
    /// plane runs in is shared with the whole index pipeline, so that cost is
    /// charged to work that could otherwise proceed. Backing off keeps the
    /// coordinator loud and alive without being expensive, which is what lets a
    /// grant that seeds late still be picked up without a restart.
    /// </para>
    /// </summary>
    internal const int MaxDenialSkipTicks = 149;

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
    /// Phase ticks still to be skipped before the next attempt at a refused corpus
    /// read. Decremented before any work, so a backed-off tick costs a comparison
    /// rather than a build step and a probe.
    /// </summary>
    private int _denialSkipTicks;

    /// <summary>
    /// The length of the run of consecutive uninterpretable corpus reads in
    /// progress, or zero. Activation-local, and correctly so: the coordinator is
    /// single-threaded and scoped to exactly one repository and embedding space, so
    /// one repository's denial episode can never suppress another's announcement.
    /// </summary>
    private int _consecutiveDenials;

    /// <summary>Whether the current denial episode has already been announced.</summary>
    private bool _announcedDenial;

    /// <summary>Whether the current denial episode has already been counted as terminal.</summary>
    private bool _announcedTerminal;

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
        // Stamp the run authority's fixed identity onto the whole tick, so the
        // corpus stream the build step drives - and the store writes and prefix
        // reclamation that follow it - carry a subject the access gate can
        // authorize.
        //
        // Without this the build is anonymous, and it cannot inherit a credential
        // from whichever call armed it: the phase timer is deliberately re-armed
        // from the activation hook (see OnActivateCoreAsync) precisely so that
        // steady-state processing is decoupled from that call, so every step runs
        // on a timer turn rather than inside the arming call's scope. On a host
        // running a default-deny gate that does NOT surface as an error: a denied
        // range read is enforced by ResolveRangeReadFilterAsync as a reject-all key
        // filter (`static _ => false`), not an exception - so RepoContextVectorSource
        // streams an EMPTY corpus, cleanly. An empty corpus is refused nowhere
        // below: the count probe reports zero, the ingest completes on its first
        // step, training drops the partitioning and returns false rather than
        // throwing, and the build reaches Ready holding zero vectors.
        //
        // The credential is the first half of the remedy and is not the whole of
        // it. Stamping an identity stops the build being anonymous; it cannot stop
        // a denial being SILENT, and a grant that seeds after the first phase tick
        // - the startup service seeds on ApplicationStarted with backoff retry,
        // while this timer fires with dueTime zero - would still produce a denied
        // read on a correctly configured host. So the second half is below: a
        // completed build holding zero vectors is classified against the gate
        // before it is banked, counted on a series whose total advances on every
        // build, and refused convergence when the read did not happen. See
        // AdmitsConvergence, and issues #2426 and #2423.
        //
        // This is the same remedy RepoIndexRunner, RepoContextSelfIndexGrain,
        // RepoContextGitSourceArmingService, and RepoContextAnnIndexSweepService
        // already apply. A host that registers no authority resolves null and the
        // ambient credential is left untouched, so an in-process host with no
        // access gate is unaffected. See issue #2426.
        var credential = runAuthority.Resolve();
        using var credentialScope = credential is null
            ? null
            : LatticeCredentialContext.With(credential);

        if (!InProgress)
        {
            await CompleteCoordinatorAsync().ConfigureAwait(true);
            return;
        }

        // Denial backoff. A refused corpus read leaves this coordinator alive on
        // purpose - see AdmitsConvergence - and alive on the two-second phase
        // cadence would be a retry every two seconds for as long as the host keeps
        // refusing. Skipped ticks cost a comparison and take no step and no probe,
        // so a permanently-denied coordinator settles at one attempt every five
        // minutes while still being able to pick up a grant that seeds late.
        if (_denialSkipTicks > 0)
        {
            _denialSkipTicks--;
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

        // The discriminator this build is otherwise missing. A tick that banks
        // nothing looks identical from outside whether the slice budget merely
        // expired on a busy box or the source cannot deliver its first item at
        // all, and those need opposite remedies. Logging the starved case - and
        // only that case - names which one is happening while the run is still
        // observable, instead of leaving it to be argued about afterwards from a
        // corpus figure that reads 0 either way.
        if (progress.IsStarvedBySource)
        {
            Logger.LogWarning(
                "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} is starved by its "
                + "source: all {Deadlined} ingest slice(s) stopped by the wall-clock budget banked nothing, so the "
                + "build is bounded but is not advancing and holds {VectorsIndexed} vector(s). The slice budget is "
                + "being enforced, so this is a source read that cannot complete rather than a budget that is too "
                + "small; raising the budget will not help.",
                repoId,
                space.ModelId,
                space.Dimension,
                progress.SlicesDeadlined,
                progress.VectorsIndexed);
        }

        if (progress.Phase != VectorIndexBuildPhase.Ready)
        {
            return;
        }

        // The build has finished and is about to be banked, which is exactly the
        // moment ILattice.GetRangeReadGateCoverageAsync names for itself: a range
        // read came back empty and the caller is about to act on that emptiness. A
        // build holding vectors needs no probe, so the ordinary path costs nothing.
        var coverage = progress.VectorsIndexed > 0
            ? RepoContextAnnBuildCorpusCoverage.NonEmpty
            : await corpusGateProbe.ProbeAsync(repoId, CancellationToken.None).ConfigureAwait(true);

        // Counted before it is acted on, and counted on every completed build
        // including the ordinary non-empty ones, so the total is a denominator and
        // a zero on coverage=denied is a measured absence rather than silence.
        corpusReporter.RecordCoverage(coverage);

        if (!AdmitsConvergence(coverage, repoId, space))
        {
            return;
        }

        // THE LATCH AND THE DIAGNOSTICS ARE TWO DIFFERENT RECORDS, AND ONLY ONE OF
        // THEM LATCHES.
        //
        // Converged governs scheduling - see InProgress - and is correctly one-way:
        // it closes once and never reopens. The two counters beside it are
        // diagnostics with no reader in this assembly, and their entire purpose is
        // to be read by a human after the fact. Writing them under the latch froze
        // them at whatever the FIRST converged build observed, so a plane that
        // later healed still described the build that preceded the heal - which is
        // precisely the field an operator would consult to confirm the heal worked.
        // See issue #2712, and #2711 for the heal this exists to make observable.
        //
        // WHY THIS IS NOT SIMPLY UNCONDITIONAL. The refresh sits BELOW the
        // AdmitsConvergence early return above, and must stay there. A Denied read
        // did not happen at all and an unterminated Unknown one cannot say whether
        // it did, so the corpus behind either is unknown rather than empty.
        // Refreshing on one would overwrite a corroborated count with an
        // uncorroborated zero - fail-open into silence, which is the hazard #2426
        // exists to remove and which this fix must not reintroduce by the back
        // door. Convergence is admitted first; only then does the record move.
        //
        // The change test is what keeps a coordinator that has settled from writing
        // durable state on every activation merely to rewrite the same two numbers.
        var firstConvergence = !state.State.Converged;
        var previousVectorsIndexed = state.State.VectorsIndexed;
        var previousPartitionsTotal = state.State.PartitionsTotal;
        var diagnosticsChanged =
            previousVectorsIndexed != progress.VectorsIndexed
            || previousPartitionsTotal != progress.PartitionsTotal;

        if (firstConvergence || diagnosticsChanged)
        {
            state.State.Converged = true;
            state.State.VectorsIndexed = progress.VectorsIndexed;
            state.State.PartitionsTotal = progress.PartitionsTotal;
            await state.WriteStateAsync().ConfigureAwait(true);
        }

        if (firstConvergence)
        {
            // Partitions are reported beside the vector count because the vector
            // count alone cannot distinguish the two ways of reaching Ready. Zero
            // partitions is a completed build serving exact exhaustive answers,
            // not a failure, and saying so here is what keeps a later reader from
            // inferring an approximate plane that was never trained.
            Logger.LogInformation(
                "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} reached Ready "
                + "holding {VectorsIndexed} vectors across {Partitions} partitions (restored from durable "
                + "state: {Restored}); the build coordinator is standing down.",
                repoId,
                space.ModelId,
                space.Dimension,
                progress.VectorsIndexed,
                progress.PartitionsTotal,
                progress.RestoredFromDurableState);
        }
        else if (diagnosticsChanged)
        {
            // A record that quietly becomes correct is strictly weaker than one
            // whose transition is observable: an operator reading a correct value
            // still cannot tell a plane that healed from one that was never broken.
            // This names the transition, and only the transition - it is emitted
            // solely when the counters actually move, so a settled coordinator is
            // silent however long it runs.
            Logger.LogInformation(
                "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} has moved since "
                + "it converged and its durable record has been refreshed: {PreviousVectors} vector(s) across "
                + "{PreviousPartitions} partition(s) is now {VectorsIndexed} vector(s) across {Partitions} "
                + "partition(s).",
                repoId,
                space.ModelId,
                space.Dimension,
                previousVectorsIndexed,
                previousPartitionsTotal,
                progress.VectorsIndexed,
                progress.PartitionsTotal);
        }

        // STRICTLY AFTER Ready. Until the replacement index can answer, the space it
        // replaces is the only thing a failed re-embed could fall back to, so
        // retiring it any earlier would trade a bounded storage cost for a window
        // with no usable index at all.
        await ReclaimSupersededSpacesAsync(repoId, space).ConfigureAwait(true);

        await CompleteCoordinatorAsync().ConfigureAwait(true);
    }

    /// <summary>
    /// Decides whether a completed build may be banked as converged, given how much
    /// of its corpus the access gate admitted, and drives the denial episode's
    /// backoff, announcement and terminal signal.
    /// <para>
    /// <b>The rule, and why the four classes are not treated alike.</b>
    /// </para>
    /// <list type="bullet">
    /// <item><description><c>NonEmpty</c> and <c>Unrestricted</c> converge. The
    /// read succeeded; an honest empty repository is a legitimate converged
    /// state and must stay one, or a fresh deployment would never settle.</description></item>
    /// <item><description><c>Filtered</c> converges. The authority resolved
    /// correctly and the gate legitimately returned a subset, so this is a
    /// complete and correct read of what the caller may see. Refusing here would
    /// permanently wedge any host that legitimately restricts content, which is a
    /// far larger harm than the one being prevented. Converge on a known subset;
    /// never on an unknown.</description></item>
    /// <item><description><c>Denied</c> never converges. The read did not happen,
    /// so the store's contents are unknown rather than empty, and banking
    /// <c>Converged</c> on it is precisely the fail-open-into-silence issue #2426
    /// exists to remove - the coordinator would stand down permanently on an index
    /// it never built, and nothing would re-drive it.</description></item>
    /// <item><description><c>Unknown</c> withholds convergence until the episode is
    /// terminal, then converges. The probe is a diagnostic on a path that has
    /// already finished its work; letting a probe that cannot answer withhold
    /// convergence forever would let the observability mechanism wedge the thing it
    /// observes, which inverts the blast radius. Bounded and loud beats
    /// unbounded and safe-looking: the terminal counter makes the outcome
    /// visible.</description></item>
    /// </list>
    /// </summary>
    /// <param name="coverage">How much of the vector prefix the gate admitted.</param>
    /// <param name="repoId">The repository, for the announcement.</param>
    /// <param name="space">The embedding space, for the announcement.</param>
    /// <returns><see langword="true"/> when the build may be banked as converged.</returns>
    private bool AdmitsConvergence(
        RepoContextAnnBuildCorpusCoverage coverage, string repoId, EmbeddingSpaceTag space)
    {
        if (coverage is RepoContextAnnBuildCorpusCoverage.NonEmpty
            or RepoContextAnnBuildCorpusCoverage.Unrestricted
            or RepoContextAnnBuildCorpusCoverage.Filtered)
        {
            if (_consecutiveDenials > 0)
            {
                // Closes the episode the warning opened. An operator who saw the
                // denial needs its end more than they need another steady-state
                // line, and without this the log would leave a resolved episode
                // looking open forever.
                Logger.LogInformation(
                    "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} read its "
                    + "corpus successfully after {Denials} consecutive uninterpretable read(s); the build is "
                    + "proceeding and {Instrument} will stop advancing on the denied arm.",
                    repoId,
                    space.ModelId,
                    space.Dimension,
                    _consecutiveDenials,
                    RepoContextAnnBuildCorpusReporter.CorpusInstrumentName);
            }

            _consecutiveDenials = 0;
            _denialSkipTicks = 0;
            _announcedDenial = false;
            _announcedTerminal = false;
            return true;
        }

        _consecutiveDenials++;
        _denialSkipTicks = ComputeDenialSkipTicks(_consecutiveDenials);
        var terminal = _consecutiveDenials >= TerminalDenialThreshold;

        if (!_announcedDenial)
        {
            // Once per episode, not once per tick. An unconditional line at the
            // two-second phase period would be 43,200 lines a day, which is how a
            // real signal gets tuned out - the same announce-once-then-count
            // discipline RepoContextAnnIndexSweepReporter established.
            _announcedDenial = true;
            Logger.LogWarning(
                "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} completed "
                + "holding zero vectors and the access gate reports coverage={Coverage} for its vector prefix, so "
                + "the corpus read cannot be interpreted as an empty repository. The build will NOT be recorded "
                + "as converged and the coordinator is backing off rather than standing down. Counted on "
                + "{Instrument}.",
                repoId,
                space.ModelId,
                space.Dimension,
                RepoContextAnnBuildCorpusReporter.DescribeCoverage(coverage),
                RepoContextAnnBuildCorpusReporter.CorpusInstrumentName);
        }

        if (terminal && !_announcedTerminal)
        {
            _announcedTerminal = true;
            corpusReporter.RecordTerminalDenial();
            Logger.LogError(
                "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} has observed "
                + "{Denials} consecutive uninterpretable corpus reads (coverage={Coverage}) and is parking on the "
                + "capped retry interval. The approximate plane will not build for this repository until the "
                + "host's access gate admits the vector prefix to the build's run authority; every semantic query "
                + "falls back to an exact scan until then. Counted on {Instrument}.",
                repoId,
                space.ModelId,
                space.Dimension,
                _consecutiveDenials,
                RepoContextAnnBuildCorpusReporter.DescribeCoverage(coverage),
                RepoContextAnnBuildCorpusReporter.TerminalDenialInstrumentName);
        }

        return coverage == RepoContextAnnBuildCorpusCoverage.Unknown && terminal;
    }

    /// <summary>
    /// The number of phase ticks to skip before the next attempt, doubling with the
    /// length of the denial run and capped at <see cref="MaxDenialSkipTicks"/>.
    /// </summary>
    /// <param name="consecutiveDenials">The length of the run so far, one or more.</param>
    /// <returns>Ticks to skip: 1, 3, 7, 15, 31, 63, 127, then the cap.</returns>
    internal static int ComputeDenialSkipTicks(int consecutiveDenials)
    {
        var shift = Math.Clamp(consecutiveDenials, 1, 8);
        return Math.Min((1 << shift) - 1, MaxDenialSkipTicks);
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
