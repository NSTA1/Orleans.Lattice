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
    RepoContextAnnBuildSliceReporter sliceReporter,
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
    /// Progress reported by the previous build step of this activation, against
    /// which the next step is classified for
    /// <see cref="RepoContextAnnBuildSliceReporter"/>.
    /// <para>
    /// Activation-local, and correctly so on both counts. The coordinator is
    /// single-threaded and scoped to one repository and embedding space, so no
    /// other build can perturb it; and the counts it is compared against -
    /// <see cref="VectorIndexBuildProgress.SlicesDeadlinedWithoutProgress"/> in
    /// particular - are per index instance and reset with the handle, so carrying a
    /// baseline across activations would compare a fresh instance's counters
    /// against a retired one's and manufacture a spurious classification. The
    /// default baseline names phase <c>NotStarted</c> and zero of everything, which
    /// is exactly what a build that has not yet stepped holds.
    /// </para>
    /// </summary>
    private VectorIndexBuildProgress _previousProgress;

    /// <summary>
    /// Whether the tick currently executing has already recorded its step on the
    /// slice counter. Reset at the top of every tick and set the moment the step is
    /// classified, so the outer fault seam can tell a tick that threw BEFORE it was
    /// counted - which must be counted as faulted, since nothing else will count it
    /// at all - from one that threw AFTER, which has already been counted on the arm
    /// its progress earned and must not be counted twice.
    /// <para>
    /// A plain field is sufficient because a grain activation processes one turn at
    /// a time, so no two ticks are ever in flight together.
    /// </para>
    /// </summary>
    private bool _sliceRecordedThisTick;

    /// <summary>
    /// Whether the tick currently executing is inside, or has passed through, the
    /// call to the plane's build step. Reset at the top of every tick, set
    /// immediately before the step and cleared immediately after it returns, so the
    /// outer fault seam can tell a fault RAISED BY THE STEP - whose phase the probe
    /// below has placed - from one raised by the coordinator around it, which has no
    /// phase and must be reported as <see cref="RepoContextAnnBuildStepPhase.Coordinating"/>.
    /// <para>
    /// Without it the probe's last reading would be reused for a fault that happened
    /// after the step returned cleanly, attributing a coordinator-state write to the
    /// persist or a gate probe to the ingest read - which is precisely the
    /// misattribution the phase dimension was added to remove.
    /// </para>
    /// </summary>
    private bool _steppedThisTick;

    /// <summary>
    /// The box the plane writes its current phase into, allocated once per
    /// activation and reused. Safe to reuse because a grain activation processes one
    /// turn at a time and the tick resets it before every step, so it can never
    /// carry one tick's phase into another.
    /// </summary>
    private readonly RepoContextAnnBuildPhaseProbe _phaseProbe = new();

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
    /// <remarks>
    /// <para>
    /// <b>The third disjunct is what keeps a REBUILD running, and without it a
    /// rebuild runs at one slice per sweep (issue #3112).</b> <c>Converged</c> is a
    /// one-way latch - see the convergence block in <c>ProcessNextPhaseAsync</c>,
    /// where it closes once and never reopens - so it records that this plane has
    /// EVER converged, not that it is converged now. Scheduling needs the second
    /// question, and for a long time asked the first.
    /// </para>
    /// <para>
    /// With the latch closed, the predicate collapses to
    /// <c>!_advancedThisActivation</c>, which is set unconditionally the moment a
    /// step returns. So the first tick of an activation takes a step and every later
    /// tick in that activation stands the coordinator down. That is exactly right
    /// for a plane that really is Ready: one confirming step per activation, which
    /// is the seam the diagnostics refresh (#2712) and the partition self-heal
    /// (#2711) both ride on, and it is deliberately cheap because a converged plane
    /// only ever needs to catch up by a small delta.
    /// </para>
    /// <para>
    /// It is catastrophic when the index has been LOST and the plane is rebuilding
    /// from nothing, because the delta is then the whole corpus. Nothing clears the
    /// latch on an index load that comes back <c>fresh</c>, so the coordinator still
    /// believes it is in its cheap confirming mode and takes a single slice per
    /// activation - and the only thing that re-activates it is
    /// <see cref="RepoContextIndexingOptions.AnnSweepInterval"/>, fifteen minutes.
    /// Against the phase timer's two-second cadence that is a 450x throughput loss:
    /// the deployment this was found on held 2,924 of 94,928 vectors and was
    /// advancing at roughly five vectors a minute, a little over twelve days to
    /// finish, and it would have started over on the next index loss.
    /// </para>
    /// <para>
    /// Consulting the observed phase asks the question scheduling actually has. A
    /// plane that is not Ready has work outstanding whatever the latch remembers, so
    /// the coordinator keeps its timer and runs at the ordinary cadence until it
    /// gets there - which is precisely how a never-converged plane already behaves,
    /// so this introduces no new regime, it stops the latched plane being excluded
    /// from the existing one. The refused-corpus path still throttles itself through
    /// <c>_denialSkipTicks</c>, so a permanently denied coordinator does not spin.
    /// </para>
    /// <para>
    /// <b>The term can only ever ADD liveness.</b> It is a disjunct, and
    /// <see cref="_previousProgress"/> is activation-local and defaults to
    /// <c>NotStarted</c>, so it can never stand a coordinator down that today keeps
    /// running - a step that throws before it reports leaves the baseline at
    /// <c>NotStarted</c>, which is not Ready, and the coordinator retries exactly as
    /// it did. That monotonicity is why this cannot regress the stand-down
    /// behaviour the fault fixtures pin.
    /// </para>
    /// </remarks>
    protected override bool InProgress =>
        options.AnnIndexSchedulingEnabled
        && state.State.Space.IsSpecified
        && (!state.State.Converged
            || !_advancedThisActivation
            || _previousProgress.Phase != VectorIndexBuildPhase.Ready);

    private string GrainKey => Context.GrainId.Key.ToString() ?? string.Empty;

    /// <summary>
    /// The repository this coordinator builds for, resolved once and cached. A key
    /// that does not parse is used verbatim, so a coordinator addressed by an
    /// unexpected key still names a repository rather than silently building for an
    /// empty one.
    /// </summary>
    private string RepoId => _repoId ??=
        RepoContextAnnIndexKeys.TryParseBuildGrainKey(GrainKey, out var parsed, out _) ? parsed : GrainKey;

    /// <summary>
    /// The backing lattice tree reported on the phase-tick failure counter: the
    /// tree this coordinator's work actually lands in, per
    /// <see cref="LatticeRepoContextAnnBackingFactory"/>.
    /// <para>
    /// The base class default would use the grain key, which here is composite -
    /// <c>{repoId}/{spaceFingerprint}</c> - and so would tag every embedding space
    /// as a distinct "tree", which is both untrue and unaggregatable. The
    /// repository id is not a substitute: it is not a tree either, and emitting it
    /// through the derived tenant label would populate a <c>tenant</c> dimension
    /// shared with genuine tree coordinators with values that are not trees, so an
    /// operator filtering by tree name would silently miss these failures and could
    /// not tell the fabricated values from the real ones. A dimension that lies is
    /// worse than one that abstains. The repository and embedding space ride the
    /// accompanying log line, at a cardinality a log affords and a metric backend
    /// does not.
    /// </para>
    /// </summary>
    protected override string MetricsTreeId => RepoContextTrees.VectorIndex;

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
    public async Task StopAsync()
    {
        // Clear the durable intent BEFORE retiring the reminder, not after. The two
        // writes cannot be made atomic, so the ordering decides which way a crash
        // between them fails. Clearing first leaves, at worst, a registered
        // keep-alive over an unspecified space: the reminder fires once, the base
        // class evaluates InProgress as false (the space is no longer specified)
        // and unregisters it, so the stray converges to stopped on its own. The
        // opposite order leaves the intent durable with no reminder to drive it -
        // a coordinator that believes it has work and will never be woken to do
        // it, which nothing repairs.
        await state.ClearStateAsync().ConfigureAwait(true);
        state.State = new RepoContextAnnIndexBuildState();

        // Disposes the phase timer, withdraws from the phase-tick census,
        // unregisters the keep-alive reminder, and deactivates on idle. Reusing the
        // base class's terminal transition rather than open-coding those four steps
        // keeps a stop indistinguishable from a convergence as far as the
        // coordinator's own bookkeeping is concerned - in particular the census
        // withdrawal, which an open-coded stop would be easy to forget and whose
        // omission would leave this coordinator counted as live forever.
        await CompleteCoordinatorAsync().ConfigureAwait(true);
    }

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
        // THE FAULT SEAM IS THE WHOLE TICK, NOT ONE CALL INSIDE IT.
        //
        // It used to wrap the build step alone, which counted the one fault path
        // somebody had thought of and left every sibling site on the same tick
        // silent: resolving the run credential, completing the coordinator on the
        // not-in-progress path, probing the range-read gate, banking durable state,
        // and standing the coordinator down. All of those are grain or storage
        // calls, which is to say all of them are exactly the calls the run-12 census
        // found timing out. A counter that covers one of six throw sites reports a
        // clean zero for the other five, and a clean zero is read as "that did not
        // happen" (issue #2880).
        //
        // The flag is what keeps the partition total. A tick that throws BEFORE its
        // step is classified has no other record anywhere, so it is counted here; a
        // tick that throws AFTER has already been counted on the arm its progress
        // earned, and counting it again would put one tick on two arms and break the
        // denominator every other reading is taken against. So the seam covers every
        // site, and records at most once.
        //
        // The catch still does nothing except count. It does not log (the base class
        // already logs the exception it receives), does not swallow, does not back
        // off, and does not set _advancedThisActivation - a step that banked nothing
        // must not be able to satisfy the once-per-activation check that lets a
        // converged coordinator stand down. Rethrowing unchanged is what keeps the
        // timer's behaviour, the checkpoint-resume path, and the reminder lifetime
        // exactly as they were.
        _sliceRecordedThisTick = false;
        _steppedThisTick = false;
        _phaseProbe.Reset();

        // Priming sits HERE, above the try and above every early return inside it,
        // and not on the path that records a step. An arm minted only by the code
        // that also writes to it is not primed in the sense that matters: the zero a
        // reader sees was produced by machinery that never ran, which is
        // byte-identical to a measured absence (issue #2952). A coordinator whose
        // every tick dies resolving its credential must still publish this plane's
        // twenty-two arms at zero, or the epic cannot tell "no steps faulted on the
        // ingest read" from "nothing ever looked".
        //
        // Guarded on a stamped space because an unstamped coordinator has no plane
        // to name: EnsureBuildingAsync refuses an unspecified space, so the guard
        // excludes only the window before the first arming call, in which there is
        // genuinely no plane rather than a plane reading zero.
        if (state.State.Space.IsSpecified)
        {
            sliceReporter.EnsurePrimed(RepoId, state.State.Space);
        }

        try
        {
            await ProcessNextPhaseCoreAsync().ConfigureAwait(true);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            if (!_sliceRecordedThisTick)
            {
                // Classified HERE, at the site, rather than inside the reporter. The
                // reporter sees a cause; only this frame sees what the tick was
                // doing when it threw.
                //
                // The phase comes from the probe only when the tick actually reached
                // the plane. A tick that threw before the step - or after it
                // returned - is Coordinating, because the probe would otherwise hand
                // back the phase of a step that is not the one that failed. That is
                // the same double-counting hazard the flag above guards, one
                // dimension over.
                sliceReporter.RecordFaulted(
                    ClassifyBuildFault(ex),
                    RepoId,
                    state.State.Space,
                    _steppedThisTick ? _phaseProbe.Phase : RepoContextAnnBuildStepPhase.Coordinating);
            }

            throw;
        }
    }

    /// <summary>
    /// Which cause a faulted build step belongs to, resolved by walking the
    /// inner-exception chain outward-in.
    /// </summary>
    /// <param name="exception">The fault the tick raised.</param>
    /// <returns>The cause to record.</returns>
    /// <remarks>
    /// <para>
    /// <b>The order of the arms is load-bearing and two of them are subtypes of a
    /// later one.</b> <see cref="ScanPageStalledException"/> derives from
    /// <see cref="TimeoutException"/>, so testing the timeout arm first would
    /// silently swallow every leaf-chain stall into
    /// <see cref="RepoContextAnnBuildFaultCause.DependencyUnavailable"/> - and those
    /// have opposite remedies, one being a tree whose leaf cannot be materialised in
    /// a single grain call and the other a cluster that has not settled.
    /// <see cref="LeafProjectionStaleException"/> and
    /// <see cref="EmbeddingSpaceMismatchException"/> both derive from
    /// <see cref="InvalidOperationException"/>, which is why no arm matches that base
    /// type: an arm that did would capture whichever of the two is tested after it.
    /// </para>
    /// <para>
    /// Silo churn is matched by type name rather than by type, because one of the two
    /// runtime exception types is internal to Orleans. This is the same match
    /// <see cref="RepoContextAnnIndexSweepService"/> already uses for the same reason.
    /// </para>
    /// <para>
    /// An unrecognised type falls through every arm and lands on
    /// <see cref="RepoContextAnnBuildFaultCause.Unexpected"/>, which is the value
    /// that pages. Failing open onto a benign-looking arm would let a fault nobody
    /// has classified present as one that is already understood.
    /// </para>
    /// </remarks>
    internal static RepoContextAnnBuildFaultCause ClassifyBuildFault(Exception exception)
    {
        for (var e = exception; e is not null; e = e.InnerException)
        {
            if (e is ScanPageStalledException)
            {
                return RepoContextAnnBuildFaultCause.ScanPageStalled;
            }

            if (e is LeafProjectionStaleException)
            {
                return RepoContextAnnBuildFaultCause.ProjectionStale;
            }

            if (e is EmbeddingSpaceMismatchException or ArgumentException or NotSupportedException)
            {
                return RepoContextAnnBuildFaultCause.PlaneRejected;
            }

            if (e is TimeoutException or System.IO.IOException)
            {
                return RepoContextAnnBuildFaultCause.DependencyUnavailable;
            }

            var typeName = e.GetType().Name;
            if (typeName.Contains("SiloUnavailableException", StringComparison.Ordinal)
                || typeName.Contains("OrleansMessageRejectionException", StringComparison.Ordinal))
            {
                return RepoContextAnnBuildFaultCause.DependencyUnavailable;
            }
        }

        return RepoContextAnnBuildFaultCause.Unexpected;
    }

    /// <summary>
    /// One phase tick, under the fault seam <see cref="ProcessNextPhaseAsync"/>
    /// wraps round it.
    /// </summary>
    private async Task ProcessNextPhaseCoreAsync()
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

        // Exactly one bounded slice. An exception propagates to the fault seam in
        // ProcessNextPhaseAsync, which counts it and rethrows to the base class,
        // which logs it and leaves the timer running - and the keep-alive reminder
        // survives a process death - so a transient store fault costs one slice and
        // the build resumes from its checkpoint rather than being abandoned until
        // some query happens to re-arm it.
        //
        // THE FAULT IS COUNTED AND RETHROWN, NOT HANDLED, and the counting now
        // happens in the seam above rather than here. A step that throws is still a
        // step this coordinator took, and leaving it uncounted reintroduces the
        // precise ambiguity the slice counter exists to remove - one layer down.
        // A corpus read that CANNOT BE SERVED is a different condition from one that
        // is served and returns nothing: the empty read completes, so it reaches the
        // record below and lands on 'idle', whereas the faulting read never reaches
        // it at all. Under a record taken only after a completed step, a coordinator
        // faulting on every single tick totals zero, which reads as "this coordinator
        // is not stepping" and sends the next investigation to the scheduler while
        // the coordinator is in fact stepping hard and failing every time. Issue
        // #2737 measured exactly that: a vector-plane partition whose projection
        // checkpoint had fallen off the write-ahead log threw on every read, and the
        // designed self-heal that would have cleared it was refused by the access
        // gate, so the fault was permanent rather than transient.
        _steppedThisTick = true;

        // The in-flight gauge is opened HERE, around the await, and closed in a
        // finally. Every other instrument on this plane records after the step
        // RETURNS, so a step that never returns is invisible to all of them - the
        // blind spot issue #3130 had to close by hand out of a container log. The
        // token is handed to the probe as well so that a phase entered deep inside
        // the step (opening, ingesting, training, persisting) is attributed live
        // rather than only once the step ends.
        var stepToken = sliceReporter.BeginStep(repoId, space);
        _phaseProbe.AttachSink(sliceReporter, stepToken);

        VectorIndexBuildProgress progress;
        try
        {
            // PhaseTickToken, NOT CancellationToken.None - this is issue #3130's
            // item 2, and the None it replaces was load-bearing rather than
            // cosmetic.
            //
            // The whole chain below already honours a token: AdvanceAsync takes the
            // handle's turn gate with it, hands it to OpenAsync - the unbounded
            // phase this issue is named for - and on into the index's own build
            // step and catch-up. Every one of those awaits was uncancellable for
            // exactly one reason: the only caller supplied a token that can never
            // be cancelled. A step measured at 23 minutes on the acceptance rig
            // therefore held a non-reentrant grain turn with no way to abandon it,
            // and deactivation had to wait the full duration out.
            //
            // The None also made a decision one layer down UNREACHABLE.
            // DurableVectorIndex's SliceDeadlineSpent separates "the slice budget
            // expired, so bank the progress and report an incomplete slice" from
            // "the caller cancelled, so stop" by asking whether the caller's token
            // is cancelled. Against None that question has one answer forever, so
            // the branch could only ever resolve one way. Supplying a real token is
            // what makes that distinction exist in production rather than only in
            // the tests that pass one.
            progress = await registry
                .BuildStepAsync(repoId, space, _phaseProbe, PhaseTickToken)
                .ConfigureAwait(true);
        }
        finally
        {
            // Detached and retired on EVERY exit, fault included. A step left open
            // would climb for ever and report the coordinator's own bookkeeping leak
            // as a build wedge, which is the one reading this gauge must not invent.
            _phaseProbe.DetachSink();
            sliceReporter.EndStep(stepToken);
        }

        _steppedThisTick = false;

        _advancedThisActivation = true;

        // THE STEP IS COUNTED HERE, ABOVE EVERY EARLY RETURN BELOW, AND THAT
        // PLACEMENT IS THE POINT OF THE COUNTER.
        //
        // Every other instrument on this plane fires at a terminal moment: the
        // corpus counter at Ready, the partitioning counter on a finished plane,
        // the sweep counter at arming. So between an armed sweep and a Ready build
        // the plane emitted no series whatsoever, and a coordinator grinding
        // through slices that bank nothing was byte-identical in telemetry to a
        // coordinator that never took a step - every arm of every counter sitting
        // at its primed zero. That ambiguity blocked a real diagnosis on the
        // acceptance rig. Counting the step before any of the returns below is what
        // makes the total a record of work attempted rather than of work finished;
        // moving this call under the Ready check would restore exactly the
        // blindness it was added to remove.
        sliceReporter.RecordSlice(
            RepoContextAnnBuildSliceReporter.Classify(_previousProgress, progress),
            repoId,
            space,
            _phaseProbe.Phase);
        _sliceRecordedThisTick = true;
        _previousProgress = progress;

        // The discriminator this build is otherwise missing. A tick that banks
        // nothing looks identical from outside whether the slice budget merely
        // expired on a busy box or the source cannot deliver its first item at
        // all, and those need opposite remedies. Logging the starved case - and
        // only that case - names which one is happening while the run is still
        // observable, instead of leaving it to be argued about afterwards from a
        // corpus figure that reads 0 either way.
        //
        // THE FIGURE REPORTED IS THE ONE THE PREDICATE READ. It is the count of
        // empty deadlines SINCE THE BUILD LAST BANKED ANYTHING, not the lifetime
        // total beside it, and the two are only equal on a build that has never
        // advanced. Logging the lifetime total here is what made this warning read
        // as an indictment of a build that was advancing perfectly well: the
        // sentence claimed "all 22 slices banked nothing" and "the build is not
        // advancing" while the corpus figure in the very same line climbed a
        // hundred vectors a tick.
        if (progress.IsStarvedBySource)
        {
            Logger.LogWarning(
                "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} is starved by its "
                + "source: the last {Deadlined} ingest slice(s) stopped by the wall-clock budget banked nothing and "
                + "nothing has been banked since, so the build is bounded but is not advancing and holds "
                + "{VectorsIndexed} vector(s). The slice budget is being enforced, so this is a source read that "
                + "cannot complete rather than a budget that is too small; raising the budget will not help.",
                repoId,
                space.ModelId,
                space.Dimension,
                progress.EmptyDeadlinesSinceLastAdvance,
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
        //
        // On the tick's token for the same reason as the step above: this is a read
        // on the tick path, it can be re-driven from scratch on the next tick, and
        // nothing is banked until it returns - so abandoning it at teardown loses
        // no progress and blocks no shutdown.
        var coverage = progress.VectorsIndexed > 0
            ? RepoContextAnnBuildCorpusCoverage.NonEmpty
            : await corpusGateProbe.ProbeAsync(repoId, PhaseTickToken).ConfigureAwait(true);

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
            // DELIBERATELY CancellationToken.None, unlike the two tick-path calls
            // in ProcessNextPhaseCoreAsync, which now run on PhaseTickToken.
            //
            // The catch below is unfiltered and reports a swallowed fault as a
            // warning. Handing it a token that is cancelled at every teardown would
            // therefore emit "could not retire its superseded embedding-space index
            // prefixes" on every orderly shutdown - a warning about a failure that
            // did not happen, on the one path whose own doc says it must never be
            // able to disturb the coordinator standing down.
            //
            // Nothing is lost by letting it finish: this is idempotent housekeeping
            // over a handful of prefixes, its durable flag is written only after it
            // succeeds, and a pass that never runs is retried by the next one.
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
