using Microsoft.Extensions.Logging;
using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex.Enrollment;

/// <summary>
/// The persistent-state object an <see cref="IndexedAttribute"/> parameter
/// receives: the grain's real state, wrapped so that reading it at activation
/// and writing it thereafter also keeps the grain's index entries in step.
/// </summary>
/// <remarks>
/// <para>
/// This is the hook point the whole enrolment path hangs from. It sits exactly
/// where the grain's durable state changes, which is why it can be both precise
/// (nothing happens on a call that does not write) and cheap (a write that does
/// not move an indexed property costs nothing beyond the projection it would
/// have to compute anyway to know that).
/// </para>
/// <para>
/// The ordering of a tracked write is load bearing, and it is this:
/// </para>
/// <list type="number">
/// <item>
/// <description>
/// project the already-mutated in-memory state, diff it against the projection
/// the index is known to hold, and stop here if nothing moved;
/// </description>
/// </item>
/// <item>
/// <description>
/// record the resulting batch durably in the outbox, <i>before</i> anything is
/// committed anywhere, so a failure from this point on is recoverable rather
/// than invisible;
/// </description>
/// </item>
/// <item>
/// <description>commit the grain's own state;</description>
/// </item>
/// <item>
/// <description>
/// apply the batch to the index tree and record the result as confirmed.
/// </description>
/// </item>
/// </list>
/// <para>
/// Step 4 failing is the case the outbox exists for: the grain's state is
/// already durable, so nothing about it is corrupted or rolled back, the caller
/// is told the index did not keep up, and the drain retries the recorded batch
/// under its original idempotency key until it lands. Step 2 failing is the
/// safer failure - nothing has been committed yet - and is surfaced before the
/// state write, so the grain and the index cannot diverge silently in either
/// direction.
/// </para>
/// <para>
/// Every plan is diffed against the last <i>confirmed</i> projection rather than
/// the last attempted one. That is what makes a write following a failed write
/// subsume it, so a stale outbox entry can only ever be replaced by one that
/// covers strictly more.
/// </para>
/// <para>
/// Every await on this path continues on the grain's own scheduler. The object
/// runs inside a grain activation, and the storage bridge underneath it asserts
/// the ambient Orleans runtime context, which a continuation resumed on the
/// thread pool would no longer be holding.
/// </para>
/// </remarks>
/// <typeparam name="TState">The grain-state type.</typeparam>
internal sealed class IndexedPersistentState<TState> : IPersistentState<TState>, ILifecycleObserver
{
    private static readonly GrainIndexEnroller<TState>[] NoEnrollers = [];
    private static readonly GrainIndexEnrollmentSlot[] NoSlots = [];

    private readonly IPersistentState<TState> _inner;
    private readonly IGrainContext _context;
    private readonly GrainIndexEnrollmentSet<TState> _set;
    private readonly ILogger _logger;

    private GrainIndexEnroller<TState>[] _enrollers = NoEnrollers;
    private GrainIndexEnrollmentSlot[] _slots = NoSlots;

    /// <summary>Initialises the wrapper.</summary>
    /// <param name="inner">The grain's real persistent state. Must not be <c>null</c>.</param>
    /// <param name="context">The activating grain's context. Must not be <c>null</c>.</param>
    /// <param name="set">The declared indexes over <typeparamref name="TState"/>. Must not be <c>null</c>.</param>
    /// <param name="logger">Reports enrolment that could not be completed. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public IndexedPersistentState(
        IPersistentState<TState> inner,
        IGrainContext context,
        GrainIndexEnrollmentSet<TState> set,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(set);
        ArgumentNullException.ThrowIfNull(logger);

        _inner = inner;
        _context = context;
        _set = set;
        _logger = logger;
    }

    /// <inheritdoc />
    public TState State
    {
        get => _inner.State;
        set => _inner.State = value;
    }

    /// <inheritdoc />
    public string? Etag => _inner.Etag;

    /// <inheritdoc />
    public bool RecordExists => _inner.RecordExists;

    /// <summary>
    /// The indexes this activation is tracked in. Empty when the grain
    /// implements no indexed grain interface, in which case every operation is a
    /// straight pass-through.
    /// </summary>
    internal GrainIndexEnroller<TState>[] Enrollers => _enrollers;

    /// <summary>The per-index enrolment state of this activation.</summary>
    internal GrainIndexEnrollmentSlot[] Slots => _slots;

    /// <summary>
    /// Attaches the wrapper to a grain's lifecycle so it enrols at activation,
    /// after the state has been read.
    /// </summary>
    /// <param name="lifecycle">The grain's lifecycle. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="lifecycle"/> is <c>null</c>.</exception>
    public void Participate(IGrainLifecycle lifecycle)
    {
        ArgumentNullException.ThrowIfNull(lifecycle);

        // Activate, not SetupState: the inner state is read during SetupState,
        // so anything earlier would project a state that has not been loaded.
        lifecycle.Subscribe(typeof(IndexedPersistentState<TState>).FullName!, GrainLifecycleStage.Activate, this);
    }

    /// <inheritdoc />
    public async Task OnStart(CancellationToken cancellationToken)
    {
        var enrollers = _set.For(_context.GrainInstance);
        if (enrollers.Length == 0)
            return;

        _enrollers = enrollers;
        _slots = new GrainIndexEnrollmentSlot[enrollers.Length];

        var grainId = _context.GrainId;
        for (var i = 0; i < enrollers.Length; i++)
        {
            // A key that cannot be encoded is a declaration error, not a
            // transient fault, so it fails the activation loudly rather than
            // leaving the grain quietly untracked.
            var grainKey = enrollers[i].EncodeKey(grainId);
            _slots[i] = new GrainIndexEnrollmentSlot(
                grainKey,
                GrainIndexProjection.Empty(grainKey),
                enrolled: false);
        }

        for (var i = 0; i < enrollers.Length; i++)
        {
            try
            {
                var confirmed = await enrollers[i]
                    .ReadBaselineAsync(_slots[i].GrainKey, cancellationToken)
                    .ConfigureAwait(true);

                if (confirmed is not null)
                {
                    _slots[i].Confirmed = confirmed;
                    _slots[i].Enrolled = true;
                }
            }
            catch (Exception ex)
            {
                // Losing the baseline is safe, only wasteful: the grain
                // re-writes entries it already owns. Failing the activation
                // instead would make an index-registry blip take the grain down.
                _logger.LogWarning(
                    ex,
                    "Grain index '{IndexName}' could not read the enrolment baseline for grain '{GrainKey}'; the grain will re-project its entries in full.",
                    enrollers[i].IndexName,
                    _slots[i].GrainKey);
            }
        }

        // A grain with nothing stored has nothing to enrol. Projecting the
        // default state here would file an entry for a grain that only exists
        // because somebody called it.
        if (!_inner.RecordExists)
            return;

        try
        {
            await PlanAsync(cancellationToken).ConfigureAwait(true);
            await CommitAsync(surfaceFailures: false, cancellationToken).ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Grain index enrolment failed during activation of grain '{GrainId}'; the recorded projection will be retried by the outbox drain.",
                grainId);
        }
    }

    /// <inheritdoc />
    public Task OnStop(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public Task ReadStateAsync() => ReadStateAsync(CancellationToken.None);

    /// <inheritdoc />
    public async Task ReadStateAsync(CancellationToken cancellationToken)
    {
        await _inner.ReadStateAsync(cancellationToken).ConfigureAwait(true);

        if (_enrollers.Length == 0 || !_inner.RecordExists)
            return;

        // A re-read can bring in a change another silo made, so the index is
        // reconciled against it. Failures are not surfaced: a read is not a
        // mutation, and the outbox entry already recorded is what converges it.
        try
        {
            await PlanAsync(cancellationToken).ConfigureAwait(true);
            await CommitAsync(surfaceFailures: false, cancellationToken).ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Grain index enrolment failed while refreshing grain '{GrainId}'; the recorded projection will be retried by the outbox drain.",
                _context.GrainId);
        }
    }

    /// <inheritdoc />
    public Task WriteStateAsync() => WriteStateAsync(CancellationToken.None);

    /// <inheritdoc />
    public async Task WriteStateAsync(CancellationToken cancellationToken)
    {
        if (_enrollers.Length == 0)
        {
            await _inner.WriteStateAsync(cancellationToken).ConfigureAwait(true);
            return;
        }

        await PlanAsync(cancellationToken).ConfigureAwait(true);
        await _inner.WriteStateAsync(cancellationToken).ConfigureAwait(true);
        await CommitAsync(surfaceFailures: true, cancellationToken).ConfigureAwait(true);
    }

    /// <inheritdoc />
    public Task ClearStateAsync() => ClearStateAsync(CancellationToken.None);

    /// <inheritdoc />
    public async Task ClearStateAsync(CancellationToken cancellationToken)
    {
        if (_enrollers.Length == 0)
        {
            await _inner.ClearStateAsync(cancellationToken).ConfigureAwait(true);
            return;
        }

        for (var i = 0; i < _enrollers.Length; i++)
        {
            _slots[i].Pending = null;
            var plan = GrainIndexUpdatePlan.Removing(_slots[i].Confirmed);
            if (!plan.IsEmpty)
            {
                _slots[i].Pending = await _enrollers[i]
                    .BeginAsync(plan, _slots[i].GrainKey, cancellationToken)
                    .ConfigureAwait(true);
            }
        }

        await _inner.ClearStateAsync(cancellationToken).ConfigureAwait(true);

        // If the removal batch fails, the failure surfaces and the outbox entry
        // survives, so the drain still retracts the entries. The seen marker is
        // then left behind carrying an empty projection, which costs a backfill
        // one skipped grain that has no state to index anyway.
        for (var i = 0; i < _enrollers.Length; i++)
        {
            if (_slots[i].Pending is { } pending)
            {
                await _enrollers[i].CommitAsync(pending, cancellationToken).ConfigureAwait(true);
                _slots[i].Pending = null;
            }

            await _enrollers[i].WithdrawAsync(_slots[i].GrainKey, cancellationToken).ConfigureAwait(true);
            _slots[i].Confirmed = GrainIndexProjection.Empty(_slots[i].GrainKey);
            _slots[i].Enrolled = false;
        }
    }

    /// <summary>
    /// Projects the current in-memory state against each index's confirmed
    /// baseline and records every non-empty batch in the outbox.
    /// </summary>
    private async Task PlanAsync(CancellationToken cancellationToken)
    {
        // The previous attempt's entries are dropped first and unconditionally.
        // Leaving one in place across a plan that produces nothing would have
        // the commit step re-apply a batch this write never planned.
        for (var i = 0; i < _enrollers.Length; i++)
            _slots[i].Pending = null;

        var state = _inner.State;
        if (state is null)
            return;

        for (var i = 0; i < _enrollers.Length; i++)
        {
            var plan = _enrollers[i].Plan(_slots[i].Confirmed, _slots[i].GrainKey, state);
            if (plan.IsEmpty)
                continue;

            _slots[i].Pending = await _enrollers[i]
                .BeginAsync(plan, _slots[i].GrainKey, cancellationToken)
                .ConfigureAwait(true);
        }
    }

    /// <summary>
    /// Applies each recorded batch and confirms it, or - in
    /// <see cref="GrainIndexProjectionMode.Eventual"/> mode - leaves it for the
    /// drain.
    /// </summary>
    /// <param name="surfaceFailures">
    /// Whether a failure is thrown to the caller. A mutation surfaces; an
    /// activation or a refresh logs instead, because failing those would turn an
    /// index outage into a grain outage. Either way the outbox entry survives
    /// and is retried.
    /// </param>
    /// <param name="cancellationToken">Cancels the batch.</param>
    private async Task CommitAsync(bool surfaceFailures, CancellationToken cancellationToken)
    {
        for (var i = 0; i < _enrollers.Length; i++)
        {
            try
            {
                if (_slots[i].Pending is { } pending)
                {
                    if (_enrollers[i].Mode == GrainIndexProjectionMode.Eventual)
                    {
                        // The entry is durable, so the drain will both apply it
                        // and write the seen marker. The confirmed baseline is
                        // deliberately not advanced: until the batch is known to
                        // have landed, the next plan has to subsume this one.
                        _slots[i].Enrolled = true;
                        continue;
                    }

                    await _enrollers[i].CommitAsync(pending, cancellationToken).ConfigureAwait(true);
                    _slots[i].Confirmed = pending.Plan.Projection;
                    _slots[i].Enrolled = true;
                    _slots[i].Pending = null;
                }
                else if (!_slots[i].Enrolled)
                {
                    // The grain contributes no entries but still has to be
                    // marked, or the backfill would revisit it on every pass.
                    await _enrollers[i]
                        .MarkEnrolledAsync(_slots[i].GrainKey, _slots[i].Confirmed, cancellationToken)
                        .ConfigureAwait(true);
                    _slots[i].Enrolled = true;
                }
            }
            catch (Exception ex) when (!surfaceFailures)
            {
                _logger.LogWarning(
                    ex,
                    "Grain index '{IndexName}' could not publish entries for grain '{GrainKey}'; the recorded projection will be retried by the outbox drain.",
                    _enrollers[i].IndexName,
                    _slots[i].GrainKey);
            }
        }
    }
}
