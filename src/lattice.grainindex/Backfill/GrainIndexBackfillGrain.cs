using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.GrainIndex.Enrollment;
using Orleans.Lattice.GrainIndex.Registry;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.GrainIndex.Backfill;

/// <summary>
/// The reminder-driven background backfill of one grain index.
/// </summary>
/// <remarks>
/// <para>
/// A pass takes at most <see cref="GrainIndexOptions.BackfillBatchSize"/> keys
/// from the index's <see cref="IGrainKeySource"/>, drops the ones the index
/// already records, activates the rest so they enrol themselves, and advances
/// one durable checkpoint. Batch size caps the work per tick and
/// <see cref="GrainIndexOptions.BackfillInterval"/> spaces the ticks, which
/// together are the crawl's rate limit.
/// </para>
/// <para>
/// Two schedulers, one job. The reminder is the durable one: it survives silo
/// restarts, fires on whichever silo holds the activation, and re-establishes
/// the crawl there. Orleans floors a reminder period at a minute, which is too
/// coarse to pace a crawl, so within an activation a grain timer runs the passes
/// at the configured interval and the reminder merely makes sure that timer
/// exists. That is the same division of labour the core tombstone-compaction
/// grain uses.
/// </para>
/// <para>
/// Nothing per grain is resolved per grain. The declaration, the key source, the
/// options snapshot, the batch buffer, and the already-indexed set are all held
/// for the activation or the pass, so the inner loop over a batch does no
/// lookups, no allocation, and no LINQ.
/// </para>
/// <para>
/// Idempotency is inherited rather than rebuilt: activating a grain the index is
/// already correct about produces an empty update plan, which writes nothing. A
/// pass that is interrupted after activating some of its grains but before its
/// checkpoint lands is therefore replayed for free.
/// </para>
/// </remarks>
internal sealed class GrainIndexBackfillGrain(
    IGrainContext context,
    IReminderRegistry reminderRegistry,
    IOptions<GrainIndexDeclarationOptions> declarations,
    IOptionsMonitor<GrainIndexOptions> indexOptions,
    IGrainIndexBackfillStore checkpoints,
    IGrainIndexRegistryStore registry,
    IGrainIndexEnrollmentStore enrollments,
    IGrainIndexBackfillActivator activator,
    IGrainKeySourceResolver keySources,
    TimeProvider timeProvider,
    ILogger<GrainIndexBackfillGrain> logger) : IGrainIndexBackfillGrain, IRemindable, IGrainBase
{
    /// <summary>
    /// The name of the durable heartbeat reminder. One per index, so unregistering
    /// it on completion leaves a finished index costing nothing.
    /// </summary>
    internal const string ReminderName = "grainindex-backfill";

    /// <summary>
    /// The heartbeat period. Orleans refuses a reminder period below a minute,
    /// and this is not the pacing knob in any case - it exists to re-establish
    /// the crawl after a restart, not to schedule its passes.
    /// </summary>
    internal static readonly TimeSpan ReminderPeriod = TimeSpan.FromMinutes(1);

    private static readonly string[] EmptyBuffer = [];

    private readonly HashSet<string> _alreadyIndexed = new(StringComparer.Ordinal);

    private string[] _buffer = EmptyBuffer;
    private GrainIndexBackfillCheckpoint? _checkpoint;
    private bool _checkpointLoaded;
    private IGrainIndexDefinition? _definition;
    private bool _definitionResolved;
    private string? _indexName;
    private IGrainKeySource? _keySource;
    private bool _keySourceResolved;
    private IGrainTimer? _timer;

    /// <summary>
    /// The index this crawl belongs to, which is the grain's key. Cached because
    /// rendering the key allocates a string, and a pass reads it several times.
    /// </summary>
    private string IndexName => _indexName ??= context.GrainId.Key.ToString()!;

    IGrainContext IGrainBase.GrainContext => context;

    /// <inheritdoc />
    public async Task<GrainIndexBackfillStatus> EnsureStartedAsync()
    {
        var record = await registry.ReadAsync(IndexName, CancellationToken.None).ConfigureAwait(true);
        if (record is null)
        {
            // The index has not been reconciled on this cluster, so there is no
            // declaration to crawl under and nothing to start.
            return CurrentStatus();
        }

        // Deliberately re-read rather than trusting this activation's copy. This
        // runs once per silo start, not per pass, and the durable checkpoint is
        // the authority on what the crawl owes - including when an operator or
        // another host has changed it since this activation last looked.
        var checkpoint = await LoadCheckpointAsync(forceReload: true).ConfigureAwait(true);
        var now = timeProvider.GetUtcNow();

        GrainIndexBackfillCheckpoint next;
        if (checkpoint is null)
        {
            if (!record.NeedsBackfill)
                return GrainIndexBackfillStatus.NotStarted(IndexName);

            next = GrainIndexBackfillCheckpoint.Start(record.Fingerprint, revisitsEnrolled: false, now);
        }
        else if (checkpoint.Fingerprint != record.Fingerprint)
        {
            // The declaration the stored entries were written under has been
            // replaced. Only a breaking change moves the fingerprint, so the
            // crawl restarts over the whole range and re-visits grains the index
            // already records: their entries describe the old declaration.
            next = GrainIndexBackfillCheckpoint.Start(record.Fingerprint, revisitsEnrolled: true, now);
            logger.LogInformation(
                "Grain index '{IndexName}' has a rebuild scheduled; its backfill restarts over the full range.",
                IndexName);
        }
        else if (checkpoint.State is GrainIndexBackfillState.Completed or GrainIndexBackfillState.Paused)
        {
            // A completed crawl has nothing left to do, and a paused one is
            // paused deliberately. Neither is something start-up should undo.
            return checkpoint.ToStatus(IndexName);
        }
        else
        {
            next = checkpoint.WithState(GrainIndexBackfillState.Running, now);
        }

        await SaveCheckpointAsync(next).ConfigureAwait(true);
        await EnsureDriverAsync().ConfigureAwait(true);
        return next.ToStatus(IndexName);
    }

    /// <inheritdoc />
    public async Task<GrainIndexBackfillStatus> GetStatusAsync()
    {
        await LoadCheckpointAsync().ConfigureAwait(true);
        return CurrentStatus();
    }

    /// <inheritdoc />
    public async Task<GrainIndexBackfillStatus> PauseAsync()
    {
        var checkpoint = await LoadCheckpointAsync().ConfigureAwait(true);
        if (checkpoint is null || checkpoint.State == GrainIndexBackfillState.Completed)
            return CurrentStatus();

        StopTimer();
        var paused = checkpoint.WithState(GrainIndexBackfillState.Paused, timeProvider.GetUtcNow());
        await SaveCheckpointAsync(paused).ConfigureAwait(true);
        return paused.ToStatus(IndexName);
    }

    /// <inheritdoc />
    public async Task<GrainIndexBackfillStatus> ResumeAsync()
    {
        var checkpoint = await LoadCheckpointAsync().ConfigureAwait(true);
        if (checkpoint is null || checkpoint.State == GrainIndexBackfillState.Completed)
            return CurrentStatus();

        var running = checkpoint.WithState(GrainIndexBackfillState.Running, timeProvider.GetUtcNow());
        await SaveCheckpointAsync(running).ConfigureAwait(true);
        await EnsureDriverAsync().ConfigureAwait(true);
        return running.ToStatus(IndexName);
    }

    /// <inheritdoc />
    public async Task<GrainIndexBackfillStatus> RestartAsync()
    {
        var record = await registry.ReadAsync(IndexName, CancellationToken.None).ConfigureAwait(true);
        var checkpoint = await LoadCheckpointAsync().ConfigureAwait(true);

        // Prefer the registry's fingerprint: a restart is meant to crawl under
        // the declaration in force now, not the one the last run captured.
        var fingerprint = record?.Fingerprint ?? checkpoint?.Fingerprint ?? default;

        // A restart re-visits already-indexed grains. Skipping them would make
        // restarting a completed crawl a no-op, which is the opposite of what
        // asking for one means.
        var restarted = GrainIndexBackfillCheckpoint.Start(
            fingerprint,
            revisitsEnrolled: true,
            timeProvider.GetUtcNow());

        await SaveCheckpointAsync(restarted).ConfigureAwait(true);
        await EnsureDriverAsync().ConfigureAwait(true);
        return restarted.ToStatus(IndexName);
    }

    /// <inheritdoc />
    public async Task<GrainIndexBackfillBatchResult> RunBatchAsync()
    {
        var checkpoint = await LoadCheckpointAsync().ConfigureAwait(true);
        if (checkpoint is null || checkpoint.State != GrainIndexBackfillState.Running)
            return GrainIndexBackfillBatchResult.None(checkpoint?.State ?? GrainIndexBackfillState.NotStarted);

        var definition = ResolveDefinition();
        if (definition is null)
        {
            logger.LogWarning(
                "The backfill for grain index '{IndexName}' cannot run here: this host declares no index by that name.",
                IndexName);
            return GrainIndexBackfillBatchResult.None(checkpoint.State);
        }

        var keySource = ResolveKeySource();
        if (keySource is null)
        {
            logger.LogWarning(
                "The backfill for grain index '{IndexName}' cannot run here: no {KeySource} is registered for it, "
                + "so the population to crawl is unknown.",
                IndexName,
                nameof(IGrainKeySource));
            return GrainIndexBackfillBatchResult.None(checkpoint.State);
        }

        var options = indexOptions.Get(IndexName);
        var batchSize = options.BackfillBatchSize > 0
            ? options.BackfillBatchSize
            : GrainIndexOptions.DefaultBackfillBatchSize;

        var taken = await FillBatchAsync(keySource, checkpoint.ResumeAfterKey, batchSize).ConfigureAwait(true);
        var exhausted = taken < batchSize;

        if (taken == 0)
            return await CompleteAsync(checkpoint).ConfigureAwait(true);

        if (!checkpoint.RevisitsEnrolled)
            await LoadAlreadyIndexedAsync(taken).ConfigureAwait(true);

        var enrolled = 0;
        var skipped = 0;
        var failed = 0;

        for (var i = 0; i < taken; i++)
        {
            var grainKey = _buffer[i];
            if (!checkpoint.RevisitsEnrolled && _alreadyIndexed.Contains(grainKey))
            {
                skipped++;
                continue;
            }

            try
            {
                await activator
                    .ActivateAsync(definition, grainKey, CancellationToken.None)
                    .ConfigureAwait(true);
                enrolled++;
            }
            catch (Exception ex)
            {
                // One unreachable grain must not stall the ones behind it. The
                // key stays uncounted as indexed, so a later rebuild revisits it.
                failed++;
                logger.LogWarning(
                    ex,
                    "The backfill for grain index '{IndexName}' could not onboard grain '{GrainKey}'; the crawl continues.",
                    IndexName,
                    grainKey);
            }
        }

        var now = timeProvider.GetUtcNow();
        var advanced = checkpoint.Advance(_buffer[taken - 1], taken, enrolled, skipped, failed, now);
        if (exhausted)
            advanced = advanced.WithState(GrainIndexBackfillState.Completed, now);

        await SaveCheckpointAsync(advanced).ConfigureAwait(true);

        if (exhausted)
            await FinishAsync().ConfigureAwait(true);

        return new GrainIndexBackfillBatchResult(taken, enrolled, skipped, failed, advanced.State, exhausted);
    }

    /// <inheritdoc />
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (!string.Equals(reminderName, ReminderName, StringComparison.Ordinal))
            return;

        var checkpoint = await LoadCheckpointAsync().ConfigureAwait(true);
        if (checkpoint is null)
            return;

        switch (checkpoint.State)
        {
            case GrainIndexBackfillState.Completed:
                // Belt and braces: the pass that completed the crawl already
                // unregistered this reminder, so a tick here means that
                // unregistration did not land.
                StopTimer();
                await UnregisterReminderAsync().ConfigureAwait(true);
                return;

            case GrainIndexBackfillState.Paused:
            case GrainIndexBackfillState.NotStarted:
                StopTimer();
                return;

            case GrainIndexBackfillState.Failed:
                // The heartbeat is the retry. Recovering here rather than from
                // the pass timer bounds retries to the reminder's cadence
                // instead of the crawl's much faster one.
                var resumed = checkpoint.WithState(
                    GrainIndexBackfillState.Running,
                    timeProvider.GetUtcNow());

                await SaveCheckpointAsync(resumed).ConfigureAwait(true);
                StartTimer();
                return;

            default:
                StartTimer();
                return;
        }
    }

    /// <summary>Disposes the pass timer when the activation goes away.</summary>
    /// <param name="reason">Why the grain is deactivating.</param>
    /// <param name="cancellationToken">Cancels deactivation work.</param>
    /// <returns>A completed task.</returns>
    public Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        StopTimer();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Takes up to <paramref name="batchSize"/> keys from the source into the
    /// activation's reusable buffer.
    /// </summary>
    private async Task<int> FillBatchAsync(
        IGrainKeySource keySource,
        string? resumeAfter,
        int batchSize)
    {
        if (_buffer.Length < batchSize)
            _buffer = new string[batchSize];

        var taken = 0;
        var keys = keySource.EnumerateKeysAsync(resumeAfter, CancellationToken.None);
        await using var enumerator = keys.GetAsyncEnumerator(CancellationToken.None);

        while (taken < batchSize && await enumerator.MoveNextAsync().ConfigureAwait(true))
            _buffer[taken++] = enumerator.Current;

        return taken;
    }

    /// <summary>
    /// Loads the seen markers covering the batch in one contiguous range read,
    /// so the loop below tests membership rather than making a call per grain.
    /// </summary>
    private async Task LoadAlreadyIndexedAsync(int taken)
    {
        _alreadyIndexed.Clear();

        var seen = enrollments.ScanSeenKeysAsync(
            IndexName,
            _buffer[0],
            _buffer[taken - 1],
            CancellationToken.None);

        await foreach (var grainKey in seen.ConfigureAwait(true))
            _alreadyIndexed.Add(grainKey);
    }

    /// <summary>Records a crawl whose key source had nothing left to give.</summary>
    private async Task<GrainIndexBackfillBatchResult> CompleteAsync(GrainIndexBackfillCheckpoint checkpoint)
    {
        var completed = checkpoint.WithState(
            GrainIndexBackfillState.Completed,
            timeProvider.GetUtcNow());

        await SaveCheckpointAsync(completed).ConfigureAwait(true);
        await FinishAsync().ConfigureAwait(true);

        return new GrainIndexBackfillBatchResult(
            visited: 0,
            enrolled: 0,
            skipped: 0,
            failed: 0,
            GrainIndexBackfillState.Completed,
            exhausted: true);
    }

    /// <summary>
    /// Idles the crawl: stops the pass timer, unregisters the heartbeat, and
    /// clears the registry's needs-backfill flag so a restart does not start the
    /// crawl again.
    /// </summary>
    private async Task FinishAsync()
    {
        StopTimer();
        await UnregisterReminderAsync().ConfigureAwait(true);
        await ClearNeedsBackfillAsync().ConfigureAwait(true);

        logger.LogInformation(
            "The backfill for grain index '{IndexName}' completed; its reminder has been unregistered.",
            IndexName);
    }

    private async Task ClearNeedsBackfillAsync()
    {
        try
        {
            var record = await registry.ReadAsync(IndexName, CancellationToken.None).ConfigureAwait(true);
            if (record is null || !record.NeedsBackfill)
                return;

            await registry
                .WriteAsync(
                    IndexName,
                    new GrainIndexRegistryRecord(
                        record.Descriptor,
                        record.KeyCodecId,
                        record.Fingerprint,
                        needsBackfill: false),
                    CancellationToken.None)
                .ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            // The checkpoint already says the crawl is complete, which is what
            // stops it running again; the flag is bookkeeping for the next
            // start-up and is safe to leave raised.
            logger.LogWarning(
                ex,
                "The backfill for grain index '{IndexName}' completed but its registry record could not be updated.",
                IndexName);
        }
    }

    /// <summary>
    /// Establishes the reminder and the pass timer, unless this host has the
    /// index's background driver switched off - in which case the crawl is still
    /// started, and is driven by explicit passes instead.
    /// </summary>
    private async Task EnsureDriverAsync()
    {
        if (!indexOptions.Get(IndexName).BackfillEnabled)
        {
            StopTimer();
            return;
        }

        await reminderRegistry
            .RegisterOrUpdateReminder(context.GrainId, ReminderName, ReminderPeriod, ReminderPeriod)
            .ConfigureAwait(true);

        StartTimer();
    }

    private void StartTimer()
    {
        if (_timer is not null)
            return;

        var options = indexOptions.Get(IndexName);
        if (!options.BackfillEnabled)
            return;

        var interval = options.BackfillInterval > TimeSpan.Zero
            ? options.BackfillInterval
            : GrainIndexOptions.DefaultBackfillInterval;

        // The first pass waits one interval rather than firing immediately, so a
        // silo start that begins several crawls does not begin them all inside
        // its own start-up.
        _timer = this.RegisterGrainTimer(
            OnPassTimerTickAsync,
            new GrainTimerCreationOptions(dueTime: interval, period: interval));
    }

    private void StopTimer()
    {
        _timer?.Dispose();
        _timer = null;
    }

    private async Task OnPassTimerTickAsync(CancellationToken cancellationToken)
    {
        try
        {
            var result = await RunBatchAsync().ConfigureAwait(true);
            if (result.State != GrainIndexBackfillState.Running)
                StopTimer();
        }
        catch (Exception ex)
        {
            // A pass-level fault - the registry briefly unavailable, say - is
            // not the crawl's fault and not per grain. It stops the fast pass
            // timer and leaves the state Failed, which the durable heartbeat
            // picks up and resumes at its own, much slower, cadence.
            logger.LogError(
                ex,
                "A backfill pass for grain index '{IndexName}' failed; the crawl is held at its checkpoint.",
                IndexName);

            StopTimer();
            await MarkFailedAsync(ex).ConfigureAwait(true);
        }
    }

    private async Task MarkFailedAsync(Exception ex)
    {
        try
        {
            if (_checkpoint is not { } checkpoint)
                return;

            var failed = checkpoint.WithState(
                GrainIndexBackfillState.Failed,
                timeProvider.GetUtcNow(),
                ex.Message);

            await SaveCheckpointAsync(failed).ConfigureAwait(true);
        }
        catch (Exception saveFailure)
        {
            logger.LogWarning(
                saveFailure,
                "The backfill for grain index '{IndexName}' could not record its failure state.",
                IndexName);
        }
    }

    private async Task UnregisterReminderAsync()
    {
        try
        {
            var reminder = await reminderRegistry
                .GetReminder(context.GrainId, ReminderName)
                .ConfigureAwait(true);

            if (reminder is not null)
            {
                await reminderRegistry
                    .UnregisterReminder(context.GrainId, reminder)
                    .ConfigureAwait(true);
            }
        }
        catch (Exception ex)
        {
            // A completed crawl whose reminder survives costs one tick a minute
            // that immediately re-tries this, so it is not worth failing over.
            logger.LogWarning(
                ex,
                "The backfill reminder for grain index '{IndexName}' could not be unregistered.",
                IndexName);
        }
    }

    private async Task<GrainIndexBackfillCheckpoint?> LoadCheckpointAsync(bool forceReload = false)
    {
        if (_checkpointLoaded && !forceReload)
            return _checkpoint;

        _checkpoint = await checkpoints.ReadAsync(IndexName, CancellationToken.None).ConfigureAwait(true);
        _checkpointLoaded = true;
        return _checkpoint;
    }

    private async Task SaveCheckpointAsync(GrainIndexBackfillCheckpoint checkpoint)
    {
        await checkpoints.WriteAsync(IndexName, checkpoint, CancellationToken.None).ConfigureAwait(true);
        _checkpoint = checkpoint;
        _checkpointLoaded = true;
    }

    private GrainIndexBackfillStatus CurrentStatus() =>
        _checkpoint?.ToStatus(IndexName) ?? GrainIndexBackfillStatus.NotStarted(IndexName);

    /// <summary>
    /// The declaration this crawl projects, resolved once per activation. The
    /// declaration set is a silo-wide singleton, so the linear search runs at
    /// most once here rather than per pass.
    /// </summary>
    private IGrainIndexDefinition? ResolveDefinition()
    {
        if (_definitionResolved)
            return _definition;

        _definitionResolved = true;
        var definitions = declarations.Value.Definitions;
        for (var i = 0; i < definitions.Count; i++)
        {
            if (string.Equals(definitions[i].Name, IndexName, StringComparison.Ordinal))
            {
                _definition = definitions[i];
                break;
            }
        }

        return _definition;
    }

    private IGrainKeySource? ResolveKeySource()
    {
        if (_keySourceResolved)
            return _keySource;

        _keySourceResolved = true;
        _keySource = keySources.Resolve(IndexName);
        return _keySource;
    }
}
