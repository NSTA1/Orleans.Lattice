using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="IWalMaterialiserPinGrain"/>. One activation per
/// <c>(tree, shard)</c> (the tree id is suffixed with a shard ordinal by the
/// <see cref="LeafCursorReporter"/> so the durable pin store is spread across
/// <see cref="LatticeOptions.WalMaterialiserPinShards"/> activations) persists
/// the leaf-materialiser checkpoint frontiers to durable grain state so the
/// WAL GC's trim floor survives a full silo or cluster restart. See
/// <see cref="IWalMaterialiserPinGrain"/> for the contract.
/// <para>
/// Durable writes are coalesced. An advancing non-birth report
/// (<see cref="ReportAsync"/> / <see cref="ReportManyAsync"/>) updates the
/// in-memory pin immediately and schedules a single <c>WriteStateAsync</c> at
/// most once per <see cref="LatticeOptions.WalMaterialiserPinFlushIntervalMs"/>
/// window through a grain timer, so a burst of reports from many leaves
/// collapses to one durable write per shard per window instead of one write
/// per report. The in-memory snapshot the WAL GC reads via
/// <see cref="GetPinsAsync"/> is always current; only the durable
/// restart-backstop is debounced, and a durable pin that lags the in-memory
/// frontier only ever retains more WAL (always GC-safe). The birth seed path
/// (<see cref="SeedManyAsync"/>) bypasses the window and writes through
/// durably, preserving the crash-safety guarantee that a new leaf's block pin
/// is durable before its data becomes reachable. A final flush runs on
/// deactivation so a clean shutdown loses no pending advance.
/// </para>
/// <para>
/// The coalesced flush is additionally <b>amortised against its own cost</b>
/// (issue #2012). A pin write is <c>O(consumers routed to this shard)</c>
/// because Orleans rewrites the whole state blob, so on a tree with thousands
/// of leaves a single write is megabytes and takes far longer than the flush
/// window. Ticking on the fixed window regardless would leave this
/// non-reentrant grain writing essentially back-to-back, so its non-reentrancy
/// queue - which every leaf's report joins - would grow without bound and
/// callers would time out. Instead a timer tick defers unless at least
/// <see cref="WriteAmortisationFactor"/> times the last write's own duration
/// has elapsed since that write completed, which bounds the share of time this
/// grain spends writing to <c>1 / (1 + factor)</c> and leaves the rest for
/// draining the queue. The mechanism is self-tuning (a cheap write on a small
/// tree barely defers at all) and always safe, because deferring only leaves
/// the durable pin staler, which retains more WAL. Explicit durability points -
/// the birth seed, <see cref="RemoveAsync"/>, <see cref="ClearAsync"/>, and the
/// deactivation flush - are never deferred.
/// </para>
/// </summary>
internal sealed class WalMaterialiserPinGrain : IGrainBase, IWalMaterialiserPinGrain
{
    private readonly IGrainContext _context;
    private readonly IPersistentState<WalMaterialiserPinState> _state;
    private readonly IOptionsMonitor<LatticeOptions> _options;
    private readonly ILogger<WalMaterialiserPinGrain>? _logger;

    private IGrainTimer? _flushTimer;
    private bool _dirty;
    private bool _flushInFlight;

    /// <summary>
    /// Optional durable-storage handle used only when
    /// <see cref="LatticeOptions.WalMaterialiserPinBuckets"/> is greater than
    /// one. At the default of one the grain persists through its injected
    /// <see cref="IPersistentState{T}"/> exactly as every pre-bucketing build
    /// did, and this is never touched.
    /// </summary>
    private readonly IGrainStorage? _pinStorage;

    /// <summary>
    /// Per-bucket durable state holders, keyed by bucket ordinal. Populated at
    /// activation and reused for every write so each slot's ETag carries
    /// forward. Empty in the default single-slot layout.
    /// </summary>
    private readonly Dictionary<int, GrainState<WalMaterialiserPinState>> _bucketStates = new();

    /// <summary>
    /// Bucket ordinals whose contents have advanced since their last durable
    /// write. Only these are rewritten, which is what turns an
    /// <c>O(consumers on this shard)</c> write into
    /// <c>O(consumers in this bucket)</c>.
    /// </summary>
    private readonly HashSet<int> _dirtyBuckets = new();

    /// <summary>
    /// The bucket count this activation resolved at startup. Cached for the
    /// activation's lifetime so a mid-flight options change cannot route a
    /// consumer's write to a different slot than the one its neighbours were
    /// read from.
    /// </summary>
    private int _bucketCount = 1;

    /// <summary>
    /// The bucket width recorded in durable storage. Held at the wider of the
    /// configured and previously-persisted counts until a narrowing
    /// consolidation has fully landed, so a crash midway through consolidation
    /// cannot leave a later activation reading a narrow layout while pins are
    /// still stranded in out-of-range slots. Narrowing the recorded width is the
    /// last step, and only taken once every in-range bucket has been written.
    /// </summary>
    private int _persistedWidth = 1;

    /// <summary>
    /// Wall-clock duration, in milliseconds, of the most recent durable write.
    /// Used to amortise the coalesced flush against its own cost; see the type
    /// remarks. Zero until the first write completes, so the first coalesced
    /// flush is never deferred.
    /// </summary>
    private long _lastWriteDurationMs;

    /// <summary>
    /// <see cref="Environment.TickCount64"/> at which the most recent durable
    /// write completed (successfully or not).
    /// </summary>
    private long _lastWriteCompletedTickMs;

    /// <summary>
    /// Multiple of the previous write's duration that a coalesced timer flush
    /// waits, beyond that write's completion, before starting the next one.
    /// Bounds the share of wall-clock time this non-reentrant grain spends
    /// inside <c>WriteStateAsync</c> to <c>1 / (1 + WriteAmortisationFactor)</c>
    /// - one tenth at the value below - so the remaining nine tenths are
    /// available to drain the non-reentrancy queue every reporting leaf joins.
    /// Only gates the debounced flush; explicit durability points are never
    /// deferred.
    /// </summary>
    private const long WriteAmortisationFactor = 9;

    /// <summary>
    /// Creates the pin grain.
    /// </summary>
    /// <param name="context">The grain activation context.</param>
    /// <param name="state">The durable pin state.</param>
    /// <param name="options">Monitor used to read the coalescing flush interval.</param>
    /// <param name="logger">Optional logger.</param>
    public WalMaterialiserPinGrain(
        IGrainContext context,
        [PersistentState(WalMaterialiserPinState.StateName, LatticeOptions.StorageProviderName)]
        IPersistentState<WalMaterialiserPinState> state,
        IOptionsMonitor<LatticeOptions> options,
        ILogger<WalMaterialiserPinGrain>? logger = null,
        [FromKeyedServices(LatticeOptions.StorageProviderName)]
        IGrainStorage? pinStorage = null)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(options);
        _context = context;
        _state = state;
        _options = options;
        _logger = logger;
        _pinStorage = pinStorage;
    }

    /// <inheritdoc />
    async Task IGrainBase.OnActivateAsync(CancellationToken cancellationToken)
    {
        _bucketCount = WalMaterialiserPinRouting.ResolveBucketCount(_options);
        if (_bucketCount <= 1 || _pinStorage is null)
        {
            // Default layout: the injected IPersistentState has already read the
            // single legacy slot, which is the whole of this shard's state.
            return;
        }

        // Bucketed layout. The injected IPersistentState has read the legacy
        // slot, whose contents (pins written before bucketing was enabled) stay
        // authoritative until each consumer re-pins. Merge every bucket on top
        // of it, monotonic-max, so no pin is ever dropped from the trim floor
        // during the transition - the same self-healing dual-read the shard
        // dimension already relies on in WalMaterialiserPinRouting.
        var toRead = await ResolvePersistedWidthAsync(cancellationToken);
        _persistedWidth = toRead;
        for (var bucket = 0; bucket < toRead; bucket++)
        {
            var slot = await ReadBucketAsync(bucket, cancellationToken);
            if (slot?.State is not { } bucketState)
            {
                continue;
            }

            foreach (var pin in bucketState.Pins)
            {
                MergeLoaded(pin.Key, pin.Value);
            }

            foreach (var offset in bucketState.Offsets)
            {
                MergeLoadedOffset(offset.Key, offset.Value);
            }
        }

        if (toRead <= _bucketCount)
        {
            return;
        }

        // The persisted layout was wider than this host's configuration: the
        // pins just merged out of the now-out-of-range slots exist only in
        // memory under the narrower layout. Consolidate them immediately rather
        // than waiting for a report that may never come, so a restart cannot
        // strand them.
        _logger?.LogInformation(
            "WAL materialiser pin store for {GrainKey} was persisted across {PersistedBuckets} buckets but is configured for {ConfiguredBuckets}; consolidating.",
            _context.GrainId.Key.ToString(),
            toRead,
            _bucketCount);
        for (var bucket = 0; bucket < _bucketCount; bucket++)
        {
            _dirtyBuckets.Add(bucket);
        }

        _dirty = true;
        try
        {
            await PersistNowAsync();

            // Every in-range bucket has landed, so the out-of-range slots are
            // now redundant and it is safe to record the narrower width. Doing
            // this only after the consolidation write succeeded is what makes a
            // crash midway through it harmless: the recorded width still names
            // the wide layout, so the next activation re-reads the stranded
            // slots and retries.
            _persistedWidth = _bucketCount;
            _dirtyBuckets.Add(0);
            _dirty = true;
            await PersistNowAsync();
        }
        catch (Exception ex)
        {
            // Best effort. The pins are in memory and every subsequent advance
            // re-marks its bucket dirty, so the consolidation retries naturally.
            _logger?.LogWarning(
                ex,
                "Consolidating the WAL materialiser pin store for {GrainKey} failed; will retry on the next flush.",
                _context.GrainId.Key.ToString());
        }
    }

    /// <summary>
    /// Resolves how many bucket slots this activation must read: the wider of
    /// the configured count and the count recorded in bucket zero by whichever
    /// build last wrote it. Reading the wider layout is what makes lowering
    /// <see cref="LatticeOptions.WalMaterialiserPinBuckets"/> safe.
    /// </summary>
    private async Task<int> ResolvePersistedWidthAsync(CancellationToken cancellationToken)
    {
        var probe = await ReadBucketAsync(0, cancellationToken);
        var persisted = probe?.State?.PersistedBucketCount ?? 0;
        return Math.Max(_bucketCount, persisted);
    }

    /// <summary>
    /// Reads one bucket slot, caching the holder so its ETag carries into later
    /// writes. Returns <see langword="null"/> when the read fails, which is
    /// treated as "nothing to merge": losing a bucket read leaves the durable
    /// floor lower than it could be, retaining more WAL, which is safe.
    /// </summary>
    private async Task<GrainState<WalMaterialiserPinState>?> ReadBucketAsync(int bucket, CancellationToken cancellationToken)
    {
        if (_bucketStates.TryGetValue(bucket, out var cached))
        {
            return cached;
        }

        var holder = new GrainState<WalMaterialiserPinState>(new WalMaterialiserPinState());
        try
        {
            await _pinStorage!
                .ReadStateAsync(WalMaterialiserPinRouting.BucketStateName(bucket), _context.GrainId, holder)
                ;
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(
                ex,
                "Reading WAL materialiser pin bucket {Bucket} for {GrainKey} failed; its pins are omitted from this activation's floor until they are re-reported.",
                bucket,
                _context.GrainId.Key.ToString());
            return null;
        }

        holder.State ??= new WalMaterialiserPinState();
        _bucketStates[bucket] = holder;
        cancellationToken.ThrowIfCancellationRequested();
        return holder;
    }

    /// <summary>
    /// Monotonic-max merge of a pin loaded from durable storage into the
    /// in-memory map, without marking anything dirty (it is already durable).
    /// </summary>
    private void MergeLoaded(string consumerId, HybridLogicalClock frontier)
    {
        if (!_state.State.Pins.TryGetValue(consumerId, out var existing) || frontier > existing)
        {
            _state.State.Pins[consumerId] = frontier;
        }
    }

    /// <summary>
    /// Monotonic-max merge of a checkpoint offset loaded from durable storage.
    /// </summary>
    private void MergeLoadedOffset(string consumerId, long offset)
    {
        if (!_state.State.Offsets.TryGetValue(consumerId, out var existing) || offset > existing)
        {
            _state.State.Offsets[consumerId] = offset;
        }
    }


    /// <inheritdoc />
    IGrainContext IGrainBase.GrainContext => _context;

    /// <inheritdoc />
    public async Task ReportAsync(string consumerId, HybridLogicalClock frontier)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerId);
        if (Merge(consumerId, frontier, checkpointOffset: -1))
        {
            await ScheduleOrFlushAsync();
        }
    }

    /// <inheritdoc />
    public async Task ReportManyAsync(IReadOnlyList<MaterialiserPinReport> reports)
    {
        ArgumentNullException.ThrowIfNull(reports);
        var changed = false;
        for (var i = 0; i < reports.Count; i++)
        {
            var report = reports[i];
            ArgumentException.ThrowIfNullOrWhiteSpace(report.ConsumerId);
            changed |= Merge(report.ConsumerId, report.Frontier, report.CheckpointOffset);
        }

        if (changed)
        {
            await ScheduleOrFlushAsync();
        }
    }

    /// <inheritdoc />
    public async Task SeedManyAsync(IReadOnlyList<MaterialiserPinReport> reports)
    {
        ArgumentNullException.ThrowIfNull(reports);
        var changed = false;
        for (var i = 0; i < reports.Count; i++)
        {
            var report = reports[i];
            ArgumentException.ThrowIfNullOrWhiteSpace(report.ConsumerId);
            changed |= Merge(report.ConsumerId, report.Frontier, report.CheckpointOffset);
        }

        // Birth path: persist through durably (awaited) so the block pin is
        // durable before the caller lets the new leaf's data become reachable.
        if (changed)
        {
            await PersistNowAsync(MaterialiserPinBirthOutcome);
        }
    }

    /// <inheritdoc />
    public Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetPinsAsync() =>
        Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
            new Dictionary<string, HybridLogicalClock>(_state.State.Pins, StringComparer.Ordinal));

    /// <inheritdoc />
    public Task<IReadOnlyDictionary<string, long>> GetPinOffsetsAsync() =>
        Task.FromResult<IReadOnlyDictionary<string, long>>(
            new Dictionary<string, long>(_state.State.Offsets, StringComparer.Ordinal));

    /// <inheritdoc />
    public async Task RemoveAsync(string consumerId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerId);

        // Resolve the bucket before removing: once the consumer is gone from the
        // in-memory map its bucket can no longer be derived from state, and the
        // bucket still has to be rewritten to make the removal durable.
        var bucket = _bucketCount > 1 ? BucketOf(consumerId) : 0;
        var removed = _state.State.Pins.Remove(consumerId);
        removed |= _state.State.Offsets.Remove(consumerId);
        if (removed)
        {
            _dirty = true;
            if (_bucketCount > 1)
            {
                _dirtyBuckets.Add(bucket);
            }

            await PersistNowAsync();
        }
    }

    /// <inheritdoc />
    public async Task ClearAsync()
    {
        if (_state.State.Pins.Count == 0 && _state.State.Offsets.Count == 0)
        {
            return;
        }

        _state.State.Pins.Clear();
        _state.State.Offsets.Clear();
        _dirty = true;
        if (_bucketCount > 1)
        {
            // Every bucket must be rewritten empty, and the legacy slot cleared
            // too. Clear is only reached on tree deletion, where retaining a
            // stale pin would keep the deleted tree's WAL pinned forever.
            for (var bucket = 0; bucket < _bucketCount; bucket++)
            {
                _dirtyBuckets.Add(bucket);
            }
        }

        await PersistNowAsync();
        if (_bucketCount > 1)
        {
            await ClearLegacySlotAsync();
        }
    }

    /// <summary>
    /// Empties the legacy single-slot blob. Only called from
    /// <see cref="ClearAsync"/>: outside tree deletion the legacy slot is read
    /// but never rewritten, so that a host which rolls back to a pre-bucketing
    /// build still finds the pins it wrote before the upgrade. Those pins are
    /// stale, which retains more WAL and is safe; deleting them would not be.
    /// </summary>
    private async Task ClearLegacySlotAsync()
    {
        try
        {
            await _state.WriteStateAsync();
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(
                ex,
                "Clearing the legacy WAL materialiser pin slot for {GrainKey} failed; stale pins may keep WAL retained until the next clear.",
                _context.GrainId.Key.ToString());
        }
    }

    /// <inheritdoc />
    async Task IGrainBase.OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        _flushTimer?.Dispose();
        _flushTimer = null;
        if (!_dirty)
        {
            return;
        }

        try
        {
            await PersistNowAsync();
        }
        catch (Exception ex)
        {
            // Best-effort: a transient storage outage must not block grain
            // deactivation. A lost advance only leaves the durable pin staler
            // (more WAL retained), which is GC-safe, and the next activation's
            // reports re-advance it.
            _logger?.LogWarning(
                ex,
                "Final WAL materialiser pin flush failed for {GrainKey} during deactivation; pending advances re-seed on next activation.",
                _context.GrainId.Key.ToString());
        }
    }

    /// <summary>
    /// Monotonic-max merge of a single pin into the in-memory state. Merges the
    /// HLC <paramref name="frontier"/> and the <paramref name="checkpointOffset"/>
    /// independently, each monotonic-max: neither ever rolls back. Returns
    /// <see langword="true"/> when <b>either</b> the stored frontier or the
    /// stored offset advanced (a new consumer, a strictly-greater frontier, or a
    /// strictly-greater offset), <see langword="false"/> when both were coalesced
    /// (at or below the stored values). The offset must advance independently of
    /// the frontier: a tombstone-compaction reap advances a leaf's applied offset
    /// while its HLC checkpoint stays flat, so an offset-only advance still has to
    /// move the durable floor.
    /// </summary>
    private bool Merge(string consumerId, HybridLogicalClock frontier, long checkpointOffset)
    {
        var changed = false;

        if (!_state.State.Pins.TryGetValue(consumerId, out var existing) || frontier > existing)
        {
            _state.State.Pins[consumerId] = frontier;
            changed = true;
        }

        if (!_state.State.Offsets.TryGetValue(consumerId, out var existingOffset) || checkpointOffset > existingOffset)
        {
            _state.State.Offsets[consumerId] = checkpointOffset;
            changed = true;
        }

        if (changed)
        {
            _dirty = true;
            MarkBucketDirty(consumerId);
        }

        return changed;
    }

    /// <summary>
    /// Marks the bucket owning <paramref name="consumerId"/> as needing a
    /// durable write. A no-op in the default single-slot layout, where the whole
    /// shard is one blob and <see cref="_dirty"/> alone drives the write.
    /// </summary>
    private void MarkBucketDirty(string consumerId)
    {
        if (_bucketCount > 1)
        {
            _dirtyBuckets.Add(BucketOf(consumerId));
        }
    }

    /// <summary>Resolves the bucket ordinal owning <paramref name="consumerId"/>.</summary>
    private int BucketOf(string consumerId)
        => WalMaterialiserPinRouting.BucketOf(consumerId, _bucketCount);

    /// <summary>
    /// Either schedules a coalesced durable flush (when the flush interval is
    /// positive and a grain timer can be armed) or persists synchronously.
    /// The synchronous fallback covers two cases: coalescing disabled
    /// (interval &lt;= 0) and no grain runtime (a unit-test harness whose
    /// substituted context cannot register a timer), so a report is never
    /// silently left unpersisted with no timer to drain it.
    /// </summary>
    private async Task ScheduleOrFlushAsync()
    {
        var intervalMs = _options.Get(string.Empty).WalMaterialiserPinFlushIntervalMs;
        if (intervalMs <= 0 || !TryArmFlushTimer(intervalMs))
        {
            await PersistNowAsync();
        }

        // Timer armed: leave _dirty set; the timer tick drains it.
    }

    private bool TryArmFlushTimer(int intervalMs)
    {
        if (_flushTimer is not null)
        {
            return true;
        }

        try
        {
            var period = TimeSpan.FromMilliseconds(intervalMs);
            _flushTimer = this.RegisterGrainTimer(
                OnFlushTimerTickAsync,
                new GrainTimerCreationOptions(dueTime: period, period: period));
            return true;
        }
        catch (Exception ex)
        {
            // No grain runtime (unit-test harness): fall back to synchronous
            // persistence so the report is not lost.
            _logger?.LogDebug(
                ex,
                "Could not register WAL materialiser pin flush timer for {GrainKey}; falling back to synchronous persistence.",
                _context.GrainId.Key.ToString());
            return false;
        }
    }

    private async Task OnFlushTimerTickAsync(CancellationToken cancellationToken)
    {
        if (!_dirty)
        {
            return;
        }

        if (ShouldDeferCoalescedFlush(Environment.TickCount64, _lastWriteCompletedTickMs, _lastWriteDurationMs))
        {
            // Amortisation: the previous write has not yet "paid for itself" in
            // queue-draining time. Stay dirty and let a later tick (or an
            // explicit durability point) persist the accumulated advances - the
            // in-memory pins the WAL GC reads are already current, and a staler
            // durable pin only retains more WAL.
            return;
        }

        try
        {
            await PersistNowAsync();
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(
                ex,
                "Coalesced WAL materialiser pin flush failed for {GrainKey}; will retry on next tick.",
                _context.GrainId.Key.ToString());
        }
    }

    /// <summary>
    /// Decides whether a coalesced timer flush should be skipped because the
    /// previous durable write has not yet been followed by
    /// <see cref="WriteAmortisationFactor"/> times its own duration of
    /// non-writing time. Returns <see langword="false"/> before any write has
    /// completed (<paramref name="lastWriteDurationMs"/> is zero), so the first
    /// flush after a burst starts immediately and the mechanism only engages
    /// once a write has proved to be expensive. Pure and side-effect free so
    /// the policy is directly testable.
    /// </summary>
    /// <param name="nowTickMs">Current <see cref="Environment.TickCount64"/>.</param>
    /// <param name="lastWriteCompletedTickMs">Tick at which the last write completed.</param>
    /// <param name="lastWriteDurationMs">Duration of the last write, in milliseconds.</param>
    internal static bool ShouldDeferCoalescedFlush(
        long nowTickMs,
        long lastWriteCompletedTickMs,
        long lastWriteDurationMs) =>
        lastWriteDurationMs > 0 &&
        nowTickMs - lastWriteCompletedTickMs < lastWriteDurationMs * WriteAmortisationFactor;

    /// <summary>
    /// Persists the current in-memory pins, coalescing with any concurrent
    /// in-flight flush. Loops until no advance remains unpersisted so a caller
    /// that requires durability (the birth seed path, removal, clear) returns
    /// only once its mutation has landed in a write that started after it.
    /// The grain is single-threaded; the only interleaving is at the
    /// <c>WriteStateAsync</c> await, after which the in-flight flag clears.
    /// </summary>
    private async Task PersistNowAsync(string outcome = MaterialiserPinCoalescedOutcome)
    {
        while (_dirty)
        {
            if (_flushInFlight)
            {
                await Task.Yield();
                continue;
            }

            _flushInFlight = true;
            _dirty = false;
            var startedTickMs = Environment.TickCount64;
            try
            {
                await WriteDurableAsync();
                LatticeMetrics.MaterialiserPinDurableWrites.Add(
                    1,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeTag),
                    new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, outcome),
                    LatticeTenantLabel.ForTree(TreeTag));
            }
            catch
            {
                _dirty = true;
                throw;
            }
            finally
            {
                _flushInFlight = false;

                // Record what this write cost so the coalesced flush can
                // amortise against it. A failed write is timed too: it consumed
                // the same grain time and the retry should back off equally.
                var completedTickMs = Environment.TickCount64;
                _lastWriteDurationMs = Math.Max(0, completedTickMs - startedTickMs);
                _lastWriteCompletedTickMs = completedTickMs;
            }
        }
    }

    /// <summary>
    /// Issues the durable write for the current in-memory pins.
    /// <para>
    /// In the default single-slot layout this is exactly the pre-bucketing
    /// <c>WriteStateAsync</c> on the injected
    /// <see cref="IPersistentState{T}"/>. When bucketing is enabled only the
    /// buckets whose contents advanced are rewritten, which is what removes the
    /// <c>O(consumers on this shard)</c> write amplification behind issue #2012:
    /// a single leaf advancing rewrites the tens of pins sharing its bucket
    /// instead of the thousands sharing its shard.
    /// </para>
    /// <para>
    /// Buckets are written concurrently. They are independent slots and each
    /// carries its own ETag, and a partial failure is safe in the same way every
    /// other failure on this path is: a bucket that did not land leaves its pins
    /// staler than memory, which retains more WAL. The bucket stays dirty and
    /// the next flush retries it.
    /// </para>
    /// </summary>
    private async Task WriteDurableAsync()
    {
        if (_bucketCount <= 1 || _pinStorage is null)
        {
            await _state.WriteStateAsync();
            return;
        }

        if (_dirtyBuckets.Count == 0)
        {
            return;
        }

        // Bucket zero carries the persisted width, and it is the only slot a
        // later activation probes to learn it. Stamping the width on whichever
        // buckets happen to be dirty is therefore not enough: if no consumer
        // ever hashed into bucket zero, the width would never be recorded, and
        // lowering the configured count would read only the narrow range and
        // strand every pin outside it - invisible to the trim floor, the one
        // genuinely unsafe direction. Force bucket zero into any batch that has
        // not yet recorded the current width.
        if (!_bucketStates.TryGetValue(0, out var widthSlot)
            || widthSlot.State is null
            || widthSlot.State.PersistedBucketCount != _persistedWidth)
        {
            _dirtyBuckets.Add(0);
        }

        var buckets = _dirtyBuckets.ToArray();
        _dirtyBuckets.Clear();
        var writes = new Task[buckets.Length];
        for (var i = 0; i < buckets.Length; i++)
        {
            writes[i] = WriteBucketAsync(buckets[i]);
        }

        try
        {
            await Task.WhenAll(writes);
        }
        catch
        {
            // Re-arm every bucket in this batch. Re-writing a bucket that
            // actually landed is harmless (the write is idempotent - it persists
            // whatever memory currently holds), and it is far cheaper than
            // tracking per-bucket success only to risk dropping one.
            foreach (var bucket in buckets)
            {
                _dirtyBuckets.Add(bucket);
            }

            throw;
        }
    }

    /// <summary>
    /// Persists the slice of the in-memory pin map owned by
    /// <paramref name="bucket"/>.
    /// </summary>
    private async Task WriteBucketAsync(int bucket)
    {
        if (!_bucketStates.TryGetValue(bucket, out var holder))
        {
            // The activation read of this slot failed, so no ETag was captured.
            // Re-read before writing rather than writing blind: a provider that
            // enforces ETags would otherwise reject every write for the rest of
            // this activation's life.
            holder = await ReadBucketAsync(bucket, CancellationToken.None)
                ?? throw new InvalidOperationException(
                    $"WAL materialiser pin bucket {bucket} could not be read before writing.");
        }

        var slice = new WalMaterialiserPinState { PersistedBucketCount = _persistedWidth };
        foreach (var pin in _state.State.Pins)
        {
            if (BucketOf(pin.Key) == bucket)
            {
                slice.Pins[pin.Key] = pin.Value;
            }
        }

        foreach (var offset in _state.State.Offsets)
        {
            if (BucketOf(offset.Key) == bucket)
            {
                slice.Offsets[offset.Key] = offset.Value;
            }
        }

        holder.State = slice;
        try
        {
            await _pinStorage!
                .WriteStateAsync(WalMaterialiserPinRouting.BucketStateName(bucket), _context.GrainId, holder)
                ;
        }
        catch
        {
            // The write did not land, so this holder's ETag no longer describes
            // any durable state we can reason about: an ETag conflict means the
            // slot moved under us, and any other failure leaves it unknown.
            // Evicting forces the next attempt through ReadBucketAsync for a
            // fresh ETag.
            //
            // Retrying this bucket is by design (WriteDurableAsync re-arms the
            // batch and the next flush retries it), but a retry that reuses the
            // stale ETag conflicts identically every time, so without this the
            // designed retry cannot terminate. See issue #2096.
            _bucketStates.Remove(bucket);
            throw;
        }
    }

    /// <summary>Outcome tag value for a birth-path (synchronous through-write) durable pin write.</summary>
    private const string MaterialiserPinBirthOutcome = "birth";

    /// <summary>Outcome tag value for a coalesced (debounced flush) durable pin write.</summary>
    private const string MaterialiserPinCoalescedOutcome = "coalesced";

    private string? _treeTag;

    /// <summary>
    /// The logical tree id this pin shard belongs to, used as the metric tree
    /// tag. The grain key is either the bare <c>{treeName}</c> (single-shard
    /// layout) or a shard-suffixed key; the suffix is stripped so every shard of
    /// a tree reports under the same tree tag.
    /// </summary>
    private string TreeTag => _treeTag ??= WalMaterialiserPinRouting.TreeNameFromKey(_context.GrainId.Key.ToString());
}
