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
/// in-memory pin immediately and schedules a durable write at most once per
/// <see cref="LatticeOptions.WalMaterialiserPinFlushIntervalMs"/> window
/// through a grain timer, so a burst of reports from many leaves collapses to
/// one durable write per shard per window instead of one write per report. The
/// in-memory snapshot the WAL GC reads via <see cref="GetPinsAsync"/> is always
/// current; only the durable restart-backstop is debounced, and a durable pin
/// that lags the in-memory frontier only ever retains more WAL (always
/// GC-safe). The birth seed path (<see cref="SeedManyAsync"/>) bypasses the
/// window and writes through durably, preserving the crash-safety guarantee
/// that a new leaf's block pin is durable before its data becomes reachable. A
/// final flush runs on deactivation so a clean shutdown loses no pending
/// advance.
/// </para>
/// <para>
/// The coalesced flush is additionally <b>amortised against its own cost</b>
/// (issue #2012): a timer tick defers unless at least
/// <see cref="WriteAmortisationFactor"/> times the last write's own duration
/// has elapsed since that write completed, which bounds the share of time this
/// non-reentrant grain spends writing to <c>1 / (1 + factor)</c> and leaves the
/// rest for draining the queue every reporting leaf joins. Deferring only
/// leaves the durable pin staler, which retains more WAL.
/// </para>
/// <para>
/// <b>Automatic slot split (issue #3576).</b> The configured
/// <see cref="LatticeOptions.WalMaterialiserPinBuckets"/> is a <em>floor</em>,
/// not the layout. The grain tracks an estimate of the serialised size of its
/// pin map and, whenever the average slot would exceed
/// <see cref="TargetSlotBytes"/>, splits the shard across more durable slots by
/// doubling the layout width, so no slot ever approaches a storage provider's
/// entity limit (Azure Table's is 983,040 bytes) however many leaves the tree
/// has. A shard whose map fits in one target slot - roughly eighty consumers -
/// keeps the default single legacy slot byte-for-byte. The width actually in
/// use is recorded in bucket zero (<see cref="WalMaterialiserPinState.PersistedBucketCount"/>),
/// which every activation probes, so a split store is always read at its full
/// width. Growth is crash-safe in any order of failure: the new slots are
/// written first, then bucket zero with the new width, then the old slots are
/// trimmed, and every width is a power-of-two multiple or divisor of the last so
/// a consumer's new slot is always either bucket zero or a slot already
/// written. A shard that has shrunk is narrowed again at activation, with a
/// factor-of-two hysteresis so a shard at the threshold does not oscillate.
/// </para>
/// <para>
/// <b>Bounded write turns.</b> Only dirty slots are written, at most
/// <see cref="MaxSlotsPerCoalescedFlush"/> per coalesced tick, and a birth seed
/// or removal writes only the slots it touched, so no single turn serialises
/// the whole shard. The exception is a relayout, a one-off rewrite of the whole
/// shard per doubling; because widths grow geometrically its lifetime cost is
/// at most twice the final shard size.
/// </para>
/// <para>
/// <b>Failure backoff.</b> A durable write that fails arms an exponential
/// backoff (<see cref="ComputeFlushBackoffMs"/>): the coalesced tick does not
/// retry until it elapses, and once <see cref="FailFastThreshold"/> writes in a
/// row have failed, a birth seed or removal inside the window fails fast
/// without touching storage (the in-memory pin is already merged and stays
/// dirty, so the next successful flush lands it). A failure the provider
/// reports as the payload being too large additionally doubles the size
/// estimate, which forces a split on the next attempt, so a store whose
/// entries are larger than estimated converges instead of failing forever.
/// </para>
/// </summary>
internal sealed class WalMaterialiserPinGrain : IGrainBase, IWalMaterialiserPinGrain
{
    /// <summary>
    /// Average slot size, in estimated serialised bytes, above which the shard
    /// is split across more slots. One Azure Table property holds 64 KiB and an
    /// entity 15 of them (983,040 bytes), so a 64 KiB target leaves an order of
    /// magnitude of headroom for estimate error and hash imbalance, and bounds
    /// the cost of any single slot write.
    /// </summary>
    internal const long TargetSlotBytes = 64 * 1024;

    /// <summary>
    /// Upper bound on the automatically chosen layout width. At the target slot
    /// size this accommodates tens of thousands of consumers per shard; a
    /// configured <see cref="LatticeOptions.WalMaterialiserPinBuckets"/> above
    /// it is honoured as given.
    /// </summary>
    internal const int MaxAutoBuckets = 1024;

    /// <summary>
    /// Upper bound on the size-estimate correction applied after a provider
    /// reports a payload as too large.
    /// </summary>
    private const int MaxEstimateScale = 64;

    /// <summary>Maximum number of slot writes issued concurrently.</summary>
    private const int SlotWriteConcurrency = 8;

    /// <summary>Maximum number of slot reads issued concurrently.</summary>
    private const int SlotReadConcurrency = 16;

    /// <summary>
    /// Maximum number of dirty slots one coalesced timer tick writes. Remaining
    /// dirty slots are drained round-robin by later ticks, so a shard with many
    /// slots never holds the non-reentrant grain for a whole-shard write.
    /// </summary>
    internal const int MaxSlotsPerCoalescedFlush = 8;

    /// <summary>Backoff after the first consecutive durable-write failure.</summary>
    internal const long InitialFlushBackoffMs = 1_000;

    /// <summary>Ceiling on the durable-write failure backoff.</summary>
    internal const long MaxFlushBackoffMs = 60_000;

    /// <summary>
    /// Consecutive durable-write failures after which a birth seed or removal
    /// inside the backoff window fails fast instead of attempting a write.
    /// </summary>
    internal const int FailFastThreshold = 3;

    /// <summary>
    /// Estimated per-entry serialisation overhead of a <see cref="WalMaterialiserPinState.Pins"/>
    /// entry beyond its key: the type-annotated <see cref="HybridLogicalClock"/>
    /// value and JSON punctuation.
    /// </summary>
    private const int PinEntryOverheadChars = 160;

    /// <summary>
    /// Estimated per-entry serialisation overhead of a <see cref="WalMaterialiserPinState.Offsets"/>
    /// entry beyond its key.
    /// </summary>
    private const int OffsetEntryOverheadChars = 40;

    private readonly IGrainContext _context;
    private readonly IPersistentState<WalMaterialiserPinState> _state;
    private readonly IOptionsMonitor<LatticeOptions> _options;
    private readonly ILogger<WalMaterialiserPinGrain>? _logger;

    private IGrainTimer? _flushTimer;
    private bool _dirty;
    private bool _flushInFlight;

    /// <summary>Grain-type label carried on a translated state-write fault.</summary>
    private const string StateWriteGrainType = "wal-materialiser-pin";

    /// <summary>
    /// Set once a single-slot pin write loses an optimistic-concurrency (ETag)
    /// check (issue #3572). The activation has asked to deactivate and fails
    /// later durable writes fast rather than retrying with a stale ETag.
    /// </summary>
    private bool _stateConflicted;

    /// <summary>
    /// Optional durable-storage handle used for the bucketed layout. Without it
    /// the grain persists through its injected <see cref="IPersistentState{T}"/>
    /// only and never splits.
    /// </summary>
    private readonly IGrainStorage? _pinStorage;

    /// <summary>
    /// Per-bucket durable state holders, keyed by bucket ordinal. Populated at
    /// activation and reused for every write so each slot's ETag carries
    /// forward. A holder is evicted when a write through it fails.
    /// </summary>
    private readonly Dictionary<int, GrainState<WalMaterialiserPinState>> _bucketStates = new();

    /// <summary>
    /// Bucket ordinals whose contents have advanced since their last durable
    /// write under the current layout. Only these are rewritten.
    /// </summary>
    private readonly HashSet<int> _dirtyBuckets = new();

    /// <summary>
    /// Slots of the current bucketed layout that must land before the legacy
    /// slot can be retired: together they hold every pin the legacy slot held.
    /// </summary>
    private readonly HashSet<int> _legacyDrainBuckets = new();

    /// <summary>
    /// Width-independent routing hash per consumer
    /// (<see cref="WalMaterialiserPinRouting.BucketHash"/>), cached so routing
    /// the whole shard to a new width is a single pass.
    /// </summary>
    private readonly Dictionary<string, uint> _hashes = new(StringComparer.Ordinal);

    /// <summary>The configured bucket count: the floor of the layout width.</summary>
    private int _configuredBuckets = 1;

    /// <summary>
    /// The layout width writes route by. One is the legacy single-slot layout
    /// persisted through the injected <see cref="IPersistentState{T}"/>.
    /// </summary>
    private int _bucketCount = 1;

    /// <summary>
    /// The width stamped into bucket zero by the next write of it. Equal to
    /// <see cref="_bucketCount"/> except in the legacy layout, where no bucket
    /// slot is written outside a relayout.
    /// </summary>
    private int _persistedWidth = 1;

    /// <summary>Number of bucket slots this activation has read, <c>[0, _readWidth)</c>.</summary>
    private int _readWidth = 1;

    /// <summary>
    /// Whether the legacy slot still holds pins that a bucketed layout has
    /// superseded. Once every slot in <see cref="_legacyDrainBuckets"/> has
    /// landed the legacy slot is emptied, so a consumer removed after the split
    /// is not resurrected from it by the next activation.
    /// </summary>
    private bool _legacyRetirePending;

    /// <summary>
    /// A pending relayout target width, or zero. While pending, every persist
    /// attempts the relayout first and normal slot writes wait for it.
    /// </summary>
    private int _relayoutTarget;

    /// <summary>Estimated serialised size of the whole in-memory pin map, in bytes.</summary>
    private long _estimatedBytes;

    /// <summary>
    /// Correction multiplier applied to <see cref="_estimatedBytes"/>, doubled
    /// each time a provider reports a payload as too large.
    /// </summary>
    private int _estimateScale = 1;

    /// <summary>Round-robin start slot for the next capped coalesced flush.</summary>
    private int _flushCursor;

    /// <summary>Durable-write failures since the last success.</summary>
    private int _consecutiveFlushFailures;

    /// <summary>
    /// <see cref="Environment.TickCount64"/> before which the coalesced tick
    /// does not retry a failed write. Zero when no backoff is armed.
    /// </summary>
    private long _nextFlushAttemptTickMs;

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
    /// writing to <c>1 / (1 + WriteAmortisationFactor)</c> - one tenth at the
    /// value below. Only gates the debounced flush; explicit durability points
    /// are never deferred by it.
    /// </summary>
    private const long WriteAmortisationFactor = 9;

    /// <summary>
    /// Creates the pin grain.
    /// </summary>
    /// <param name="context">The grain activation context.</param>
    /// <param name="state">The durable pin state (the legacy single slot).</param>
    /// <param name="options">Monitor used to read the flush interval and bucket floor.</param>
    /// <param name="logger">Optional logger.</param>
    /// <param name="pinStorage">Optional durable-storage handle for the bucketed
    /// layout. Without it the grain persists through <paramref name="state"/>
    /// exactly as every pre-bucketing build did and never splits.</param>
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
    IGrainContext IGrainBase.GrainContext => _context;

    private string GrainKey => _context.GrainId.Key.ToString();

    /// <summary>The size estimate with the oversize correction applied.</summary>
    private long EffectiveEstimate => _estimatedBytes * _estimateScale;

    /// <inheritdoc />
    async Task IGrainBase.OnActivateAsync(CancellationToken cancellationToken)
    {
        PrimeAdvanceArms();
        _configuredBuckets = WalMaterialiserPinRouting.ResolveBucketCount(_options);
        if (_pinStorage is null)
        {
            // No provider handle: the injected IPersistentState has already read
            // the single legacy slot, which is the whole of this shard's state.
            RecomputeEstimate();
            return;
        }

        // The injected IPersistentState has read the legacy slot. It stays
        // authoritative for any pin not re-reported since, so it is always part
        // of the union. Bucket zero records the width the store was last laid
        // out at; probe it first so a split store is read at its full width
        // whatever this host is configured with.
        //
        // Every read here is fail-closed: if any slot of the recorded layout
        // cannot be read, activation fails, exactly as it does when the legacy
        // slot itself cannot be read. A caller then sees the pin shard as
        // unavailable (the WAL GC logs its census as unavailable and retries)
        // instead of a map silently missing that slot's pins, which would raise
        // the trim floor. Without bucket zero the width is unknown, so no
        // narrower assumption is safe either.
        var legacyIds = SnapshotLegacyIds();
        var probe = await ReadSlotRawAsync(0);
        if (probe.Holder is not { } probeHolder)
        {
            throw new InvalidOperationException(
                $"WAL materialiser pin bucket 0 for '{GrainKey}' could not be read, so the persisted layout width is unknown.");
        }

        _bucketStates[0] = probeHolder;
        MergeSlotContents(probeHolder.State, layoutWidth: 0);
        var stamp = probeHolder.State.PersistedBucketCount;

        // A stamp of one means the store was narrowed back to the legacy slot;
        // zero means no bucketed layout was ever recorded, so the configured
        // count is the layout, exactly as in every pre-split build.
        _bucketCount = stamp >= 1 ? stamp : _configuredBuckets;
        _persistedWidth = _bucketCount;
        _readWidth = Math.Max(1, Math.Max(_configuredBuckets, _bucketCount));
        await ReadSlotsAsync(1, _readWidth, cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();

        MarkMisroutedConsumersDirty();
        RecomputeEstimate();

        var target = ResolveTargetWidth(
            _bucketCount, _configuredBuckets, EffectiveEstimate, allowNarrow: true);
        if (target == _bucketCount)
        {
            ScheduleLegacyRetirement(legacyIds);
            return;
        }

        if (_bucketCount == 1)
        {
            SwitchFromLegacy(target);
            return;
        }

        // The relayout rewrites every slot of the new layout from the merged
        // map, which carries every legacy pin; once it lands the legacy slot
        // can be retired.
        _legacyRetirePending = legacyIds is not null;

        _logger?.LogInformation(
            "WAL materialiser pin store for {GrainKey} is laid out across {PersistedBuckets} buckets; re-laying out across {TargetBuckets} (configured floor {ConfiguredBuckets}, estimated {EstimatedBytes} bytes).",
            GrainKey,
            _bucketCount,
            target,
            _configuredBuckets,
            EffectiveEstimate);
        _relayoutTarget = target;
        _dirty = true;
        try
        {
            await PersistAsync(MaterialiserPinCoalescedOutcome, PersistScope.All);
        }
        catch (Exception ex)
        {
            // Best effort. Bucket zero still records the old width until the
            // relayout has fully landed, so the next activation reads every slot
            // the relayout could not move, and every later persist retries it.
            _logger?.LogWarning(
                ex,
                "Re-laying out the WAL materialiser pin store for {GrainKey} failed; will retry on the next flush.",
                GrainKey);
        }
    }

    /// <summary>
    /// Resolves the layout width a shard should use: the current
    /// <paramref name="layoutWidth"/> unless the configured floor or the size
    /// estimate requires otherwise. Pure so the policy is directly testable.
    /// <list type="bullet">
    /// <item>From the legacy layout (width one) the result is the configured
    /// floor, doubled while the average slot would exceed half the target; a
    /// shard whose whole map is within the target stays legacy.</item>
    /// <item>From a bucketed layout the width only ever moves by powers of two,
    /// so every consumer's new slot is congruent to its old slot: it grows while
    /// the average slot would exceed half the target once it exceeds the
    /// target, is raised to at least the configured floor, and (only when
    /// <paramref name="allowNarrow"/>) halves while the halved layout stays at
    /// or above the floor and its average slot within half the target.</item>
    /// </list>
    /// </summary>
    /// <param name="layoutWidth">The current layout width (one for legacy).</param>
    /// <param name="configuredBuckets">The configured bucket floor.</param>
    /// <param name="estimatedBytes">Estimated serialised size of the shard's pins.</param>
    /// <param name="allowNarrow">Whether the width may shrink.</param>
    /// <returns>The target layout width.</returns>
    internal static int ResolveTargetWidth(int layoutWidth, int configuredBuckets, long estimatedBytes, bool allowNarrow)
    {
        var floor = Math.Max(1, configuredBuckets);
        var width = Math.Max(1, layoutWidth);
        if (width == 1)
        {
            if (floor == 1 && estimatedBytes <= TargetSlotBytes)
            {
                return 1;
            }

            width = floor;
            if (estimatedBytes / width > TargetSlotBytes || floor == 1)
            {
                width = Grow(width, estimatedBytes);
            }

            return width;
        }

        while (width < floor && width <= int.MaxValue / 2)
        {
            width *= 2;
        }

        if (estimatedBytes / width > TargetSlotBytes)
        {
            return Grow(width, estimatedBytes);
        }

        if (allowNarrow)
        {
            while (width % 2 == 0
                && width / 2 >= floor
                && estimatedBytes / (width / 2) <= TargetSlotBytes / 2)
            {
                width /= 2;
            }
        }

        return width;

        static int Grow(int from, long bytes)
        {
            var grown = from;
            while (bytes / grown > TargetSlotBytes / 2 && grown * 2 <= MaxAutoBuckets)
            {
                grown *= 2;
            }

            return grown;
        }
    }

    /// <summary>
    /// Moves from the legacy single slot to a bucketed layout of
    /// <paramref name="target"/> slots. No relayout write is needed: the legacy
    /// slot is read by every activation, so each pin stays covered by it until
    /// its bucket lands, and every bucket is marked dirty so the whole map is
    /// drained into the new layout by the ordinary bounded flushes.
    /// </summary>
    private void SwitchFromLegacy(int target)
    {
        _logger?.LogInformation(
            "WAL materialiser pin store for {GrainKey} is moving from the single legacy slot to {TargetBuckets} buckets (estimated {EstimatedBytes} bytes, configured floor {ConfiguredBuckets}).",
            GrainKey,
            target,
            EffectiveEstimate,
            _configuredBuckets);
        _bucketCount = target;
        _persistedWidth = target;
        _legacyDrainBuckets.Clear();
        for (var bucket = 0; bucket < target; bucket++)
        {
            _dirtyBuckets.Add(bucket);
            _legacyDrainBuckets.Add(bucket);
        }

        _legacyRetirePending = true;
        _dirty = true;
    }

    /// <summary>
    /// Captures the consumer ids the legacy slot held at activation, before any
    /// bucket is merged over them, or <see langword="null"/> when it held none.
    /// </summary>
    private HashSet<string>? SnapshotLegacyIds()
    {
        var legacy = _state.State;
        if (legacy.Pins.Count == 0 && legacy.Offsets.Count == 0)
        {
            return null;
        }

        var ids = new HashSet<string>(legacy.Pins.Keys, StringComparer.Ordinal);
        ids.UnionWith(legacy.Offsets.Keys);
        return ids;
    }

    /// <summary>
    /// Under a bucketed layout, a non-empty legacy slot means a move out of it
    /// never finished: the slot is only emptied once every pin it held has
    /// landed in its bucket. Every one of its pins is therefore still live and
    /// is kept, and the buckets that receive them are rewritten so the legacy
    /// slot can then be retired.
    /// </summary>
    private void ScheduleLegacyRetirement(HashSet<string>? legacyIds)
    {
        if (legacyIds is null || _bucketCount < 2)
        {
            return;
        }

        foreach (var consumerId in legacyIds)
        {
            var slot = SlotOf(consumerId, _bucketCount);
            _dirtyBuckets.Add(slot);
            _legacyDrainBuckets.Add(slot);
        }

        _legacyRetirePending = true;
        _dirty = true;
    }

    /// <summary>
    /// Empties the legacy slot once the bucketed layout holds everything it
    /// held. Only the legacy blob is emptied: the in-memory map, which is also
    /// the injected state's <c>State</c>, is swapped back straight after the
    /// write, which is safe because the grain is non-reentrant. A failure is
    /// logged and not retried by this activation; the next activation finds
    /// the slot non-empty and schedules the retirement again.
    /// </summary>
    private async Task TryRetireLegacySlotAsync()
    {
        if (!_legacyRetirePending
            || _legacyDrainBuckets.Count != 0
            || _bucketCount < 2
            || _relayoutTarget != 0
            || WidthStampPending())
        {
            return;
        }

        _legacyRetirePending = false;
        var live = _state.State;
        _state.State = new WalMaterialiserPinState();
        try
        {
            await WriteLegacySlotAsync();
            _logger?.LogInformation(
                "WAL materialiser pin store for {GrainKey} retired its legacy slot; every pin now lives in its {Buckets} buckets.",
                GrainKey,
                _bucketCount);
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(
                ex,
                "Retiring the legacy WAL materialiser pin slot for {GrainKey} failed; the next activation retries it.",
                GrainKey);
        }
        finally
        {
            _state.State = live;
        }
    }

    /// <summary>
    /// Reads bucket slots <c>[from, to)</c> concurrently and merges them,
    /// throwing if any cannot be read. Activation must fail rather than serve a
    /// partial map: the WAL GC's floor is a minimum over the pins it is given,
    /// so an omitted pin raises the floor and could let the GC reclaim WAL a
    /// dormant leaf still needs.
    /// </summary>
    private async Task ReadSlotsAsync(int from, int to, CancellationToken cancellationToken)
    {
        for (var start = from; start < to; start += SlotReadConcurrency)
        {
            var end = Math.Min(to, start + SlotReadConcurrency);
            var reads = new Task<(int Bucket, GrainState<WalMaterialiserPinState>? Holder)>[end - start];
            for (var bucket = start; bucket < end; bucket++)
            {
                reads[bucket - start] = ReadSlotRawAsync(bucket);
            }

            var results = await Task.WhenAll(reads);
            foreach (var (bucket, holder) in results)
            {
                if (holder is null)
                {
                    throw new InvalidOperationException(
                        $"WAL materialiser pin bucket {bucket} for '{GrainKey}' could not be read.");
                }

                _bucketStates[bucket] = holder;
                MergeSlotContents(holder.State, layoutWidth: 0);
            }

            cancellationToken.ThrowIfCancellationRequested();
        }
    }

    /// <summary>
    /// Reads one bucket slot without touching grain state, so reads can be
    /// issued concurrently. A failed read is logged and yields a null holder;
    /// every caller treats that as fatal to the operation, because omitting a
    /// slot's pins would raise the WAL GC floor.
    /// </summary>
    private async Task<(int Bucket, GrainState<WalMaterialiserPinState>? Holder)> ReadSlotRawAsync(int bucket)
    {
        var holder = new GrainState<WalMaterialiserPinState>(new WalMaterialiserPinState());
        try
        {
            await _pinStorage!.ReadStateAsync(WalMaterialiserPinRouting.BucketStateName(bucket), _context.GrainId, holder);
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(
                ex,
                "Reading WAL materialiser pin bucket {Bucket} for {GrainKey} failed.",
                bucket,
                GrainKey);
            return (bucket, null);
        }

        holder.State ??= new WalMaterialiserPinState();
        return (bucket, holder);
    }

    /// <summary>
    /// Monotonic-max merges one slot's durable contents into memory. When
    /// <paramref name="layoutWidth"/> is at least two, every merged consumer's
    /// own bucket under that width is marked dirty, because the contents are
    /// about to be rewritten and must not be dropped from the durable store.
    /// </summary>
    private void MergeSlotContents(WalMaterialiserPinState contents, int layoutWidth)
    {
        foreach (var pin in contents.Pins)
        {
            if (MergeLoaded(pin.Key, pin.Value) && layoutWidth >= 2)
            {
                MarkBucketDirty(pin.Key, layoutWidth);
            }
        }

        foreach (var offset in contents.Offsets)
        {
            if (MergeLoadedOffset(offset.Key, offset.Value) && layoutWidth >= 2)
            {
                MarkBucketDirty(offset.Key, layoutWidth);
            }
        }
    }

    /// <summary>
    /// Marks dirty the own bucket of any consumer that was read from a slot it
    /// does not route to under the current layout (a slot written under an
    /// earlier layout that was never trimmed), so the consumer is persisted to
    /// the slot later activations will look for it in.
    /// </summary>
    private void MarkMisroutedConsumersDirty()
    {
        if (_bucketCount < 2)
        {
            return;
        }

        foreach (var (bucket, holder) in _bucketStates)
        {
            foreach (var consumerId in holder.State.Pins.Keys)
            {
                if (SlotOf(consumerId, _bucketCount) != bucket)
                {
                    MarkBucketDirty(consumerId, _bucketCount);
                }
            }
        }
    }

    /// <summary>
    /// Monotonic-max merge of a pin loaded from durable storage into the
    /// in-memory map, without marking anything dirty (it is already durable).
    /// Returns <see langword="true"/> when memory changed.
    /// </summary>
    private bool MergeLoaded(string consumerId, HybridLogicalClock frontier)
    {
        if (_state.State.Pins.TryGetValue(consumerId, out var existing))
        {
            if (frontier > existing)
            {
                _state.State.Pins[consumerId] = frontier;
                return true;
            }

            return false;
        }

        _state.State.Pins[consumerId] = frontier;
        _estimatedBytes += EstimatePinEntryBytes(consumerId);
        return true;
    }

    /// <summary>
    /// Monotonic-max merge of a checkpoint offset loaded from durable storage.
    /// Returns <see langword="true"/> when memory changed.
    /// </summary>
    private bool MergeLoadedOffset(string consumerId, long offset)
    {
        if (_state.State.Offsets.TryGetValue(consumerId, out var existing))
        {
            if (offset > existing)
            {
                _state.State.Offsets[consumerId] = offset;
                return true;
            }

            return false;
        }

        _state.State.Offsets[consumerId] = offset;
        _estimatedBytes += EstimateOffsetEntryBytes(consumerId);
        return true;
    }

    /// <summary>
    /// Estimated serialised size of one <see cref="WalMaterialiserPinState.Pins"/>
    /// entry. Deliberately generous: storage providers typically persist the
    /// state as UTF-16 JSON with type annotations, which is what the per-entry
    /// overhead models.
    /// </summary>
    internal static long EstimatePinEntryBytes(string consumerId)
        => 2L * (consumerId.Length + PinEntryOverheadChars);

    /// <summary>Estimated serialised size of one <see cref="WalMaterialiserPinState.Offsets"/> entry.</summary>
    internal static long EstimateOffsetEntryBytes(string consumerId)
        => 2L * (consumerId.Length + OffsetEntryOverheadChars);

    /// <summary>Recomputes <see cref="_estimatedBytes"/> from the in-memory map.</summary>
    private void RecomputeEstimate()
    {
        long total = 0;
        foreach (var consumerId in _state.State.Pins.Keys)
        {
            total += EstimatePinEntryBytes(consumerId);
        }

        foreach (var consumerId in _state.State.Offsets.Keys)
        {
            total += EstimateOffsetEntryBytes(consumerId);
        }

        _estimatedBytes = total;
    }

    /// <inheritdoc />
    public async Task ReportAsync(string consumerId, HybridLogicalClock frontier)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerId);
        if (Merge(consumerId, frontier, checkpointOffset: NoOffset))
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
        List<uint>? changedHashes = null;
        for (var i = 0; i < reports.Count; i++)
        {
            var report = reports[i];
            ArgumentException.ThrowIfNullOrWhiteSpace(report.ConsumerId);
            if (Merge(report.ConsumerId, report.Frontier, report.CheckpointOffset))
            {
                (changedHashes ??= new List<uint>(reports.Count)).Add(HashOf(report.ConsumerId));
            }
        }

        if (changedHashes is null)
        {
            return;
        }

        // Birth path: persist through durably (awaited) so the block pin is
        // durable before the caller lets the new leaf's data become reachable.
        // Only the seeded consumers' slots are written, so the cost of a seed
        // is bounded by a slot rather than by the shard. While the store is
        // failing persistently the seed fails fast instead: the pin is already
        // merged in memory (which is what the WAL GC reads) and stays dirty, so
        // the next successful flush makes it durable.
        ThrowIfFailingFast("seed");
        await PersistAsync(MaterialiserPinBirthOutcome, PersistScope.Consumers, changedHashes);
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

        // Resolve the routing hash before removing: once the consumer is gone
        // from the in-memory map its bucket still has to be rewritten to make
        // the removal durable.
        var hash = HashOf(consumerId);
        var removedPin = _state.State.Pins.Remove(consumerId);
        var removedOffset = _state.State.Offsets.Remove(consumerId);
        if (!removedPin && !removedOffset)
        {
            _hashes.Remove(consumerId);
            return;
        }

        if (removedPin)
        {
            _estimatedBytes -= EstimatePinEntryBytes(consumerId);
        }

        if (removedOffset)
        {
            _estimatedBytes -= EstimateOffsetEntryBytes(consumerId);
        }

        _dirty = true;
        if (_bucketCount > 1)
        {
            _dirtyBuckets.Add((int)(hash % (uint)_bucketCount));
        }

        _hashes.Remove(consumerId);
        ThrowIfFailingFast("removal");
        await PersistAsync(MaterialiserPinCoalescedOutcome, PersistScope.Consumers, new[] { hash });
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
        _hashes.Clear();
        _estimatedBytes = 0;
        _dirty = true;
        var bucketed = _bucketCount > 1;
        if (bucketed)
        {
            // Every bucket must be rewritten empty, and the legacy slot cleared
            // too. Clear is only reached on tree deletion, where retaining a
            // stale pin would keep the deleted tree's WAL pinned forever. A
            // pending relayout runs first and rewrites every slot of its layout.
            for (var bucket = 0; bucket < _bucketCount; bucket++)
            {
                _dirtyBuckets.Add(bucket);
            }
        }

        await PersistAsync(MaterialiserPinCoalescedOutcome, PersistScope.All);
        if (bucketed)
        {
            await ClearLegacySlotAsync();
        }
    }

    /// <summary>
    /// Empties the legacy single-slot blob on tree deletion. Under a bucketed
    /// layout the legacy slot is otherwise only rewritten by
    /// <see cref="TryRetireLegacySlotAsync"/>, once every bucket holding its
    /// pins has landed, and by a narrowing back to the legacy layout.
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
                GrainKey);
        }
    }

    /// <inheritdoc />
    async Task IGrainBase.OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        _flushTimer?.Dispose();
        _flushTimer = null;

        // A conflicted activation's cached ETag is stale, so a final flush can
        // only fail again; the next activation reloads the row (issue #3572).
        if (!_dirty || _stateConflicted)
        {
            return;
        }

        try
        {
            await PersistAsync(MaterialiserPinCoalescedOutcome, PersistScope.All);
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
                GrainKey);
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
        var frontierAdvanced = false;

        var hadPin = _state.State.Pins.TryGetValue(consumerId, out var existing);
        if (!hadPin || frontier > existing)
        {
            _state.State.Pins[consumerId] = frontier;
            changed = true;
            frontierAdvanced = true;
            if (!hadPin)
            {
                _estimatedBytes += EstimatePinEntryBytes(consumerId);
            }
        }

        var hadOffset = _state.State.Offsets.TryGetValue(consumerId, out var existingOffset);

        // An absent offset is the same floor as the no-offset sentinel, so a
        // brand-new consumer reporting NoOffset has not moved anything the WAL
        // GC can use even though it does mark the pin dirty below.
        var offsetAdvanced = checkpointOffset > (hadOffset ? existingOffset : NoOffset);

        if (!hadOffset || checkpointOffset > existingOffset)
        {
            _state.State.Offsets[consumerId] = checkpointOffset;
            changed = true;
            if (!hadOffset)
            {
                _estimatedBytes += EstimateOffsetEntryBytes(consumerId);
            }
        }

        // Classify what this merge actually moved, naming both axes rather than
        // the first one that happened to move. Offset advancement is the
        // quantity that lets the GC offset floor move, and until issue #3163 an
        // offset advance masked whatever the frontier did alongside it.
        RecordPinAdvance(offsetAdvanced, frontierAdvanced);

        if (changed)
        {
            _dirty = true;
            MarkBucketDirty(consumerId, _bucketCount);
        }

        return changed;
    }

    /// <summary>
    /// Records one merged pin report on
    /// <see cref="LatticeMetrics.MaterialiserPinAdvances"/>, tagged with the
    /// <see cref="MaterialiserPinAdvanceOutcome"/> naming <b>both</b> axes -
    /// never the first one that happened to move. Tag pairs are materialised
    /// once per activation, so the hot path adds no allocation.
    /// </summary>
    private void RecordPinAdvance(bool offsetAdvanced, bool frontierAdvanced)
        => RecordPinAdvance(
            (offsetAdvanced, frontierAdvanced) switch
            {
                (true, true) => MaterialiserPinAdvanceOutcome.Both,
                (true, false) => MaterialiserPinAdvanceOutcome.OffsetOnly,
                (false, true) => MaterialiserPinAdvanceOutcome.FrontierOnly,
                (false, false) => MaterialiserPinAdvanceOutcome.None,
            });

    /// <summary>
    /// Adds <paramref name="delta"/> to the advance counter under
    /// <paramref name="outcome"/>. Called with one to report a real merge and
    /// with zero to prime an arm, so both paths carry an identical tag tuple and
    /// cannot split the series between them.
    /// </summary>
    private void RecordPinAdvance(MaterialiserPinAdvanceOutcome outcome, long delta = 1)
    {
        _ = TreeTag;
        LatticeMetrics.MaterialiserPinAdvances.Add(
            delta,
            _treeTagPair,
            ClassifyPinAdvance(outcome),
            _tenantTagPair);
    }

    /// <summary>
    /// Maps a <see cref="MaterialiserPinAdvanceOutcome"/> onto its pre-allocated
    /// <see cref="LatticeMetrics.TagOutcome"/> tag. Held exhaustively armed by
    /// the instrumented-enum gate, so an outcome added later cannot be reported
    /// under another outcome's arm or under none.
    /// </summary>
    private static KeyValuePair<string, object?> ClassifyPinAdvance(MaterialiserPinAdvanceOutcome outcome)
        => outcome switch
        {
            MaterialiserPinAdvanceOutcome.None => LatticeMetrics.OutcomePinNoAdvance,
            MaterialiserPinAdvanceOutcome.FrontierOnly => LatticeMetrics.OutcomePinFrontierOnly,
            MaterialiserPinAdvanceOutcome.OffsetOnly => LatticeMetrics.OutcomePinOffsetOnly,
            MaterialiserPinAdvanceOutcome.Both => LatticeMetrics.OutcomePinBothAdvanced,
            _ => throw new ArgumentOutOfRangeException(
                nameof(outcome), outcome, "Unarmed leaf-materialiser pin advance outcome."),
        };

    /// <summary>
    /// Zero-primes every <see cref="MaterialiserPinAdvanceOutcome"/> arm for this
    /// shard's tree, so an absent series means no pin grain is live for the tree
    /// on this silo rather than that the arm's condition never occurred.
    /// <para>
    /// Without this, the arm a reader most wants is the one least likely to
    /// exist: a consumer whose frontier never moves emits no <c>frontier_only</c>
    /// and no <c>both</c> at all, and an absent series is byte-identical at the
    /// query to a build that was never deployed. Adding zero creates the series
    /// without perturbing any value.
    /// </para>
    /// </summary>
    private void PrimeAdvanceArms()
    {
        foreach (var outcome in Enum.GetValues<MaterialiserPinAdvanceOutcome>())
        {
            RecordPinAdvance(outcome, delta: 0);
        }
    }

    /// <summary>
    /// Marks the bucket owning <paramref name="consumerId"/> under
    /// <paramref name="width"/> as needing a durable write. A no-op in the
    /// legacy single-slot layout, where the whole shard is one blob and
    /// <see cref="_dirty"/> alone drives the write.
    /// </summary>
    private void MarkBucketDirty(string consumerId, int width)
    {
        if (width > 1)
        {
            _dirtyBuckets.Add(SlotOf(consumerId, width));
        }
    }

    /// <summary>Returns the cached routing hash of <paramref name="consumerId"/>.</summary>
    private uint HashOf(string consumerId)
    {
        if (!_hashes.TryGetValue(consumerId, out var hash))
        {
            hash = WalMaterialiserPinRouting.BucketHash(consumerId);
            _hashes[consumerId] = hash;
        }

        return hash;
    }

    /// <summary>
    /// Resolves the bucket owning <paramref name="consumerId"/> under
    /// <paramref name="width"/>; identical to
    /// <see cref="WalMaterialiserPinRouting.BucketOf"/>.
    /// </summary>
    private int SlotOf(string consumerId, int width)
        => width <= 1 ? 0 : (int)(HashOf(consumerId) % (uint)width);

    /// <summary>
    /// Either schedules a coalesced durable flush (when the flush interval is
    /// positive and a grain timer can be armed) or persists synchronously.
    /// The synchronous fallback covers two cases: coalescing disabled
    /// (interval &lt;= 0) and no grain runtime (a unit-test harness whose
    /// substituted context cannot register a timer), so a report is never
    /// silently left unpersisted with no timer to drain it. A synchronous
    /// persist is skipped, leaving the advance dirty, while the store is failing
    /// persistently.
    /// </summary>
    private async Task ScheduleOrFlushAsync()
    {
        var intervalMs = _options.Get(string.Empty).WalMaterialiserPinFlushIntervalMs;
        if (intervalMs > 0 && TryArmFlushTimer(intervalMs))
        {
            // Timer armed: leave _dirty set; the timer tick drains it.
            return;
        }

        if (IsFailingFast(Environment.TickCount64))
        {
            return;
        }

        await PersistAsync(MaterialiserPinCoalescedOutcome, PersistScope.All);
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
                GrainKey);
            return false;
        }
    }

    private async Task OnFlushTimerTickAsync(CancellationToken cancellationToken)
    {
        if (!_dirty)
        {
            return;
        }

        var nowTickMs = Environment.TickCount64;
        if (nowTickMs < _nextFlushAttemptTickMs)
        {
            // Backoff: a recent write failed. Retrying on every tick would spend
            // the non-reentrant grain on a write that is failing anyway; the
            // in-memory pins stay current and dirty, and a staler durable pin
            // only retains more WAL.
            return;
        }

        if (ShouldDeferCoalescedFlush(nowTickMs, _lastWriteCompletedTickMs, _lastWriteDurationMs))
        {
            // Amortisation: the previous write has not yet "paid for itself" in
            // queue-draining time. Stay dirty and let a later tick (or an
            // explicit durability point) persist the accumulated advances.
            return;
        }

        try
        {
            await PersistAsync(MaterialiserPinCoalescedOutcome, PersistScope.Tick);
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(
                ex,
                "Coalesced WAL materialiser pin flush failed for {GrainKey} ({ConsecutiveFailures} consecutive); will retry in {BackoffMs} ms.",
                GrainKey,
                _consecutiveFlushFailures,
                ComputeFlushBackoffMs(_consecutiveFlushFailures));
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
    /// Returns the backoff armed after <paramref name="consecutiveFailures"/>
    /// durable-write failures in a row: <see cref="InitialFlushBackoffMs"/>
    /// doubling per failure, capped at <see cref="MaxFlushBackoffMs"/>, and zero
    /// when nothing has failed.
    /// </summary>
    /// <param name="consecutiveFailures">Failures since the last success.</param>
    internal static long ComputeFlushBackoffMs(int consecutiveFailures)
    {
        if (consecutiveFailures <= 0)
        {
            return 0;
        }

        var shift = Math.Min(consecutiveFailures - 1, 16);
        return Math.Min(MaxFlushBackoffMs, InitialFlushBackoffMs << shift);
    }

    /// <summary>
    /// True when <paramref name="exception"/>, or any exception it wraps,
    /// reports that the persisted payload exceeded the provider's size limit
    /// (Azure Table's <c>Data too large to write</c>, Cosmos DB's
    /// <c>Request size is too large</c>, and similar).
    /// </summary>
    internal static bool IsOversizeFailure(Exception? exception)
    {
        for (var depth = 0; exception is not null && depth < 16; depth++)
        {
            var message = exception.Message;
            if (message.Contains("too large", StringComparison.OrdinalIgnoreCase)
                || message.Contains("TooLarge", StringComparison.Ordinal))
            {
                return true;
            }

            if (exception is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions)
                {
                    if (IsOversizeFailure(inner))
                    {
                        return true;
                    }
                }

                return false;
            }

            exception = exception.InnerException;
        }

        return false;
    }

    private bool IsFailingFast(long nowTickMs)
        => _consecutiveFlushFailures >= FailFastThreshold && nowTickMs < _nextFlushAttemptTickMs;

    private void ThrowIfFailingFast(string operation)
    {
        if (IsFailingFast(Environment.TickCount64))
        {
            throw new InvalidOperationException(
                $"The durable WAL materialiser pin store for '{GrainKey}' has failed {_consecutiveFlushFailures} consecutive writes; the {operation} is held in memory and retried after the backoff.");
        }
    }

    private void RecordFlushFailure(Exception exception)
    {
        _consecutiveFlushFailures++;
        _nextFlushAttemptTickMs = Environment.TickCount64 + ComputeFlushBackoffMs(_consecutiveFlushFailures);
        if (IsOversizeFailure(exception) && _estimateScale < MaxEstimateScale)
        {
            _estimateScale *= 2;
            _logger?.LogWarning(
                "The durable WAL materialiser pin store for {GrainKey} rejected a write as too large; correcting the size estimate by {EstimateScale}x so the next attempt splits the store further.",
                GrainKey,
                _estimateScale);
        }
    }

    private void RecordFlushSuccess()
    {
        _consecutiveFlushFailures = 0;
        _nextFlushAttemptTickMs = 0;
    }

    /// <summary>Which pending slots a persist writes.</summary>
    private enum PersistScope
    {
        /// <summary>Every dirty slot.</summary>
        All,

        /// <summary>At most <see cref="MaxSlotsPerCoalescedFlush"/> dirty slots, round-robin.</summary>
        Tick,

        /// <summary>The dirty slots owning the supplied routing hashes.</summary>
        Consumers,
    }

    /// <summary>
    /// Persists pending advances within <paramref name="scope"/>, recording the
    /// write cost for amortisation and the outcome for backoff. The grain is
    /// non-reentrant; the in-flight guard only covers the awaits.
    /// </summary>
    private async Task PersistAsync(string outcome, PersistScope scope, IReadOnlyList<uint>? scopeHashes = null)
    {
        if (_stateConflicted)
        {
            throw GrainStateWriteFaults.ConflictedActivation(StateWriteGrainType, GrainKey);
        }

        while (_flushInFlight)
        {
            await Task.Yield();
        }

        _flushInFlight = true;
        var startedTickMs = Environment.TickCount64;
        try
        {
            if (await PersistCoreAsync(scope, scopeHashes))
            {
                LatticeMetrics.MaterialiserPinDurableWrites.Add(
                    1,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeTag),
                    new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, outcome),
                    LatticeTenantLabel.ForTree(TreeTag));
            }

            RecordFlushSuccess();
        }
        catch (Exception ex)
        {
            RecordFlushFailure(ex);
            throw;
        }
        finally
        {
            _flushInFlight = false;
            RefreshDirty();

            // Record what this write cost so the coalesced flush can amortise
            // against it. A failed write is timed too: it consumed the same
            // grain time and the retry should back off equally.
            var completedTickMs = Environment.TickCount64;
            _lastWriteDurationMs = Math.Max(0, completedTickMs - startedTickMs);
            _lastWriteCompletedTickMs = completedTickMs;
        }
    }

    private void RefreshDirty()
    {
        if (_bucketCount > 1 || _relayoutTarget != 0)
        {
            _dirty = _dirtyBuckets.Count > 0 || _relayoutTarget != 0;
        }
    }

    /// <summary>
    /// Grows the layout if the shard has outgrown it, runs any pending
    /// relayout, then writes the in-scope pending slots. Returns whether any
    /// durable write was issued.
    /// </summary>
    private async Task<bool> PersistCoreAsync(PersistScope scope, IReadOnlyList<uint>? scopeHashes)
    {
        var wrote = false;
        var target = ResolveTargetWidth(_bucketCount, _configuredBuckets, EffectiveEstimate, allowNarrow: false);
        if (_pinStorage is not null && target > _bucketCount && _relayoutTarget < target)
        {
            target = ResolveTargetWidth(_bucketCount, _configuredBuckets, EffectiveEstimate, allowNarrow: false);
            if (target > _bucketCount)
            {
                if (_bucketCount == 1)
                {
                    SwitchFromLegacy(target);
                }
                else
                {
                    _relayoutTarget = target;
                }
            }
        }

        if (_relayoutTarget != 0)
        {
            await RelayoutAsync(_relayoutTarget);
            wrote = true;
        }

        if (_bucketCount <= 1)
        {
            if (!_dirty)
            {
                return wrote;
            }

            await WriteLegacySlotAsync();
            _dirty = false;
            return true;
        }

        var slots = SelectSlots(scope, scopeHashes);
        if (slots.Count != 0)
        {
            await WriteSlotsAsync(slots);
            wrote = true;
        }

        await TryRetireLegacySlotAsync();
        return wrote;
    }

    /// <summary>
    /// Writes the legacy single slot through the injected
    /// <see cref="IPersistentState{T}"/>, translating an optimistic-concurrency
    /// (ETag) conflict (issue #3572). The write may have landed while the cached
    /// ETag went stale, so every later write here would fail the same way: the
    /// activation marks itself conflicted, deactivates so the next call reloads
    /// the row, and fails later persists fast. A lost advance only leaves the
    /// durable pin staler, which is GC-safe, and a re-sent seed that already
    /// landed merges as a no-op.
    /// </summary>
    private async Task WriteLegacySlotAsync()
    {
        try
        {
            await _state.WriteStateAsync();
        }
        catch (Exception ex) when (GrainStateWriteFaults.IsConflict(ex))
        {
            _stateConflicted = true;
            _logger?.LogWarning(
                ex,
                "WAL materialiser pin write for {GrainKey} lost an optimistic-concurrency check (the write may have landed); deactivating so the next call reloads durable state.",
                GrainKey);
            this.DeactivateOnIdle();
            throw new LatticeStateWriteFailedException(StateWriteGrainType, GrainKey, ex, conflict: true);
        }
    }

    /// <summary>
    /// True when bucket zero does not yet record <see cref="_persistedWidth"/>.
    /// Bucket zero is the only slot a later activation probes to learn the
    /// width, so it is forced into any batch until the width has landed:
    /// otherwise, if no consumer ever hashed into it, the width would never be
    /// recorded and a later activation could read too narrow a range.
    /// </summary>
    private bool WidthStampPending()
        => !_bucketStates.TryGetValue(0, out var widthSlot)
            || widthSlot.State is null
            || widthSlot.State.PersistedBucketCount != _persistedWidth;

    /// <summary>Selects the slots a persist of <paramref name="scope"/> writes.</summary>
    private List<int> SelectSlots(PersistScope scope, IReadOnlyList<uint>? scopeHashes)
    {
        var slots = new List<int>();
        var width = _bucketCount;
        var stampPending = WidthStampPending();
        switch (scope)
        {
            case PersistScope.All:
                if (_dirtyBuckets.Count == 0 && !stampPending)
                {
                    return slots;
                }

                slots.AddRange(_dirtyBuckets);
                break;

            case PersistScope.Consumers:
                if (scopeHashes is not null)
                {
                    foreach (var hash in scopeHashes)
                    {
                        var slot = (int)(hash % (uint)width);
                        if (_dirtyBuckets.Contains(slot) && !slots.Contains(slot))
                        {
                            slots.Add(slot);
                        }
                    }
                }

                if (slots.Count == 0)
                {
                    return slots;
                }

                break;

            case PersistScope.Tick:
                if (_dirtyBuckets.Count == 0 && !stampPending)
                {
                    return slots;
                }

                var ordered = new List<int>(_dirtyBuckets);
                var cursor = _flushCursor % width;
                ordered.Sort((a, b) => ((a - cursor + width) % width).CompareTo((b - cursor + width) % width));
                var budget = MaxSlotsPerCoalescedFlush - (stampPending && !_dirtyBuckets.Contains(0) ? 1 : 0);
                for (var i = 0; i < ordered.Count && slots.Count < budget; i++)
                {
                    slots.Add(ordered[i]);
                }

                if (slots.Count > 0)
                {
                    _flushCursor = (slots[^1] + 1) % width;
                }

                break;
        }

        if (stampPending && !slots.Contains(0))
        {
            slots.Add(0);
        }

        // Bucket zero first, so the width stamp is issued in the first group.
        slots.Sort();
        return slots;
    }

    /// <summary>
    /// Writes <paramref name="slots"/> from current memory under the current
    /// layout, <see cref="SlotWriteConcurrency"/> at a time. A slot is marked
    /// clean only once its write has landed.
    /// </summary>
    private async Task WriteSlotsAsync(List<int> slots)
    {
        await EnsureHoldersAsync(slots);
        var slices = BuildSlices(_bucketCount, slots, _persistedWidth);
        await WriteGroupsAsync(slots, slices);
        foreach (var slot in slots)
        {
            _dirtyBuckets.Remove(slot);
            _legacyDrainBuckets.Remove(slot);
        }
    }

    /// <summary>
    /// Writes each slot's slice in groups of <see cref="SlotWriteConcurrency"/>,
    /// stopping at the first failing group.
    /// </summary>
    private async Task WriteGroupsAsync(IReadOnlyList<int> slots, IReadOnlyDictionary<int, WalMaterialiserPinState> slices)
    {
        for (var start = 0; start < slots.Count; start += SlotWriteConcurrency)
        {
            var end = Math.Min(slots.Count, start + SlotWriteConcurrency);
            var writes = new Task[end - start];
            for (var i = start; i < end; i++)
            {
                writes[i - start] = WriteBucketAsync(slots[i], slices[slots[i]]);
            }

            await Task.WhenAll(writes);
        }
    }

    /// <summary>
    /// Ensures a cached holder (and therefore an ETag) exists for every slot in
    /// <paramref name="slots"/>, re-reading any that are missing. Every slot of
    /// the recorded layout was read and merged at activation, so a missing
    /// holder is either one evicted by a failed write or a slot outside the
    /// recorded layout (stale content from an earlier, wider layout); neither
    /// is merged, only its ETag is taken.
    /// </summary>
    private async Task EnsureHoldersAsync(IEnumerable<int> slots)
    {
        var missing = new List<int>();
        foreach (var slot in slots)
        {
            if (!_bucketStates.ContainsKey(slot))
            {
                missing.Add(slot);
            }
        }

        for (var start = 0; start < missing.Count; start += SlotReadConcurrency)
        {
            var end = Math.Min(missing.Count, start + SlotReadConcurrency);
            var reads = new Task<(int Bucket, GrainState<WalMaterialiserPinState>? Holder)>[end - start];
            for (var i = start; i < end; i++)
            {
                reads[i - start] = ReadSlotRawAsync(missing[i]);
            }

            var results = await Task.WhenAll(reads);
            foreach (var (bucket, holder) in results)
            {
                if (holder is null)
                {
                    throw new InvalidOperationException(
                        $"WAL materialiser pin bucket {bucket} could not be read before writing.");
                }

                _bucketStates[bucket] = holder;
            }
        }
    }

    /// <summary>
    /// Builds the slices for <paramref name="slots"/> under
    /// <paramref name="width"/> in a single pass over the in-memory map, each
    /// stamped with <paramref name="stamp"/>.
    /// </summary>
    private Dictionary<int, WalMaterialiserPinState> BuildSlices(int width, IEnumerable<int> slots, int stamp)
    {
        var slices = new Dictionary<int, WalMaterialiserPinState>();
        foreach (var slot in slots)
        {
            slices[slot] = new WalMaterialiserPinState { PersistedBucketCount = stamp };
        }

        foreach (var pin in _state.State.Pins)
        {
            if (slices.TryGetValue(SlotOf(pin.Key, width), out var slice))
            {
                slice.Pins[pin.Key] = pin.Value;
            }
        }

        foreach (var offset in _state.State.Offsets)
        {
            if (slices.TryGetValue(SlotOf(offset.Key, width), out var slice))
            {
                slice.Offsets[offset.Key] = offset.Value;
            }
        }

        return slices;
    }

    /// <summary>
    /// Re-lays the bucketed store out from the current width to
    /// <paramref name="target"/> in an order that is crash-safe at every step:
    /// <list type="bullet">
    /// <item><b>Growth</b> writes the new slots <c>[from, target)</c> first
    /// (unread under the old width), then bucket zero with the new width, then
    /// trims the old slots <c>[1, from)</c>. Because <paramref name="target"/>
    /// is a power-of-two multiple of the old width, a consumer leaving old
    /// bucket zero lands in a slot at or above <c>from</c>, which is already
    /// written by the time bucket zero drops it.</item>
    /// <item><b>Narrowing</b> writes the new slots <c>[1, target)</c> (each a
    /// superset of the old slot of the same ordinal), then bucket zero with the
    /// new width; the old slots above <c>target</c> are simply no longer
    /// read.</item>
    /// <item><b>Narrowing to the legacy slot</b> writes the full map to the
    /// legacy slot, then records a width of one in bucket zero.</item>
    /// </list>
    /// A failure at any step leaves bucket zero naming a layout whose slots
    /// together still hold every pin, and the relayout stays pending.
    /// </summary>
    private async Task RelayoutAsync(int target)
    {
        var from = _bucketCount;
        if (from == target)
        {
            _relayoutTarget = 0;
            return;
        }

        await EnsureHoldersAsync(Enumerable.Range(0, Math.Max(from, Math.Max(target, _readWidth))));

        if (target == 1)
        {
            await WriteLegacySlotAsync();
            var emptyWidth = new Dictionary<int, WalMaterialiserPinState>
            {
                [0] = new WalMaterialiserPinState { PersistedBucketCount = 1 },
            };
            await WriteGroupsAsync(new[] { 0 }, emptyWidth);
        }
        else
        {
            var all = Enumerable.Range(0, target).ToArray();
            var slices = BuildSlices(target, all, target);
            if (target > from)
            {
                await WriteGroupsAsync(Enumerable.Range(from, target - from).ToArray(), slices);
                await WriteGroupsAsync(new[] { 0 }, slices);
                await WriteGroupsAsync(Enumerable.Range(1, from - 1).ToArray(), slices);
            }
            else
            {
                await WriteGroupsAsync(Enumerable.Range(1, target - 1).ToArray(), slices);
                await WriteGroupsAsync(new[] { 0 }, slices);
            }
        }

        _logger?.LogInformation(
            "WAL materialiser pin store for {GrainKey} re-laid out from {FromBuckets} to {TargetBuckets} buckets (estimated {EstimatedBytes} bytes).",
            GrainKey,
            from,
            target,
            EffectiveEstimate);
        _bucketCount = target;
        _persistedWidth = target;
        _relayoutTarget = 0;
        _dirtyBuckets.Clear();
        _legacyDrainBuckets.Clear();
        if (target == 1)
        {
            // The legacy slot is the live layout again.
            _legacyRetirePending = false;
        }

        _flushCursor = 0;
        _dirty = false;
    }

    /// <summary>
    /// Persists <paramref name="slice"/> to <paramref name="bucket"/> through
    /// its cached holder.
    /// </summary>
    private async Task WriteBucketAsync(int bucket, WalMaterialiserPinState slice)
    {
        if (!_bucketStates.TryGetValue(bucket, out var holder))
        {
            throw new InvalidOperationException(
                $"WAL materialiser pin bucket {bucket} could not be read before writing.");
        }

        var previous = holder.State;
        holder.State = slice;
        try
        {
            await _pinStorage!.WriteStateAsync(WalMaterialiserPinRouting.BucketStateName(bucket), _context.GrainId, holder);
        }
        catch
        {
            // The write did not land, so this holder's ETag no longer describes
            // any durable state we can reason about: an ETag conflict means the
            // slot moved under us, and any other failure leaves it unknown.
            // Evicting forces the next attempt through a fresh read for a fresh
            // ETag; reusing the stale one would conflict identically on every
            // retry (issue #2096).
            holder.State = previous;
            _bucketStates.Remove(bucket);
            throw;
        }
    }

    /// <summary>Outcome tag value for a birth-path (synchronous through-write) durable pin write.</summary>
    private const string MaterialiserPinBirthOutcome = "birth";

    /// <summary>Outcome tag value for a coalesced (debounced flush) durable pin write.</summary>
    private const string MaterialiserPinCoalescedOutcome = "coalesced";

    private string? _treeTag;
    private KeyValuePair<string, object?> _treeTagPair;
    private KeyValuePair<string, object?> _tenantTagPair;

    /// <summary>
    /// The no-offset sentinel a consumer reports when it has no durable
    /// checkpoint offset to pin (the value <see cref="ReportAsync"/> supplies).
    /// Also the floor an absent offset is compared against, so "no offset yet"
    /// and "explicitly no offset" classify identically.
    /// </summary>
    private const long NoOffset = -1;

    /// <summary>
    /// The logical tree id this pin shard belongs to, used as the metric tree
    /// tag. The grain key is either the bare <c>{treeName}</c> (single-shard
    /// layout) or a shard-suffixed key; the suffix is stripped so every shard of
    /// a tree reports under the same tree tag. Materialising it also caches the
    /// tree and tenant tag pairs used by the per-report advance counter.
    /// </summary>
    private string TreeTag
    {
        get
        {
            if (_treeTag is null)
            {
                _treeTag = WalMaterialiserPinRouting.TreeNameFromKey(_context.GrainId.Key.ToString());
                _treeTagPair = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, _treeTag);
                _tenantTagPair = LatticeTenantLabel.ForTree(_treeTag);
            }

            return _treeTag;
        }
    }
}
