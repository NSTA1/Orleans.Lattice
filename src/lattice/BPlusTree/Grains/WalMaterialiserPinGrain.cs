using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

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
        ILogger<WalMaterialiserPinGrain>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(options);
        _context = context;
        _state = state;
        _options = options;
        _logger = logger;
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

        var removed = _state.State.Pins.Remove(consumerId);
        removed |= _state.State.Offsets.Remove(consumerId);
        if (removed)
        {
            _dirty = true;
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
        await PersistNowAsync();
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
        }

        return changed;
    }

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
            try
            {
                await _state.WriteStateAsync();
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
            }
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
