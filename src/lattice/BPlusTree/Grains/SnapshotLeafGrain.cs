using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Implementation of <see cref="ISnapshotLeafGrain"/>. Transient
/// (in-memory only) per-shard snapshot leaf used by zero-observable-
/// writes snapshot cursors: rebuilds a read-only view of one shard's
/// projection by replaying the per-shard write-ahead log up to the
/// captured offset, then serves range-scan queries off that view.
/// <para>
/// Idle-evicts after
/// <see cref="LatticeOptions.SnapshotLeafIdleTtl"/>; a subsequent
/// access transparently rebuilds via
/// <c>ILeafReplayCoordinatorGrain</c>. The underlying WAL prefix is
/// kept alive by the snapshot's <c>IWalCursorRegistry</c> pin held
/// by the owning <see cref="LatticeCursorGrain"/>.
/// </para>
/// </summary>
internal sealed class SnapshotLeafGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<SnapshotLeafGrain> logger) : Grain, ISnapshotLeafGrain
{
    /// <summary>
    /// Per-slice WAL read budget. Mirrors the activation-time
    /// materialiser's <c>ReplaySliceBudget</c> so a snapshot rebuild
    /// imposes the same coordinator-RPC granularity as a live leaf's
    /// fall-off-log recovery.
    /// </summary>
    private const int ReplaySliceBudget = 256;

    /// <summary>Tree this snapshot leaf belongs to (set on first <see cref="OpenAsync"/>).</summary>
    private string _treeId = string.Empty;

    /// <summary>Virtual shard index this snapshot leaf materialises.</summary>
    private int _shardIndex = -1;

    /// <summary>
    /// Upper-bound (exclusive) per-partition WAL offsets the snapshot
    /// replays to. Indexed by WAL partition number; on a single-
    /// partition tree this is a single-element list. Sourced from the
    /// snapshot coordinate captured at <c>OpenSnapshot*Async</c> time.
    /// </summary>
    private IReadOnlyList<long> _capturedOffsetsByPartition = Array.Empty<long>();

    /// <summary>
    /// The sorted, ascending set of virtual slots the pinned snapshot
    /// shard map assigns to this leaf's <see cref="_shardIndex"/>, or
    /// <see langword="null"/> when ownership filtering is disabled
    /// (single-shard snapshot or a legacy coordinate carrying no pinned
    /// map). When non-null, a replayed key is surfaced only when its
    /// virtual slot is a member of this set; keys whose slot the pinned
    /// map assigns to a sibling shard are donor orphans left behind by an
    /// adaptive shard split and are dropped from every scan. See
    /// <see cref="LatticeSnapshotCoordinate.PinnedShardMap"/>.
    /// </summary>
    private int[]? _ownedVirtualSlots;

    /// <summary>
    /// Virtual shard count the pinned map was sized at; used to recompute
    /// a key's virtual slot for the <see cref="_ownedVirtualSlots"/>
    /// membership check. Only meaningful when
    /// <see cref="_ownedVirtualSlots"/> is non-null.
    /// </summary>
    private int _ownedVirtualShardCount;

    /// <summary>
    /// True once the WAL replay has completed and the snapshot
    /// projection is stable.
    /// </summary>
    private bool _opened;

    /// <summary>
    /// The transient WAL fold backing this snapshot leaf's projection.
    /// Created in <see cref="OpenAsync"/>; on the legacy from-zero replay
    /// path it absorbs the captured WAL prefix, and on the frozen-baseline
    /// path it is seeded from the durable per-shard
    /// <see cref="State.SnapshotShardBaseline"/> with no further replay.
    /// Range scans iterate <see cref="SnapshotProjectionFolder.Entries"/>.
    /// <see langword="null"/> until the leaf is opened.
    /// </summary>
    private SnapshotProjectionFolder? _folder;

    /// <summary>
    /// The per-cursor frozen-baseline token carried on the snapshot
    /// coordinate (<see cref="LatticeSnapshotCoordinate.SnapshotBaselineToken"/>).
    /// <see cref="Guid.Empty"/> selects the legacy from-zero replay path for
    /// coordinates persisted before the frozen-baseline store existed; a
    /// non-empty token addresses this leaf's durable
    /// <see cref="State.SnapshotShardBaseline"/> row.
    /// </summary>
    private Guid _baselineToken;

    /// <summary>
    /// True once this leaf's projection folder has been populated, by either
    /// the in-memory <see cref="SeedAsync"/> seed (shard-root capture), the
    /// durable baseline reload, or the legacy WAL replay. Distinguishes a
    /// pre-seeded activation (which <see cref="OpenAsync"/> must NOT reload from
    /// storage) from a fresh reactivation (which must).
    /// </summary>
    private bool _materialised;

    /// <summary>
    /// True once a durable per-shard <see cref="State.SnapshotShardBaseline"/>
    /// exists for this leaf's token: set when seeded from the durable store
    /// (already persisted by definition) or after <see cref="EnsurePersistedAsync"/>
    /// flushes the in-memory seed. Gates the lazy flush and the sliding-TTL
    /// touch; never set on the legacy empty-token replay path.
    /// </summary>
    private bool _persisted;

    /// <summary>
    /// The in-memory frozen baseline handed to <see cref="SeedAsync"/> by the
    /// shard-root capture, retained by reference so a later
    /// <see cref="EnsurePersistedAsync"/> can flush the identical rows without
    /// re-materialising them from the folder. <see langword="null"/> on the
    /// durable-reload and legacy paths (nothing to flush).
    /// </summary>
    private State.SnapshotShardBaseline? _seedBaseline;

    /// <summary>
    /// Wall-clock UTC of the last durable-baseline TTL slide issued from the
    /// serve path, used to throttle the sliding-TTL touch to at most one
    /// reminder-table refresh per <see cref="ResolveBaselineTouchInterval"/>.
    /// </summary>
    private DateTime _lastTtlTouchUtc = DateTime.MinValue;

    private CrdtShapeRegistry? _resolvedCrdtShapeRegistry;

    private CrdtShapeRegistry ResolveCrdtShapeRegistry() =>
        _resolvedCrdtShapeRegistry ??=
            context.ActivationServices.GetService(typeof(CrdtShapeRegistry)) as CrdtShapeRegistry
            ?? throw new InvalidOperationException(
                "No CrdtShapeRegistry is registered in the snapshot leaf's activation services. "
                + "AddLattice registers it unconditionally; a missing registration indicates a host wiring bug.");

    /// <inheritdoc />
    public async Task OpenAsync(string treeId, int shardIndex, IReadOnlyList<long> capturedOffsetsByPartition, IReadOnlyList<int>? ownedVirtualSlots, int virtualShardCount, Guid baselineToken, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(capturedOffsetsByPartition);
        if (shardIndex < 0)
            throw new ArgumentOutOfRangeException(nameof(shardIndex), "Shard index must be non-negative.");
        if (capturedOffsetsByPartition.Count == 0)
            throw new ArgumentException("Captured offsets list must contain at least one partition.", nameof(capturedOffsetsByPartition));
        for (var i = 0; i < capturedOffsetsByPartition.Count; i++)
        {
            if (capturedOffsetsByPartition[i] < 0)
                throw new ArgumentOutOfRangeException(nameof(capturedOffsetsByPartition), $"Captured offset for partition {i} must be non-negative.");
        }
        if (ownedVirtualSlots is not null && virtualShardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(virtualShardCount), "Must be greater than 0 when an owned-slot set is supplied.");
        cancellationToken.ThrowIfCancellationRequested();

        var ownedSlots = ownedVirtualSlots is null
            ? null
            : ownedVirtualSlots as int[] ?? ownedVirtualSlots.ToArray();

        if (_opened)
        {
            // Idempotent re-open with the same coordinate is a no-op;
            // a different coordinate would target a different grain
            // key, so a mismatch here indicates a programming error
            // upstream and must surface loudly. Compare the captured
            // per-partition arrays element-wise.
            if (_treeId != treeId
                || _shardIndex != shardIndex
                || _baselineToken != baselineToken
                || !PartitionOffsetsEqual(_capturedOffsetsByPartition, capturedOffsetsByPartition)
                || _ownedVirtualShardCount != (ownedSlots is null ? 0 : virtualShardCount)
                || !OwnedSlotsEqual(_ownedVirtualSlots, ownedSlots))
            {
                throw new InvalidOperationException(
                    $"SnapshotLeafGrain for '{this.GetPrimaryKeyString()}' was already opened against ({_treeId}, {_shardIndex}, [{string.Join(',', _capturedOffsetsByPartition)}]); refusing to re-open against ({treeId}, {shardIndex}, [{string.Join(',', capturedOffsetsByPartition)}]).");
            }

            // The cursor calls OpenAsync on every page (idempotent). Use that
            // cadence to slide the durable baseline's leak-guard TTL while the
            // scan is actively serving - throttled so a long scan refreshes the
            // reminder rarely rather than on every page.
            await MaybeTouchBaselineTtlAsync();
            return;
        }

        if (_materialised)
        {
            // Pre-seeded in memory by the shard-root capture (SeedAsync) in this
            // same activation. The rows are already folded; OpenAsync only needs
            // to attach the cursor's read-time donor-orphan ownership filter and
            // mark the leaf open. No durable load and no WAL replay - that is the
            // whole point of the lazy-persist design (issue #916): a single-page
            // scan serves entirely from this in-memory freeze and never persists.
            if (_treeId != treeId
                || _shardIndex != shardIndex
                || _baselineToken != baselineToken
                || !PartitionOffsetsEqual(_capturedOffsetsByPartition, capturedOffsetsByPartition))
            {
                throw new InvalidOperationException(
                    $"SnapshotLeafGrain for '{this.GetPrimaryKeyString()}' was seeded against ({_treeId}, {_shardIndex}, [{string.Join(',', _capturedOffsetsByPartition)}], token {_baselineToken:N}); refusing to open against ({treeId}, {shardIndex}, [{string.Join(',', capturedOffsetsByPartition)}], token {baselineToken:N}).");
            }
            _ownedVirtualSlots = ownedSlots;
            _ownedVirtualShardCount = ownedSlots is null ? 0 : virtualShardCount;
            _opened = true;
            return;
        }

        _treeId = treeId;
        _shardIndex = shardIndex;
        _capturedOffsetsByPartition = capturedOffsetsByPartition;
        _ownedVirtualSlots = ownedSlots;
        _ownedVirtualShardCount = ownedSlots is null ? 0 : virtualShardCount;
        _baselineToken = baselineToken;
        _folder = new SnapshotProjectionFolder(treeId, ResolveCrdtShapeRegistry());

        if (baselineToken != Guid.Empty)
        {
            // Frozen-baseline reload path: this is a fresh reactivation (the
            // in-memory seed was lost to idle eviction or failover) so reload the
            // durable per-shard baseline that EnsurePersistedAsync flushed when
            // the cursor first paged past page 1. No WAL replay happens here, so
            // a WAL GC that trims the prefix after capture cannot perturb this
            // view, and the reloaded rows are byte-identical to the freeze for a
            // stable point-in-time view across failover. A baseline that was
            // never persisted (single-page cursor, or a leaf evicted before its
            // first read) surfaces as the load-failure below.
            await SeedFromBaselineAsync(cancellationToken);
            _persisted = true;
            _lastTtlTouchUtc = DateTime.UtcNow;
        }
        else
        {
            // Legacy from-zero replay path for coordinates persisted before
            // the frozen-baseline store existed (wire/back-compat).
            await ReplayWalAsync(cancellationToken);
        }

        _materialised = true;
        _opened = true;

        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug(
                "SnapshotLeafGrain opened: tree={TreeId}, shard={ShardIndex}, baselineToken={BaselineToken}, capturedOffsets=[{CapturedOffsets}], entries={EntryCount}, pendingSagas={PendingSagaCount}.",
                treeId, shardIndex, baselineToken, string.Join(',', capturedOffsetsByPartition), _folder.Entries.Count, _folder.PendingSagaCount);
        }

        _ = optionsMonitor;
        _ = context;
    }

    /// <inheritdoc />
    public Task SeedAsync(string treeId, int shardIndex, State.SnapshotShardBaseline baseline, Guid baselineToken, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(baseline);
        if (shardIndex < 0)
            throw new ArgumentOutOfRangeException(nameof(shardIndex), "Shard index must be non-negative.");
        if (baselineToken == Guid.Empty)
            throw new ArgumentException("Seed requires a non-empty baseline token; the in-memory seed path does not exist for legacy coordinates.", nameof(baselineToken));
        var capturedHead = baseline.CapturedHeadPerPartition;
        if (capturedHead.Count == 0)
            throw new ArgumentException("Baseline must carry at least one partition's captured head.", nameof(baseline));
        cancellationToken.ThrowIfCancellationRequested();

        if (_materialised)
        {
            // Idempotent re-seed: the shard-root capture retries (per-shard
            // ShardActivationRetry) can replay a seed against an activation that
            // already absorbed it. The activation is token-keyed, so a mismatch
            // here is an upstream wiring bug and must surface loudly.
            if (_treeId != treeId
                || _shardIndex != shardIndex
                || _baselineToken != baselineToken
                || !PartitionOffsetsEqual(_capturedOffsetsByPartition, capturedHead))
            {
                throw new InvalidOperationException(
                    $"SnapshotLeafGrain for '{this.GetPrimaryKeyString()}' was already materialised against ({_treeId}, {_shardIndex}, token {_baselineToken:N}); refusing to re-seed against ({treeId}, {shardIndex}, token {baselineToken:N}).");
            }
            return Task.CompletedTask;
        }

        _treeId = treeId;
        _shardIndex = shardIndex;
        _capturedOffsetsByPartition = capturedHead;
        _baselineToken = baselineToken;
        _seedBaseline = baseline;
        _folder = new SnapshotProjectionFolder(treeId, ResolveCrdtShapeRegistry());

        // Seed the rows verbatim. Donor-orphan ownership filtering is applied by
        // the read path against the cursor-supplied owned-slot set (set on the
        // subsequent OpenAsync), exactly as the durable-reload path filters at
        // seed time - so the in-memory and reloaded views are identical.
        foreach (var row in baseline.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _folder.SeedRow(row.Key, row.Value);
        }

        _materialised = true;

        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug(
                "SnapshotLeafGrain seeded in memory: tree={TreeId}, shard={ShardIndex}, baselineToken={BaselineToken}, capturedOffsets=[{CapturedOffsets}], rows={RowCount}.",
                treeId, shardIndex, baselineToken, string.Join(',', capturedHead), _folder.Entries.Count);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task EnsurePersistedAsync(CancellationToken cancellationToken)
    {
        // Already durable (flushed earlier, or reloaded from the durable store),
        // or a legacy replay leaf with nothing to flush.
        if (_persisted || _baselineToken == Guid.Empty)
            return;
        if (_seedBaseline is null)
        {
            // No in-memory baseline to flush. This only happens on the legacy
            // replay path (guarded above) or a leaf that has not been seeded;
            // either way there is nothing durable to write.
            return;
        }
        cancellationToken.ThrowIfCancellationRequested();

        var baselineGrain = grainFactory.GetGrain<ISnapshotBaselineStorageGrain>(
            BuildBaselineKey(_treeId, _shardIndex, _baselineToken));
        await baselineGrain.SaveAsync(_seedBaseline, cancellationToken);
        _persisted = true;
        _lastTtlTouchUtc = DateTime.UtcNow;

        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug(
                "SnapshotLeafGrain flushed frozen baseline to durable store: tree={TreeId}, shard={ShardIndex}, baselineToken={BaselineToken}, rows={RowCount}.",
                _treeId, _shardIndex, _baselineToken, _seedBaseline.Rows.Count);
        }
    }

    /// <summary>
    /// Slides the durable baseline's leak-guard TTL while the cursor is actively
    /// paging, throttled to at most one reminder-table refresh per
    /// <see cref="ResolveBaselineTouchInterval"/> so a long scan keeps its
    /// baseline alive without rewriting the reminder on every page. Best-effort:
    /// a storage hiccup only risks the baseline expiring early, which a later
    /// page would surface as a reload failure (the cursor then reopens).
    /// </summary>
    private async Task MaybeTouchBaselineTtlAsync()
    {
        if (!_persisted || _baselineToken == Guid.Empty)
            return;
        var interval = ResolveBaselineTouchInterval();
        if (interval == Timeout.InfiniteTimeSpan)
            return;
        var now = DateTime.UtcNow;
        if (now - _lastTtlTouchUtc < interval)
            return;
        _lastTtlTouchUtc = now;

        try
        {
            var baselineGrain = grainFactory.GetGrain<ISnapshotBaselineStorageGrain>(
                BuildBaselineKey(_treeId, _shardIndex, _baselineToken));
            await baselineGrain.TouchAsync(default);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "SnapshotLeafGrain {Key}: failed to slide frozen-baseline TTL; the baseline may expire early and force a cursor reopen.",
                this.GetPrimaryKeyString());
        }
    }

    /// <summary>
    /// Resolves the minimum interval between sliding-TTL touches as half the
    /// configured <see cref="LatticeOptions.SnapshotBaselineTtl"/> (clamped to a
    /// one-minute floor), so an active scan refreshes the reminder at most a
    /// couple of times per TTL window. Returns
    /// <see cref="Timeout.InfiniteTimeSpan"/> when the TTL is disabled, which
    /// suppresses touching entirely.
    /// </summary>
    private TimeSpan ResolveBaselineTouchInterval()
    {
        var ttl = optionsMonitor.Get(_treeId).SnapshotBaselineTtl;
        if (ttl == Timeout.InfiniteTimeSpan)
            return Timeout.InfiniteTimeSpan;
        var half = TimeSpan.FromTicks(ttl.Ticks / 2);
        return half < TimeSpan.FromMinutes(1) ? TimeSpan.FromMinutes(1) : half;
    }

    /// <see cref="State.SnapshotShardBaseline"/> captured at open time,
    /// filtering each row through the same <see cref="IsKeyOwned"/> donor-orphan
    /// exclusion the scan path applies so a key migrated to a sibling shard by
    /// an adaptive split is surfaced only by its pinned-map owner. Performs no
    /// WAL replay.
    /// </summary>
    private async Task SeedFromBaselineAsync(CancellationToken cancellationToken)
    {
        var baselineGrain = grainFactory.GetGrain<ISnapshotBaselineStorageGrain>(
            BuildBaselineKey(_treeId, _shardIndex, _baselineToken));
        var baseline = await baselineGrain.LoadAsync(cancellationToken);
        if (baseline is null)
        {
            throw new LatticeSnapshotExpiredException(
                $"SnapshotLeafGrain for '{this.GetPrimaryKeyString()}' could not load its frozen baseline "
                + $"('{BuildBaselineKey(_treeId, _shardIndex, _baselineToken)}'). The in-memory baseline was lost "
                + "(idle eviction or failover before the cursor's first page made it durable) and no durable row "
                + "exists; the open cannot fall back to a from-zero WAL replay without risking an empty/partial view "
                + "on a GC-trimmed log. Open a fresh snapshot cursor.");
        }

        var folder = _folder!;
        foreach (var row in baseline.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (IsKeyOwned(row.Key))
                folder.SeedRow(row.Key, row.Value);
        }
    }

    /// <summary>
    /// Builds the durable per-cursor, per-shard frozen-baseline grain key
    /// <c>{treeId}/{shardIndex}/{baselineToken:N}</c>. Shared with the
    /// shard-root capture path so both address the same storage row.
    /// </summary>
    internal static string BuildBaselineKey(string treeId, int shardIndex, Guid baselineToken) =>
        $"{treeId}/{shardIndex}/{baselineToken:N}";

    private static bool PartitionOffsetsEqual(IReadOnlyList<long> a, IReadOnlyList<long> b)
    {
        if (a.Count != b.Count) return false;
        for (var i = 0; i < a.Count; i++)
        {
            if (a[i] != b[i]) return false;
        }
        return true;
    }

    private static bool OwnedSlotsEqual(int[]? a, int[]? b)
    {
        if (ReferenceEquals(a, b)) return true;
        if (a is null || b is null) return false;
        if (a.Length != b.Length) return false;
        for (var i = 0; i < a.Length; i++)
        {
            if (a[i] != b[i]) return false;
        }
        return true;
    }

    /// <summary>
    /// Returns <see langword="true"/> when this leaf should surface
    /// <paramref name="key"/> under the pinned snapshot shard map. When
    /// ownership filtering is disabled (<see cref="_ownedVirtualSlots"/>
    /// is null) every key is surfaced. Otherwise the key is surfaced only
    /// when the pinned map still routes its virtual slot to this leaf's
    /// shard; a key whose slot the map assigns elsewhere is a donor orphan
    /// left behind by an adaptive shard split and is dropped. The check
    /// mirrors the live read path's <c>IsKeyMovedAway</c> guard but resolves
    /// ownership positively against the pinned map's owned-slot set rather
    /// than the source leaf's current moved-away set, keeping the exclusion
    /// point-in-time consistent with the snapshot coordinate.
    /// </summary>
    private bool IsKeyOwned(string key)
    {
        var owned = _ownedVirtualSlots;
        if (owned is null) return true;
        var slot = ShardMap.GetVirtualSlot(key, _ownedVirtualShardCount);
        return Array.BinarySearch(owned, slot) >= 0;
    }

    /// <summary>
    /// Returns <see langword="true"/> when the per-key mutation
    /// <paramref name="mutation"/> belongs to this leaf's owning shard.
    /// When the pinned snapshot map is available (multi-shard snapshot)
    /// ownership is resolved by the key's virtual slot under that map, so
    /// a shadow-forwarded record carrying a sibling shard's stamp is still
    /// applied by the shard the pinned map identifies as the key's owner.
    /// Falls back to the stamped <c>ShardIndex</c> match for single-shard
    /// or legacy coordinates that carry no pinned map (no split can have
    /// produced a foreign stamp there).
    /// </summary>
    private bool IsMutationOwned(in LatticeMutation mutation)
    {
        if (_ownedVirtualSlots is not null)
            return IsKeyOwned(mutation.Key);
        return mutation.ShardIndex == _shardIndex;
    }

    /// <inheritdoc />
    public Task<List<string>> GetKeysAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null, int limit = int.MaxValue, LatticePredicateNode? predicate = null, bool reverse = false)
    {
        EnsureOpened();
        if (limit <= 0)
            return Task.FromResult(new List<string>());
        var result = new List<string>();
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        foreach (var (key, value) in _folder!.Entries)
        {
            if (!InRange(key, startInclusive, endExclusive, afterExclusive, beforeExclusive))
            {
                // SortedDictionary iterates in ordinal order, so once
                // we pass the end bound we can early-exit. Don't
                // early-exit on the start bound, because the iteration
                // begins from the dictionary head and we have to skip
                // the prefix below the start key first.
                if (endExclusive is not null && string.CompareOrdinal(key, endExclusive) >= 0)
                    break;
                if (beforeExclusive is not null && string.CompareOrdinal(key, beforeExclusive) >= 0)
                    break;
                continue;
            }
            if (value.IsTombstone || value.IsExpired(nowTicks))
                continue;
            if (predicate is { } pred && !LatticePredicateEvaluator.Matches(value.Value, pred))
                continue;
            // Drop donor orphans: a key the pinned snapshot map no longer
            // routes to this shard was migrated to a sibling shard by an
            // adaptive split and is surfaced by that shard's leaf instead.
            if (!IsKeyOwned(key))
                continue;
            result.Add(key);
            if (reverse)
            {
                // Keep only the largest `limit` matches: the dictionary is
                // ascending, so drop the smallest from the window head once it
                // overflows. The retained slice stays ascending for the cursor's
                // reverse k-way merge.
                if (result.Count > limit)
                    result.RemoveAt(0);
            }
            else if (result.Count >= limit)
            {
                break;
            }
        }
        return Task.FromResult(result);
    }

    /// <inheritdoc />
    public Task<List<KeyValuePair<string, byte[]>>> GetEntriesAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null, int limit = int.MaxValue, LatticePredicateNode? predicate = null, bool reverse = false)
    {
        EnsureOpened();
        if (limit <= 0)
            return Task.FromResult(new List<KeyValuePair<string, byte[]>>());
        var result = new List<KeyValuePair<string, byte[]>>();
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        foreach (var (key, value) in _folder!.Entries)
        {
            if (!InRange(key, startInclusive, endExclusive, afterExclusive, beforeExclusive))
            {
                if (endExclusive is not null && string.CompareOrdinal(key, endExclusive) >= 0)
                    break;
                if (beforeExclusive is not null && string.CompareOrdinal(key, beforeExclusive) >= 0)
                    break;
                continue;
            }
            if (value.IsTombstone || value.IsExpired(nowTicks))
                continue;
            if (value.Value is null)
                continue;
            if (predicate is { } pred && !LatticePredicateEvaluator.Matches(value.Value, pred))
                continue;
            // Drop donor orphans: see GetKeysAsync. Doing this in the leaf
            // (before the value is reduced to bytes) is load-bearing -
            // the cursor-side k-way merge has no HLC to pick an LWW winner,
            // so a stale orphan that reaches the merge could mask the owning
            // shard's post-split update or its post-split delete.
            if (!IsKeyOwned(key))
                continue;
            result.Add(new KeyValuePair<string, byte[]>(key, value.Value));
            if (reverse)
            {
                if (result.Count > limit)
                    result.RemoveAt(0);
            }
            else if (result.Count >= limit)
            {
                break;
            }
        }
        return Task.FromResult(result);
    }

    /// <summary>
    /// Mutation deferred during pass 1 of the snapshot-leaf replay to
    /// be applied in pass 2 once every partition's per-key Set/Delete
    /// entries have been absorbed into the folder's projection and
    /// every prepare into its pending-saga buckets. Same rationale as
    /// the activation-time materialiser's <c>DeferredTerminal</c> -
    /// see <c>BPlusLeafGrain.Activation.cs</c> for the saga atomicity
    /// and <see cref="MutationKind.DeleteRange"/> ordering proofs.
    /// </summary>
    private readonly record struct DeferredTerminal(LatticeMutation Mutation);

    /// <summary>
    /// Drives the per-partition WAL replay loop for the snapshot leaf.
    /// Iterates every partition's <c>(empty, capturedOffsets[p]]</c>
    /// slice through <see cref="ILeafReplayCoordinatorGrain.ReadSliceAsync"/>
    /// in <see cref="ReplaySliceBudget"/>-sized chunks. Saga terminals
    /// and <see cref="MutationKind.DeleteRange"/> mutations are
    /// deferred to a pass-2 drain after every partition's pass-1 has
    /// completed so the same atomicity and ordering invariants the
    /// live-leaf two-pass replay enforces also hold for snapshot
    /// reads. Cancellation is honoured between slices and between
    /// entries.
    /// </summary>
    private async Task ReplayWalAsync(CancellationToken cancellationToken)
    {
        var partitionCount = _capturedOffsetsByPartition.Count;
        long totalEntriesObserved = 0;
        var sw = System.Diagnostics.Stopwatch.StartNew();

        // Pass 1: per-partition Set/Delete/prepare absorption. Saga
        // terminals (TxCommit/TxAbort) and DeleteRange are deferred.
        var deferred = new List<DeferredTerminal>();
        for (var partition = 0; partition < partitionCount; partition++)
        {
            var capturedOffset = _capturedOffsetsByPartition[partition];
            if (capturedOffset == 0)
                continue;

            var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
                $"{_treeId}/{partition}");

            // ReadSliceAsync semantics: (fromExclusive, toInclusive].
            // We want [0, capturedOffset), so the inclusive upper
            // bound is capturedOffset - 1.
            long fromExclusive = -1;
            long toInclusive = capturedOffset - 1;

            while (fromExclusive < toInclusive)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var slice = await coordinator.ReadSliceAsync(
                    fromExclusive,
                    toInclusive,
                    ReplaySliceBudget,
                    cancellationToken);

                if (slice.Count == 0)
                    break;

                foreach (var entry in slice)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (SnapshotProjectionFolder.IsDeferredKind(entry.Mutation.Kind))
                    {
                        deferred.Add(new DeferredTerminal(entry.Mutation));
                    }
                    else
                    {
                        // Per-shard ownership filter for per-key records: the
                        // shared WAL partition multiplexes every shard's
                        // writes, so a record this leaf does not own must not
                        // be absorbed. Resolved by the key's virtual slot
                        // under the pinned snapshot map when available, else by
                        // the stamped shard index. Saga terminals / range
                        // deletes are deferred above and applied unconditionally
                        // in pass 2 (the range tombstone iterates only this
                        // leaf's already-filtered entries, and terminals are
                        // pre-routed by shard).
                        if (entry.Mutation.Kind is MutationKind.Set or MutationKind.Delete or MutationKind.Tombstone
                            && !IsMutationOwned(entry.Mutation))
                        {
                            totalEntriesObserved++;
                            continue;
                        }
                        _folder!.Apply(entry.Mutation);
                    }
                    totalEntriesObserved++;
                }

                var lastOffset = slice[^1].Offset;
                if (lastOffset <= fromExclusive)
                    break; // defensive: never spin if the slice failed to advance.
                fromExclusive = lastOffset;
            }
        }

        // Pass 2: drain every deferred saga terminal and range-delete
        // tombstone. By this point pass 1 has fully populated the
        // folder's pending buckets across every partition and its entries
        // carry every Set/Delete, so each terminal's pending-bucket flip is
        // complete and each range tombstone iterates the full
        // pre-tombstone projection.
        foreach (var d in deferred)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _folder!.Apply(d.Mutation);
        }

        sw.Stop();
        // Tags allocated once per replay (one open call) so the
        // allocation cost is amortised over the WAL slice loop.
        var tags = new KeyValuePair<string, object?>[]
        {
            new(LatticeMetrics.TagTree, _treeId),
            new(LatticeMetrics.TagShard, _shardIndex),
        };
        LatticeMetrics.SnapshotReplayDuration.Record(sw.Elapsed.TotalMilliseconds, tags);
        if (totalEntriesObserved > 0)
        {
            LatticeMetrics.SnapshotReplayEntries.Add(totalEntriesObserved, tags);
        }
    }

    private static bool InRange(string key, string? startInclusive, string? endExclusive, string? afterExclusive, string? beforeExclusive)
    {
        if (startInclusive is not null && string.CompareOrdinal(key, startInclusive) < 0)
            return false;
        if (endExclusive is not null && string.CompareOrdinal(key, endExclusive) >= 0)
            return false;
        if (afterExclusive is not null && string.CompareOrdinal(key, afterExclusive) <= 0)
            return false;
        if (beforeExclusive is not null && string.CompareOrdinal(key, beforeExclusive) >= 0)
            return false;
        return true;
    }

    /// <summary>
    /// Validates that <see cref="OpenAsync"/> has been called before
    /// any read; throws <see cref="InvalidOperationException"/>
    /// otherwise to surface a wiring bug rather than silently
    /// returning empty pages.
    /// </summary>
    private void EnsureOpened()
    {
        if (!_opened)
        {
            throw new InvalidOperationException(
                $"SnapshotLeafGrain for '{this.GetPrimaryKeyString()}' has not been opened. Call OpenAsync before reading.");
        }
    }
}

