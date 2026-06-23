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
    /// Sorted by key. Committed projection entries (live values plus
    /// tombstones; tombstones are filtered out at scan time). LWW
    /// merge applied during replay so the dictionary's value for any
    /// key is the highest-timestamp variant the WAL prefix carries.
    /// </summary>
    private readonly SortedDictionary<string, LwwValue<byte[]>> _entries = new(StringComparer.Ordinal);

    /// <summary>
    /// Pending saga buckets indexed by transaction id. A prepared
    /// <see cref="MutationKind.Set"/> / <see cref="MutationKind.Delete"/>
    /// is buffered here until its terminal <see cref="MutationKind.TxCommit"/>
    /// / <see cref="MutationKind.TxAbort"/> appears later in the same
    /// WAL prefix; sagas whose terminal lands after the captured
    /// offset stay pending and are invisible to scans (the snapshot
    /// view hides incomplete sagas, mirroring the registry-snapshot
    /// semantics for the foreground read path).
    /// </summary>
    private readonly Dictionary<Guid, Dictionary<string, LwwValue<byte[]>>> _pendingTx = new();

    /// <summary>
    /// Parallel side-map to <see cref="_pendingTx"/> recording, per
    /// <c>(transactionId, key)</c>, the typed CRDT delta and merge mode a
    /// prepared mutation carried. On the saga's terminal
    /// <see cref="MutationKind.TxCommit"/> the drain folds the recorded
    /// delta into this snapshot's current visible state via the matching
    /// primitive's <c>MergeDelta</c> instead of merging the prepared LWW
    /// value, so the read-snapshot view of a CRDT key mid-saga matches the
    /// live leaf's terminal-fold behaviour (the per-replica union) rather
    /// than diverging to last-writer-wins. Only populated for prepared
    /// CRDT-mode mutations; LWW prepares leave it untouched.
    /// </summary>
    private readonly Dictionary<Guid, Dictionary<string, (byte[] Delta, LatticeMergeMode Mode)>> _pendingTxDeltas = new();

    private CrdtShapeRegistry? _resolvedCrdtShapeRegistry;

    private CrdtShapeRegistry ResolveCrdtShapeRegistry() =>
        _resolvedCrdtShapeRegistry ??=
            context.ActivationServices.GetService(typeof(CrdtShapeRegistry)) as CrdtShapeRegistry
            ?? throw new InvalidOperationException(
                "No CrdtShapeRegistry is registered in the snapshot leaf's activation services. "
                + "AddLattice registers it unconditionally; a missing registration indicates a host wiring bug.");

    /// <inheritdoc />
    public async Task OpenAsync(string treeId, int shardIndex, IReadOnlyList<long> capturedOffsetsByPartition, IReadOnlyList<int>? ownedVirtualSlots, int virtualShardCount, CancellationToken cancellationToken)
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
                || !PartitionOffsetsEqual(_capturedOffsetsByPartition, capturedOffsetsByPartition)
                || _ownedVirtualShardCount != (ownedSlots is null ? 0 : virtualShardCount)
                || !OwnedSlotsEqual(_ownedVirtualSlots, ownedSlots))
            {
                throw new InvalidOperationException(
                    $"SnapshotLeafGrain for '{this.GetPrimaryKeyString()}' was already opened against ({_treeId}, {_shardIndex}, [{string.Join(',', _capturedOffsetsByPartition)}]); refusing to re-open against ({treeId}, {shardIndex}, [{string.Join(',', capturedOffsetsByPartition)}]).");
            }
            return;
        }

        _treeId = treeId;
        _shardIndex = shardIndex;
        _capturedOffsetsByPartition = capturedOffsetsByPartition;
        _ownedVirtualSlots = ownedSlots;
        _ownedVirtualShardCount = ownedSlots is null ? 0 : virtualShardCount;

        await ReplayWalAsync(cancellationToken);

        _opened = true;

        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug(
                "SnapshotLeafGrain opened: tree={TreeId}, shard={ShardIndex}, capturedOffsets=[{CapturedOffsets}], entries={EntryCount}, pendingSagas={PendingSagaCount}.",
                treeId, shardIndex, string.Join(',', capturedOffsetsByPartition), _entries.Count, _pendingTx.Count);
        }

        _ = optionsMonitor;
        _ = context;
    }

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
        foreach (var (key, value) in _entries)
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
        foreach (var (key, value) in _entries)
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
    /// entries have been absorbed into <see cref="_entries"/> and
    /// every prepare into <see cref="_pendingTx"/>. Same rationale as
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
                    if (entry.Mutation.Kind is MutationKind.TxCommit or MutationKind.TxAbort or MutationKind.DeleteRange)
                    {
                        deferred.Add(new DeferredTerminal(entry.Mutation));
                    }
                    else
                    {
                        ApplyEntry(entry.Mutation);
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
        // tombstone. By this point pass 1 has fully populated
        // _pendingTx across every partition and _entries carries every
        // Set/Delete, so each terminal's pending-bucket flip is
        // complete and each range tombstone iterates the full
        // pre-tombstone Cache.
        foreach (var d in deferred)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ApplyEntry(d.Mutation);
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

    /// <summary>
    /// Replays a single WAL mutation against the snapshot leaf's
    /// in-memory projection. Mirrors the kind-dispatch in
    /// <c>BPlusLeafGrain.Projection.cs</c> but stores into the
    /// snapshot's own dictionaries rather than the live leaf state.
    /// </summary>
    private void ApplyEntry(in LatticeMutation mutation)
    {
        // Per-shard filter: under multi-partition WAL replay the
        // snapshot leaf reads from coordinators 0..WalPartitions-1,
        // each of which carries mutations for EVERY shard whose
        // shardIndex modulo WalPartitions hashes there (saga
        // terminals) and EVERY key whose WalPartitionHash hashes
        // there (per-key writes). The snapshot leaf must filter out
        // mutations that do not belong to this leaf's owning shard so
        // it does not absorb sibling shards' data.
        //
        // Ownership is resolved by the key's virtual slot under the
        // pinned snapshot map (when available), NOT by the mutation's
        // stamped ShardIndex. The stamp records the shard that authored
        // the record, which is not the same as the shard that owns the
        // key after an adaptive split: a write routed to a donor shard
        // for an already-moved slot is shadow-forwarded into the target
        // shard's WAL but keeps the donor's stamp. Filtering on the stamp
        // would drop that forwarded record on the target's snapshot leaf
        // (a cold, checkpoint-free replay) and resurrect the pre-forward
        // value - e.g. a post-split delete forwarded through the donor
        // would be lost and the deleted key would reappear with its stale
        // drained value. Resolving ownership by slot keeps the forwarded
        // record so the LWW merge applies it, while still dropping donor
        // orphans (a moved key's pre-split copy retained on the donor) and
        // sibling-shard entries multiplexed into a shared WAL partition.
        // See issue #907 and docs/lattice/shard-splitting.md.
        //
        // Range / TxCommit / TxAbort apply unconditionally - the
        // range tombstone iterates only this snapshot's _entries
        // (already ownership-filtered) and saga terminals are pre-routed
        // by shard via shardIndex % WalPartitions.
        if (mutation.Kind is MutationKind.Set or MutationKind.Delete or MutationKind.Tombstone
            && !IsMutationOwned(mutation))
        {
            return;
        }
        switch (mutation.Kind)
        {
            case MutationKind.Set:
                if (mutation.IsPrepared)
                    AddPreparedMutation(mutation.TransactionId, mutation.Key, BuildLww(mutation, isTombstone: mutation.IsTombstone), mutation.Delta, mutation.Mode);
                else
                    MergeIntoEntries(mutation.Key, BuildLww(mutation, isTombstone: mutation.IsTombstone));
                break;
            case MutationKind.Delete:
                if (mutation.IsPrepared)
                    AddPreparedMutation(mutation.TransactionId, mutation.Key, BuildLww(mutation, isTombstone: true));
                else
                    MergeIntoEntries(mutation.Key, BuildLww(mutation, isTombstone: true));
                break;
            case MutationKind.DeleteRange:
                ApplyDeleteRange(mutation);
                break;
            case MutationKind.TxCommit:
                ApplyTxCommit(mutation.TransactionId);
                break;
            case MutationKind.TxAbort:
                ApplyTxAbort(mutation.TransactionId);
                break;
            case MutationKind.Tombstone:
                ApplyTombstoneReap(mutation);
                break;
            default:
                // Defensive forward-compat: unknown kinds are dropped,
                // matching BPlusLeafGrain.ShouldApplyDuringReplay.
                break;
        }
    }

    private static LwwValue<byte[]> BuildLww(in LatticeMutation mutation, bool isTombstone) => new()
    {
        Value = isTombstone ? null : mutation.Value,
        Timestamp = mutation.Timestamp,
        IsTombstone = isTombstone,
        ExpiresAtTicks = isTombstone ? 0 : mutation.ExpiresAtTicks,
        OriginClusterId = mutation.OriginClusterId,
        VectorClock = mutation.VectorClock,
    };

    private void MergeIntoEntries(string key, LwwValue<byte[]> incoming)
    {
        if (_entries.TryGetValue(key, out var existing))
        {
            _entries[key] = LwwValue<byte[]>.Merge(existing, incoming);
        }
        else
        {
            _entries[key] = incoming;
        }
    }

    private void AddPreparedMutation(Guid txId, string key, LwwValue<byte[]> incoming, byte[]? delta = null, LatticeMergeMode mode = LatticeMergeMode.LwwRegister)
    {
        if (!_pendingTx.TryGetValue(txId, out var bucket))
        {
            bucket = new Dictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal);
            _pendingTx[txId] = bucket;
        }
        bucket[key] = incoming;

        // Record the typed CRDT delta + merge mode so the terminal drain
        // folds it (the per-replica union) rather than merging the prepared
        // LWW value, keeping the read-snapshot view consistent with the live
        // leaf. LWW prepares carry no delta and leave the side-map untouched.
        if (delta is not null && mode != LatticeMergeMode.LwwRegister)
        {
            if (!_pendingTxDeltas.TryGetValue(txId, out var deltaBucket))
            {
                deltaBucket = new Dictionary<string, (byte[], LatticeMergeMode)>(StringComparer.Ordinal);
                _pendingTxDeltas[txId] = deltaBucket;
            }
            deltaBucket[key] = (delta, mode);
        }
    }

    private void ApplyDeleteRange(in LatticeMutation mutation)
    {
        var endExclusive = mutation.EndExclusiveKey;
        if (endExclusive is null)
            return;
        var startInclusive = mutation.Key;
        if (string.CompareOrdinal(startInclusive, endExclusive) >= 0)
            return;

        List<string>? toRewrite = null;
        var matchedKeys = mutation.MatchedKeys;
        if (matchedKeys is not null)
        {
            // Predicate-filtered range delete: tombstone exactly the matched
            // key set the authoring leaf recorded, without re-evaluating the
            // predicate against the snapshot's (possibly divergent) values.
            foreach (var key in matchedKeys)
            {
                if (string.CompareOrdinal(key, startInclusive) < 0
                    || string.CompareOrdinal(key, endExclusive) >= 0)
                    continue;
                if (_entries.ContainsKey(key))
                    (toRewrite ??= []).Add(key);
            }
        }
        else
        {
            foreach (var (key, _) in _entries)
            {
                if (string.CompareOrdinal(key, startInclusive) < 0)
                    continue;
                if (string.CompareOrdinal(key, endExclusive) >= 0)
                    break;
                (toRewrite ??= []).Add(key);
            }
        }

        if (toRewrite is null)
            return;

        var tombstone = new LwwValue<byte[]>
        {
            Value = null,
            Timestamp = mutation.Timestamp,
            IsTombstone = true,
            ExpiresAtTicks = 0,
            OriginClusterId = mutation.OriginClusterId,
            VectorClock = mutation.VectorClock,
        };

        foreach (var key in toRewrite)
        {
            MergeIntoEntries(key, tombstone);
        }
    }

    private void ApplyTxCommit(Guid txId)
    {
        _pendingTxDeltas.Remove(txId, out var deltaBucket);
        if (!_pendingTx.Remove(txId, out var bucket))
            return;
        foreach (var (key, value) in bucket)
        {
            if (deltaBucket is not null && deltaBucket.TryGetValue(key, out var dm))
            {
                // CRDT-mode prepared entry: fold the typed delta into this
                // snapshot's current visible state rather than merging the
                // prepared LWW value, matching the live leaf's terminal-fold
                // behaviour so the read snapshot converges by the per-replica
                // union. The fold preserves the prepared entry's HLC so the
                // snapshot's as-of ordering is unchanged.
                var folded = FoldPreparedCrdtDelta(key, dm.Delta, dm.Mode);
                MergeIntoEntries(key, value with { Value = folded });
            }
            else
            {
                MergeIntoEntries(key, value);
            }
        }
    }

    /// <summary>
    /// Folds a prepared CRDT mutation's typed <paramref name="delta"/> into
    /// this snapshot's current visible state for <paramref name="key"/> under
    /// <paramref name="mode"/>, returning the re-serialised post-fold state
    /// bytes. Mirrors the live leaf's terminal-commit fold so a read snapshot
    /// of a CRDT key mid-saga converges by the per-replica union.
    /// </summary>
    private byte[] FoldPreparedCrdtDelta(string key, byte[] delta, LatticeMergeMode mode)
    {
        var registry = ResolveCrdtShapeRegistry();
        var shape = registry.TryGet(_treeId, mode)
            ?? throw new InvalidOperationException(
                $"No CrdtShape is registered for tree '{_treeId}' at mode '{mode}'. "
                + "A prepared CRDT-mode entry cannot fold its typed delta on the snapshot "
                + "leaf's terminal commit without a shape descriptor.");

        var typedDelta = shape.DeserializeDelta(delta);
        object typedState;
        if (_entries.TryGetValue(key, out var existing)
            && !existing.IsTombstone
            && existing.Value is { Length: > 0 } existingBytes)
        {
            typedState = shape.DeserializeState(existingBytes);
        }
        else
        {
            typedState = shape.CreateEmpty();
        }
        shape.MergeDelta(typedState, typedDelta);
        return shape.SerializeState(typedState);
    }

    private void ApplyTxAbort(Guid txId)
    {
        _pendingTxDeltas.Remove(txId);
        _pendingTx.Remove(txId);
    }

    private void ApplyTombstoneReap(in LatticeMutation mutation)
    {
        if (!_entries.TryGetValue(mutation.Key, out var existing))
            return;
        if (existing.Timestamp > mutation.Timestamp)
            return;
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        if (!existing.IsTombstone && !existing.IsExpired(nowTicks))
            return;
        _entries.Remove(mutation.Key);
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

