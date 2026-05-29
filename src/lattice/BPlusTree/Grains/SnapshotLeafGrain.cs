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
    LatticeOptionsResolver optionsResolver,
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

    /// <summary>Upper-bound (exclusive) WAL offset the snapshot replays to.</summary>
    private long _capturedOffset = -1;

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

    /// <inheritdoc />
    public async Task OpenAsync(string treeId, int shardIndex, long capturedOffset, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        if (shardIndex < 0)
            throw new ArgumentOutOfRangeException(nameof(shardIndex), "Shard index must be non-negative.");
        if (capturedOffset < 0)
            throw new ArgumentOutOfRangeException(nameof(capturedOffset), "Captured offset must be non-negative.");
        cancellationToken.ThrowIfCancellationRequested();

        if (_opened)
        {
            // Idempotent re-open with the same coordinate is a no-op;
            // a different coordinate would target a different grain
            // key, so a mismatch here indicates a programming error
            // upstream and must surface loudly.
            if (_treeId != treeId || _shardIndex != shardIndex || _capturedOffset != capturedOffset)
            {
                throw new InvalidOperationException(
                    $"SnapshotLeafGrain for '{this.GetPrimaryKeyString()}' was already opened against ({_treeId}, {_shardIndex}, {_capturedOffset}); refusing to re-open against ({treeId}, {shardIndex}, {capturedOffset}).");
            }
            return;
        }

        _treeId = treeId;
        _shardIndex = shardIndex;
        _capturedOffset = capturedOffset;

        await ReplayWalAsync(cancellationToken);

        _opened = true;

        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug(
                "SnapshotLeafGrain opened: tree={TreeId}, shard={ShardIndex}, capturedOffset={CapturedOffset}, entries={EntryCount}, pendingSagas={PendingSagaCount}.",
                treeId, shardIndex, capturedOffset, _entries.Count, _pendingTx.Count);
        }

        _ = optionsMonitor;
        _ = context;
    }

    /// <inheritdoc />
    public Task<List<string>> GetKeysAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null, int limit = int.MaxValue)
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
            result.Add(key);
            if (result.Count >= limit)
                break;
        }
        return Task.FromResult(result);
    }

    /// <inheritdoc />
    public Task<List<KeyValuePair<string, byte[]>>> GetEntriesAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null, int limit = int.MaxValue)
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
            result.Add(new KeyValuePair<string, byte[]>(key, value.Value));
            if (result.Count >= limit)
                break;
        }
        return Task.FromResult(result);
    }

    /// <summary>
    /// Drives the per-slice WAL replay loop. Reads
    /// <c>[0, _capturedOffset)</c> through
    /// <see cref="ILeafReplayCoordinatorGrain.ReadSliceAsync"/> in
    /// <see cref="ReplaySliceBudget"/>-sized chunks, applying each
    /// mutation to the in-memory projection. Cancellation is honoured
    /// between slices and between entries.
    /// </summary>
    private async Task ReplayWalAsync(CancellationToken cancellationToken)
    {
        if (_capturedOffset == 0)
            return;

        var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
            $"{_treeId}/{_shardIndex}");

        // ReadSliceAsync semantics: (fromExclusive, toInclusive]. We
        // want [0, capturedOffset), so the inclusive upper bound is
        // capturedOffset - 1.
        long fromExclusive = -1;
        long toInclusive = _capturedOffset - 1;
        long entriesObserved = 0;
        var sw = System.Diagnostics.Stopwatch.StartNew();

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
                ApplyEntry(entry.Mutation);
                entriesObserved++;
            }

            var lastOffset = slice[^1].Offset;
            if (lastOffset <= fromExclusive)
                break; // defensive: never spin if the slice failed to advance.
            fromExclusive = lastOffset;
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
        if (entriesObserved > 0)
        {
            LatticeMetrics.SnapshotReplayEntries.Add(entriesObserved, tags);
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
        // The shard's WAL only carries entries authored against this
        // shard; no shard-index filter is needed. Per-leaf key-range
        // filtering is irrelevant at the shard scope - the snapshot
        // view covers every key in the shard regardless of which leaf
        // currently owns it.
        switch (mutation.Kind)
        {
            case MutationKind.Set:
                if (mutation.IsPrepared)
                    AddPreparedMutation(mutation.TransactionId, mutation.Key, BuildLww(mutation, isTombstone: mutation.IsTombstone));
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

    private void AddPreparedMutation(Guid txId, string key, LwwValue<byte[]> incoming)
    {
        if (!_pendingTx.TryGetValue(txId, out var bucket))
        {
            bucket = new Dictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal);
            _pendingTx[txId] = bucket;
        }
        bucket[key] = incoming;
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
        foreach (var (key, _) in _entries)
        {
            if (string.CompareOrdinal(key, startInclusive) < 0)
                continue;
            if (string.CompareOrdinal(key, endExclusive) >= 0)
                break;
            (toRewrite ??= []).Add(key);
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
        if (!_pendingTx.Remove(txId, out var bucket))
            return;
        foreach (var (key, value) in bucket)
        {
            MergeIntoEntries(key, value);
        }
    }

    private void ApplyTxAbort(Guid txId)
    {
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

