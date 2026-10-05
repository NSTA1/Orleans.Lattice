using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Per-tree bounded FIFO buffer holding <see cref="WalRecord"/>
/// records the receiver-side <see cref="ReplicationApplier"/> could
/// not apply because their declared causal dependencies were not yet
/// satisfied (the tree's high-water-mark grain checks them, see
/// <see cref="RequiredDependencies"/>). Drained by its owning
/// <see cref="Grains.CausalApplyBufferGrain"/> after every park, whenever a
/// high-water-mark advance or the bootstrap pin asks for it, and on every
/// replication maintenance tick; entries whose deps
/// remain unsatisfied stay parked. Overflows route the oldest entry
/// to the per-tree dead-letter queue with reason
/// <see cref="LatticeReplicationMetrics.ReasonHlcSkew"/>.
/// <para>
/// One instance is the in-memory mirror inside each tree's durable
/// <see cref="Grains.CausalApplyBufferGrain"/> (#4464), which serializes every
/// park and drain for the tree and persists every change; the lock below keeps
/// the type safe for direct use as well. There is no
/// cross-tree coordination - each tree's buffer is independent. Each
/// instance carries its own pre-built tag arrays (tagged
/// <see cref="LatticeReplicationMetrics.TagTree"/> and
/// <see cref="LatticeReplicationMetrics.TagShard"/>) so the
/// causal-apply observability instruments can be recorded without a
/// per-call allocation on the hot path.
/// </para>
/// </summary>
internal sealed class CausalApplyBuffer
{
    /// <summary>
    /// Canonical shard tag value for the per-tree buffer. The buffer is
    /// one-per-tree today; the shard tag dimension is reserved for a
    /// future per-shard partitioning of the buffer without a wire-format
    /// (= metric tag) break.
    /// </summary>
    private const string DefaultShard = "0";

    private readonly object _gate = new();
    private readonly LinkedList<BufferedEntry> _entries = new();
    private readonly Dictionary<EntryKey, LinkedListNode<BufferedEntry>> _index = new();
    private readonly KeyValuePair<string, object?>[] _treeShardTags;
    private readonly KeyValuePair<string, object?>[] _treeTags;
    private long _totalBytes;

    /// <summary>
    /// Shared empty eviction list handed back on the common no-eviction path
    /// of <see cref="TryAdd"/>. Callers observe the out parameter only when the
    /// outcome is <see cref="AddOutcome.AddedWithEviction"/> (which always
    /// carries a freshly allocated list), so this instance is only ever read as
    /// empty and must be treated as read-only.
    /// </summary>
    private static readonly List<WalRecord> EmptyEvicted = new();

    /// <summary>
    /// Creates a buffer that publishes its observability instruments
    /// tagged with the supplied tree id and a constant shard tag value
    /// of <c>"0"</c>.
    /// </summary>
    public CausalApplyBuffer(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        _treeShardTags =
        [
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeId),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagShard, DefaultShard),
            LatticeTenantLabel.ForTree(treeId),
        ];
        _treeTags =
        [
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeId),
            LatticeTenantLabel.ForTree(treeId),
        ];
    }

    /// <summary>
    /// Test-only convenience constructor that uses an empty tree id for
    /// metric tagging. Production code paths use the
    /// <see cref="CausalApplyBuffer(string)"/> overload.
    /// </summary>
    public CausalApplyBuffer() : this(string.Empty)
    {
    }

    /// <summary>
    /// Cumulative byte size of every parked entry's serialised footprint.
    /// </summary>
    public long TotalBytes
    {
        get
        {
            lock (_gate)
            {
                return _totalBytes;
            }
        }
    }

    /// <summary>The number of entries currently parked.</summary>
    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _entries.Count;
            }
        }
    }

    /// <summary>
    /// Tries to enqueue <paramref name="entry"/>. Returns the eviction
    /// outcome and a list of entries displaced to make room.
    /// Re-enqueuing an entry whose
    /// <c>(treeId, originClusterId, timestamp, key, op)</c> tuple is
    /// already parked is a no-op and reports
    /// <see cref="AddOutcome.Duplicate"/>.
    /// </summary>
    public AddOutcome TryAdd(WalRecord entry, int maxEntries, long maxBytes, out List<WalRecord> evicted)
    {
        // Steady state never evicts: the buffer holds well under its caps, so
        // the eviction list stays empty. Hand back a shared empty sentinel on
        // that common path and materialise a real list only when the eviction
        // loop actually displaces an entry, removing the per-add list allocation
        // from the hot receiver path. The sole caller reads this out parameter
        // only when the outcome is AddedWithEviction (which implies a real list),
        // so the shared instance is never mutated or observed non-empty.
        evicted = EmptyEvicted;
        List<WalRecord>? displaced = null;
        var size = EstimateSize(entry);
        var key = EntryKey.From(entry);
        long evictedBytes = 0;
        AddOutcome outcome;

        lock (_gate)
        {
            if (_index.ContainsKey(key))
            {
                return AddOutcome.Duplicate;
            }

            // Evict from the head of the FIFO until the new entry fits
            // under both caps. A single entry larger than the byte cap
            // is appended without evicting the entire buffer (the cap
            // is soft guidance, not a per-entry hard limit).
            while ((_entries.Count + 1 > maxEntries
                    || (size <= maxBytes && _totalBytes + size > maxBytes))
                   && _entries.First is { } head)
            {
                (displaced ??= new List<WalRecord>()).Add(head.Value.Entry);
                evictedBytes += head.Value.SizeBytes;
                _index.Remove(EntryKey.From(head.Value.Entry));
                _totalBytes -= head.Value.SizeBytes;
                _entries.RemoveFirst();
            }

            if (displaced is not null)
            {
                evicted = displaced;
            }

            var buffered = new BufferedEntry(entry, size, DateTime.UtcNow.Ticks);
            var node = _entries.AddLast(buffered);
            _index[key] = node;
            _totalBytes += size;
            outcome = evicted.Count == 0 ? AddOutcome.Added : AddOutcome.AddedWithEviction;
        }

        // Emit observability outside the lock to keep the critical
        // section minimal. UpDownCounter / Counter are thread-safe.
        LatticeReplicationMetrics.ApplyCausalViolationsBlocked.Add(1, _treeTags);
        LatticeReplicationMetrics.ApplyBufferedEntries.Add(1, _treeShardTags);
        LatticeReplicationMetrics.ApplyBufferBytes.Add(size, _treeShardTags);
        if (evicted.Count > 0)
        {
            LatticeReplicationMetrics.ApplyBufferedEntries.Add(-evicted.Count, _treeShardTags);
            LatticeReplicationMetrics.ApplyBufferBytes.Add(-evictedBytes, _treeShardTags);
        }

        return outcome;
    }

    /// <summary>
    /// Re-inserts an entry restored from the durable buffer state on
    /// activation, at the tail of the FIFO, keeping its original park time.
    /// Updates the buffered-entry and buffer-byte gauges like a park, but does
    /// not count a new causal violation (the violation was counted when the
    /// entry was first parked). A duplicate identity is ignored.
    /// </summary>
    public void Restore(WalRecord entry, long parkedAtTicks)
    {
        var size = EstimateSize(entry);
        var key = EntryKey.From(entry);
        lock (_gate)
        {
            if (_index.ContainsKey(key))
            {
                return;
            }

            _index[key] = _entries.AddLast(new BufferedEntry(entry, size, parkedAtTicks));
            _totalBytes += size;
        }

        LatticeReplicationMetrics.ApplyBufferedEntries.Add(1, _treeShardTags);
        LatticeReplicationMetrics.ApplyBufferBytes.Add(size, _treeShardTags);
    }

    /// <summary>
    /// Returns the parked entries, oldest first, with their original park
    /// times - the shape the durable buffer state persists.
    /// </summary>
    public List<(WalRecord Entry, long ParkedAtTicks)> Snapshot()
    {
        lock (_gate)
        {
            var snapshot = new List<(WalRecord, long)>(_entries.Count);
            foreach (var buffered in _entries)
            {
                snapshot.Add((buffered.Entry, buffered.ParkedAtTicks));
            }
            return snapshot;
        }
    }

    /// <summary>
    /// Empties the buffer and withdraws its contribution from the
    /// buffered-entry and buffer-byte gauges. Called when the owning
    /// activation discards this in-memory copy (deactivation, or a rebuild
    /// from durable state after a failed write) so the gauges do not drift.
    /// </summary>
    public void Release()
    {
        int count;
        long bytes;
        lock (_gate)
        {
            count = _entries.Count;
            bytes = _totalBytes;
            _entries.Clear();
            _index.Clear();
            _totalBytes = 0;
        }

        if (count > 0)
        {
            LatticeReplicationMetrics.ApplyBufferedEntries.Add(-count, _treeShardTags);
            LatticeReplicationMetrics.ApplyBufferBytes.Add(-bytes, _treeShardTags);
        }
    }

    /// <summary>
    /// Removes and returns, in FIFO order, every parked entry for which
    /// <paramref name="isSatisfied"/> returns <see langword="true"/>. The
    /// owning grain evaluates each entry's dependencies on the tree's
    /// high-water-mark grain first (see <see cref="RequiredDependencies"/>) and
    /// passes the verdicts in, so this call stays synchronous under the
    /// buffer's lock.
    /// </summary>
    public List<WalRecord> DrainSatisfied(Func<WalRecord, bool> isSatisfied)
    {
        ArgumentNullException.ThrowIfNull(isSatisfied);
        List<WalRecord> ready;
        // Single auxiliary list for per-entry wait samples; bytes
        // accumulate into a scalar so there is no second list.
        // Allocated lazily on first drained entry.
        List<long>? waitTicks = null;
        long drainedBytesTotal = 0;
        var nowTicks = DateTime.UtcNow.Ticks;

        lock (_gate)
        {
            // Pre-size to the parked-entry count read under the gate: the drain
            // yields at most one record per buffered entry, so this is a tight
            // upper bound. On the empty steady-state buffer this is a zero-capacity
            // list (no backing array), so the common no-reorder path is unaffected.
            ready = new List<WalRecord>(_entries.Count);
            var node = _entries.First;
            while (node is not null)
            {
                var next = node.Next;
                if (isSatisfied(node.Value.Entry))
                {
                    ready.Add(node.Value.Entry);
                    drainedBytesTotal += node.Value.SizeBytes;
                    (waitTicks ??= new List<long>()).Add(nowTicks - node.Value.ParkedAtTicks);
                    _index.Remove(EntryKey.From(node.Value.Entry));
                    _totalBytes -= node.Value.SizeBytes;
                    _entries.Remove(node);
                }
                node = next;
            }
        }

        if (ready.Count > 0)
        {
            LatticeReplicationMetrics.ApplyBufferedEntries.Add(-ready.Count, _treeShardTags);
            LatticeReplicationMetrics.ApplyBufferBytes.Add(-drainedBytesTotal, _treeShardTags);

            // waitTicks is non-null whenever ready.Count > 0 by construction.
            var samples = waitTicks!;
            for (var i = 0; i < samples.Count; i++)
            {
                var deltaTicks = samples[i];
                if (deltaTicks < 0)
                {
                    deltaTicks = 0;
                }
                var ms = deltaTicks / (double)TimeSpan.TicksPerMillisecond;
                LatticeReplicationMetrics.ApplyDependencyWaitMs.Record(ms, _treeTags);
            }
        }

        return ready;
    }

    /// <summary>
    /// Returns the dependencies of <paramref name="entry"/> the receiver must
    /// see applied before it applies the entry, or <see langword="null"/> when
    /// there are none. Each <c>(origin, t)</c> in the entry's vector-clock
    /// frontier names <em>origin's write at HLC t</em>; the tree's
    /// high-water-mark grain checks it
    /// (<see cref="Grains.IReplicationHighWaterMarkGrain.CheckDependenciesAsync"/>),
    /// and reports a dependency on a write the tree lost for good as
    /// <see cref="CausalDependencyVerdict.Lost"/> (#4603).
    /// <para>
    /// Two components are excluded. The entry's own origin diagonal: that
    /// origin's writes reach the receiver over the same shipper as the entry,
    /// and requiring it would deadlock the diagonal. And a dependency on
    /// <paramref name="localClusterId"/> (the receiver's own cluster): the
    /// receiver, by definition, durably holds every write it authored itself,
    /// so a foreign entry that depends on one of them is trivially satisfiable.
    /// Without this exemption such an entry parks forever (the receiver's own
    /// diagonal never advances), which stalls convergence whenever a peer's write causally
    /// follows a write the receiver originated - e.g. after an A-C partition
    /// heals and C's post-partition write carries a dependency on A's
    /// pre-partition write.
    /// </para>
    /// </summary>
    public static VersionVector? RequiredDependencies(in WalRecord entry, string? localClusterId = null)
    {
        var vc = entry.VectorClock;
        if (vc is null || vc.Entries.Count == 0)
        {
            return null;
        }

        VersionVector? required = null;
        foreach (var (origin, ts) in vc.Entries)
        {
            if (string.Equals(origin, entry.OriginClusterId, StringComparison.Ordinal))
            {
                continue;
            }

            if (localClusterId is not null
                && string.Equals(origin, localClusterId, StringComparison.Ordinal))
            {
                continue;
            }

            (required ??= new VersionVector()).Entries[origin] = ts;
        }

        return required;
    }

    private static long EstimateSize(WalRecord entry)
    {
        var keyLen = entry.Key?.Length ?? 0;
        var endLen = entry.EndExclusiveKey?.Length ?? 0;
        var valueLen = entry.Value?.Length ?? 0;
        return ((long)keyLen * 2L) + ((long)endLen * 2L) + valueLen + 128L;
    }

    private readonly record struct BufferedEntry(WalRecord Entry, long SizeBytes, long ParkedAtTicks);

    private readonly record struct EntryKey(
        string TreeId,
        string OriginClusterId,
        HybridLogicalClock Timestamp,
        string Key,
        MutationKind Op)
    {
        public static EntryKey From(WalRecord entry) =>
            new(
                entry.TreeId ?? string.Empty,
                entry.OriginClusterId ?? string.Empty,
                entry.Timestamp,
                entry.Key ?? string.Empty,
                entry.Op);
    }
}

/// <summary>
/// Outcome of a <see cref="CausalApplyBuffer.TryAdd"/> call.
/// </summary>
internal enum AddOutcome
{
    /// <summary>The entry was parked without evicting any existing entries.</summary>
    Added,

    /// <summary>The entry was parked after evicting one or more older entries to honour the configured caps.</summary>
    AddedWithEviction,

    /// <summary>An entry with the same identity tuple was already parked; the call was a no-op.</summary>
    Duplicate,
}

