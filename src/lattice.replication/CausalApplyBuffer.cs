using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Per-tree bounded FIFO buffer holding <see cref="ReplogEntry"/>
/// records the receiver-side <see cref="ReplicationApplier"/> could
/// not apply because their declared causal dependencies were not yet
/// satisfied by the local vector clock. Drained on every successful
/// apply that advances the local vector clock; entries whose deps
/// remain unsatisfied stay parked. Overflows route the oldest entry
/// to the per-tree dead-letter queue with reason
/// <see cref="LatticeReplicationMetrics.ReasonHlcSkew"/>.
/// <para>
/// The buffer is a single per-tree instance owned by the applier
/// singleton; concurrent receiver-side calls into the same applier
/// serialize through the buffer's private lock. There is no
/// cross-tree coordination — each tree's buffer is independent. Each
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
        ];
        _treeTags =
        [
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeId),
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
    public AddOutcome TryAdd(ReplogEntry entry, int maxEntries, long maxBytes, out List<ReplogEntry> evicted)
    {
        evicted = new List<ReplogEntry>();
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
                evicted.Add(head.Value.Entry);
                evictedBytes += head.Value.SizeBytes;
                _index.Remove(EntryKey.From(head.Value.Entry));
                _totalBytes -= head.Value.SizeBytes;
                _entries.RemoveFirst();
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
    /// Removes and returns every parked entry whose declared
    /// dependencies are dominated by <paramref name="localVc"/>.
    /// Iteration is FIFO so causally-earlier entries unblock first.
    /// </summary>
    public List<ReplogEntry> DrainSatisfied(VersionVector localVc)
    {
        ArgumentNullException.ThrowIfNull(localVc);
        var ready = new List<ReplogEntry>();
        // Single auxiliary list for per-entry wait samples; bytes
        // accumulate into a scalar so there is no second list.
        // Allocated lazily on first drained entry.
        List<long>? waitTicks = null;
        long drainedBytesTotal = 0;
        var nowTicks = DateTime.UtcNow.Ticks;

        lock (_gate)
        {
            var node = _entries.First;
            while (node is not null)
            {
                var next = node.Next;
                if (DependenciesSatisfied(node.Value.Entry, localVc))
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
    /// Returns <see langword="true"/> when every origin component in
    /// <paramref name="entry"/>'s vector-clock frontier is
    /// dominated-or-equal by the corresponding component on
    /// <paramref name="localVc"/>. The entry's own origin diagonal
    /// is excluded — the per-origin high-water-mark table is the
    /// authoritative dedup key for that component, and including it
    /// here would deadlock the diagonal.
    /// </summary>
    public static bool DependenciesSatisfied(ReplogEntry entry, VersionVector localVc)
    {
        ArgumentNullException.ThrowIfNull(localVc);
        var vc = entry.VectorClock;
        if (vc is null || vc.Entries.Count == 0)
        {
            return true;
        }

        foreach (var (origin, ts) in vc.Entries)
        {
            if (string.Equals(origin, entry.OriginClusterId, StringComparison.Ordinal))
            {
                continue;
            }

            if (localVc.GetClock(origin) < ts)
            {
                return false;
            }
        }

        return true;
    }

    private static long EstimateSize(ReplogEntry entry)
    {
        var keyLen = entry.Key?.Length ?? 0;
        var endLen = entry.EndExclusiveKey?.Length ?? 0;
        var valueLen = entry.Value?.Length ?? 0;
        return ((long)keyLen * 2L) + ((long)endLen * 2L) + valueLen + 128L;
    }

    private readonly record struct BufferedEntry(ReplogEntry Entry, long SizeBytes, long ParkedAtTicks);

    private readonly record struct EntryKey(
        string TreeId,
        string OriginClusterId,
        HybridLogicalClock Timestamp,
        string Key,
        ReplogOp Op)
    {
        public static EntryKey From(ReplogEntry entry) =>
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

