namespace Orleans.Lattice.Replication;

/// <summary>
/// Per-partition offset cursor for <see cref="IChangeFeed.Subscribe(string, ChangeFeedCursor, bool, System.Threading.CancellationToken)"/>.
/// Carries one <em>exclusive</em> lower-bound sequence number per WAL
/// partition - the offset of the NEXT entry the consumer wants to read.
/// The next <c>Subscribe</c> call yields every entry whose
/// <c>(partition, sequence)</c> position is greater than or equal to
/// the matching cursor entry. <see cref="Initial"/> stores zero (or no
/// entry) for every partition, which yields every entry from offset 0
/// onwards.
/// <para>
/// <b>Why offset, not HLC.</b> The legacy HLC-cursor shape silently
/// dropped any entry whose <see cref="Orleans.Lattice.HybridLogicalClock"/>
/// timestamp was less than or equal to the previously-reported
/// cursor. That predicate assumed HLC monotonicity per WAL partition,
/// which does not hold under parallel cross-leaf appends to a shared
/// WAL partition: each leaf advances its own HLC independently, two
/// leaves can produce out-of-order HLCs that arrive interleaved at
/// the WAL grain, and the lower-HLC entry was filtered out as
/// already-delivered (see Phase D1b for the symptom). WAL offsets
/// are by construction monotonic per partition (assigned under the
/// WAL grain activation's lock), so a per-partition offset cursor
/// has the property the HLC cursor was assumed to have, without the
/// silently-broken assumption.
/// </para>
/// <para>
/// The type is a defensive snapshot: callers may freely mutate the
/// dictionary they passed at construction time without affecting the
/// stored cursor. Equality is structural over the partition map.
/// </para>
/// </summary>
public readonly struct ChangeFeedCursor : IEquatable<ChangeFeedCursor>
{
    private readonly IReadOnlyDictionary<int, long>? _partitionOffsets;

    /// <summary>
    /// The initial cursor that yields every entry across every
    /// partition. Equivalent to passing
    /// <see cref="Orleans.Lattice.HybridLogicalClock.Zero"/>
    /// to the legacy HLC overload.
    /// </summary>
    public static ChangeFeedCursor Initial { get; } = default;

    /// <summary>
    /// Builds a cursor from a partition -&gt; exclusive-lower-bound-offset
    /// snapshot (the offset of the NEXT entry the consumer wants to
    /// read on that partition). The dictionary is defensively cloned so
    /// subsequent caller-side mutation cannot poison the stored cursor.
    /// </summary>
    /// <param name="partitionOffsets">
    /// Partition -&gt; cursor entry. Partitions absent from the map are
    /// implicitly at offset <c>0</c> (every entry of an absent partition
    /// is yielded). Pass <see langword="null"/> or an empty dictionary
    /// for the same effect as <see cref="Initial"/>.
    /// </param>
    public ChangeFeedCursor(IReadOnlyDictionary<int, long>? partitionOffsets)
    {
        if (partitionOffsets is null || partitionOffsets.Count == 0)
        {
            _partitionOffsets = null;
            return;
        }

        var snapshot = new Dictionary<int, long>(partitionOffsets.Count);
        foreach (var kv in partitionOffsets)
        {
            if (kv.Value < 0)
            {
                throw new ArgumentException(
                    $"Partition {kv.Key} cursor offset {kv.Value} is negative; offsets must be non-negative.",
                    nameof(partitionOffsets));
            }
            snapshot[kv.Key] = kv.Value;
        }
        _partitionOffsets = snapshot;
    }

    /// <summary>
    /// Returns the exclusive lower-bound offset for
    /// <paramref name="partition"/> - the offset of the next entry to
    /// read. Entries with <c>sequence &gt;= </c> this value are yielded
    /// by the next <c>Subscribe</c> call. Partitions absent from the
    /// cursor return <c>0</c> (every entry yielded).
    /// </summary>
    /// <param name="partition">Zero-based WAL partition index.</param>
    public long GetOffsetForPartition(int partition)
    {
        if (_partitionOffsets is null)
        {
            return 0L;
        }
        return _partitionOffsets.TryGetValue(partition, out var offset) ? offset : 0L;
    }

    /// <summary>
    /// Returns the underlying partition -&gt; offset map (a defensive
    /// snapshot owned by this cursor). Returns an empty enumerable for
    /// <see cref="Initial"/>. Intended for callers that need to thread
    /// the cursor through a persistence boundary; per-partition reads
    /// should prefer <see cref="GetOffsetForPartition(int)"/>.
    /// </summary>
    public IReadOnlyDictionary<int, long> PartitionOffsets
        => _partitionOffsets ?? EmptyMap;

    private static readonly IReadOnlyDictionary<int, long> EmptyMap
        = new Dictionary<int, long>();

    /// <summary>
    /// Structural equality over the partition map. Two cursors are
    /// equal when they have the same set of partition entries and
    /// each entry has the same offset.
    /// </summary>
    public bool Equals(ChangeFeedCursor other)
    {
        var a = _partitionOffsets;
        var b = other._partitionOffsets;
        if (ReferenceEquals(a, b)) return true;
        if (a is null) return b is null || b.Count == 0;
        if (b is null) return a.Count == 0;
        if (a.Count != b.Count) return false;
        foreach (var kv in a)
        {
            if (!b.TryGetValue(kv.Key, out var otherOffset) || otherOffset != kv.Value)
            {
                return false;
            }
        }
        return true;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj)
        => obj is ChangeFeedCursor other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        if (_partitionOffsets is null || _partitionOffsets.Count == 0)
        {
            return 0;
        }
        var hash = new HashCode();
        // Hash order-independent: XOR per-entry hashes so the result
        // does not depend on dictionary iteration order.
        var combined = 0;
        foreach (var kv in _partitionOffsets)
        {
            combined ^= HashCode.Combine(kv.Key, kv.Value);
        }
        hash.Add(combined);
        return hash.ToHashCode();
    }

    /// <summary>Equality operator over <see cref="ChangeFeedCursor"/>.</summary>
    public static bool operator ==(ChangeFeedCursor left, ChangeFeedCursor right)
        => left.Equals(right);

    /// <summary>Inequality operator over <see cref="ChangeFeedCursor"/>.</summary>
    public static bool operator !=(ChangeFeedCursor left, ChangeFeedCursor right)
        => !left.Equals(right);
}
