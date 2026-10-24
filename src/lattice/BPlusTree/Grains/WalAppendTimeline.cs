namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Bounded, process-local append ages for one WAL partition. Mutation HLCs are
/// deliberately not used: a newly appended reap or merge may carry an old stamp.
/// </summary>
internal sealed class WalAppendTimeline
{
    internal const int Capacity = 8192;
    private readonly object _gate = new();
    private readonly SortedDictionary<long, long> _appendedAt = new();
    private long _head = -1;

    internal void Record(long offset, long utcTicks)
    {
        lock (_gate)
        {
            _head = Math.Max(_head, offset);
            _appendedAt.TryAdd(offset, utcTicks);
            while (_appendedAt.Count > Capacity)
                _appendedAt.Remove(_appendedAt.First().Key);
        }
    }

    // Missing history (restart, a remote writer, or bounded eviction) yields a
    // lower bound from the first observed unread append, never an HLC fallback.
    internal long? GetLagTicks(long checkpoint, long nowTicks, out bool incomplete)
    {
        lock (_gate)
        {
            incomplete = false;
            if (_head < 0 || checkpoint > _head)
            {
                incomplete = true;
                return null;
            }
            if (checkpoint == _head)
                return 0;
            if (_appendedAt.TryGetValue(checkpoint + 1, out var appendedAt))
                return Math.Max(0, nowTicks - appendedAt);
            incomplete = true;
            foreach (var entry in _appendedAt)
            {
                if (entry.Key > checkpoint)
                    return Math.Max(0, nowTicks - entry.Value);
            }
            return null;
        }
    }
}
