namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Bounded, process-local append ages for one WAL partition. Mutation HLCs are
/// deliberately not used: a newly appended reap or merge may carry an old stamp.
/// </summary>
/// <remarks>
/// The retained appends are a ring of parallel offset and time arrays sorted by offset
/// (issue #6166). Appends are acknowledged almost in offset order, so recording one is an
/// append at the newest end and evicting one advances the oldest, both without allocating;
/// an out-of-order acknowledgement shifts the few newer entries it lands behind. The drain-lag
/// sampler asks once per fresh consumer per tick for the first retained append past a
/// checkpoint, and that lookup is a binary search. The arrays start small and double up to
/// <see cref="Capacity"/>, so a partition that sees few appends holds little.
/// </remarks>
internal sealed class WalAppendTimeline
{
    /// <summary>The most appends retained; a power of two, as the ring's indexing requires.</summary>
    internal const int Capacity = 8192;

    private const int InitialLength = 16;

    /// <summary>Test hook, invoked on the querying thread as a lag query starts.</summary>
    internal static Action? LagQueriedForTest;

    private readonly object _gate = new();
    private long[] _offsets = new long[InitialLength];
    private long[] _appendedAt = new long[InitialLength];

    /// <summary>The ring slot of the oldest retained append.</summary>
    private int _first;

    /// <summary>How many appends are retained.</summary>
    private int _count;

    private long _head = -1;

    /// <summary>
    /// The highest offset acknowledged by an append on this process, or -1. A lower
    /// bound on the partition's last offset: remote and pre-restart appends are absent.
    /// </summary>
    internal long Head
    {
        get
        {
            lock (_gate)
            {
                return _head;
            }
        }
    }

    /// <summary>
    /// Records that <paramref name="offset"/> was appended at <paramref name="utcTicks"/>.
    /// The first record of an offset stands; once <see cref="Capacity"/> appends are
    /// retained the oldest is evicted, so an offset older than all of them is not retained.
    /// </summary>
    internal void Record(long offset, long utcTicks)
    {
        lock (_gate)
        {
            _head = Math.Max(_head, offset);
            var index = _count == 0 || offset > _offsets[Slot(_count - 1)] ? _count : LowerBound(offset);
            if (index < _count && _offsets[Slot(index)] == offset)
            {
                return;
            }

            if (_count == Capacity)
            {
                if (index == 0)
                {
                    return;
                }

                _first = Slot(1);
                _count--;
                index--;
            }
            else if (_count == _offsets.Length)
            {
                Grow();
            }

            for (var i = _count; i > index; i--)
            {
                var to = Slot(i);
                var from = Slot(i - 1);
                _offsets[to] = _offsets[from];
                _appendedAt[to] = _appendedAt[from];
            }

            var slot = Slot(index);
            _offsets[slot] = offset;
            _appendedAt[slot] = utcTicks;
            _count++;
        }
    }

    // Missing history (restart, a remote writer, or bounded eviction) yields a
    // lower bound from the first observed unread append, never an HLC fallback.
    internal long? GetLagTicks(long checkpoint, long nowTicks, out bool incomplete)
    {
        LagQueriedForTest?.Invoke();
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
            var index = LowerBound(checkpoint + 1);
            if (index == _count)
            {
                incomplete = true;
                return null;
            }

            var slot = Slot(index);
            incomplete = _offsets[slot] != checkpoint + 1;
            return Math.Max(0, nowTicks - _appendedAt[slot]);
        }
    }

    /// <summary>The ring slot of the <paramref name="index"/>-th oldest retained append.</summary>
    private int Slot(int index) => (_first + index) & (_offsets.Length - 1);

    /// <summary>The index of the oldest retained append at or past <paramref name="offset"/>, or the count when none is.</summary>
    private int LowerBound(long offset)
    {
        int low = 0, high = _count;
        while (low < high)
        {
            var mid = (low + high) >>> 1;
            if (_offsets[Slot(mid)] < offset)
                low = mid + 1;
            else
                high = mid;
        }

        return low;
    }

    /// <summary>Doubles the ring, laying the retained appends out oldest first.</summary>
    private void Grow()
    {
        var offsets = new long[_offsets.Length * 2];
        var appendedAt = new long[offsets.Length];
        for (var i = 0; i < _count; i++)
        {
            var slot = Slot(i);
            offsets[i] = _offsets[slot];
            appendedAt[i] = _appendedAt[slot];
        }

        _offsets = offsets;
        _appendedAt = appendedAt;
        _first = 0;
    }
}
