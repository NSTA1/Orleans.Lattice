namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The pages of one WAL partition a <see cref="LeafReplayCoordinatorGrain"/>
/// has read from storage (issue #5896), so every leaf replaying that partition
/// is served from one read of each page rather than reading it again itself.
/// </summary>
/// <remarks>
/// <para>
/// <b>Not thread-safe; it relies on the grain.</b> Every member runs inside the
/// owning coordinator's turn. The coordinator's reads interleave, so members are
/// called between awaits, but never concurrently.
/// </para>
/// <para>
/// <b>Incarnation (conditions I2, I3).</b> The ring holds pages of exactly one
/// partition incarnation. <see cref="Reset"/> drops every page and advances the
/// fill epoch, so a fill that started before it can never insert
/// (<see cref="TryInsert"/>).
/// </para>
/// <para>
/// <b>Memory (condition A-f).</b> A ring holds at most
/// <see cref="LatticeOptions.WalReplayPageRingBytes"/> of estimated page bytes,
/// evicting its lowest pages first, which every replaying leaf has already
/// passed; and the rings of one process together never exceed that same budget,
/// because a page that would take the process past it is served without being
/// cached. The worst case is therefore the budget plus the fills in flight,
/// each bounded by the slice budget.
/// </para>
/// </remarks>
internal sealed class ReplayPageRing(long budgetBytes)
{
    private static long _processBytes;

    private readonly List<ReplayPage> _pages = [];
    private readonly Dictionary<long, (Task<ReplayPage?> Fill, long StartedAt)> _fills = [];
    private long _bytes;

    /// <summary>Estimated bytes all rings in this process hold.</summary>
    internal static long ProcessBytes => Volatile.Read(ref _processBytes);

    /// <summary>The partition incarnation the ring's pages belong to, or <see langword="null"/> before the first check.</summary>
    public long? Incarnation { get; private set; }

    /// <summary>Advances whenever the ring is reset; a fill inserts only at the epoch it started at.</summary>
    public long Epoch { get; private set; }

    /// <summary>The pages, ascending by <see cref="ReplayPage.FromExclusive"/>.</summary>
    public IReadOnlyList<ReplayPage> Pages => _pages;

    /// <summary>Estimated bytes this ring holds.</summary>
    public long Bytes => _bytes;

    /// <summary>
    /// Drops every page and starts the ring at <paramref name="incarnation"/>.
    /// Fills in flight are abandoned: they finish, but never insert.
    /// </summary>
    public void Reset(long incarnation)
    {
        foreach (var page in _pages)
            Release(page);
        _pages.Clear();
        _fills.Clear();
        Incarnation = incarnation;
        Epoch++;
    }

    /// <summary>
    /// The pages from the one covering <paramref name="fromExclusive"/>
    /// onwards, for <see cref="ReplaySliceProjection.Project"/>; empty when no
    /// page covers it.
    /// </summary>
    public IReadOnlyList<ReplayPage> RunFrom(long fromExclusive)
    {
        for (var i = 0; i < _pages.Count; i++)
        {
            if (_pages[i].CoversFrom(fromExclusive))
                return _pages.GetRange(i, _pages.Count - i);
        }

        return [];
    }

    /// <summary>
    /// A fill already reading from <paramref name="fromExclusive"/> that a new
    /// request may join, unless it has been in flight longer than
    /// <paramref name="agingBoundTicks"/> (condition A-e), in which case the
    /// caller issues its own.
    /// </summary>
    public Task<ReplayPage?>? JoinableFill(long fromExclusive, long now, long agingBoundTicks) =>
        _fills.TryGetValue(fromExclusive, out var fill) && now - fill.StartedAt <= agingBoundTicks
            ? fill.Fill
            : null;

    /// <summary>Registers a fill so later requests from the same offset join it.</summary>
    public void RegisterFill(long fromExclusive, Task<ReplayPage?> fill, long now)
    {
        if (!fill.IsCompleted)
            _fills[fromExclusive] = (fill, now);
    }

    /// <summary>Forgets a finished fill, unless a newer one has taken its offset.</summary>
    public void CompleteFill(long fromExclusive, Task<ReplayPage?> fill)
    {
        if (_fills.TryGetValue(fromExclusive, out var current) && ReferenceEquals(current.Fill, fill))
            _fills.Remove(fromExclusive);
    }

    /// <summary>
    /// Inserts a filled page, if the ring is still at the epoch and incarnation
    /// the fill started at and the byte budgets allow it.
    /// </summary>
    /// <returns>Whether the page is now cached.</returns>
    public bool TryInsert(ReplayPage page, long epoch)
    {
        ArgumentNullException.ThrowIfNull(page);
        if (epoch != Epoch || page.Incarnation != Incarnation || budgetBytes <= 0)
            return false;

        var size = EstimateBytes(page);
        if (size > budgetBytes)
            return false;

        while (_pages.Count > 0 && _bytes + size > budgetBytes)
        {
            Release(_pages[0]);
            _pages.RemoveAt(0);
        }

        if (Interlocked.Add(ref _processBytes, size) > long.MaxValue)
        {
            Interlocked.Add(ref _processBytes, -size);
            return false;
        }

        _bytes += size;
        var index = _pages.FindIndex(p => p.FromExclusive > page.FromExclusive);
        _pages.Insert(index < 0 ? _pages.Count : index, page);
        return true;
    }

    /// <summary>
    /// Reads one page from storage through the commit-log reader's clamped
    /// unfiltered read (condition A-g) and caches it. The read is bracketed by
    /// two reads of the partition incarnation: a reset that lands while the
    /// page is read changes it, and the page is then discarded and the ring
    /// reset (condition I2). Runs on no caller's token, because several
    /// requests may be waiting on it.
    /// </summary>
    /// <returns>The page read, or <see langword="null"/> when the window held nothing or a reset landed during the read.</returns>
    public async Task<ReplayPage?> FillAsync(
        ICommitLogReader reader, string treeId, int partition, long fromExclusive, long toInclusive, int budget)
    {
        ArgumentNullException.ThrowIfNull(reader);
        var epoch = Epoch;
        var incarnation = Incarnation ?? await reader.GetIncarnationAsync(treeId, partition, CancellationToken.None);
        var entries = new List<CommitLogSliceEntry>();
        await foreach (var (offset, mutation) in reader.ReadAsync(treeId, partition, fromExclusive, CancellationToken.None))
        {
            if (offset > toInclusive)
                break;
            entries.Add(new CommitLogSliceEntry(offset, mutation));
            if (entries.Count >= budget)
                break;
        }

        var after = await reader.GetIncarnationAsync(treeId, partition, CancellationToken.None);
        if (after != incarnation)
        {
            if (Epoch == epoch)
                Reset(after);
            return null;
        }

        if (entries.Count == 0)
            return null;

        var page = new ReplayPage(fromExclusive, entries.ToArray(), incarnation);
        TryInsert(page, epoch);
        return page;
    }

    /// <summary>Drops every page, returning its bytes to the process budget.</summary>
    public void Clear()
    {
        foreach (var page in _pages)
            Release(page);
        _pages.Clear();
        _fills.Clear();
    }

    private void Release(ReplayPage page)
    {
        var size = EstimateBytes(page);
        _bytes -= size;
        Interlocked.Add(ref _processBytes, -size);
    }

    /// <summary>An estimate of the bytes a page holds: per entry, its key, value and a fixed overhead.</summary>
    internal static long EstimateBytes(ReplayPage page)
    {
        long bytes = 64;
        foreach (var entry in page.Entries)
        {
            bytes += 96
                + ((entry.Mutation.Key?.Length ?? 0) * 2L)
                + ((entry.Mutation.EndExclusiveKey?.Length ?? 0) * 2L)
                + (entry.Mutation.Value?.Length ?? 0);
        }

        return bytes;
    }

    /// <summary>Test-only: forgets the process-wide tally.</summary>
    internal static void ResetProcessBytesForTest() => Volatile.Write(ref _processBytes, 0);
}
