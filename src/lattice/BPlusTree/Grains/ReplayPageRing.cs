using System.Diagnostics;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The pages of one WAL partition a <see cref="LeafReplayCoordinatorGrain"/>
/// has read from storage (issue #5896), so every leaf replaying that partition
/// is served from one read of each page rather than reading it again itself.
/// </summary>
/// <remarks>
/// <para>
/// <b>Threading.</b> The fill bookkeeping, the epoch and the incarnation are
/// touched only inside the owning coordinator's turn. The pages and the demand
/// table are also read, and pages removed, by <see cref="ReplayPageLedger"/>
/// on behalf of other rings, so every change to either runs under the ledger's
/// lock, and the pages are published as an immutable array: a serve that took
/// the array before an eviction keeps projecting from it, and pages are
/// immutable, so that serve is unaffected (condition AC2).
/// </para>
/// <para>
/// <b>Incarnation (conditions I2, I3).</b> The ring holds pages of exactly one
/// partition incarnation. <see cref="Reset"/> drops every page and advances the
/// fill epoch, so a fill that started before it can never insert
/// (<see cref="TryInsert"/>).
/// </para>
/// <para>
/// <b>Memory (condition A-f, issue #5877).</b> Page bytes are charged to the
/// process's <see cref="ReplayPageLedger"/>, which bounds every ring together
/// and chooses what to evict across all of them. The worst case is that bound
/// plus the fills in flight, each bounded by the slice budget.
/// </para>
/// <para>
/// <b>Demand (issue #5877).</b> Leaves queued for, or reading, this partition
/// declare their next offset, so the ledger evicts the pages no one still needs
/// before those the next admission wave will read. The table is bounded and
/// leased, and it is advisory only (condition DT2).
/// </para>
/// </remarks>
internal sealed class ReplayPageRing
{
    /// <summary>The most declared readers a ring tracks; the oldest lease is dropped beyond it (condition DT3).</summary>
    internal const int MaxDemandEntries = 4096;

    private readonly ReplayPageLedger _ledger;
    private readonly Dictionary<long, (Task<ReplayPage?> Fill, long StartedAt)> _fills = [];
    private readonly Dictionary<string, (long Next, long ExpiresAt)> _demand = new(StringComparer.Ordinal);
    private readonly object _demandGate = new();
    private Slot[] _slots = [];
    private long _bytes;
    private bool _detached;
    private long[]? _liveDemand;
    private long _liveDemandValidUntil = long.MinValue;

    /// <summary>One cached page, with its estimated size and the order it was admitted in.</summary>
    internal readonly record struct Slot(ReplayPage Page, long Bytes, long Sequence);

    /// <summary>Creates a ring charged to <paramref name="ledger"/>.</summary>
    internal ReplayPageRing(ReplayPageLedger ledger)
    {
        ArgumentNullException.ThrowIfNull(ledger);
        _ledger = ledger;
        ledger.Attach(this);
    }

    /// <summary>The ledger this ring is charged to.</summary>
    internal ReplayPageLedger Ledger => _ledger;

    /// <summary>The partition incarnation the ring's pages belong to, or <see langword="null"/> before the first check.</summary>
    public long? Incarnation { get; private set; }

    /// <summary>Advances whenever the ring is reset; a fill inserts only at the epoch it started at.</summary>
    public long Epoch { get; private set; }

    /// <summary>
    /// The pages, ascending by <see cref="ReplayPage.FromExclusive"/>. Copies;
    /// for tests and diagnostics only - a serve uses <see cref="RunFrom"/>.
    /// </summary>
    internal IReadOnlyList<ReplayPage> Pages => Array.ConvertAll(Volatile.Read(ref _slots), s => s.Page);

    /// <summary>
    /// Whether the ring has been cleared and detached from the ledger. Read
    /// under the ledger's lock; a detached ring admits nothing (issue #5877).
    /// </summary>
    internal bool IsDetachedLocked => _detached;

    /// <summary>Marks the ring detached. Runs under the ledger's lock.</summary>
    internal void MarkDetachedLocked() => _detached = true;

    /// <summary>Estimated bytes this ring holds.</summary>
    public long Bytes => Volatile.Read(ref _bytes);

    /// <summary>The cached pages, for the ledger. Read under its lock.</summary>
    internal Slot[] SlotsLocked => _slots;

    /// <summary>
    /// Drops every page and starts the ring at <paramref name="incarnation"/>.
    /// Fills in flight are abandoned: they finish, but never insert.
    /// </summary>
    public void Reset(long incarnation)
    {
        lock (_ledger.Gate)
        {
            var pages = _slots.Length;
            _ledger.ReleaseLocked(DropAllLocked(), pages);
        }
        _fills.Clear();
        Incarnation = incarnation;
        Epoch++;
    }

    /// <summary>
    /// The pages from the one covering <paramref name="fromExclusive"/>
    /// onwards, for <see cref="ReplaySliceProjection.Project"/>; empty when no
    /// page covers it. The result is a snapshot: a later eviction does not
    /// change it, and nothing is copied: the view reads the slot array current
    /// when it was taken, which the ring replaces rather than mutates (issue
    /// #5997).
    /// </summary>
    public RunView RunFrom(long fromExclusive)
    {
        var slots = Volatile.Read(ref _slots);
        for (var i = 0; i < slots.Length; i++)
        {
            if (slots[i].Page.CoversFrom(fromExclusive))
                return new RunView(slots, i);
        }

        return new RunView([], 0);
    }

    /// <summary>
    /// A run of pages: the ring's slots from one position onwards, read from a
    /// snapshot the ring never mutates, followed by any pages a serve has filled
    /// since. <see cref="Sorted"/> holds while the pages ascend by start offset,
    /// which the ring's slots always do.
    /// </summary>
    internal sealed class RunView(Slot[] slots, int start) : IReadOnlyList<ReplayPage>
    {
        private List<ReplayPage>? _appended;

        /// <summary>Whether the pages ascend by <see cref="ReplayPage.FromExclusive"/>.</summary>
        public bool Sorted { get; private set; } = true;

        /// <inheritdoc />
        public int Count => slots.Length - start + (_appended?.Count ?? 0);

        /// <inheritdoc />
        public ReplayPage this[int index] =>
            index < slots.Length - start ? slots[start + index].Page : _appended![index - (slots.Length - start)];

        /// <summary>Appends a page a serve filled after the view was taken.</summary>
        public void Add(ReplayPage page)
        {
            ArgumentNullException.ThrowIfNull(page);
            if (Count > 0 && this[Count - 1].FromExclusive > page.FromExclusive)
                Sorted = false;
            (_appended ??= []).Add(page);
        }

        /// <inheritdoc />
        public IEnumerator<ReplayPage> GetEnumerator()
        {
            for (var i = 0; i < Count; i++)
                yield return this[i];
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
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
    /// the fill started at and the ledger can make room for it.
    /// </summary>
    /// <returns>Whether the page is now cached.</returns>
    public bool TryInsert(ReplayPage page, long epoch)
    {
        ArgumentNullException.ThrowIfNull(page);
        if (epoch != Epoch || page.Incarnation != Incarnation)
            return false;

        return _ledger.TryAdmit(this, page, EstimateBytes(page), Stopwatch.GetTimestamp());
    }

    /// <summary>
    /// Records that <paramref name="reader"/> will next read this partition
    /// after <paramref name="nextExclusive"/>, until <paramref name="leaseTicks"/>
    /// from now unless renewed. <paramref name="now"/> is a
    /// <see cref="Stopwatch"/> timestamp; tests supply their own clock.
    /// </summary>
    public void NoteDemand(string reader, long nextExclusive, long leaseTicks, long? now = null)
    {
        ArgumentNullException.ThrowIfNull(reader);
        var at = now ?? Stopwatch.GetTimestamp();
        lock (_demandGate)
        {
            if (!_demand.ContainsKey(reader) && _demand.Count >= MaxDemandEntries)
            {
                string? oldest = null;
                var oldestExpiry = long.MaxValue;
                foreach (var (key, entry) in _demand)
                {
                    if (entry.ExpiresAt < oldestExpiry)
                        (oldest, oldestExpiry) = (key, entry.ExpiresAt);
                }

                _demand.Remove(oldest!);
            }

            _demand[reader] = (nextExclusive, at + leaseTicks);
            _liveDemand = null;
        }
    }

    /// <summary>Forgets <paramref name="reader"/>'s demand: its replay of this partition is over.</summary>
    public void ClearDemand(string reader)
    {
        ArgumentNullException.ThrowIfNull(reader);
        lock (_demandGate)
        {
            if (_demand.Remove(reader))
                _liveDemand = null;
        }
    }

    /// <summary>The number of readers currently declared, live or not.</summary>
    internal int DemandCount
    {
        get { lock (_demandGate) { return _demand.Count; } }
    }

    /// <summary>
    /// The next offsets of the readers whose lease has not expired, ascending,
    /// dropping the expired ones. Recomputed only when the table changed or a
    /// lease lapsed, so repeated eviction passes reuse one array.
    /// </summary>
    internal long[] LiveDemand(long now)
    {
        lock (_demandGate)
        {
            if (_liveDemand is not null && now < _liveDemandValidUntil)
                return _liveDemand;

            List<string>? expired = null;
            var live = new long[_demand.Count];
            var count = 0;
            var validUntil = long.MaxValue;
            foreach (var (reader, entry) in _demand)
            {
                if (entry.ExpiresAt <= now)
                {
                    (expired ??= []).Add(reader);
                    continue;
                }

                live[count++] = entry.Next;
                validUntil = Math.Min(validUntil, entry.ExpiresAt);
            }

            if (expired is not null)
            {
                foreach (var reader in expired)
                    _demand.Remove(reader);
            }

            Array.Resize(ref live, count);
            Array.Sort(live);
            _liveDemand = live;
            _liveDemandValidUntil = validUntil;
            return live;
        }
    }

    /// <summary>Adds a page in offset order. Runs under the ledger's lock.</summary>
    internal void AddLocked(ReplayPage page, long bytes, long sequence)
    {
        var slots = _slots;
        var index = Array.FindIndex(slots, s => s.Page.FromExclusive > page.FromExclusive);
        if (index < 0)
            index = slots.Length;
        var next = new Slot[slots.Length + 1];
        Array.Copy(slots, next, index);
        next[index] = new Slot(page, bytes, sequence);
        Array.Copy(slots, index, next, index + 1, slots.Length - index);
        Volatile.Write(ref _slots, next);
        Volatile.Write(ref _bytes, _bytes + bytes);
    }

    /// <summary>
    /// Removes the pages of <paramref name="candidates"/> in
    /// <c>[start, end)</c>, all of this ring, rebuilding the page array once.
    /// Runs under the ledger's lock.
    /// </summary>
    /// <returns>The bytes freed.</returns>
    internal long RemoveManyLocked(ReplayPageLedger.Candidate[] candidates, int start, int end)
    {
        var slots = _slots;
        var victims = new HashSet<ReplayPage>(end - start, ReferenceEqualityComparer.Instance);
        for (var i = start; i < end; i++)
            victims.Add(candidates[i].Slot.Page);

        var keep = new List<Slot>(slots.Length);
        var freed = 0L;
        foreach (var slot in slots)
        {
            if (victims.Contains(slot.Page))
                freed += slot.Bytes;
            else
                keep.Add(slot);
        }

        Volatile.Write(ref _slots, keep.ToArray());
        Volatile.Write(ref _bytes, _bytes - freed);
        return freed;
    }

    /// <summary>Drops every page. Runs under the ledger's lock.</summary>
    /// <returns>The bytes freed.</returns>
    internal long DropAllLocked()
    {
        var freed = _bytes;
        Volatile.Write(ref _slots, []);
        Volatile.Write(ref _bytes, 0);
        return freed;
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
        var entries = new List<CommitLogSliceEntry>(Math.Min(budget, 1024));
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

    /// <summary>Drops every page and detaches from the ledger: the coordinator is deactivating.</summary>
    public void Clear()
    {
        _ledger.Detach(this);
        _fills.Clear();

        // A fill still in flight - one joined past its aging bound, say - finishes
        // after this; the new epoch and the detached flag both refuse its page.
        Epoch++;
        lock (_demandGate)
        {
            _demand.Clear();
            _liveDemand = null;
        }
    }

    /// <summary>An estimate of the bytes a page holds: per entry, its key, value and a fixed overhead, plus its owner index (issue #5997).</summary>
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

        return bytes + page.Index.EstimatedBytes;
    }
}
