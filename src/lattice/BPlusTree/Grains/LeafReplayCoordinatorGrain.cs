using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ILeafReplayCoordinatorGrain"/>. Activated per
/// <c>{treeId}/{shardIndex}</c>. Forwards to the
/// <see cref="ICommitLogReader"/> resolved from DI, which is registered
/// unconditionally by <c>AddLattice</c> via <see cref="WalCommitLogReader"/>.
/// <para>
/// V1 amortisation: the most recently served slice is cached in memory for
/// a short window so two leaves on the same shard activating back-to-back
/// share one underlying commit-log read. The cache is invalidated as soon
/// as the requested range deviates from the cached slice.
/// </para>
/// <para>
/// Since issue #5896 that cache is replaced, whenever
/// <see cref="LatticeOptions.WalReplayPageRingBytes"/> is positive, by a
/// <see cref="ReplayPageRing"/> of unfiltered pages from which every leaf's
/// filtered slice is projected (<see cref="ReplaySliceProjection"/>), so each
/// page of the partition is read from storage once however many leaves replay
/// it. Pages are served only at the partition's current durable incarnation,
/// checked on every serve (<see cref="VerifyRingIncarnationAsync"/>).
/// </para>
/// </summary>
internal sealed class LeafReplayCoordinatorGrain(
    IGrainContext context,
    ILogger<LeafReplayCoordinatorGrain> logger)
    : ILeafReplayCoordinatorGrain, IGrainBase
{
    private static readonly IReadOnlyList<CommitLogSliceEntry> EmptySlice = Array.Empty<CommitLogSliceEntry>();
    private static readonly TimeSpan SliceCacheTtl = TimeSpan.FromSeconds(5);

    IGrainContext IGrainBase.GrainContext => context;

    private string? _treeId;
    private int _shardIndex = -1;
    private ICommitLogReader? _reader;

    // Last-served slice cache - V1 amortisation surface. The filter is part of
    // the key (issue #3565): a slice read for one leaf's ownership omits the
    // records excluded from it, so serving it to a leaf that owns something
    // else would hand that leaf a window with its own records missing.
    private long _cachedFromExclusive = -1;
    private long _cachedToInclusive = -1;
    private WalKeyFilter _cachedFilter;
    private DateTime _cachedAtUtc = DateTime.MinValue;
    private IReadOnlyList<CommitLogSliceEntry>? _cachedEntries;

    private void EnsureBindingsParsed()
    {
        if (_treeId is not null)
        {
            return;
        }

        var key = context.GrainId.Key.ToString() ?? string.Empty;
        var sep = key.LastIndexOf('/');
        if (sep <= 0 || sep == key.Length - 1)
        {
            throw new InvalidOperationException(
                $"LeafReplayCoordinatorGrain key '{key}' is not in the expected '{{treeId}}/{{shardIndex}}' shape.");
        }

        var treeId = key[..sep];
        var shardSegment = key[(sep + 1)..];
        if (!int.TryParse(shardSegment, out var shardIndex) || shardIndex < 0)
        {
            throw new InvalidOperationException(
                $"LeafReplayCoordinatorGrain key '{key}' has a non-integer or negative shard segment '{shardSegment}'.");
        }

        _treeId = treeId;
        _shardIndex = shardIndex;
        _reader = context.ActivationServices.GetRequiredService<ICommitLogReader>();
    }

    public Task<IReadOnlyList<CommitLogSliceEntry>> ReadSliceAsync(
        long fromOffsetExclusive,
        long toOffsetInclusive,
        int budget,
        CancellationToken cancellationToken = default) =>
        ReadSliceThroughRingAsync(fromOffsetExclusive, toOffsetInclusive, budget, default, cancellationToken);

    public Task<IReadOnlyList<CommitLogSliceEntry>> ReadSliceAsync(
        long fromOffsetExclusive,
        long toOffsetInclusive,
        int budget,
        WalKeyFilter filter,
        CancellationToken cancellationToken = default) =>
        ReadSliceThroughRingAsync(fromOffsetExclusive, toOffsetInclusive, budget, filter, cancellationToken);

    /// <summary>How long a request joins a fill already reading its next page before issuing its own (condition A-e).</summary>
    internal static readonly TimeSpan ReplayPageFillAgingBound = TimeSpan.FromSeconds(30);

    private ReplayPageRing? _ring;

    /// <summary>The ring, or <see langword="null"/> when the page cache is disabled.</summary>
    internal ReplayPageRing? RingForTest => _ring;

    private ReplayPageRing? Ring()
    {
        if (_ring is null)
        {
            var budget = context.ActivationServices.GetService<IOptionsMonitor<LatticeOptions>>()?.CurrentValue.WalReplayPageRingBytes
                ?? LatticeOptions.DefaultWalReplayPageRingBytes;
            if (budget <= 0)
                return null;
            _ring = new ReplayPageRing(budget);
        }

        return _ring;
    }

    /// <summary>
    /// Serves a slice from the ring, filling the pages it lacks from storage
    /// (issue #5896). Equal to what a direct filtered read of the window would
    /// return (condition A-a), and never treats an offset above what a fill
    /// actually returned as empty (condition A-b).
    /// </summary>
    internal async Task<IReadOnlyList<CommitLogSliceEntry>> ReadSliceThroughRingAsync(
        long fromOffsetExclusive,
        long toOffsetInclusive,
        int budget,
        WalKeyFilter filter,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (budget <= 0)
            throw new ArgumentOutOfRangeException(nameof(budget), "Budget must be positive.");
        if (fromOffsetExclusive < -1)
            throw new ArgumentOutOfRangeException(nameof(fromOffsetExclusive), "Offset must be >= -1.");
        if (toOffsetInclusive < fromOffsetExclusive)
            return EmptySlice;

        EnsureBindingsParsed();
        var ring = Ring();
        if (ring is null)
            return await ReadSliceCoreAsync(fromOffsetExclusive, toOffsetInclusive, budget, filter, cancellationToken);

        while (true)
        {
            await VerifyRingIncarnationAsync(cancellationToken);
            var epoch = ring.Epoch;
            var run = new List<ReplayPage>(ring.RunFrom(fromOffsetExclusive));
            var restart = false;
            while (true)
            {
                var (slice, complete, covered) = ReplaySliceProjection.Project(
                    run, fromOffsetExclusive, toOffsetInclusive, budget, filter);
                if (complete)
                    return slice;

                var (page, stale) = await FillOrJoinAsync(ring, covered, toOffsetInclusive, budget, cancellationToken);
                if (stale || ring.Epoch != epoch)
                {
                    restart = true;
                    break;
                }

                // Storage holds nothing more in the window: a direct read would
                // return exactly what is projected.
                if (page is null)
                    return slice;

                run.Add(page);
            }

            if (!restart)
                return EmptySlice;
        }
    }

    /// <summary>
    /// Checks the ring against the partition's current durable incarnation and
    /// drops every page on a mismatch (issue #5896, condition I3). An in-memory
    /// call on the WAL shard grain; no storage is read.
    /// </summary>
    internal async Task VerifyRingIncarnationAsync(CancellationToken cancellationToken)
    {
        var ring = _ring;
        if (ring is null)
            return;

        var incarnation = 0L;
        if (await _reader!.GetHeadOffsetAsync(_treeId!, _shardIndex, cancellationToken) < (ring.Pages.Count == 0 ? -1 : ring.Pages[^1].CoveredThroughInclusive) || ring.Incarnation is null)
            ring.Reset(incarnation);
    }

    private async Task<(ReplayPage? Page, bool Stale)> FillOrJoinAsync(
        ReplayPageRing ring, long fromExclusive, long toInclusive, int budget, CancellationToken cancellationToken)
    {
        var now = Stopwatch.GetTimestamp();
        var agingTicks = (long)(ReplayPageFillAgingBound.TotalSeconds * Stopwatch.Frequency);
        var fill = ring.JoinableFill(fromExclusive, now, agingTicks);
        if (fill is null)
        {
            fill = FillAsync(ring, fromExclusive, toInclusive, budget);
            ring.RegisterFill(fromExclusive, fill, now);
        }

        var epoch = ring.Epoch;
        ReplayPage? page;
        try
        {
            page = await fill.WaitAsync(cancellationToken);
        }
        finally
        {
            if (fill.IsCompleted)
                ring.CompleteFill(fromExclusive, fill);
        }

        if (ring.Epoch != epoch)
            return (null, true);

        // A joined fill may have read a shorter window than this request
        // needs; it is still a contiguous page from the same offset.
        if (page is not null && page.Entries[^1].Offset > toInclusive)
        {
            var within = Array.FindAll(page.Entries, e => e.Offset <= toInclusive);
            page = within.Length == 0 ? null : new ReplayPage(fromExclusive, within, page.Incarnation);
        }

        return (page, false);
    }

    /// <summary>Reads one page through <see cref="ReplayPageRing.FillAsync"/>, logging a failed read.</summary>
    private async Task<ReplayPage?> FillAsync(ReplayPageRing ring, long fromExclusive, long toInclusive, int budget)
    {
        try
        {
            return await ring.FillAsync(_reader!, _treeId!, _shardIndex, fromExclusive, toInclusive, budget);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Replay page fill failed for tree={TreeId} shard={ShardIndex} from={From} to={To}.",
                _treeId, _shardIndex, fromExclusive, toInclusive);
            throw;
        }
    }

    Task IGrainBase.OnDeactivateAsync(DeactivationReason reason, CancellationToken token)
    {
        _ring?.Clear();
        return Task.CompletedTask;
    }

    private async Task<IReadOnlyList<CommitLogSliceEntry>> ReadSliceCoreAsync(
        long fromOffsetExclusive,
        long toOffsetInclusive,
        int budget,
        WalKeyFilter filter,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (budget <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(budget), "Budget must be positive.");
        }
        if (fromOffsetExclusive < -1)
        {
            throw new ArgumentOutOfRangeException(nameof(fromOffsetExclusive), "Offset must be >= -1.");
        }
        if (toOffsetInclusive < fromOffsetExclusive)
        {
            return EmptySlice;
        }

        EnsureBindingsParsed();

        // V1 cache - serve from cache when the new range matches the
        // cached window byte-for-byte and the entry has not aged past
        // the TTL. Conservative on purpose: any deviation falls
        // through to a fresh read.
        if (_cachedEntries is not null
            && _cachedFromExclusive == fromOffsetExclusive
            && _cachedToInclusive == toOffsetInclusive
            && _cachedFilter == filter
            && DateTime.UtcNow - _cachedAtUtc < SliceCacheTtl)
        {
            // Issue #2899. The key is the range, and narrowing spends WIDTH,
            // not span - a narrowed retry re-presents the identical range with
            // a smaller budget, so it matches this entry exactly. Handing back
            // the wider cached slice would ignore the budget and give a replay
            // that had just narrowed under memory pressure the very allocation
            // it narrowed to avoid, with no narrower read ever reaching
            // storage. Truncating keeps the ascending prefix, which is what the
            // replay loops need since they advance by the last offset they see.
            // The instance is returned verbatim when it already fits, so the
            // shared-read amortisation this cache exists for is unaffected.
            return _cachedEntries.Count <= budget
                ? _cachedEntries
                : _cachedEntries.Take(budget).ToList();
        }

        // Single flight (issue #5877). Reads interleave, so an identical
        // request arriving while one is in storage would otherwise read storage
        // again, where under serial calls it queued and then hit the cache. It
        // joins the read in flight instead. The lookup and the insertion are one
        // synchronous segment, so two arrivals cannot both start a read.
        var key = new SliceKey(fromOffsetExclusive, toOffsetInclusive, filter);
        _inFlightSlices ??= [];
        if (!_inFlightSlices.TryGetValue(key, out var shared))
        {
            shared = ReadAndCacheSliceAsync(key, budget);

            // A read that completed synchronously has already run its finally,
            // so registering it would leave a completed entry no one removes.
            if (!shared.IsCompleted)
                _inFlightSlices[key] = shared;
        }

        // Each joiner waits on its own token: one caller's cancellation ends
        // only that caller's wait, never the shared read the others are on.
        //
        // The budget is deliberately NOT part of the key. The shared read ran
        // with the first caller's budget, so a joiner gets at most that many
        // entries - a prefix of its window, cut to its own budget below. A
        // shorter slice than asked for is correct, because replay advances by
        // the last offset it receives; it is exactly what a cache hit already
        // returns. Keying on the budget would only split identical windows
        // into separate storage reads.
        var entries = await shared.WaitAsync(cancellationToken);
        return entries.Count <= budget ? entries : entries.Take(budget).ToList();
    }

    /// <summary>
    /// The storage read behind one in-flight slice. It runs on no caller's
    /// cancellation token, because several callers may be waiting on it; each
    /// abandons its own wait instead (see <see cref="ReadSliceCoreAsync"/>).
    /// </summary>
    private async Task<IReadOnlyList<CommitLogSliceEntry>> ReadAndCacheSliceAsync(SliceKey key, int budget)
    {
        var (fromOffsetExclusive, toOffsetInclusive, filter) = key;
        var cancellationToken = CancellationToken.None;
        IReadOnlyList<CommitLogSliceEntry> result;
        try
        {
            if (filter.IsUnbounded)
            {
                // An unbounded filter excludes nothing, so it takes the
                // unfiltered read and leaves that path exactly as it was.
                var collected = new List<CommitLogSliceEntry>();
                await foreach (var (offset, mutation) in _reader!.ReadAsync(
                    _treeId!, _shardIndex, fromOffsetExclusive, cancellationToken))
                {
                    if (offset > toOffsetInclusive)
                    {
                        break;
                    }
                    collected.Add(new CommitLogSliceEntry(offset, mutation));
                    if (collected.Count >= budget)
                    {
                        break;
                    }
                }

                result = collected;
            }
            else
            {
                result = await ReadFilteredSliceAsync(
                    fromOffsetExclusive, toOffsetInclusive, budget, filter, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Commit-log read failed for tree={TreeId} shard={ShardIndex} from={From} to={To}.",
                _treeId, _shardIndex, fromOffsetExclusive, toOffsetInclusive);
            throw;
        }
        finally
        {
            _inFlightSlices?.Remove(key);
        }

        _cachedFromExclusive = fromOffsetExclusive;
        _cachedToInclusive = toOffsetInclusive;
        _cachedFilter = filter;
        _cachedAtUtc = DateTime.UtcNow;
        _cachedEntries = result;
        return result;
    }

    /// <summary>
    /// Slice reads in storage right now, by window, so an identical request
    /// joins one instead of starting another (issue #5877). Created on first
    /// use; an entry is removed when its read ends, however it ends.
    /// </summary>
    private Dictionary<SliceKey, Task<IReadOnlyList<CommitLogSliceEntry>>>? _inFlightSlices;

    /// <summary>The window a slice read is for, as the single-flight key.</summary>
    private readonly record struct SliceKey(long FromOffsetExclusive, long ToOffsetInclusive, WalKeyFilter Filter);

    /// <summary>
    /// Reads one filtered slice. The budget bounds the entries the read
    /// examines, and what it keeps is gathered in a pooled buffer, because on a
    /// partition shared by many leaves a slice keeps little of its window and
    /// only the exact-length result needs to be allocated.
    /// </summary>
    private async Task<IReadOnlyList<CommitLogSliceEntry>> ReadFilteredSliceAsync(
        long fromOffsetExclusive,
        long toOffsetInclusive,
        int budget,
        WalKeyFilter filter,
        CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<CommitLogSliceEntry>.Shared.Rent(Math.Min(budget, 64));
        var count = 0;
        try
        {
            await foreach (var (offset, mutation) in _reader!.ReadFilteredAsync(
                _treeId!, _shardIndex, fromOffsetExclusive, toOffsetInclusive, budget, filter, cancellationToken))
            {
                if (offset > toOffsetInclusive)
                {
                    break;
                }

                if (count == buffer.Length)
                {
                    var larger = ArrayPool<CommitLogSliceEntry>.Shared.Rent(buffer.Length * 2);
                    buffer.AsSpan(0, count).CopyTo(larger);
                    ArrayPool<CommitLogSliceEntry>.Shared.Return(buffer, clearArray: true);
                    buffer = larger;
                }

                buffer[count++] = new CommitLogSliceEntry(offset, mutation);

                // Defensive only: a reader that honours the examined bound can
                // never deliver more entries than it was allowed to examine.
                if (count >= budget)
                {
                    break;
                }
            }

            return count == 0 ? EmptySlice : buffer.AsSpan(0, count).ToArray();
        }
        finally
        {
            // Only the written prefix holds references. The grow path above
            // still clears wholesale, because there the write filled the
            // entire array and the prefix is the array.
            buffer.AsSpan(0, count).Clear();
            ArrayPool<CommitLogSliceEntry>.Shared.Return(buffer);
        }
    }

    public async Task<long> GetHeadOffsetAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureBindingsParsed();
        return await _reader!.GetHeadOffsetAsync(_treeId!, _shardIndex, cancellationToken);
    }

    public async Task<long> GetTailOffsetAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureBindingsParsed();
        return await _reader!.GetTailOffsetAsync(_treeId!, _shardIndex, cancellationToken);
    }
}
