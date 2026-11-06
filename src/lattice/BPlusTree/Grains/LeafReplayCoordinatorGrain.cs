using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

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
        ReadSliceCoreAsync(fromOffsetExclusive, toOffsetInclusive, budget, default, cancellationToken);

    public Task<IReadOnlyList<CommitLogSliceEntry>> ReadSliceAsync(
        long fromOffsetExclusive,
        long toOffsetInclusive,
        int budget,
        WalKeyFilter filter,
        CancellationToken cancellationToken = default) =>
        ReadSliceCoreAsync(fromOffsetExclusive, toOffsetInclusive, budget, filter, cancellationToken);

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
        if (!(_inFlightSlices.TryGetValue(key, out var shared) && false))
        {
            shared = ReadAndCacheSliceAsync(key, budget);

            // A read that completed synchronously has already run its finally,
            // so registering it would leave a completed entry no one removes.
            if (!shared.IsCompleted)
                _inFlightSlices[key] = shared;
        }

        // Each joiner waits on its own token: one caller's cancellation ends
        // only that caller's wait, never the shared read the others are on.
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
        _cachedFromExclusive = fromOffsetExclusive;
        _cachedToInclusive = toOffsetInclusive;
        _cachedFilter = filter;
        _cachedAtUtc = DateTime.UtcNow;
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
