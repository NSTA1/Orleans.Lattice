using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ILeafReplayCoordinatorGrain"/>. Activated per
/// <c>{treeId}/{shardIndex}</c>. Forwards to the
/// <see cref="ICommitLogReader"/> resolved from DI (registered by the
/// <c>Orleans.Lattice.Replication</c> package). When the commit-log reader
/// is absent — i.e. no replication package is registered — every method
/// returns the empty / head-zero defaults so an activating leaf safely
/// short-circuits its replay loop.
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

    // Last-served slice cache — V1 amortisation surface.
    private long _cachedFromExclusive = -1;
    private long _cachedToInclusive = -1;
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
        _reader = context.ActivationServices.GetService<ICommitLogReader>();
    }

    public async Task<IReadOnlyList<CommitLogSliceEntry>> ReadSliceAsync(
        long fromOffsetExclusive,
        long toOffsetInclusive,
        int budget,
        CancellationToken cancellationToken = default)
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
        if (_reader is null)
        {
            return EmptySlice;
        }

        // V1 cache — serve from cache when the new range matches the
        // cached window byte-for-byte and the entry has not aged past
        // the TTL. Conservative on purpose: any deviation falls
        // through to a fresh read.
        if (_cachedEntries is not null
            && _cachedFromExclusive == fromOffsetExclusive
            && _cachedToInclusive == toOffsetInclusive
            && DateTime.UtcNow - _cachedAtUtc < SliceCacheTtl)
        {
            return _cachedEntries;
        }

        var collected = new List<CommitLogSliceEntry>();
        try
        {
            await foreach (var (offset, mutation) in _reader.ReadAsync(
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
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Commit-log read failed for tree={TreeId} shard={ShardIndex} from={From} to={To}.",
                _treeId, _shardIndex, fromOffsetExclusive, toOffsetInclusive);
            throw;
        }

        var result = (IReadOnlyList<CommitLogSliceEntry>)collected;
        _cachedFromExclusive = fromOffsetExclusive;
        _cachedToInclusive = toOffsetInclusive;
        _cachedAtUtc = DateTime.UtcNow;
        _cachedEntries = result;
        return result;
    }

    public async Task<long> GetHeadOffsetAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureBindingsParsed();
        if (_reader is null)
        {
            return 0;
        }
        return await _reader.GetHeadOffsetAsync(_treeId!, _shardIndex, cancellationToken);
    }

    public async Task<long> GetTailOffsetAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureBindingsParsed();
        if (_reader is null)
        {
            return 0;
        }
        return await _reader.GetTailOffsetAsync(_treeId!, _shardIndex, cancellationToken);
    }
}
