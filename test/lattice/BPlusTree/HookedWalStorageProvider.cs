using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// A WAL storage provider that delegates every call to an
/// <see cref="InMemoryWalStorageProvider"/> and can run a one-shot hook inside a
/// <see cref="GetHighestOffsetAsync"/> call. The move coordinator reads the
/// target's highest offset once before it copies and once more, after the final
/// quiesce of the source, to verify the copy. Arming the hook at the copied tail
/// therefore runs it in the window between the last quiesce and the placement
/// flip, which is where issue #4525 loses an acknowledged append.
/// </summary>
internal sealed class HookedWalStorageProvider(InMemoryWalStorageProvider inner) : IWalStorageProvider
{
    private readonly IWalStorageProvider _inner = inner;
    private Func<Task>? _hook;
    private long _hookThreshold = long.MaxValue;
    private int _hookRuns;

    /// <summary>The in-memory store every call is delegated to.</summary>
    public InMemoryWalStorageProvider Inner { get; } = inner;

    /// <summary>How many times a hook has run since the last <see cref="ArmOnceAtHighest"/>.</summary>
    public int HookRuns => Volatile.Read(ref _hookRuns);

    /// <summary>
    /// Arms <paramref name="hook"/> to run once, inside the first
    /// <see cref="GetHighestOffsetAsync"/> call whose answer is at least
    /// <paramref name="threshold"/>, before that answer is returned.
    /// </summary>
    public void ArmOnceAtHighest(long threshold, Func<Task> hook)
    {
        ArgumentNullException.ThrowIfNull(hook);
        Volatile.Write(ref _hookRuns, 0);
        Volatile.Write(ref _hookThreshold, threshold);
        Volatile.Write(ref _hook, hook);
    }

    /// <summary>Disarms any armed hook that has not yet run.</summary>
    public void Disarm() => Volatile.Write(ref _hook, null);

    public async Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
    {
        var highest = await _inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);
        if (highest >= Volatile.Read(ref _hookThreshold) && Interlocked.Exchange(ref _hook, null) is { } hook)
        {
            Interlocked.Increment(ref _hookRuns);
            await hook();
        }
        return highest;
    }

    public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        => _inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken);

    public Task AppendEncodedBatchAsync(
        string treeId, int shardIndex, ReadOnlyMemory<ArraySegment<byte>> encodedEntries,
        ReadOnlyMemory<long> offsets, IWalRecordEncoder encoder, CancellationToken cancellationToken)
        => _inner.AppendEncodedBatchAsync(treeId, shardIndex, encodedEntries, offsets, encoder, cancellationToken);

    public IAsyncEnumerable<WalEntry> ReadAsync(
        string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
        => _inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);

    public Task<WalShardEncodedPage> ReadEncodedAsync(
        string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries,
        IWalRecordEncoder encoder, CancellationToken cancellationToken)
        => _inner.ReadEncodedAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, encoder, cancellationToken);

    public IAsyncEnumerable<WalEntry> ReadFilteredAsync(
        string treeId, int shardIndex, long fromOffsetExclusive, long toOffsetInclusive, int maxEntries,
        WalKeyFilter filter, CancellationToken cancellationToken)
        => _inner.ReadFilteredAsync(treeId, shardIndex, fromOffsetExclusive, toOffsetInclusive, maxEntries, filter, cancellationToken);

    public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        => _inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

    public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
        => _inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);

    public Task EvaluateCompactionAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        => _inner.EvaluateCompactionAsync(treeId, shardIndex, cancellationToken);

    public Task ReconcileAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        => _inner.ReconcileAsync(treeId, shardIndex, cancellationToken);

    public Task<long> GetRetainedByteSizeAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        => _inner.GetRetainedByteSizeAsync(treeId, shardIndex, cancellationToken);

    public Task<long> GetPhysicalByteSizeAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        => _inner.GetPhysicalByteSizeAsync(treeId, shardIndex, cancellationToken);
}
