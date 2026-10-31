namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.Silo;

/// <summary>
/// The secondary WAL provider as the journey silo registers it: the real Azure Table provider,
/// with the writes and tail reads a WAL move makes against its target held while
/// <see cref="JourneyWalTargetGate"/> holds the tree (A12, issue #5382). Every call for a tree
/// that is not held, and every read a move makes of its source, passes straight through.
/// </summary>
/// <param name="inner">The real provider.</param>
internal sealed class JourneyGatedWalStorageProvider(IWalStorageProvider inner) : IWalStorageProvider, IAsyncDisposable
{
    /// <inheritdoc />
    public int MaxAppendBatchEntries => inner.MaxAppendBatchEntries;

    /// <inheritdoc />
    public async Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
    {
        await JourneyWalTargetGate.WaitAsync(treeId, cancellationToken);
        await inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken);
    }

    /// <inheritdoc />
    public async Task AppendEncodedBatchAsync(
        string treeId,
        int shardIndex,
        ReadOnlyMemory<ArraySegment<byte>> encodedEntries,
        ReadOnlyMemory<long> offsets,
        IWalRecordEncoder encoder,
        CancellationToken cancellationToken)
    {
        await JourneyWalTargetGate.WaitAsync(treeId, cancellationToken);
        await inner.AppendEncodedBatchAsync(treeId, shardIndex, encodedEntries, offsets, encoder, cancellationToken);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken) =>
        inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);

    /// <inheritdoc />
    public Task<WalShardEncodedPage> ReadEncodedAsync(
        string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, IWalRecordEncoder encoder, CancellationToken cancellationToken) =>
        inner.ReadEncodedAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, encoder, cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<WalEntry> ReadFilteredAsync(
        string treeId, int shardIndex, long fromOffsetExclusive, long toOffsetInclusive, int maxEntries, WalKeyFilter filter, CancellationToken cancellationToken) =>
        inner.ReadFilteredAsync(treeId, shardIndex, fromOffsetExclusive, toOffsetInclusive, maxEntries, filter, cancellationToken);

    /// <inheritdoc />
    public async Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
    {
        await JourneyWalTargetGate.WaitAsync(treeId, cancellationToken);
        return await inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);
    }

    /// <inheritdoc />
    public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken) =>
        inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

    /// <inheritdoc />
    public async Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
    {
        await JourneyWalTargetGate.WaitAsync(treeId, cancellationToken);
        await inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);
    }

    /// <inheritdoc />
    public Task<long?> GetTrimWatermarkAsync(string treeId, int shardIndex, CancellationToken cancellationToken) =>
        inner.GetTrimWatermarkAsync(treeId, shardIndex, cancellationToken);

    /// <inheritdoc />
    public Task EvaluateCompactionAsync(string treeId, int shardIndex, CancellationToken cancellationToken) =>
        inner.EvaluateCompactionAsync(treeId, shardIndex, cancellationToken);

    /// <inheritdoc />
    public Task ReconcileAsync(string treeId, int shardIndex, CancellationToken cancellationToken) =>
        inner.ReconcileAsync(treeId, shardIndex, cancellationToken);

    /// <inheritdoc />
    public Task<long> GetRetainedByteSizeAsync(string treeId, int shardIndex, CancellationToken cancellationToken) =>
        inner.GetRetainedByteSizeAsync(treeId, shardIndex, cancellationToken);

    /// <inheritdoc />
    public Task<long> GetPhysicalByteSizeAsync(string treeId, int shardIndex, CancellationToken cancellationToken) =>
        inner.GetPhysicalByteSizeAsync(treeId, shardIndex, cancellationToken);

    /// <inheritdoc />
    public ValueTask DisposeAsync() => inner is IAsyncDisposable disposable ? disposable.DisposeAsync() : ValueTask.CompletedTask;
}
