namespace Orleans.Lattice.Tests;

/// <summary>
/// Default-implementation contract tests for
/// <see cref="IWalStorageProvider.GetTrimWatermarkAsync"/> (issue #4621). A
/// provider that does not override it keeps no trim watermark, so it must answer
/// <see langword="null"/>: that keeps every reader on the conservative rule that
/// treats any jump in offsets as a trim, rather than trusting a watermark that was
/// never written.
/// </summary>
[TestFixture]
public class IWalStorageProviderTrimWatermarkDefaultTests
{
    [Test]
    public async Task Default_GetTrimWatermarkAsync_reports_no_watermark_without_touching_the_log()
    {
        IWalStorageProvider provider = new MinimalProvider();

        Assert.That(await provider.GetTrimWatermarkAsync("tree", 0, CancellationToken.None), Is.Null);
    }

    [Test]
    public void Default_GetTrimWatermarkAsync_rejects_null_treeId()
    {
        IWalStorageProvider provider = new MinimalProvider();

        Assert.That(
            async () => await provider.GetTrimWatermarkAsync(null!, 0, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Default_GetTrimWatermarkAsync_observes_cancellation()
    {
        IWalStorageProvider provider = new MinimalProvider();

        Assert.That(
            async () => await provider.GetTrimWatermarkAsync("tree", 0, new CancellationToken(canceled: true)),
            Throws.InstanceOf<OperationCanceledException>());
    }

    /// <summary>
    /// <see cref="IWalStorageProvider"/> implementation supplying the bare minimum
    /// needed to invoke the default body through the interface. Every other member
    /// throws, so a default that derived a watermark from the log surfaces at once.
    /// </summary>
    private sealed class MinimalProvider : IWalStorageProvider
    {
        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => throw new NotSupportedException();
    }
}
