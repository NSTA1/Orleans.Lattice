namespace Orleans.Lattice.Tests;

/// <summary>
/// Default-implementation contract tests for
/// <see cref="IWalStorageProvider.ReconcileAsync"/>. The interface ships
/// a no-op default so existing third-party providers compile and behave
/// correctly without changes; this fixture pins that contract.
/// </summary>
[TestFixture]
public class IWalStorageProviderReconcileDefaultTests
{
    [Test]
    public async Task Default_ReconcileAsync_completes_synchronously_as_noop()
    {
        IWalStorageProvider provider = new MinimalProvider();

        var reconcile = provider.ReconcileAsync("tree", 0, CancellationToken.None);

        // The name's two claims are both asserted rather than assumed. The
        // no-op must not invoke any other interface method - the minimal
        // provider below throws on every other call, so a regression that
        // delegated to ReadAsync/GetHighestOffsetAsync surfaces as a failure
        // - and it must complete synchronously, which is the property that
        // makes the default free for third-party providers to inherit. A
        // default body that started awaiting real I/O would leave the task
        // pending here even though it would still eventually succeed.
        Assert.That(reconcile.IsCompletedSuccessfully, Is.True,
            "the default ReconcileAsync body returns Task.CompletedTask without awaiting, "
            + "so it must already be complete before it is awaited");

        await reconcile;
    }

    [Test]
    public void Default_ReconcileAsync_rejects_null_treeId()
    {
        IWalStorageProvider provider = new MinimalProvider();

        Assert.That(
            async () => await provider.ReconcileAsync(null!, 0, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Default_ReconcileAsync_observes_pre_cancelled_token()
    {
        IWalStorageProvider provider = new MinimalProvider();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await provider.ReconcileAsync("tree", 0, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task InMemoryWalStorageProvider_inherits_default_noop()
    {
        // Single-transaction backends have no orphan state, so the
        // in-memory provider intentionally does not override the
        // default. A future change that adds an override should
        // explicitly re-evaluate this assumption; until then the
        // default is the contract.
        var provider = new InMemoryWalStorageProvider();

        // Default interface methods are only dispatchable through the
        // interface, never through the concrete type - the cast is the
        // contract: a regression that adds an override to the concrete
        // class surfaces as a compile-time signature change rather
        // than a silent behaviour delta on the default path.
        await ((IWalStorageProvider)provider).ReconcileAsync("tree", 0, CancellationToken.None);

        // Reconcile on an empty WAL must not surface any state.
        var highest = await provider.GetHighestOffsetAsync("tree", 0, CancellationToken.None);
        Assert.That(highest, Is.EqualTo(-1L));
    }

    /// <summary>
    /// <see cref="IWalStorageProvider"/> implementation that supplies
    /// the bare minimum needed to invoke <c>ReconcileAsync</c> via the
    /// interface's default body. Every other method throws so a
    /// regression that accidentally delegates surfaces immediately.
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
