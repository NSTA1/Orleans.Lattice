using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Default-implementation contract tests for
/// <see cref="IWalStorageProvider.EvaluateCompactionAsync"/> (issue #3207).
/// <para>
/// The seam ships a no-op default, and unlike most defaults that is a
/// statement about correctness rather than a courtesy to third parties. The
/// method asks a backend to reconsider reclaiming space it has already
/// classified as dead, which is a question only a log-structured backend can
/// answer: a provider whose trim issues real deletes has no dead bytes to
/// reclaim, so for it the correct answer is to do nothing. Declining is
/// therefore the right default and not an unimplemented stub, which is why
/// this fixture pins it rather than treating it as a placeholder.
/// </para>
/// </summary>
[TestFixture]
public class IWalStorageProviderEvaluateCompactionDefaultTests
{
    [Test]
    public async Task Default_EvaluateCompactionAsync_completes_synchronously_as_noop()
    {
        IWalStorageProvider provider = new MinimalProvider();

        var evaluate = provider.EvaluateCompactionAsync("tree", 0, CancellationToken.None);

        // Both claims in the name are asserted rather than assumed. The no-op
        // must not reach any other interface method - the minimal provider
        // throws on every one, so a regression that tried to derive dead bytes
        // from ReadAsync surfaces here - and it must complete synchronously.
        // The synchronous completion is load-bearing: the collector calls this
        // once per shard on every sweep of every tree, so a default that
        // allocated or awaited would tax every provider that has nothing to do.
        Assert.That(evaluate.IsCompletedSuccessfully, Is.True,
            "the default body returns Task.CompletedTask without awaiting, so it must already be complete "
            + "before it is awaited");

        await evaluate;
    }

    [Test]
    public void Default_EvaluateCompactionAsync_rejects_null_treeId()
    {
        IWalStorageProvider provider = new MinimalProvider();

        Assert.That(
            async () => await provider.EvaluateCompactionAsync(null!, 0, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Default_EvaluateCompactionAsync_observes_pre_cancelled_token()
    {
        IWalStorageProvider provider = new MinimalProvider();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await provider.EvaluateCompactionAsync("tree", 0, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task InMemoryWalStorageProvider_inherits_default_noop_and_retains_its_entries()
    {
        // A backend whose trim removes entries outright holds no dead bytes,
        // so it has nothing to evaluate. The stronger claim is that the call
        // is inert: the collector now issues it on shards it did not trim, so
        // a provider that mistook it for an instruction to discard would lose
        // data on exactly the passes that were meant to be read-only.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            "tree",
            0,
            new[] { Entry(0), Entry(1), Entry(2) },
            CancellationToken.None);

        // Default interface methods dispatch only through the interface, so
        // the cast is the contract: adding an override to the concrete type
        // surfaces as a signature change rather than a silent behaviour delta.
        await ((IWalStorageProvider)provider).EvaluateCompactionAsync("tree", 0, CancellationToken.None);

        var lowest = await provider.GetLowestOffsetAsync("tree", 0, CancellationToken.None);
        var highest = await provider.GetHighestOffsetAsync("tree", 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(lowest, Is.EqualTo(0L), "An evaluation must never advance the retention floor.");
            Assert.That(highest, Is.EqualTo(2L), "An evaluation must never disturb the append head.");
        });
    }

    private static WalEntry Entry(long offset) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.Set,
            Key = "k" + offset.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Value = new byte[] { 1 },
            Timestamp = new HybridLogicalClock { WallClockTicks = 10 + offset, Counter = 0 },
            OriginClusterId = "site-a",
        },
    };

    /// <summary>
    /// <see cref="IWalStorageProvider"/> implementation supplying the bare
    /// minimum needed to invoke the default body through the interface. Every
    /// other member throws, so a regression that delegates surfaces at once.
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
