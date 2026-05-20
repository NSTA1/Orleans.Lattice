using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Activation-time reconciliation coverage for the
/// <see cref="IWalStorageProvider.ReconcileAsync"/> hook the WAL grain
/// invokes between resolving its provider and reading the tail offset.
/// </summary>
public partial class WalShardGrainTests
{
    [Test]
    public async Task OnActivateAsync_invokes_ReconcileAsync_once_with_grain_coordinates()
    {
        var provider = new ReconcileRecordingWalStorageProvider();

        await CreateGrainAsync(provider);

        Assert.Multiple(() =>
        {
            Assert.That(provider.ReconcileCalls, Has.Count.EqualTo(1));
            Assert.That(provider.ReconcileCalls[0].TreeId, Is.EqualTo(TreeId));
            Assert.That(provider.ReconcileCalls[0].ShardIndex, Is.EqualTo(ShardIndex));
        });
    }

    [Test]
    public async Task OnActivateAsync_invokes_ReconcileAsync_before_GetHighestOffsetAsync()
    {
        // The grain's tail-recovery is read-after-reconcile so a
        // multi-phase backend gets a chance to roll missing manifest
        // rows forward before the grain commits to a _nextOffset
        // value. The order of the two recorded calls is the contract.
        var provider = new ReconcileRecordingWalStorageProvider();

        await CreateGrainAsync(provider);

        Assert.That(
            provider.OperationLog,
            Is.EqualTo(new[] { "Reconcile", "GetHighestOffset" }));
    }

    [Test]
    public async Task OnActivateAsync_observes_reconciled_tail_when_assigning_first_offset()
    {
        // Reconciliation that surfaces new committed entries (for
        // example by rolling a missed manifest commit forward) must be
        // visible to GetHighestOffsetAsync; the grain must therefore
        // resume numbering after the reconciled tail, not before.
        var inner = new InMemoryWalStorageProvider();
        var provider = new ReconcilingPrefixWalStorageProvider(inner, prefixCount: 3);

        var grain = await CreateGrainAsync(provider);
        var seq = await grain.AppendAsync(MakeEntry("post-reconcile"), CancellationToken.None);

        Assert.That(seq, Is.EqualTo(3L));
    }

    [Test]
    public void OnActivateAsync_propagates_reconcile_failures_to_activation()
    {
        var provider = new ReconcileThrowingWalStorageProvider("reconcile-boom");

        Assert.That(
            async () => await CreateGrainAsync(provider),
            Throws.InvalidOperationException.With.Message.EqualTo("reconcile-boom"));
    }

    /// <summary>
    /// <see cref="IWalStorageProvider"/> double that records every
    /// <c>ReconcileAsync</c> / <c>GetHighestOffsetAsync</c> invocation
    /// in order so tests can assert both the count and the relative
    /// activation-step ordering.
    /// </summary>
    private sealed class ReconcileRecordingWalStorageProvider : IWalStorageProvider
    {
        public List<(string TreeId, int ShardIndex)> ReconcileCalls { get; } = new();

        public List<string> OperationLog { get; } = new();

        public Task ReconcileAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        {
            ReconcileCalls.Add((treeId, shardIndex));
            OperationLog.Add("Reconcile");
            return Task.CompletedTask;
        }

        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => AsyncEnumerable.Empty<WalEntry>();

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        {
            OperationLog.Add("GetHighestOffset");
            return Task.FromResult(-1L);
        }

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Task.FromResult(-1L);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    /// <summary>
    /// <see cref="IWalStorageProvider"/> double that pre-loads a fixed
    /// number of committed entries into an inner provider during
    /// <c>ReconcileAsync</c>; models the "manifest roll-forward"
    /// behaviour a multi-phase backend exhibits when it discovers
    /// orphan batch partitions on activation.
    /// </summary>
    private sealed class ReconcilingPrefixWalStorageProvider(InMemoryWalStorageProvider inner, int prefixCount) : IWalStorageProvider
    {
        public async Task ReconcileAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        {
            var entries = new WalEntry[prefixCount];
            for (var i = 0; i < prefixCount; i++)
            {
                entries[i] = new WalEntry
                {
                    Offset = i,
                    Mutation = WalRecordConverter.FromWalRecord(new WalRecord
                    {
                        TreeId = treeId,
                        Op = MutationKind.Set,
                        Key = $"recovered-{i}",
                        Value = new byte[] { 0x01 },
                        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                        OriginClusterId = "site-a",
                    }),
                };
            }
            await inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken).ConfigureAwait(false);
        }

        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
            => inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken);

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);
    }

    /// <summary>
    /// <see cref="IWalStorageProvider"/> double that throws on
    /// <c>ReconcileAsync</c>; lets the activation contract test pin
    /// that reconciliation failures surface to the caller rather than
    /// being swallowed.
    /// </summary>
    private sealed class ReconcileThrowingWalStorageProvider(string message) : IWalStorageProvider
    {
        public Task ReconcileAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => throw new InvalidOperationException(message);

        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => AsyncEnumerable.Empty<WalEntry>();

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Task.FromResult(-1L);

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Task.FromResult(-1L);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }
}
