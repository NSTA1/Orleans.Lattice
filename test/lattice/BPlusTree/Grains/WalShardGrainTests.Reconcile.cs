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

    [Test]
    public async Task FlushFailure_resyncs_from_reconciled_phase1_tail()
    {
        // The #824 conflict-storm regression. A surfaced phase-1 fault
        // can leave durable phase-1 entry rows committed above the
        // phase-2 TAIL (a lost-response / brown-out timeout whose
        // transaction actually landed). The failure handler must
        // reconcile before reading the tail so the resync resumes from
        // the durable phase-1 tail - not the stale phase-2 one - and the
        // next coalesced flush lands in a fresh offset window instead of
        // re-driving divergent content onto the still-durable offsets.
        var inner = new InMemoryWalStorageProvider();
        var provider = new FlushFailingReconcileForwardWalStorageProvider(inner, orphanCount: 3);
        var grain = await CreateGrainAsync(provider, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 1,
            WalFlushTimeout = TimeSpan.FromSeconds(5),
        });

        // First append's flush faults; recovery reconciles (rolling the
        // 3 durable phase-1 orphans forward, advancing the tail to 2).
        Assert.That(
            async () => await grain.AppendAsync(MakeEntry("a"), CancellationToken.None),
            Throws.InvalidOperationException.With.Message.EqualTo("phase1-boom"));

        // The next append must resume after the reconciled tail (offset
        // 3), not re-use offset 0 beneath the durable phase-1 orphans.
        var offset = await grain.AppendAsync(MakeEntry("b"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(offset, Is.EqualTo(3L));
    }

    [Test]
    public async Task FlushFailure_reconciles_before_reading_tail()
    {
        // The ordering contract of the failure-recovery resync: it
        // reconciles, then reads the tail - mirroring activation. The
        // op log therefore carries the activation pair (Reconcile,
        // GetHighestOffset) followed by the recovery pair.
        var inner = new InMemoryWalStorageProvider();
        var provider = new FlushFailingReconcileForwardWalStorageProvider(inner, orphanCount: 3);
        var grain = await CreateGrainAsync(provider, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 1,
            WalFlushTimeout = TimeSpan.FromSeconds(5),
        });

        Assert.That(
            async () => await grain.AppendAsync(MakeEntry("a"), CancellationToken.None),
            Throws.InvalidOperationException);

        Assert.Multiple(() =>
        {
            Assert.That(provider.ReconcileCallCount, Is.EqualTo(2));
            Assert.That(
                provider.OperationLog,
                Is.EqualTo(new[]
                {
                    "Reconcile", "GetHighestOffset", "Reconcile", "GetHighestOffset",
                }));
        });
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

    /// <summary>
    /// <see cref="IWalStorageProvider"/> double that models the #824
    /// conflict-storm ignition: the first flush call faults <i>after</i>
    /// its phase-1 entry rows are durable above the visible phase-2 tail
    /// (a lost-response / brown-out timeout). The durable phase-1 orphan
    /// only becomes visible through <c>ReconcileAsync</c> (the roll-
    /// forward), and only once a flush has actually been attempted - so
    /// activation reconciliation is a no-op and the orphan surfaces solely
    /// during failure recovery. Records the reconcile / tail-read op log
    /// so the recovery ordering can be asserted.
    /// </summary>
    private sealed class FlushFailingReconcileForwardWalStorageProvider(
        InMemoryWalStorageProvider inner,
        int orphanCount) : IWalStorageProvider
    {
        private int _flushCalls;
        private bool _rolledForward;

        public List<string> OperationLog { get; } = new();

        public int ReconcileCallCount { get; private set; }

        public async Task ReconcileAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        {
            ReconcileCallCount++;
            OperationLog.Add("Reconcile");

            // The durable phase-1 orphan only exists once the failed
            // flush has attempted (and durably landed) its phase-1
            // transaction; before that, reconciliation has nothing to
            // roll forward. Idempotent: a second reconcile is a no-op.
            if (_rolledForward || Volatile.Read(ref _flushCalls) == 0)
            {
                return;
            }

            _rolledForward = true;
            var entries = new WalEntry[orphanCount];
            for (var i = 0; i < orphanCount; i++)
            {
                entries[i] = new WalEntry
                {
                    Offset = i,
                    Mutation = WalRecordConverter.FromWalRecord(new WalRecord
                    {
                        TreeId = treeId,
                        Op = MutationKind.Set,
                        Key = $"orphan-{i}",
                        Value = new byte[] { 0x01 },
                        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                        OriginClusterId = "site-a",
                    }),
                };
            }

            await inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken).ConfigureAwait(false);
        }

        public async Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        {
            var ordinal = Interlocked.Increment(ref _flushCalls);
            if (ordinal == 1)
            {
                // The first flush's phase-1 lands durably (modelled by
                // the deferred roll-forward in ReconcileAsync) but its
                // ack never arrives - the fault the storm rides in on.
                throw new InvalidOperationException("phase1-boom");
            }

            await inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken).ConfigureAwait(false);
        }

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        {
            OperationLog.Add("GetHighestOffset");
            return inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);
        }

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);
    }
}
