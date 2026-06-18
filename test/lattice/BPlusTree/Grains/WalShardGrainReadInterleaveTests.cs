using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Concurrency;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Proves that catch-up reads against the single per-partition
/// <see cref="IWalShardGrain"/> activation do not head-of-line-block
/// foreground appends. Before the read methods were marked
/// <see cref="AlwaysInterleaveAttribute"/> a <see cref="IWalShardGrain.ReadAsync"/>
/// call held the grain's non-reentrant turn for the full duration of the
/// underlying provider read (an Azure Table page fetch, paginated over the
/// whole backlog), queuing every concurrent <see cref="IWalShardGrain.AppendAsync"/>
/// behind it - the contention the materialised-view maintainer and the
/// replication shipper newly exercise as sustained readers.
/// </summary>
[TestFixture]
[Category("Integration")]
public class WalShardGrainReadInterleaveTests
{
    private const string ClusterId = "wal-interleave-site";

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        GatingWalStorageProvider.Reset();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public void ReadAsync_and_ReadShippingAsync_are_marked_AlwaysInterleave()
    {
        // The wire-contract half of the guarantee: without the attribute
        // the behavioural interleave below cannot hold, so pin it on the
        // interface so a future edit cannot silently drop it.
        var readMethod = typeof(IWalShardGrain).GetMethod(nameof(IWalShardGrain.ReadAsync));
        var shipMethod = typeof(IWalShardGrain).GetMethod(nameof(IWalShardGrain.ReadShippingAsync));

        Assert.Multiple(() =>
        {
            Assert.That(readMethod, Is.Not.Null);
            Assert.That(shipMethod, Is.Not.Null);
            Assert.That(readMethod!.GetCustomAttribute<AlwaysInterleaveAttribute>(), Is.Not.Null,
                "ReadAsync must be [AlwaysInterleave] so catch-up reads do not block foreground appends.");
            Assert.That(shipMethod!.GetCustomAttribute<AlwaysInterleaveAttribute>(), Is.Not.Null,
                "ReadShippingAsync must be [AlwaysInterleave] so shipper drains do not block foreground appends.");
        });
    }

    [Test]
    public async Task Append_completes_while_a_read_is_parked_inside_the_provider()
    {
        const string tree = "mv-interleave-tree";
        var grain = _cluster.Client.GetGrain<IWalShardGrain>($"{tree}/0");

        // Seed a few entries so the read has something to page over.
        for (var i = 0; i < 3; i++)
        {
            await grain.AppendAsync(MakeEntry(tree, $"seed{i}"), CancellationToken.None);
        }

        GatingWalStorageProvider.Arm(tree);
        try
        {
            // Start a read; it enters the gated provider and parks before
            // yielding, holding the conceptual "long Azure read" open.
            var readTask = grain.ReadAsync(0, 100, CancellationToken.None);
            await GatingWalStorageProvider.ReadEntered!.Task.WaitAsync(TimeSpan.FromSeconds(15));

            // With the read parked, a foreground append must still complete.
            // Without [AlwaysInterleave] on ReadAsync it would queue behind
            // the read's turn and only complete once the gate is released.
            var appendTask = grain.AppendAsync(MakeEntry(tree, "foreground"), CancellationToken.None);
            var winner = await Task.WhenAny(appendTask, Task.Delay(TimeSpan.FromSeconds(10)));

            Assert.That(winner, Is.SameAs(appendTask),
                "A foreground append must interleave with a read that is parked inside the WAL grain turn.");
            var appendedOffset = await appendTask;
            Assert.That(appendedOffset, Is.EqualTo(3L),
                "The interleaved append must take the next dense offset.");
        }
        finally
        {
            // Release the parked read and drain it so the activation is
            // left clean for teardown.
            GatingWalStorageProvider.Release();
        }

        var page = await grain.ReadAsync(0, 100, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(15));
        Assert.That(page.Entries, Has.Count.GreaterThanOrEqualTo(4),
            "After release the read sees the seeds plus the interleaved append.");
    }

    private static WalRecord MakeEntry(string tree, string key) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = ClusterId,
    };

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.ConfigureLattice(o => o.WalPartitions = 1);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();

            // Swap the default in-memory WAL provider for one that can park
            // a read mid-flight, exercising the read/append interleave on
            // the real grain activation.
            siloBuilder.Services.Replace(
                ServiceDescriptor.Singleton<IWalStorageProvider>(_ => new GatingWalStorageProvider()));
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

    /// <summary>
    /// Decorates <see cref="InMemoryWalStorageProvider"/> and, when armed
    /// for a specific tree, parks <see cref="ReadAsync"/> on a gate before
    /// yielding so a test can hold a read open and observe whether a
    /// concurrent append interleaves. The control state is static because
    /// the TestingHost silo runs in-process and the gate is scoped to a
    /// unique tree id, so it cannot leak into other fixtures.
    /// </summary>
    private sealed class GatingWalStorageProvider : IWalStorageProvider
    {
        private readonly IWalStorageProvider _inner = new InMemoryWalStorageProvider();

        internal static volatile TaskCompletionSource? ReadGate;
        internal static volatile TaskCompletionSource? ReadEntered;
        private static volatile string? _gatedTree;

        internal static void Arm(string tree)
        {
            _gatedTree = tree;
            ReadEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            ReadGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        internal static void Release() => ReadGate?.TrySetResult();

        internal static void Reset()
        {
            _gatedTree = null;
            ReadGate = null;
            ReadEntered = null;
        }

        public async IAsyncEnumerable<WalEntry> ReadAsync(
            string treeId,
            int shardIndex,
            long fromOffsetExclusive,
            int maxEntries,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var gate = ReadGate;
            if (gate is not null && string.Equals(treeId, _gatedTree, StringComparison.Ordinal))
            {
                ReadEntered?.TrySetResult();
                await gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            await foreach (var entry in _inner
                .ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken)
                .ConfigureAwait(false))
            {
                yield return entry;
            }
        }

        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
            => _inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken);

        public Task AppendEncodedBatchAsync(string treeId, int shardIndex, ReadOnlyMemory<ArraySegment<byte>> encodedEntries, ReadOnlyMemory<long> offsets, IWalRecordEncoder encoder, CancellationToken cancellationToken)
            => _inner.AppendEncodedBatchAsync(treeId, shardIndex, encodedEntries, offsets, encoder, cancellationToken);

        public Task<WalShardEncodedPage> ReadEncodedAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, IWalRecordEncoder encoder, CancellationToken cancellationToken)
            => _inner.ReadEncodedAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, encoder, cancellationToken);

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => _inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => _inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => _inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);

        public Task ReconcileAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => _inner.ReconcileAsync(treeId, shardIndex, cancellationToken);

        public Task<long> GetRetainedByteSizeAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => _inner.GetRetainedByteSizeAsync(treeId, shardIndex, cancellationToken);
    }
}
