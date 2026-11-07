using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The cold-restart census for the replay page ring (issue #5896): many leaves
/// replaying one WAL partition from the start, through the real coordinator,
/// WAL shard grain and storage provider, with the entries storage examines
/// counted. With the ring each page of the log is read about once; without it,
/// once per leaf, which is the O(leaves x depth) cost of issue #5877.
/// </summary>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class ReplayPageRingColdRestartCensusTests
{
    private const int Entries = 4096;
    private const int Leaves = 32;
    private const int SliceBudget = 256;

    [TestCase(true)]
    [TestCase(false)]
    public async Task Cold_restart_census_reads_each_wal_page_about_once_with_the_ring(bool ring)
    {
        CountingWalStorageProvider.Reset();
        RingSwitch.Bytes = ring ? LatticeOptions.DefaultWalReplayPageRingBytes : 0;
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        try
        {
            var tree = $"census-{ring}-{Guid.NewGuid():N}";
            var shard = cluster.Client.GetGrain<IWalShardGrain>($"{tree}/0");
            for (var start = 0; start < Entries; start += 512)
            {
                var batch = Enumerable.Range(start, 512).Select(i => new WalRecord
                {
                    TreeId = tree,
                    Op = MutationKind.Set,
                    Key = $"k{i:D5}",
                    Value = [(byte)i],
                    Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                    OriginClusterId = "census",
                }).ToList();
                await shard.AppendBatchAsync(batch, CancellationToken.None);
            }

            var head = await shard.GetNextSequenceAsync(CancellationToken.None) - 1;
            Assert.That(head, Is.EqualTo(Entries - 1), "instrument validation: the log holds every entry");
            CountingWalStorageProvider.Reset();

            var coordinator = cluster.Client.GetGrain<ILeafReplayCoordinatorGrain>($"{tree}/0");
            var perLeaf = Entries / Leaves;
            var delivered = await Task.WhenAll(Enumerable.Range(0, Leaves).Select(async leaf =>
            {
                var filter = new WalKeyFilter(
                    $"k{leaf * perLeaf:D5}",
                    leaf == Leaves - 1 ? null : $"k{(leaf + 1) * perLeaf:D5}");
                var from = -1L;
                var owned = 0;
                while (from < head)
                {
                    var slice = await coordinator.ReadSliceAsync(from, head, SliceBudget, filter, CancellationToken.None);
                    if (slice.Count == 0)
                        break;
                    owned += slice.Count(e => e.Mutation.Value is not null);
                    from = slice[^1].Offset;
                }

                return owned;
            }));

            var examined = CountingWalStorageProvider.EntriesExamined;
            var readsPerEntry = examined / (double)Entries;
            TestContext.Out.WriteLine(
                $"[census] ring={ring} leaves={Leaves} entries={Entries} examined={examined} "
                + $"providerReads={CountingWalStorageProvider.Reads} readsPerPage={readsPerEntry:0.00}");

            Assert.That(delivered.Sum(), Is.EqualTo(Entries), "every entry reached exactly its owning leaf");
            if (ring)
                Assert.That(readsPerEntry, Is.LessThan(1.5), "each page of the log is read from storage about once");
            else
                Assert.That(readsPerEntry, Is.GreaterThan(Leaves / 2), "control: without the ring every leaf reads the log itself");
        }
        finally
        {
            await cluster.StopAllSilosAsync();
            await cluster.DisposeAsync();
        }
    }

    private static class RingSwitch
    {
        public static long Bytes;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.ConfigureLattice(o =>
            {
                o.WalPartitions = 1;
                o.WalReplayPageRingBytes = RingSwitch.Bytes;
            });
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
            siloBuilder.Services.Replace(
                ServiceDescriptor.Singleton<IWalStorageProvider>(_ => new CountingWalStorageProvider()));
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

    /// <summary>
    /// Counts the entries storage hands back. Filtered reads take the
    /// interface's default implementation, which drains <see cref="ReadAsync"/>,
    /// so they are counted at what they examine.
    /// </summary>
    private sealed class CountingWalStorageProvider : IWalStorageProvider
    {
        private readonly IWalStorageProvider _inner = new InMemoryWalStorageProvider();
        private static long _examined;
        private static long _reads;

        public static long EntriesExamined => Interlocked.Read(ref _examined);

        public static long Reads => Interlocked.Read(ref _reads);

        public static void Reset()
        {
            Interlocked.Exchange(ref _examined, 0);
            Interlocked.Exchange(ref _reads, 0);
        }

        public async IAsyncEnumerable<WalEntry> ReadAsync(
            string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _reads);
            await foreach (var entry in _inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken))
            {
                Interlocked.Increment(ref _examined);
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
