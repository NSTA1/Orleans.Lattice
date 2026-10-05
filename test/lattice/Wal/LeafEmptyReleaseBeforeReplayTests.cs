using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.BPlusTree.PublicApiContract;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Wal;

/// <summary>
/// Issue #4669 (the F08 finding of epic #4430): a cold-activated leaf must never publish an empty
/// release - a real frontier with no offset - for a WAL partition its replay has
/// not yet read. Such a partition holds no row in the cache only because nothing
/// has been replayed into it, so the release would let the WAL GC trim
/// acknowledged writes the leaf still owns there, and a further cold activation
/// replays the partition from the "nothing applied" sentinel and cannot see the
/// trimmed prefix. Runs real grains on storage that outlives the silo, holds the
/// replay of one partition while the other replays, runs the GC, loses the silo,
/// and reads the write back from a fresh cluster.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LeafEmptyReleaseBeforeReplayTests
{
    private const int Partitions = 2;


    private static readonly PartitionReadGateProvider Wal = new(new InMemoryWalStorageProvider());

    /// <summary>Whether the next deployment holds checkpoints off.</summary>
    private static volatile bool SuppressCheckpoints;

    private string _tree = "";

    [SetUp]
    public void SetUp()
    {
        _tree = "f08-" + Guid.NewGuid().ToString("N")[..8];
        ProcessScopeMemoryGrainStorage.Reset();
        Wal.Open();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        ProcessScopeMemoryGrainStorage.Reset();
        Wal.Open();
    }

    // Partition 0 cannot be held: the activation itself reads it, so nothing
    // can publish while it waits.
    [TestCase(1)]
    public async Task A_cold_leaf_never_releases_a_partition_its_replay_has_not_read(int Held)
    {
        // The held partition gets the larger backlog, so pass 1 - which sweeps
        // partitions by backlog ascending - replays the other partition first and
        // banks its checkpoint before it reaches the held one.
        var (heldKeys, otherKeys) = PickKeys(Held);
        var heldKey = heldKeys[0];

        // The owning leaf applies a write to the held partition and later writes to
        // the other one, and is lost before it checkpoints either.
        SuppressCheckpoints = true;
        var first = await DeployAsync();
        Guid leaf;
        try
        {
            await first.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
                _tree,
                new TreeRegistryEntry { ShardCount = 1, WalPartitions = Partitions, MaxLeafKeys = 64, MaxInternalChildren = 4 });
            var tree = first.Client.GetGrain<ILattice>(_tree);
            foreach (var key in heldKeys)
            {
                await tree.SetAsync(key, [7]);
            }

            await tree.SetAsync(otherKeys[0], [1]);

            leaf = await LeafOfAsync(first);
            await first.KillSiloAsync(first.Primary);
        }
        finally
        {
            await first.DisposeAsync();
        }

        // Cold activation with the held partition's replay held.
        Wal.Hold(Held);
        var released = false;
        SuppressCheckpoints = false;
        var second = await DeployAsync();
        try
        {
            var services = ((InProcessSiloHandle)second.Primary).SiloHost.Services;
            // Activates the leaf without waiting on its replay, so the leaf stays
            // free to serve the pin store while the held partition's replay waits.
            var activation = second.Client.GetGrain<IBPlusLeafGrain>(leaf).GetTreeIdAsync();
            await Wal.HeldReadReached.Task.WaitAsync(TimeSpan.FromSeconds(30));

            // Give every publisher that can run while the replay is held - the pass-1
            // checkpoint flush tail, the activation seed, a timer - its chance.
            var deadline = Environment.TickCount64 + 15_000;
            var otherCheckpointed = false;
            while (Environment.TickCount64 < deadline && !released)
            {
                released = await HeldPartitionReleasedAsync(second, services, Held);
                otherCheckpointed |= await PartitionCheckpointedAsync(second, services, 1 - Held);
                await Task.Delay(100);
            }

            TestContext.Progress.WriteLine(
                $"F08 probe held={Held}: released={released} otherCheckpointedDuringHold={otherCheckpointed} activated={activation.IsCompletedSuccessfully}");

            // The GC reads the held partition too; its reads, on this test's own
            // flow, pass the gate the leaf's replay is held at.
            PartitionReadGateProvider.Bypass.Value = true;
            await new LatticeWalGc(
                    services,
                    services.GetRequiredService<IWalCursorRegistry>(),
                    new FixedLatticeOptionsMonitor(new LatticeOptions { WalDurabilityHoldCeilingBytes = 0 }))
                .RunOnceAsync(_tree);

            await second.KillSiloAsync(second.Primary);
        }
        finally
        {
            Wal.Open();
            await second.DisposeAsync();
        }

        var third = await DeployAsync();
        try
        {
            var value = await third.Client.GetGrain<ILattice>(_tree).GetAsync(heldKey);
            Assert.Multiple(() =>
            {
                Assert.That(released, Is.False,
                    "the leaf published an empty release for a partition its replay had not read");
                Assert.That(value, Is.EqualTo(new byte[] { 7 }), "an acknowledged write was lost to a trim");
            });
        }
        finally
        {
            await third.StopAllSilosAsync();
            await third.DisposeAsync();
        }
    }

    private static (string[] Held, string[] Others) PickKeys(int Held)
    {
        var held = new List<string>();
        var others = new List<string>();
        for (var i = 0; held.Count < 3 || others.Count < 1; i++)
        {
            var key = $"k{i:D3}";
            if (WalPartitionHash.Compute(key, Partitions) == Held)
            {
                if (held.Count < 3)
                {
                    held.Add(key);
                }
            }
            else if (others.Count < 1)
            {
                others.Add(key);
            }
        }

        return (held.ToArray(), others.ToArray());
    }

    private async Task<Guid> LeafOfAsync(TestCluster cluster)
    {
        var services = ((InProcessSiloHandle)cluster.Primary).SiloHost.Services;
        var pinKeys = WalMaterialiserPinRouting.EnumerateReadKeys(
            _tree,
            WalMaterialiserPinRouting.ResolveShardCount(services.GetService<Microsoft.Extensions.Options.IOptionsMonitor<LatticeOptions>>()));
        foreach (var pinKey in pinKeys)
        {
            foreach (var consumerId in (await cluster.Client.GetGrain<IWalMaterialiserPinGrain>(pinKey).GetPinsAsync()).Keys)
            {
                var start = consumerId.IndexOf("bplusleaf/", StringComparison.Ordinal);
                var end = consumerId.LastIndexOf('_');
                if (start >= 0 && end > start + 10 && Guid.TryParseExact(consumerId[(start + 10)..end], "N", out var id))
                {
                    return id;
                }
            }
        }

        throw new AssertionException("the tree's leaf published no durable pin");
    }

    private async Task<bool> PartitionCheckpointedAsync(TestCluster cluster, IServiceProvider services, int partition)
    {
        var pinKeys = WalMaterialiserPinRouting.EnumerateReadKeys(
            _tree,
            WalMaterialiserPinRouting.ResolveShardCount(services.GetService<Microsoft.Extensions.Options.IOptionsMonitor<LatticeOptions>>()));
        foreach (var pinKey in pinKeys)
        {
            foreach (var (consumerId, offset) in await cluster.Client.GetGrain<IWalMaterialiserPinGrain>(pinKey).GetPinOffsetsAsync())
            {
                if (consumerId.EndsWith("_" + partition, StringComparison.Ordinal) && offset >= 0)
                {
                    return true;
                }
            }
        }

        return false;
    }

    private async Task<bool> HeldPartitionReleasedAsync(TestCluster cluster, IServiceProvider services, int Held)
    {
        var pinKeys = WalMaterialiserPinRouting.EnumerateReadKeys(
            _tree,
            WalMaterialiserPinRouting.ResolveShardCount(services.GetService<Microsoft.Extensions.Options.IOptionsMonitor<LatticeOptions>>()));
        foreach (var pinKey in pinKeys)
        {
            var grain = cluster.Client.GetGrain<IWalMaterialiserPinGrain>(pinKey);
            var offsets = await grain.GetPinOffsetsAsync();
            foreach (var (consumerId, pin) in await grain.GetPinsAsync())
            {
                if (consumerId.EndsWith("_" + Held, StringComparison.Ordinal)
                    && pin > HybridLogicalClock.Zero
                    && offsets.GetValueOrDefault(consumerId, -1) < 0)
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static async Task<TestCluster> DeployAsync()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) =>
                silo.Services.AddKeyedSingleton<Orleans.Storage.IGrainStorage>(
                    name,
                    (_, _) => new ProcessScopeMemoryGrainStorage()));
            siloBuilder.AddWalStorage(_ => Wal);
            siloBuilder.AddWalCursorRegistry();
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.ConfigureLattice(o =>
            {
                // The test drives the GC itself, and the first leaf must not
                // checkpoint on its own before its silo is lost.
                o.WalGcInterval = TimeSpan.Zero;
                if (SuppressCheckpoints)
                {
                    o.MaterialiserCheckpointInterval = TimeSpan.FromHours(1);
                }
                else
                {
                    // Persist a checkpoint on every advance, so a replay that publishes
                    // mid-way does so as early as it can.
                    o.MaterialiserCheckpointEntries = 1;
                    o.MaterialiserCheckpointInterval = TimeSpan.FromMilliseconds(1);
                }
            });
        }
    }

    /// <summary>
    /// A WAL whose reads of one partition wait while it is held: the leaf's replay
    /// of that partition stalls while every other partition replays.
    /// </summary>
    private sealed class PartitionReadGateProvider(InMemoryWalStorageProvider inner) : IWalStorageProvider
    {
        internal static readonly AsyncLocal<bool> Bypass = new();

        private volatile int _held = -1;
        private TaskCompletionSource _open = NewOpen();

        internal TaskCompletionSource HeldReadReached { get; private set; } = NewReached();

        public void Hold(int partition)
        {
            _open = NewOpen();
            HeldReadReached = NewReached();
            _held = partition;
        }

        public void Open()
        {
            _held = -1;
            _open.TrySetResult();
        }

        private static TaskCompletionSource NewOpen() => new(TaskCreationOptions.RunContinuationsAsynchronously);

        private static TaskCompletionSource NewReached() => new(TaskCreationOptions.RunContinuationsAsynchronously);

        private async Task WaitIfHeldAsync(int shardIndex)
        {
            if (shardIndex == _held && !Bypass.Value)
            {
                HeldReadReached.TrySetResult();
                await _open.Task.ConfigureAwait(false);
            }
        }

        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
            => inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken);

        public async IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await WaitIfHeldAsync(shardIndex);
            await foreach (var entry in inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken))
            {
                yield return entry;
            }
        }

        public async IAsyncEnumerable<WalEntry> ReadFilteredAsync(string treeId, int shardIndex, long fromOffsetExclusive, long toOffsetInclusive, int maxEntries, WalKeyFilter filter, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await WaitIfHeldAsync(shardIndex);
            await foreach (var entry in inner.ReadFilteredAsync(treeId, shardIndex, fromOffsetExclusive, toOffsetInclusive, maxEntries, filter, cancellationToken))
            {
                yield return entry;
            }
        }

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);

        public Task<long> GetRetainedByteSizeAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetRetainedByteSizeAsync(treeId, shardIndex, cancellationToken);
    }
}
