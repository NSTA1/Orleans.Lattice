using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.BPlusTree.PublicApiContract;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Wal;

/// <summary>
/// Issue #4622: a WAL GC pass must not remove an acknowledged write owned by a leaf
/// that has never checkpointed, whether a <see cref="LatticeOptions.WalRetention"/>
/// ceiling or the leaf's own in-memory cursor admits it. Such a leaf is held only by
/// its standing durable block pin, and if its silo is then lost before it
/// checkpoints, its cold activation replays from the "nothing applied" sentinel and
/// cannot see a trimmed prefix. Runs real grains on a cluster whose grain state and
/// WAL outlive the silo, kills the silo, and reads the write back from a fresh one.
/// The durability hold is disabled, as it is whenever a durable consumer (a shipper
/// or a view) also reads the tree, or once the hold's ceiling is spent.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class WalRetentionBlockPinTests
{
    private string TreeName(bool retention) => (retention ? "ttl-block-pin-" : "cursor-block-pin-") + _run;

    private string _run = "";

    [SetUp]
    public void SetUp() => _run = Guid.NewGuid().ToString("N")[..8];

    private static readonly InMemoryWalStorageProvider SharedWal = new();

    [OneTimeSetUp]
    public void OneTimeSetUp() => ProcessScopeMemoryGrainStorage.Reset();

    [OneTimeTearDown]
    public void OneTimeTearDown() => ProcessScopeMemoryGrainStorage.Reset();

    [TestCase(true, true, TestName = "A_retention_trim_keeps_an_acknowledged_write_a_never_checkpointed_leaf_owns")]
    [TestCase(false, true, TestName = "A_cursor_trim_keeps_an_acknowledged_write_a_never_checkpointed_leaf_owns")]
    [TestCase(false, false, TestName = "A_killed_silo_alone_loses_no_acknowledged_write_a_never_checkpointed_leaf_owns")]
    public async Task A_trim_keeps_an_acknowledged_write_a_never_checkpointed_leaf_owns(bool retention, bool gcPass)
    {
        var first = await DeployAsync();
        try
        {
            await first.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
                TreeName(retention),
                new TreeRegistryEntry { ShardCount = 1, WalPartitions = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
            var tree = first.Client.GetGrain<ILattice>(TreeName(retention));
            await tree.SetAsync("k", [7]);

            var services = ((InProcessSiloHandle)first.Primary).SiloHost.Services;
            var pinKeys = WalMaterialiserPinRouting.EnumerateReadKeys(
                TreeName(retention),
                WalMaterialiserPinRouting.ResolveShardCount(services.GetService<Microsoft.Extensions.Options.IOptionsMonitor<LatticeOptions>>()));
            var offsets = new List<long>();
            foreach (var pinKey in pinKeys)
            {
                offsets.AddRange((await first.Client.GetGrain<IWalMaterialiserPinGrain>(pinKey).GetPinOffsetsAsync()).Values);
            }

            Assert.That(offsets.Where(o => o >= 0), Is.Empty,
                "the owning leaf has not checkpointed, so only its block pin holds the write");

            // A one-millisecond retention window: the write is older than it as soon
            // as the pass runs.
            await Task.Delay(5);
            if (gcPass)
            {
            await new LatticeWalGc(
                    services,
                    services.GetRequiredService<IWalCursorRegistry>(),
                    new FixedLatticeOptionsMonitor(new LatticeOptions
                    {
                        WalRetention = retention ? TimeSpan.FromMilliseconds(1) : null,
                        WalDurabilityHoldCeilingBytes = 0,
                    }))
                .RunOnceAsync(TreeName(retention));
            }

            // The silo is lost before the leaf checkpoints or captures a snapshot.
            await first.KillSiloAsync(first.Primary);
        }
        finally
        {
            await first.DisposeAsync();
        }

        var second = await DeployAsync();
        try
        {
            Assert.That(await second.Client.GetGrain<ILattice>(TreeName(retention)).GetAsync("k"), Is.EqualTo(new byte[] { 7 }),
                "an acknowledged write was lost to a trim");
        }
        finally
        {
            await second.StopAllSilosAsync();
            await second.DisposeAsync();
        }
    }

    [Test]
    public async Task A_retention_trim_keeps_a_write_a_leaf_applied_after_it_released_the_partition_empty()
    {
        // 57704fda's trace: the leaf releases partition q empty at frontier F (no
        // rows there, no checkpoint), then writes w to q. The pin store merges
        // frontiers by max, so q's durable pin stays (F, -1) rather than falling
        // back to a Zero block pin, and nothing but the retention hold keeps w.
        var treeName = "ttl-empty-release-" + _run;
        var first = await DeployAsync();
        string writeKey;
        try
        {
            await first.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
                treeName,
                new TreeRegistryEntry { ShardCount = 1, WalPartitions = 2, MaxLeafKeys = 64, MaxInternalChildren = 4 });
            var tree = first.Client.GetGrain<ILattice>(treeName);
            var (otherKey, key, q) = PickKeys(2);
            writeKey = key;
            var services = ((InProcessSiloHandle)first.Primary).SiloHost.Services;

            // A write to the other partition, then a graceful deactivation, whose
            // barrier checkpoints and captures that write and publishes the leaf's
            // pin for q at a real frontier: q has no rows and no checkpoint, so it
            // releases empty.
            await tree.SetAsync(otherKey, [1]);
            await first.Client.GetGrain<IBPlusLeafGrain>(await ReadLeafAsync(first, services, treeName)).ForceDeactivateAsync();
            await Task.Delay(250);
            var released = await ReadPinsAsync(first, services, treeName, q);
            Assert.That(released, Is.Not.Empty, "the leaf published its pin for the partition");
            Assert.That(released.All(p => p.Frontier > HybridLogicalClock.Zero && p.Offset < 0), Is.True,
                "the leaf released the partition empty: a real frontier and no offset ("
                + string.Join(", ", released) + ")");

            // The leaf reactivates and applies w to q; it does not checkpoint it.
            await tree.SetAsync(writeKey, [7]);
            var afterWrite = await ReadPinsAsync(first, services, treeName, q);
            Assert.That(afterWrite.All(p => p.Frontier > HybridLogicalClock.Zero && p.Offset < 0), Is.True,
                "the write did not lower the max-merged frontier, and the leaf has not checkpointed ("
                + string.Join(", ", afterWrite) + ")");

            await Task.Delay(5);
            await new LatticeWalGc(
                    services,
                    services.GetRequiredService<IWalCursorRegistry>(),
                    new FixedLatticeOptionsMonitor(new LatticeOptions
                    {
                        WalRetention = TimeSpan.FromMilliseconds(1),
                        WalDurabilityHoldCeilingBytes = 0,
                    }))
                .RunOnceAsync(treeName);

            // The silo is lost before the leaf checkpoints or captures a snapshot.
            await first.KillSiloAsync(first.Primary);
        }
        finally
        {
            await first.DisposeAsync();
        }

        var second = await DeployAsync();
        try
        {
            Assert.That(await second.Client.GetGrain<ILattice>(treeName).GetAsync(writeKey), Is.EqualTo(new byte[] { 7 }),
                "an acknowledged write was lost to a retention trim");
        }
        finally
        {
            await second.StopAllSilosAsync();
            await second.DisposeAsync();
        }
    }

    private static (string Other, string Key, int Q) PickKeys(int partitions)
    {
        const int q = 0;
        string? other = null;
        string? key = null;
        for (var i = 0; other is null || key is null; i++)
        {
            var candidate = $"k{i:D3}";
            if (WalPartitionHash.Compute(candidate, partitions) == q)
            {
                key ??= candidate;
            }
            else
            {
                other ??= candidate;
            }
        }

        return (other, key, q);
    }

    private static async Task<Guid> ReadLeafAsync(TestCluster cluster, IServiceProvider services, string treeName)
    {
        var pinKeys = WalMaterialiserPinRouting.EnumerateReadKeys(
            treeName,
            WalMaterialiserPinRouting.ResolveShardCount(services.GetService<Microsoft.Extensions.Options.IOptionsMonitor<LatticeOptions>>()));
        foreach (var pinKey in pinKeys)
        {
            foreach (var consumerId in (await cluster.Client.GetGrain<IWalMaterialiserPinGrain>(pinKey).GetPinsAsync()).Keys)
            {
                var start = consumerId.IndexOf("bplusleaf/", StringComparison.Ordinal);
                var end = consumerId.LastIndexOf('_');
                if (start >= 0 && end > start + 10 && Guid.TryParseExact(consumerId[(start + 10)..end], "N", out var leaf))
                {
                    return leaf;
                }
            }
        }

        throw new AssertionException("the tree's leaf published no durable pin");
    }

    private static async Task<List<(HybridLogicalClock Frontier, long Offset)>> ReadPinsAsync(
        TestCluster cluster,
        IServiceProvider services,
        string treeName,
        int partition)
    {
        var pinKeys = WalMaterialiserPinRouting.EnumerateReadKeys(
            treeName,
            WalMaterialiserPinRouting.ResolveShardCount(services.GetService<Microsoft.Extensions.Options.IOptionsMonitor<LatticeOptions>>()));
        var pins = new List<(HybridLogicalClock, long)>();
        foreach (var pinKey in pinKeys)
        {
            var grain = cluster.Client.GetGrain<IWalMaterialiserPinGrain>(pinKey);
            var offsets = await grain.GetPinOffsetsAsync();
            foreach (var (consumerId, pin) in await grain.GetPinsAsync())
            {
                if (consumerId.EndsWith("_" + partition, StringComparison.Ordinal))
                {
                    pins.Add((pin, offsets.GetValueOrDefault(consumerId, -1)));
                }
            }
        }

        return pins;
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
            siloBuilder.AddWalStorage(_ => SharedWal);
            siloBuilder.AddWalCursorRegistry();
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.ConfigureLattice(o =>
            {
                // The test drives the GC itself, and the leaf must not checkpoint on
                // its own before the silo is lost.
                o.WalGcInterval = TimeSpan.Zero;
                o.MaterialiserCheckpointInterval = TimeSpan.FromHours(1);
            });
        }
    }
}
