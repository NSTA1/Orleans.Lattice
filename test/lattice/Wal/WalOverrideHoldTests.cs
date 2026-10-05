using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.BPlusTree.PublicApiContract;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Wal;

/// <summary>
/// Issue #4641: a WAL GC pass must not remove an acknowledged write a leaf
/// stamped below the frontier it had published for that partition by an empty
/// release. The write is stamped from an override - a replicated apply, a
/// replicated range delete - so it sits below the leaf's clock, and the pin
/// store's max merge keeps the partition's pin at that frontier, so every
/// stamp-based arm of the GC would admit it. The leaf raises a durable override
/// hold before it appends such a write, and the GC reads a held consumer whose
/// offset is still <c>-1</c> as a block pin.
/// <para>
/// Runs real grains on a cluster whose grain state and WAL outlive the silo,
/// kills the silo before the leaf checkpoints the write, and reads it back from a
/// fresh one. The durability hold is disabled, as it is whenever a durable
/// consumer (a shipper or a view) also reads the tree.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class WalOverrideHoldTests
{
    private const int WritePartition = 0;
    private const int OtherPartition = 1;

    private static readonly InMemoryWalStorageProvider SharedWal = new();

    private string _run = "";

    [SetUp]
    public void SetUp() => _run = Guid.NewGuid().ToString("N")[..8];

    [OneTimeSetUp]
    public void OneTimeSetUp() => ProcessScopeMemoryGrainStorage.Reset();

    [OneTimeTearDown]
    public void OneTimeTearDown() => ProcessScopeMemoryGrainStorage.Reset();

    /// <summary>A replicated stamp well below anything the leaf's clock can hold.</summary>
    private static HybridLogicalClock AncientStamp() =>
        new() { WallClockTicks = DateTimeOffset.UtcNow.AddDays(-1).UtcTicks };

    [TestCase(false, TestName = "A_cursor_trim_keeps_a_replicated_write_stamped_below_an_empty_release_frontier")]
    [TestCase(true, TestName = "A_retention_trim_keeps_a_replicated_write_stamped_below_an_empty_release_frontier")]
    public async Task A_trim_keeps_a_replicated_write_stamped_below_an_empty_release_frontier(bool retention)
    {
        var treeName = (retention ? "ttl-override-" : "cursor-override-") + _run;
        var (otherKey, writeKey, _) = PickKeys();
        var first = await DeployAsync();
        try
        {
            var services = await ReleaseWritePartitionEmptyAsync(first, treeName, otherKey);

            // The replicated write lands in the released partition, stamped a day
            // ago: far below the frontier the leaf published for it.
            var tree = first.Client.GetGrain<ILattice>(treeName);
            using (LatticeHlcOverrideContext.With(AncientStamp()))
            {
                await tree.SetAsync(writeKey, [7]);
            }

            var report = await RunGcAsync(services, treeName, retention);
            Assert.That(report.BlockingConsumerIds, Is.Not.Null.And.Some.EndsWith("_" + WritePartition),
                "the held partition is named to the blocked-leaf remedy, which drives it to coverage");
            await first.KillSiloAsync(first.Primary);
        }
        finally
        {
            await first.DisposeAsync();
        }

        await AssertReadBackAsync(treeName, writeKey, new byte[] { 7 }, "an acknowledged replicated write was lost to a trim");
    }

    [Test]
    public async Task A_trim_never_resurrects_keys_a_replicated_range_delete_removed()
    {
        // A range delete is keyed by its start, so its record can land in a
        // partition where the leaf holds no rows and has released empty, while the
        // rows it deletes live, snapshot-covered, in another. Losing the record
        // brings them back.
        var treeName = "range-delete-override-" + _run;
        var (start, victim, survivor) = PickRangeDeleteKeys();
        var first = await DeployAsync();
        try
        {
            await first.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
                treeName,
                new TreeRegistryEntry { ShardCount = 1, WalPartitions = 2, MaxLeafKeys = 64, MaxInternalChildren = 4 });
            var tree = first.Client.GetGrain<ILattice>(treeName);
            var services = ((InProcessSiloHandle)first.Primary).SiloHost.Services;

            await tree.SetAsync(victim, [1]);
            var leaf = first.Client.GetGrain<IBPlusLeafGrain>(await ReadLeafAsync(first, services, treeName));
            var victimStamp = await leaf.GetClockAsync();
            await Task.Delay(5);
            await tree.SetAsync(survivor, [2]);

            // Graceful deactivation: both rows are checkpointed and captured, and
            // the start key's partition, which holds no row, is released empty at
            // a frontier past both writes.
            await leaf.ForceDeactivateAsync();
            await Task.Delay(250);
            var released = await ReadPinsAsync(first, services, treeName, WritePartition);
            Assert.That(released.All(p => p.Frontier > HybridLogicalClock.Zero && p.Offset < 0), Is.True,
                "the start key's partition was released empty (" + string.Join(", ", released) + ")");

            // A replicated delete stamped just past the victim's write: it wins the
            // victim, and it is below the released frontier.
            var deleteStamp = new HybridLogicalClock
            {
                WallClockTicks = victimStamp.WallClockTicks,
                Counter = victimStamp.Counter + 1,
            };
            using (LatticeHlcOverrideContext.With(deleteStamp))
            {
                await tree.DeleteRangeAsync(start, survivor);
            }

            Assert.That(await tree.GetAsync(victim), Is.Null, "the replicated delete removed the victim");
            await RunGcAsync(services, treeName, retention: false);
            await first.KillSiloAsync(first.Primary);
        }
        finally
        {
            await first.DisposeAsync();
        }

        await AssertReadBackAsync(treeName, victim, null, "a trimmed range delete resurrected its key");
    }

    [Test]
    public async Task A_hold_survives_a_restart_between_the_write_and_its_coverage()
    {
        var treeName = "restart-override-" + _run;
        var (otherKey, writeKey, _) = PickKeys();
        var first = await DeployAsync();
        try
        {
            await ReleaseWritePartitionEmptyAsync(first, treeName, otherKey);
            using (LatticeHlcOverrideContext.With(AncientStamp()))
            {
                await first.Client.GetGrain<ILattice>(treeName).SetAsync(writeKey, [7]);
            }

            // Lost before any GC pass and before the leaf covers the write.
            await first.KillSiloAsync(first.Primary);
        }
        finally
        {
            await first.DisposeAsync();
        }

        var second = await DeployAsync();
        try
        {
            var services = ((InProcessSiloHandle)second.Primary).SiloHost.Services;

            // The leaf has not reactivated: only its durable pin and hold speak for it.
            await RunGcAsync(services, treeName, retention: false);
            var tree = second.Client.GetGrain<ILattice>(treeName);
            Assert.That(await tree.GetAsync(writeKey), Is.EqualTo(new byte[] { 7 }),
                "a restart dropped the hold, and the write with it");

            // The hold is released by coverage alone: a graceful deactivation
            // checkpoints and captures the partition, and the store drops the hold
            // in the write that lands the real offset.
            Assert.That(await ReadHoldsAsync(second, services, treeName), Is.Not.Empty, "the hold outlived the restart");
            var leaf = second.Client.GetGrain<IBPlusLeafGrain>(await ReadLeafAsync(second, services, treeName));
            await leaf.ForceDeactivateAsync();
            await Task.Delay(250);
            Assert.That(await ReadHoldsAsync(second, services, treeName), Is.Empty,
                "a covered partition still holds the WAL");
        }
        finally
        {
            await second.StopAllSilosAsync();
            await second.DisposeAsync();
        }
    }

    [Test]
    public async Task A_block_report_after_the_write_does_not_release_the_hold()
    {
        // A leaf that persists a checkpoint without capturing it publishes the
        // block pin (Zero, -1) for the partition. Nothing then covers the write,
        // so the hold must stand through that report.
        var treeName = "block-report-override-" + _run;
        var (otherKey, writeKey, _) = PickKeys();
        var first = await DeployAsync();
        try
        {
            var services = await ReleaseWritePartitionEmptyAsync(first, treeName, otherKey);
            using (LatticeHlcOverrideContext.With(AncientStamp()))
            {
                await first.Client.GetGrain<ILattice>(treeName).SetAsync(writeKey, [7]);
            }

            var consumer = await ReadConsumerAsync(first, services, treeName, WritePartition);
            var pinKey = WalMaterialiserPinRouting.ShardKey(
                treeName, consumer, WalMaterialiserPinRouting.ResolveShardCount(services.GetService<IOptionsMonitor<LatticeOptions>>()));
            await first.Client.GetGrain<IWalMaterialiserPinGrain>(pinKey).ReportManyAsync(
                [new MaterialiserPinReport(consumer, HybridLogicalClock.Zero, -1)]);

            await RunGcAsync(services, treeName, retention: true);
            await first.KillSiloAsync(first.Primary);
        }
        finally
        {
            await first.DisposeAsync();
        }

        await AssertReadBackAsync(treeName, writeKey, new byte[] { 7 }, "a block report released the hold early");
    }

    [Test]
    public async Task A_carried_stamp_saga_terminal_holds_the_touched_leaf_on_the_terminal_partition()
    {
        var treeName = "terminal-override-" + _run;
        var cluster = await DeployAsync();
        try
        {
            await cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
                treeName,
                new TreeRegistryEntry { ShardCount = 1, WalPartitions = 2, MaxLeafKeys = 64, MaxInternalChildren = 4 });
            await cluster.Client.GetGrain<ILattice>(treeName).SetAsync("seed", [1]);
            var services = ((InProcessSiloHandle)cluster.Primary).SiloHost.Services;
            var physical = await cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
                .ResolveAsync(treeName);
            var shard = cluster.Client.GetGrain<IShardRootGrain>($"{physical}/0");

            using (LatticeHlcOverrideContext.With(AncientStamp()))
            {
                await shard.AppendTxTerminalAsync(Guid.NewGuid(), committed: false);
            }

            // Shard 0's terminal routes to partition 0 % 2.
            var holds = await ReadHoldsAsync(cluster, services, treeName);
            Assert.That(holds, Is.Not.Empty.And.All.EndsWith("_0"),
                "the carried-stamp terminal was appended without holding the leaf it resolves");
        }
        finally
        {
            await cluster.StopAllSilosAsync();
            await cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// Registers a two-partition single-leaf tree, writes <paramref name="otherKey"/>
    /// to the other partition, and gracefully deactivates the leaf, whose barrier
    /// publishes the write partition's pin as an empty release: a real frontier and
    /// no offset.
    /// </summary>
    private static async Task<IServiceProvider> ReleaseWritePartitionEmptyAsync(
        TestCluster cluster, string treeName, string otherKey)
    {
        await cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
            treeName,
            new TreeRegistryEntry { ShardCount = 1, WalPartitions = 2, MaxLeafKeys = 64, MaxInternalChildren = 4 });
        var services = ((InProcessSiloHandle)cluster.Primary).SiloHost.Services;
        await cluster.Client.GetGrain<ILattice>(treeName).SetAsync(otherKey, [1]);
        await cluster.Client.GetGrain<IBPlusLeafGrain>(await ReadLeafAsync(cluster, services, treeName)).ForceDeactivateAsync();
        await Task.Delay(250);
        var released = await ReadPinsAsync(cluster, services, treeName, WritePartition);
        Assert.That(released, Is.Not.Empty, "the leaf published its pin for the partition");
        Assert.That(released.All(p => p.Frontier > HybridLogicalClock.Zero && p.Offset < 0), Is.True,
            "the leaf released the partition empty (" + string.Join(", ", released) + ")");
        return services;
    }

    private static async Task<LatticeWalGcReport> RunGcAsync(IServiceProvider services, string treeName, bool retention)
    {
        // A one-millisecond retention window: the write is older than it as soon
        // as the pass runs.
        await Task.Delay(5);
        return await new LatticeWalGc(
                services,
                services.GetRequiredService<IWalCursorRegistry>(),
                new FixedLatticeOptionsMonitor(new LatticeOptions
                {
                    WalRetention = retention ? TimeSpan.FromMilliseconds(1) : null,
                    WalDurabilityHoldCeilingBytes = 0,
                }))
            .RunOnceAsync(treeName);
    }

    private static async Task AssertReadBackAsync(string treeName, string key, byte[]? expected, string because)
    {
        var second = await DeployAsync();
        try
        {
            Assert.That(await second.Client.GetGrain<ILattice>(treeName).GetAsync(key), Is.EqualTo(expected), because);
        }
        finally
        {
            await second.StopAllSilosAsync();
            await second.DisposeAsync();
        }
    }

    private static (string Other, string Key, int Partition) PickKeys()
    {
        string? other = null;
        string? key = null;
        for (var i = 0; other is null || key is null; i++)
        {
            var candidate = $"k{i:D3}";
            if (WalPartitionHash.Compute(candidate, 2) == WritePartition)
            {
                key ??= candidate;
            }
            else
            {
                other ??= candidate;
            }
        }

        return (other, key, WritePartition);
    }

    /// <summary>
    /// A range start in the write partition, a victim inside the range and a
    /// survivor at its exclusive end, both in the other partition, ordered
    /// start &lt; victim &lt; survivor.
    /// </summary>
    private static (string Start, string Victim, string Survivor) PickRangeDeleteKeys()
    {
        for (var i = 0; i < 1000; i++)
        {
            var start = $"r{i:D3}";
            if (WalPartitionHash.Compute(start, 2) != WritePartition)
            {
                continue;
            }

            string? victim = null;
            for (var j = 0; j < 1000; j++)
            {
                var candidate = $"{start}-{j:D3}";
                if (WalPartitionHash.Compute(candidate, 2) != OtherPartition)
                {
                    continue;
                }

                if (victim is null)
                {
                    victim = candidate;
                }
                else
                {
                    return (start, victim, candidate);
                }
            }
        }

        throw new AssertionException("no key triple found");
    }

    private static IReadOnlyList<string> PinKeys(IServiceProvider services, string treeName)
        => WalMaterialiserPinRouting.EnumerateReadKeys(
            treeName,
            WalMaterialiserPinRouting.ResolveShardCount(services.GetService<IOptionsMonitor<LatticeOptions>>()));

    private static async Task<Guid> ReadLeafAsync(TestCluster cluster, IServiceProvider services, string treeName)
    {
        foreach (var pinKey in PinKeys(services, treeName))
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

    private static async Task<string> ReadConsumerAsync(
        TestCluster cluster, IServiceProvider services, string treeName, int partition)
    {
        foreach (var pinKey in PinKeys(services, treeName))
        {
            foreach (var consumerId in (await cluster.Client.GetGrain<IWalMaterialiserPinGrain>(pinKey).GetPinsAsync()).Keys)
            {
                if (consumerId.EndsWith("_" + partition, StringComparison.Ordinal))
                {
                    return consumerId;
                }
            }
        }

        throw new AssertionException("the tree's leaf published no pin for the partition");
    }

    private static async Task<List<string>> ReadHoldsAsync(TestCluster cluster, IServiceProvider services, string treeName)
    {
        var holds = new List<string>();
        foreach (var pinKey in PinKeys(services, treeName))
        {
            holds.AddRange(await cluster.Client.GetGrain<IWalMaterialiserPinGrain>(pinKey).GetOverrideHoldsAsync());
        }

        return holds;
    }

    private static async Task<List<(HybridLogicalClock Frontier, long Offset)>> ReadPinsAsync(
        TestCluster cluster,
        IServiceProvider services,
        string treeName,
        int partition)
    {
        var pins = new List<(HybridLogicalClock, long)>();
        foreach (var pinKey in PinKeys(services, treeName))
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
