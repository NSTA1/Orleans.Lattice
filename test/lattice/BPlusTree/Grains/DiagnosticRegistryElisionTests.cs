using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public sealed class DiagnosticRegistryElisionTests
{
    private const string TreeId = "diagnostic-elision";

    private static TreeRegistryEntry Entry(string? physical = null, int partitions = 1, long? ceiling = null) => new()
    {
        MaxLeafKeys = 128,
        MaxInternalChildren = 128,
        ShardCount = 1,
        WalPartitions = partitions,
        PhysicalTreeId = physical,
        WalMaxRetainedBytes = ceiling,
    };

    private static LatticeWalUsageGrain Create(
        IGrainFactory factory, LatticeStorageUsageMetrics metrics, LatticeOptions options)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("ol.gwu", TreeId));
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        return new LatticeWalUsageGrain(
            context, factory, new LatticeOptionsResolver(factory, monitor),
            metrics, NullLogger<LatticeWalUsageGrain>.Instance);
    }

    private static (IGrainFactory Factory, ILatticeRegistry Registry, IWalShardGrain Wal) Arrange()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetEntryAsync(TreeId).Returns(Entry());
        var wal = Substitute.For<IWalShardGrain>();
        wal.GetByteSizesAsync(Arg.Any<CancellationToken>()).Returns((100L, 800L));
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(wal);
        return (factory, registry, wal);
    }

    [Test]
    public async Task GetWalUsageAsync_complete_pin_miss_reads_topology_and_final_policy_only()
    {
        var (factory, registry, wal) = Arrange();
        using var metrics = new LatticeStorageUsageMetrics();
        var grain = Create(factory, metrics, new LatticeOptions());

        var report = await grain.GetWalUsageAsync(CancellationToken.None);

        Assert.That(report.WalPhysicalBytes, Is.EqualTo(800));
        await registry.Received(2).GetEntryAsync(TreeId);
        await registry.DidNotReceiveWithAnyArgs().RegisterAsync(default!);
        await registry.DidNotReceiveWithAnyArgs().RegisterAndGetEntryAsync(default!);
        await wal.Received(1).GetByteSizesAsync(CancellationToken.None);
        await wal.DidNotReceiveWithAnyArgs().GetPhysicalByteSizeAsync(default);
        await wal.DidNotReceiveWithAnyArgs().GetRetainedByteSizeAsync(default);
    }

    [Test]
    public async Task GetWalUsageAsync_in_flight_join_does_not_read_registry()
    {
        var (factory, registry, wal) = Arrange();
        using var metrics = new LatticeStorageUsageMetrics();
        var gate = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        wal.GetByteSizesAsync(Arg.Any<CancellationToken>()).Returns(async _ => (100L, await gate.Task));
        var grain = Create(factory, metrics, new LatticeOptions());
        var first = grain.GetWalUsageAsync(CancellationToken.None);
        Assert.That(first.IsCompleted, Is.False);
        registry.ClearReceivedCalls();

        var joined = grain.GetWalUsageAsync(CancellationToken.None);
        await registry.DidNotReceive().GetEntryAsync(Arg.Any<string>());
        gate.SetResult(800);
        var reports = await Task.WhenAll(first, joined);

        Assert.That(reports[1], Is.EqualTo(reports[0]));
        await registry.Received(1).GetEntryAsync(TreeId);
        await wal.Received(1).GetByteSizesAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetWalUsageAsync_observes_ceiling_changed_during_fan_out()
    {
        var (factory, registry, wal) = Arrange();
        using var metrics = new LatticeStorageUsageMetrics();
        var gate = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        wal.GetByteSizesAsync(Arg.Any<CancellationToken>()).Returns(async _ => (100L, await gate.Task));
        var grain = Create(factory, metrics, new LatticeOptions());
        var pending = grain.GetWalUsageAsync(CancellationToken.None);
        registry.GetEntryAsync(TreeId).Returns(Entry(ceiling: 500));
        gate.SetResult(800);

        await pending;

        Assert.That(GaugeScrape.ReadSingle(LatticeMetrics.Meter,
            LatticeMetrics.StoragePolicyOverThresholdName, LatticeMetrics.TagTree, TreeId), Is.EqualTo(1));
    }

    [Test]
    public async Task GetWalUsageAsync_cache_hit_reloads_runtime_policy()
    {
        var (factory, registry, wal) = Arrange();
        using var metrics = new LatticeStorageUsageMetrics();
        var grain = Create(factory, metrics,
            new LatticeOptions { StorageUsageCacheTtl = TimeSpan.FromMinutes(5) });
        var first = await grain.GetWalUsageAsync(CancellationToken.None);
        registry.GetEntryAsync(TreeId).Returns(Entry(ceiling: 500));
        registry.ClearReceivedCalls();

        var cached = await grain.GetWalUsageAsync(CancellationToken.None);

        Assert.That(cached, Is.EqualTo(first));
        await registry.Received(1).GetEntryAsync(TreeId);
        await wal.Received(1).GetByteSizesAsync(Arg.Any<CancellationToken>());
        Assert.That(GaugeScrape.ReadSingle(LatticeMetrics.Meter,
            LatticeMetrics.StoragePolicyOverThresholdName, LatticeMetrics.TagTree, TreeId), Is.EqualTo(1));
    }

    [Test]
    public async Task GetWalUsageAsync_alias_uses_physical_owner_partition_pin()
    {
        var (factory, registry, wal) = Arrange();
        using var metrics = new LatticeStorageUsageMetrics();
        registry.GetEntryAsync(TreeId).Returns(Entry("physical-copy", partitions: 1));
        registry.GetEntryAsync("physical-copy").Returns(Entry(partitions: 2));
        factory.GetGrain<IWalShardGrain>("physical-copy/0").Returns(wal);
        factory.GetGrain<IWalShardGrain>("physical-copy/1").Returns(wal);
        var grain = Create(factory, metrics, new LatticeOptions { WalPartitions = 7 });

        var report = await grain.GetWalUsageAsync(CancellationToken.None);

        Assert.That(report.WalPhysicalBytes, Is.EqualTo(1600));
        await registry.Received(1).GetEntryAsync("physical-copy");
        await wal.Received(2).GetByteSizesAsync(Arg.Any<CancellationToken>());
        factory.DidNotReceive().GetGrain<IWalShardGrain>($"{TreeId}/0");
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task GetWalUsageAsync_absent_or_incomplete_entry_keeps_lazy_seeding(bool absent)
    {
        var (factory, registry, _) = Arrange();
        using var metrics = new LatticeStorageUsageMetrics();
        var incomplete = absent ? null : new TreeRegistryEntry();
        registry.GetEntryAsync(TreeId).Returns(incomplete, incomplete, Entry(), Entry());
        registry.RegisterAndGetEntryAsync(TreeId, incomplete).Returns((absent, Entry()));
        var deletion = Substitute.For<ITreeDeletionGrain>();
        factory.GetGrain<ITreeDeletionGrain>(TreeId).Returns(deletion);
        var grain = Create(factory, metrics, new LatticeOptions());

        var report = await grain.GetWalUsageAsync(CancellationToken.None);

        Assert.That(report.WalPhysicalBytes, Is.EqualTo(800));
        await registry.Received(1).RegisterAndGetEntryAsync(TreeId, incomplete);
        await registry.DidNotReceiveWithAnyArgs().RegisterAsync(default!);
    }
}
