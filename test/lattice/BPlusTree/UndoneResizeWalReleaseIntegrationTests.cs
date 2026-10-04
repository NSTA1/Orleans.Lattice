using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #3930: an undone resize must release its discarded destination's
/// write-ahead-log retention. Before the fix the destination was only
/// soft-deleted, so its leaves' durable materialiser pins - never checkpointed,
/// because the drain wrote them - held its cursor floor, and its log was
/// retained in full for the whole soft-delete window while the WAL GC kept
/// reactivating its leaves to try to lift a floor nothing could lift.
/// <para>
/// Each test first proves the destination actually holds pins and log entries,
/// so the drained assertion cannot pass on a destination that never had any.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class UndoneResizeWalReleaseIntegrationTests
{
    private static readonly TimeSpan SettleBudget = TimeSpan.FromSeconds(15);

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        SiloServicesCapture.Reset();

        // One silo, so the captured provider is the singleton the WAL shard
        // grains append through.
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
        SiloServicesCapture.Reset();
    }

    [Test]
    public async Task Undoing_a_completed_resize_releases_the_discarded_destinations_pins_and_wal()
    {
        var treeName = $"undo-wal-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        await WriteKeysAsync(tree);

        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeName);
        // A small destination leaf size makes the drain split, and a split
        // sibling is born with a durable pin - the never-checkpointed kind
        // that held the floor in the field.
        await resize.ResizeAsync(4, 4);
        await resize.RunResizePassAsync();

        var destination = await Registry.ResolveAsync(treeName);
        Assert.That(destination, Is.Not.EqualTo(treeName), "the resize should have aliased the tree to its destination");

        // Read through the alias so the destination's leaves are resident and
        // publishing, which is the state an undo actually finds them in.
        Assert.That(await tree.GetAsync("key-0000"), Is.Not.Null);
        await AssertHoldsRetentionAsync(destination);

        await resize.UndoResizeAsync();

        await AssertRetentionReleasedAsync(destination);
        Assert.That(await tree.GetAsync("key-0000"), Is.Not.Null, "the undo must still restore the original tree");
    }

    [Test]
    public async Task Discarding_a_drained_destination_that_was_never_aliased_releases_its_pins_and_wal()
    {
        // The shape a pre-swap undo discards: a copy the online drain has
        // written into, which no alias has ever pointed at. A pre-swap undo
        // cannot be pinned deterministically in-cluster - the resize's phase
        // timer is due immediately and drives the drain through to the swap
        // before the next client call is admitted - so the discard it performs
        // is driven here directly; the undo's wiring to it is pinned by
        // TreeResizeGrainTests.UndoResize_during_drain_discards_destination_tree.
        var source = $"discard-src-{Guid.NewGuid():N}";
        var destination = $"{source}/resized/{Guid.NewGuid():N}";
        await WriteKeysAsync(_cluster.GrainFactory.GetGrain<ILattice>(source));

        var snapshot = _cluster.GrainFactory.GetGrain<ITreeSnapshotGrain>(source);
        await snapshot.SnapshotAsync(destination, SnapshotMode.Online, maxLeafKeys: 4, maxInternalChildren: 4);
        await snapshot.RunSnapshotPassAsync();
        Assert.That(await _cluster.GrainFactory.GetGrain<ILattice>(destination).GetAsync("key-0000"), Is.Not.Null);
        await AssertHoldsRetentionAsync(destination);

        var deletion = _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(destination);
        await deletion.DiscardDerivedPhysicalTreeAsync();

        await AssertRetentionReleasedAsync(destination);
        Assert.That(await deletion.GetPhysicalRetentionAsync(), Is.EqualTo(PhysicalTreeRetention.Discarded));
        Assert.ThrowsAsync<InvalidOperationException>(() => deletion.RecoverPhysicalAsync(),
            "a discarded copy whose WAL was released must not be recoverable");

        // Its leaves must still purge cleanly although the log they would
        // replay has been trimmed away beneath their checkpoints.
        await deletion.PurgeNowAsync();
        Assert.That((await deletion.GetDeletionStatusAsync()).PurgeComplete, Is.True);
    }

    [Test]
    public async Task A_destination_retired_by_an_earlier_build_is_discarded_once_no_resize_names_it()
    {
        // The state an estate is already in when it upgrades: an undone resize's
        // destination soft-deleted by DeleteDerivedPhysicalTreeAsync, holding its
        // pins and its WAL for the soft-delete window. The WAL GC asks the
        // deletion grain to discard such a copy; this drives that call directly.
        var source = $"legacy-src-{Guid.NewGuid():N}";
        var destination = $"{source}/resized/{Guid.NewGuid():N}";
        await WriteKeysAsync(_cluster.GrainFactory.GetGrain<ILattice>(source));

        var snapshot = _cluster.GrainFactory.GetGrain<ITreeSnapshotGrain>(source);
        await snapshot.SnapshotAsync(destination, SnapshotMode.Online, maxLeafKeys: 4, maxInternalChildren: 4);
        await snapshot.RunSnapshotPassAsync();
        Assert.That(await _cluster.GrainFactory.GetGrain<ILattice>(destination).GetAsync("key-0000"), Is.Not.Null);

        var deletion = _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(destination);
        await deletion.DeleteDerivedPhysicalTreeAsync();
        await AssertHoldsRetentionAsync(destination);
        Assert.That(await deletion.GetPhysicalRetentionAsync(), Is.EqualTo(PhysicalTreeRetention.Deleted));

        Assert.That(await deletion.DiscardIfAbandonedDerivedCopyAsync(), Is.True);

        await AssertRetentionReleasedAsync(destination);
        Assert.That(await deletion.GetPhysicalRetentionAsync(), Is.EqualTo(PhysicalTreeRetention.Discarded));
    }

    private ILatticeRegistry Registry =>
        _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    private static async Task WriteKeysAsync(ILattice tree)
    {
        for (var i = 0; i < 40; i++)
        {
            await tree.SetAsync($"key-{i:D4}", [(byte)i]);
        }
    }

    private async Task AssertHoldsRetentionAsync(string physicalTreeId)
    {
        var deadline = Environment.TickCount64 + (long)SettleBudget.TotalMilliseconds;
        int pins;
        long entries;
        do
        {
            pins = await CountMaterialiserPinsAsync(physicalTreeId);
            entries = await CountRetainedWalEntriesAsync(physicalTreeId);
            if (pins > 0 && entries > 0) return;
            await Task.Delay(100);
        }
        while (Environment.TickCount64 < deadline);

        Assert.Fail($"precondition: the destination should hold materialiser pins and WAL entries, but held {pins} pins and {entries} entries.");
    }

    private async Task AssertRetentionReleasedAsync(string physicalTreeId)
    {
        // Polled because a still-resident leaf can briefly re-publish a pin
        // after the release; without the fix neither count ever reaches zero.
        var deadline = Environment.TickCount64 + (long)SettleBudget.TotalMilliseconds;
        int pins;
        long entries;
        do
        {
            pins = await CountMaterialiserPinsAsync(physicalTreeId);
            entries = await CountRetainedWalEntriesAsync(physicalTreeId);
            if (pins == 0 && entries == 0) return;
            await Task.Delay(100);
        }
        while (Environment.TickCount64 < deadline);

        Assert.Fail($"the discarded destination should retain no materialiser pins and no WAL, but still held {pins} pins and {entries} entries.");
    }

    /// <summary>
    /// Counts the tree's leaf materialiser pins in both places the WAL GC reads
    /// them: the durable pin store, which survives a restart, and the in-memory
    /// cursor registry a resident leaf reports into.
    /// </summary>
    private async Task<int> CountMaterialiserPinsAsync(string physicalTreeId)
    {
        var options = SiloServices.GetRequiredService<IOptionsMonitor<LatticeOptions>>();
        var prefix = ILeafCursorReporter.MaterialiserConsumerIdPrefix + physicalTreeId + "_";
        var registered = await SiloServices.GetRequiredService<IWalCursorRegistry>().SnapshotAsync(physicalTreeId);
        var count = registered.Count(c => c.ConsumerId.StartsWith(prefix, StringComparison.Ordinal));
        foreach (var key in WalMaterialiserPinRouting.EnumerateReadKeys(
                     physicalTreeId, WalMaterialiserPinRouting.ResolveShardCount(options)))
        {
            var pins = await _cluster.GrainFactory.GetGrain<IWalMaterialiserPinGrain>(key).GetPinsAsync();
            count += pins.Keys.Count(id => id.StartsWith(prefix, StringComparison.Ordinal));
        }

        return count;
    }

    private static async Task<long> CountRetainedWalEntriesAsync(string physicalTreeId)
    {
        var resolver = SiloServices.GetRequiredService<LatticeOptionsResolver>();
        var partitions = await resolver.GetWalPartitionsAsync(physicalTreeId);
        long entries = 0;
        for (var partition = 0; partition < partitions; partition++)
        {
            var (provider, _, _) = await resolver.GetWalProviderAsync(physicalTreeId, partition);
            var lowest = await provider.GetLowestOffsetAsync(physicalTreeId, partition, CancellationToken.None);
            if (lowest < 0) continue;
            var highest = await provider.GetHighestOffsetAsync(physicalTreeId, partition, CancellationToken.None);
            entries += highest - lowest + 1;
        }

        return entries;
    }

    private static IServiceProvider SiloServices =>
        SiloServicesCapture.Captured ?? throw new InvalidOperationException("The silo service provider was not captured.");

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));

            // The durable-pin-aware reporter, as a durable-WAL host registers
            // it: without it leaves keep no durable materialiser pins at all.
            siloBuilder.AddWalCursorRegistry();
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<SiloServicesCapture>();
            siloBuilder.Services.AddHostedService(sp => sp.GetRequiredService<SiloServicesCapture>());
        }
    }

    private sealed class SiloServicesCapture(IServiceProvider services) : IHostedService
    {
        public static IServiceProvider? Captured { get; private set; }

        public static void Reset() => Captured = null;

        public Task StartAsync(CancellationToken cancellationToken)
        {
            Captured = services;
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
