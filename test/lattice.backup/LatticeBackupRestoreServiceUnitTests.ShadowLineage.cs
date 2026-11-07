using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Backup.Tests;

public sealed partial class LatticeBackupRestoreServiceUnitTests
{
    /// <summary>
    /// Issue #5813, S3: the orphan-shadow GC purges every shard before it
    /// unregisters the shadow, so for the whole fan-out the purged copy is still
    /// registered under the lineage its builder stamped onto its bulk loads. The GC
    /// rotates that lineage before the first purge, so a stale load of the same
    /// registration landing in the window is refused rather than re-seeding a shard
    /// the unregister then strands.
    /// </summary>
    [Test]
    public async Task DeleteShadow_rotates_the_shadow_lineage_before_purging_any_shard()
    {
        var built = Guid.NewGuid();
        var entry = new TreeRegistryEntry
        {
            RestoreShadowOfTreeId = "orders",
            DerivedFrom = "orders",
            Lineage = built,
        };
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync("shadow").Returns(Task.FromResult<TreeRegistryEntry?>(entry));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var lattice = Substitute.For<ILattice>();
        var routing = new RoutingInfo("shadow", ShardMap.CreateDefault(64, 1));
        lattice.GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RoutingInfo>(routing));
        factory.GetGrain<ILattice>("shadow").Returns(lattice);
        var shard = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>("shadow/0").Returns(shard);
        var gate = Substitute.For<ILatticeAccessGate>();
        gate.AuthorizeAsync(Arg.Any<LatticeAccessRequest>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Allow()));
        var service = new LatticeBackupRestoreService(
            factory, Substitute.For<ILatticeBackupSink>(), Substitute.For<ILatticeBackupCatalogStore>(),
            new BackupAccessAuthorizer(gate), _serializer, Substitute.For<ITagIndexReconcileTrigger>(),
            Substitute.For<IServiceProvider>(), Substitute.For<ILatticeBackupTenantScope>(),
            NullLogger<LatticeBackupRestoreService>.Instance);

        await service.DeleteShadowAsync("shadow");

        Received.InOrder(() =>
        {
            registry.UpdateAsync("shadow", Arg.Is<TreeRegistryEntry>(e =>
                e.Lineage != null
                && e.Lineage != built
                && e.RestoreShadowOfTreeId == "orders"
                && e.DerivedFrom == "orders"));
            shard.PurgeAsync();
            registry.UnregisterAsync("shadow");
        });
    }
}
