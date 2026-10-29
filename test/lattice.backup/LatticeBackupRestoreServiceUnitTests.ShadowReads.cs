using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Backup.Tests;

public sealed partial class LatticeBackupRestoreServiceUnitTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task DeleteShadow_reads_existence_and_provenance_once_before_any_purge(bool stamped)
    {
        var (service, registry, gate) = CreateShadowDeleteService(
            new TreeRegistryEntry { RestoreShadowOfTreeId = stamped ? "orders" : null });

        if (stamped)
        {
            gate.AuthorizeAsync(Arg.Any<LatticeAccessRequest>(), Arg.Any<CancellationToken>())
                .Returns(new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Deny("test denial")));
            Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => service.DeleteShadowAsync("shadow"));
            await gate.Received(1).AuthorizeAsync(
                Arg.Is<LatticeAccessRequest>(r => r.TreeId == "orders"), Arg.Any<CancellationToken>());
        }
        else
        {
            Assert.ThrowsAsync<LatticeRestoreValidationException>(() => service.DeleteShadowAsync("shadow"));
            await gate.DidNotReceive().AuthorizeAsync(Arg.Any<LatticeAccessRequest>(), Arg.Any<CancellationToken>());
        }

        await registry.Received(1).GetEntryAsync("shadow");
        await registry.DidNotReceive().ExistsAsync(Arg.Any<string>());
    }

    [Test]
    public async Task DeleteShadow_missing_entry_is_an_idempotent_single_read()
    {
        var (service, registry, gate) = CreateShadowDeleteService(null);

        await service.DeleteShadowAsync("shadow");

        await registry.Received(1).GetEntryAsync("shadow");
        await registry.DidNotReceive().ExistsAsync(Arg.Any<string>());
        await gate.DidNotReceive().AuthorizeAsync(Arg.Any<LatticeAccessRequest>(), Arg.Any<CancellationToken>());
    }

    private (LatticeBackupRestoreService Service, ILatticeRegistry Registry, ILatticeAccessGate Gate)
        CreateShadowDeleteService(TreeRegistryEntry? entry)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync("shadow").Returns(Task.FromResult(entry));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var gate = Substitute.For<ILatticeAccessGate>();
        var service = new LatticeBackupRestoreService(
            factory, Substitute.For<ILatticeBackupSink>(), Substitute.For<ILatticeBackupCatalogStore>(),
            new BackupAccessAuthorizer(gate), _serializer, Substitute.For<ITagIndexReconcileTrigger>(),
            Substitute.For<IServiceProvider>(), Substitute.For<ILatticeBackupTenantScope>(),
            NullLogger<LatticeBackupRestoreService>.Instance);
        return (service, registry, gate);
    }
}
