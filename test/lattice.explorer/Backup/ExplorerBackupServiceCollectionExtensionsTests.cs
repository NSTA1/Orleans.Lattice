using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Backup;

[TestFixture]
public class ExplorerBackupServiceCollectionExtensionsTests
{
    [Test]
    public void AddExplorerBackup_null_services_throws()
    {
        Assert.That(() => ((IServiceCollection)null!).AddExplorerBackup(), Throws.ArgumentNullException);
    }

    [Test]
    public void AddExplorerBackup_registers_navigation_store_and_backup_services()
    {
        var services = new ServiceCollection();

        services.AddExplorerBackup();

        Assert.Multiple(() =>
        {
            Assert.That(services.Any(d => d.ServiceType == typeof(IExplorerCapabilityStore)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(IBackupControlClient)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(IBackupCatalogReader)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(IBackupCapabilityService)), Is.True);
        });
    }

    [Test]
    public async Task AddExplorerBackup_capability_store_resolves()
    {
        var services = new ServiceCollection();
        services.AddExplorerBackup();
        await using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<IExplorerCapabilityStore>(), Is.InstanceOf<ExplorerCapabilityStore>());
    }

    [Test]
    public async Task AddExplorerBackup_reader_resolves_over_a_fake_client()
    {
        var services = new ServiceCollection();
        services.AddExplorerBackup();
        services.AddSingleton<IBackupControlClient, FakeBackupControlClient>();
        await using var provider = services.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetRequiredService<IBackupCatalogReader>(), Is.InstanceOf<BackupCatalogReader>());
            Assert.That(provider.GetRequiredService<IBackupCapabilityService>(), Is.InstanceOf<BackupCapabilityService>());
        });
    }
}
