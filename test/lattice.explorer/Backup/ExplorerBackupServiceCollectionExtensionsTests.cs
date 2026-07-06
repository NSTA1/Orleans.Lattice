using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
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

    [Test]
    public async Task AddExplorerBackup_control_client_owns_orleans_serializer()
    {
        // Regression: the control client must build its own Orleans serializer
        // provider. If it captured the application root provider (which has no
        // AddSerializer), resolving its per-message serializers throws
        // InvalidOperationException before any network call, and the Backups area
        // silently greys out. With a real serializer the call instead proceeds to
        // the transport and fails to reach the dead endpoint with an RpcException.
        var session = Substitute.For<IExplorerSession>();
        session.Current.Returns(new ExplorerConfiguration
        {
            Endpoint = "http://127.0.0.1:1",
            AllowUnencryptedHttp2 = true,
        });
        var auth = Substitute.For<IExplorerAuthSession>();

        var services = new ServiceCollection();
        services.AddSingleton(session);
        services.AddSingleton(auth);
        services.AddExplorerBackup();
        await using var provider = services.BuildServiceProvider();

        var client = provider.GetRequiredService<IBackupControlClient>();

        Assert.That(
            async () => await client.ListBackupsAsync(new BackupCatalogRequest { PageSize = 1 }),
            Throws.InstanceOf<RpcException>());
    }
}
