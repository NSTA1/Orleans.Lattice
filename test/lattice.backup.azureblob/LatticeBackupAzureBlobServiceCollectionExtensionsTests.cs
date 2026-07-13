using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Backup.AzureBlob.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeBackupAzureBlobServiceCollectionExtensions"/>.
/// Cover the null-argument guards, options binding, and the registration
/// contract that the Azure Blob sink replaces the in-cluster default
/// <see cref="ILatticeBackupSink"/> regardless of registration order. The sink
/// factory is asserted by descriptor shape rather than resolved, since resolving
/// it needs the full Orleans serializer graph.
/// </summary>
[TestFixture]
public class LatticeBackupAzureBlobServiceCollectionExtensionsTests
{
    private const string DevConnectionString = "UseDevelopmentStorage=true";

    [Test]
    public void AddLatticeBackupAzureBlob_throws_on_null_builder()
    {
        Assert.That(
            () => ((ISiloBuilder)null!).AddLatticeBackupAzureBlob(_ => { }),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeBackupAzureBlob_throws_on_null_configure()
    {
        var siloBuilder = new StubSiloBuilder(new ServiceCollection());

        Assert.That(
            () => siloBuilder.AddLatticeBackupAzureBlob(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeBackupAzureBlob_binds_options_via_IOptions()
    {
        var services = new ServiceCollection();
        var siloBuilder = new StubSiloBuilder(services);

        siloBuilder.AddLatticeBackupAzureBlob(o =>
        {
            o.ConnectionString = DevConnectionString;
            o.ContainerName = "my-backup-container";
        });

        var sp = services.BuildServiceProvider();
        var bound = sp.GetRequiredService<IOptions<LatticeBackupAzureBlobOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(bound.ConnectionString, Is.EqualTo(DevConnectionString));
            Assert.That(bound.ContainerName, Is.EqualTo("my-backup-container"));
        });
    }

    [Test]
    public void AddLatticeBackupAzureBlob_registers_a_single_sink_factory()
    {
        var services = new ServiceCollection();
        var siloBuilder = new StubSiloBuilder(services);

        siloBuilder.AddLatticeBackupAzureBlob(o => o.ConnectionString = DevConnectionString);

        var descriptors = services.Where(d => d.ServiceType == typeof(ILatticeBackupSink)).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(descriptors, Has.Count.EqualTo(1));
            Assert.That(descriptors[0].Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
            Assert.That(descriptors[0].ImplementationFactory, Is.Not.Null);
        });
    }

    [Test]
    public void AddLatticeBackupAzureBlob_replaces_a_prior_default_sink()
    {
        var services = new ServiceCollection();
        var siloBuilder = new StubSiloBuilder(services);

        // Mimic the in-cluster default sink that AddLatticeBackup installs.
        services.TryAddSingleton<ILatticeBackupSink, StubBackupSink>();
        siloBuilder.AddLatticeBackupAzureBlob(o => o.ConnectionString = DevConnectionString);

        var descriptors = services.Where(d => d.ServiceType == typeof(ILatticeBackupSink)).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(descriptors, Has.Count.EqualTo(1),
                "AddLatticeBackupAzureBlob must Replace the default sink, not stack a second descriptor.");
            Assert.That(descriptors[0].ImplementationFactory, Is.Not.Null,
                "The remaining descriptor must be the Azure Blob factory, not the default (which has an ImplementationType).");
            Assert.That(descriptors[0].ImplementationType, Is.Null);
        });
    }

    private sealed class StubBackupSink : ILatticeBackupSink
    {
        public Task WriteArtifactAsync(string artifactId, IAsyncEnumerable<ReadOnlyMemory<byte>> content, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public async IAsyncEnumerable<ReadOnlyMemory<byte>> ReadArtifactAsync(string artifactId, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }

        public Task<bool> DeleteArtifactAsync(string artifactId, CancellationToken cancellationToken = default) => Task.FromResult(false);

        public async IAsyncEnumerable<string> ListArtifactIdsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }

        public Task WriteManifestAsync(BackupManifest manifest, CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task<BackupManifest?> ReadManifestAsync(string backupId, CancellationToken cancellationToken = default) => Task.FromResult<BackupManifest?>(null);

        public async IAsyncEnumerable<BackupManifest> ListManifestsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }

        public Task<bool> DeleteManifestAsync(string backupId, CancellationToken cancellationToken = default) => Task.FromResult(false);

        public Task<bool> ManifestExistsAsync(string backupId, CancellationToken cancellationToken = default) => Task.FromResult(false);

        public Task<BackupSinkResolution> ProbeAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(new BackupSinkResolution(backupId, manifestPresent: false, Array.Empty<string>()));
    }

    private sealed class StubSiloBuilder(IServiceCollection services) : ISiloBuilder
    {
        public IServiceCollection Services { get; } = services;

        public Microsoft.Extensions.Configuration.IConfiguration Configuration { get; }
            = new Microsoft.Extensions.Configuration.ConfigurationBuilder().Build();
    }
}
