using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Backup.AzureBlob.Tests;

/// <summary>
/// Resolves the <see cref="ILatticeBackupSink"/> that
/// <see cref="LatticeBackupAzureBlobServiceCollectionExtensions.AddLatticeBackupAzureBlob"/>
/// registers, rather than inspecting the descriptor it produced.
/// <para>
/// <see cref="LatticeBackupAzureBlobServiceCollectionExtensionsTests"/> deliberately
/// stops at descriptor shape, so the factory body itself - reading the bound options,
/// building the container client from them, and pairing it with the manifest
/// serializer - was never executed by any test. A registration that compiles and
/// registers correctly can still fail on first resolution: a missing options
/// registration, a mis-ordered <c>Configure</c>, or a serializer the silo does not
/// supply all surface only when something asks for the sink. This fixture asks.
/// </para>
/// <para>
/// No storage is touched: the Azure SDK builds a container client lazily and issues
/// no request until an operation is invoked, so this stays a pure-unit fixture.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeBackupAzureBlobSinkResolutionTests
{
    private const string DevConnectionString = "UseDevelopmentStorage=true";

    private static ServiceProvider BuildProvider(Action<LatticeBackupAzureBlobOptions> configure)
    {
        var services = new ServiceCollection()
            .AddSerializer(b => b.AddAssembly(typeof(BackupManifest).Assembly));
        new StubSiloBuilder(services).AddLatticeBackupAzureBlob(configure);
        return services.BuildServiceProvider();
    }

    [Test]
    public void The_registered_factory_resolves_the_azure_blob_sink()
    {
        using var provider = BuildProvider(o => o.ConnectionString = DevConnectionString);

        var sink = provider.GetRequiredService<ILatticeBackupSink>();

        Assert.Multiple(() =>
        {
            Assert.That(sink, Is.InstanceOf<AzureBlobLatticeBackupSink>());
            Assert.That(sink.IsDurable, Is.True,
                "The off-cluster sink must report itself durable, since a restore may need "
                + "to outlive the capturing cluster.");
        });
    }

    [Test]
    public void The_registered_factory_produces_a_singleton()
    {
        using var provider = BuildProvider(o => o.ConnectionString = DevConnectionString);

        Assert.That(
            provider.GetRequiredService<ILatticeBackupSink>(),
            Is.SameAs(provider.GetRequiredService<ILatticeBackupSink>()));
    }

    [Test]
    public void The_registered_factory_honours_the_configured_container_name()
    {
        // The factory must build its client from the bound options, not from the
        // package defaults; the resolved sink's container is the proof.
        using var provider = BuildProvider(o =>
        {
            o.ConnectionString = DevConnectionString;
            o.ContainerName = "configured-container";
        });

        var sink = (AzureBlobLatticeBackupSink)provider.GetRequiredService<ILatticeBackupSink>();

        Assert.That(ContainerNameOf(sink), Is.EqualTo("configured-container"));
    }

    [Test]
    public void The_registered_factory_resolves_after_a_prior_default_sink_was_registered()
    {
        // AddLatticeBackup may have run first and installed the in-cluster default.
        // The replacement must still resolve to the Azure Blob sink, not leave a
        // half-replaced registration that resolves the wrong implementation.
        var services = new ServiceCollection()
            .AddSerializer(b => b.AddAssembly(typeof(BackupManifest).Assembly));
        services.AddSingleton<ILatticeBackupSink>(_ => throw new InvalidOperationException(
            "The default sink factory must have been replaced, so it must never be invoked."));
        new StubSiloBuilder(services).AddLatticeBackupAzureBlob(o => o.ConnectionString = DevConnectionString);

        using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<ILatticeBackupSink>(), Is.InstanceOf<AzureBlobLatticeBackupSink>());
    }

    [Test]
    public void Resolving_the_sink_throws_when_no_authentication_mode_is_configured()
    {
        // The options are validated when the container client is built, which happens
        // inside the factory - so a misconfigured host fails at resolution with a
        // diagnosable message rather than on its first backup.
        using var provider = BuildProvider(_ => { });

        Assert.That(
            () => provider.GetRequiredService<ILatticeBackupSink>(),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Resolving_the_sink_throws_when_the_manifest_serializer_is_absent()
    {
        // The factory takes Serializer<BackupManifest> as a required service. Without
        // Orleans serialization registered, resolution must fail loudly rather than
        // hand back a sink that cannot round-trip a manifest.
        var services = new ServiceCollection();
        new StubSiloBuilder(services).AddLatticeBackupAzureBlob(o => o.ConnectionString = DevConnectionString);
        using var provider = services.BuildServiceProvider();

        Assert.That(
            () => provider.GetRequiredService<ILatticeBackupSink>(),
            Throws.InstanceOf<InvalidOperationException>());
    }

    /// <summary>
    /// Reads the container name off the sink's blob client. Asserting on the client
    /// the factory actually built is what distinguishes "the options were bound" from
    /// "the options were used".
    /// </summary>
    private static string ContainerNameOf(AzureBlobLatticeBackupSink sink)
    {
        var field = typeof(AzureBlobLatticeBackupSink)
            .GetField("_container", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert.That(field, Is.Not.Null, "The sink no longer holds its container client in '_container'.");
        var container = (Azure.Storage.Blobs.BlobContainerClient)field!.GetValue(sink)!;
        return container.Name;
    }

    private sealed class StubSiloBuilder(IServiceCollection services) : ISiloBuilder
    {
        public IServiceCollection Services { get; } = services;

        public Microsoft.Extensions.Configuration.IConfiguration Configuration { get; }
            = new Microsoft.Extensions.Configuration.ConfigurationBuilder().Build();
    }
}
