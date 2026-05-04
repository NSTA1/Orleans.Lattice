using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Unit tests for
/// <see cref="LatticeAzureTableServiceCollectionExtensions"/>.
/// Covers null-argument guards on the public extension and the
/// idempotency contract inherited from
/// <see cref="LatticeServiceCollectionExtensions.AddWalStorage"/>.
/// </summary>
[TestFixture]
public class LatticeAzureTableServiceCollectionExtensionsTests
{
    [Test]
    public void AddAzureTableWalStorage_throws_on_null_builder()
    {
        Assert.That(
            () => ((ISiloBuilder)null!).AddAzureTableWalStorage(_ => { }),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddAzureTableWalStorage_throws_on_null_configure()
    {
        var siloBuilder = new StubSiloBuilder(new ServiceCollection());

        Assert.That(
            () => siloBuilder.AddAzureTableWalStorage(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddAzureTableWalStorage_binds_options_via_IOptions()
    {
        var services = new ServiceCollection();
        var siloBuilder = new StubSiloBuilder(services);

        siloBuilder.AddAzureTableWalStorage(o =>
        {
            o.ConnectionString = "UseDevelopmentStorage=true";
            o.TableName = "MyCustomWal";
        });

        var sp = services.BuildServiceProvider();
        var bound = sp.GetRequiredService<IOptions<AzureTableWalStorageOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(bound.ConnectionString, Is.EqualTo("UseDevelopmentStorage=true"));
            Assert.That(bound.TableName, Is.EqualTo("MyCustomWal"));
        });
    }

    [Test]
    public void AddAzureTableWalStorage_registers_provider_factory_under_IWalStorageProvider()
    {
        var services = new ServiceCollection();
        var siloBuilder = new StubSiloBuilder(services);

        siloBuilder.AddAzureTableWalStorage(o => o.ConnectionString = "UseDevelopmentStorage=true");

        var descriptor = services.Single(d => d.ServiceType == typeof(IWalStorageProvider));

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
            // The factory points at AzureTableWalStorageProvider; we
            // verify by descriptor rather than resolving (resolution
            // would require the full Orleans serializer graph, which
            // is out of scope for a DI-shape unit test).
            Assert.That(descriptor.ImplementationFactory, Is.Not.Null);
        });
    }

    [Test]
    public void AddAzureTableWalStorage_is_idempotent_against_prior_AddWalStorage_registration()
    {
        var services = new ServiceCollection();
        var siloBuilder = new StubSiloBuilder(services);

        // Pretend the host (or another sibling package) registered an
        // in-memory provider first via the core extension. The Azure
        // overload must not stack a second registration.
        siloBuilder.AddWalStorage();
        siloBuilder.AddAzureTableWalStorage(o => o.ConnectionString = "UseDevelopmentStorage=true");

        var registrations = services.Count(d => d.ServiceType == typeof(IWalStorageProvider));

        // TryAddSingleton on the second call short-circuits, so exactly
        // one IWalStorageProvider descriptor remains - the in-memory
        // one. This is the idempotency contract the core
        // AddWalStorage helper documents.
        Assert.That(registrations, Is.EqualTo(1));
    }

    /// <summary>
    /// Minimal <see cref="ISiloBuilder"/> stub that exposes a
    /// <see cref="IServiceCollection"/> for assertions. The Orleans
    /// extension methods we call (<c>AddWalStorage</c>,
    /// <c>AddAzureTableWalStorage</c>) only touch <c>Services</c>, so
    /// the rest of the interface is left unimplemented.
    /// </summary>
    private sealed class StubSiloBuilder(IServiceCollection services) : ISiloBuilder
    {
        public IServiceCollection Services { get; } = services;
        public Microsoft.Extensions.Configuration.IConfiguration Configuration { get; }
            = new Microsoft.Extensions.Configuration.ConfigurationBuilder().Build();
    }
}
