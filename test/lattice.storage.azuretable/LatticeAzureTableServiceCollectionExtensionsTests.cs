using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Unit tests for
/// <see cref="LatticeAzureTableServiceCollectionExtensions"/>.
/// Covers null-argument guards on the public extension and the
/// order-independent registration contract inherited from
/// <see cref="LatticeServiceCollectionExtensions.AddWalStorage"/> -
/// specifically, that the Azure factory wins regardless of whether the
/// in-memory baseline (installed by <c>AddLattice</c> via the no-factory
/// <c>AddWalStorage</c> overload) is registered before or after it.
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
    public void AddAzureTableWalStorage_replaces_prior_in_memory_baseline()
    {
        var services = new ServiceCollection();
        var siloBuilder = new StubSiloBuilder(services);

        // The exact shape of the bug this fixture exists to pin:
        // AddLattice installs the in-memory baseline via its internal
        // AddWalStorage() call, then the host calls
        // AddAzureTableWalStorage. Under the historical TryAddSingleton
        // path the Azure factory was silently dropped; under the
        // Services.Replace path the factory wins regardless of order.
        siloBuilder.AddWalStorage(); // baseline (mimics what AddLattice does)
        siloBuilder.AddAzureTableWalStorage(o => o.ConnectionString = "UseDevelopmentStorage=true");

        var providers = services.Where(d => d.ServiceType == typeof(IWalStorageProvider)).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(providers, Has.Count.EqualTo(1),
                "AddAzureTableWalStorage must Replace the baseline, not stack a second descriptor.");
            Assert.That(providers[0].ImplementationFactory, Is.Not.Null,
                "The remaining descriptor must be the Azure factory, not the in-memory baseline (which has an ImplementationType).");
            Assert.That(providers[0].ImplementationType, Is.Null);
        });
    }

    [Test]
    public void AddAzureTableWalStorage_wins_when_called_before_baseline()
    {
        // Symmetry: the host calls AddAzureTableWalStorage first and
        // then a subsequent (no-factory) AddWalStorage() must not
        // displace the factory.
        var services = new ServiceCollection();
        var siloBuilder = new StubSiloBuilder(services);

        siloBuilder.AddAzureTableWalStorage(o => o.ConnectionString = "UseDevelopmentStorage=true");
        siloBuilder.AddWalStorage(); // baseline must no-op

        var providers = services.Where(d => d.ServiceType == typeof(IWalStorageProvider)).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(providers, Has.Count.EqualTo(1));
            Assert.That(providers[0].ImplementationFactory, Is.Not.Null);
            Assert.That(providers[0].ImplementationType, Is.Null);
        });
    }

    [Test]
    public void AddAzureTableWalStorage_registers_a_single_Zstd_compressor_fallback()
    {
        var services = new ServiceCollection();
        var siloBuilder = new StubSiloBuilder(services);

        siloBuilder.AddAzureTableWalStorage(o => o.ConnectionString = "UseDevelopmentStorage=true");

        var compressorDescriptors = services
            .Where(d => d.ServiceType == typeof(ILatticeCompressor))
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(compressorDescriptors, Has.Count.EqualTo(1),
                "AddAzureTableWalStorage must register exactly one ILatticeCompressor fallback.");
            Assert.That(compressorDescriptors[0].Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
        });
    }

    [Test]
    public void AddAzureTableWalStorage_does_not_duplicate_a_preregistered_compressor()
    {
        // TryAddEnumerable deduplicates by (ServiceType, ImplementationType),
        // so a host that pre-registers its own ZstdLatticeCompressor at a
        // custom level keeps exactly that instance - the extension's
        // fallback is a no-op.
        var services = new ServiceCollection();
        var siloBuilder = new StubSiloBuilder(services);

        services.AddLatticeCompressor(new ZstdLatticeCompressor(9));
        siloBuilder.AddAzureTableWalStorage(o => o.ConnectionString = "UseDevelopmentStorage=true");

        var compressorDescriptors = services
            .Where(d => d.ServiceType == typeof(ILatticeCompressor))
            .ToList();

        Assert.That(compressorDescriptors, Has.Count.EqualTo(1),
            "A pre-registered ZstdLatticeCompressor must not be duplicated by the extension fallback.");
    }

    [Test]
    public void DefaultCompressionLevel_is_three()
    {
        Assert.That(LatticeAzureTableServiceCollectionExtensions.DefaultCompressionLevel, Is.EqualTo(3));
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
