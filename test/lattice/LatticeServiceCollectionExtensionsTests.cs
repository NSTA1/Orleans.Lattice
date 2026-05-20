using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests;

public class LatticeServiceCollectionExtensionsTests
{
    [Test]
    public void AddLattice_invokes_delegate_with_builder_and_storage_provider_name()
    {
        var builder = Substitute.For<ISiloBuilder>();
        string? capturedName = null;
        ISiloBuilder? capturedBuilder = null;

        builder.AddLattice((b, name) =>
        {
            capturedBuilder = b;
            capturedName = name;
        });

        Assert.That(capturedBuilder, Is.SameAs(builder));
        Assert.That(capturedName, Is.EqualTo(LatticeOptions.StorageProviderName));
    }

    [Test]
    public void AddLattice_returns_builder_for_fluent_chaining()
    {
        var builder = Substitute.For<ISiloBuilder>();

        var result = builder.AddLattice((_, _) => { });

        Assert.That(result, Is.SameAs(builder));
    }

    [Test]
    public void AddWalStorage_throws_on_null_builder()
    {
        Assert.That(
            () => LatticeServiceCollectionExtensions.AddWalStorage(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddWalStorage_returns_builder_for_fluent_chaining()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        var result = builder.AddWalStorage();

        Assert.That(result, Is.SameAs(builder));
    }

    [Test]
    public void AddWalStorage_without_factory_registers_in_memory_provider()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddWalStorage();

        var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IWalStorageProvider>();
        Assert.That(resolved, Is.InstanceOf<InMemoryWalStorageProvider>());
    }

    [Test]
    public void AddWalStorage_with_factory_registers_supplied_factory_result()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        var sentinel = new InMemoryWalStorageProvider();
        builder.AddWalStorage(_ => sentinel);

        var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IWalStorageProvider>();
        Assert.That(resolved, Is.SameAs(sentinel));
    }

    [Test]
    public void AddWalStorage_no_factory_is_idempotent_first_baseline_wins()
    {
        // The no-factory overload installs the in-memory baseline via
        // TryAddSingleton. First call wins; a second baseline call is
        // a no-op. This is the contract AddLattice relies on when it
        // self-installs the baseline at the top of its own setup.
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddWalStorage();
        builder.AddWalStorage();

        Assert.That(
            services.Count(d => d.ServiceType == typeof(IWalStorageProvider)),
            Is.EqualTo(1));
    }

    [Test]
    public void AddWalStorage_factory_after_baseline_replaces_in_memory_default()
    {
        // Regression: under the old TryAddSingleton path the factory was
        // silently dropped because AddLattice (or any earlier
        // AddWalStorage() call) had already installed the baseline.
        // Under the Replace contract the factory wins regardless of
        // order; this is the contract that makes
        // `siloBuilder.AddLattice(...).AddAzureTableWalStorage(...)`
        // work as a host expects.
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddWalStorage(); // baseline (TryAdd)
        var sentinel = new InMemoryWalStorageProvider();
        builder.AddWalStorage(_ => sentinel); // factory must replace baseline

        var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IWalStorageProvider>();
        Assert.Multiple(() =>
        {
            Assert.That(resolved, Is.SameAs(sentinel));
            Assert.That(
                services.Count(d => d.ServiceType == typeof(IWalStorageProvider)),
                Is.EqualTo(1),
                "Replace must produce a single descriptor, not stack a second one.");
        });
    }

    [Test]
    public void AddWalStorage_baseline_after_factory_does_not_displace_factory()
    {
        // TryAddSingleton on the baseline must not displace a previously
        // registered factory. This is the symmetry case: a host that
        // calls AddWalStorage(factory) before AddLattice still keeps
        // the factory because AddLattice's internal AddWalStorage() is
        // a TryAdd no-op.
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        var sentinel = new InMemoryWalStorageProvider();
        builder.AddWalStorage(_ => sentinel);
        builder.AddWalStorage(); // baseline must no-op

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IWalStorageProvider>(), Is.SameAs(sentinel));
    }

    [Test]
    public void AddWalStorage_factory_is_last_call_wins()
    {
        // Two host-supplied factories: the second replaces the first.
        // This is the contract package-level overloads like
        // AddAzureTableWalStorage rely on when a host accidentally
        // configures the package twice.
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        var first = new InMemoryWalStorageProvider();
        var second = new InMemoryWalStorageProvider();
        builder.AddWalStorage(_ => first);
        builder.AddWalStorage(_ => second);

        var provider = services.BuildServiceProvider();
        Assert.Multiple(() =>
        {
            Assert.That(provider.GetRequiredService<IWalStorageProvider>(), Is.SameAs(second));
            Assert.That(
                services.Count(d => d.ServiceType == typeof(IWalStorageProvider)),
                Is.EqualTo(1),
                "Replace must not stack descriptors.");
        });
    }
}
