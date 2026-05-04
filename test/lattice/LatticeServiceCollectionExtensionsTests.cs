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
    public void AddWalStorage_is_idempotent_first_registration_wins()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        var first = new InMemoryWalStorageProvider();
        var second = new InMemoryWalStorageProvider();
        builder.AddWalStorage(_ => first);
        builder.AddWalStorage(_ => second);

        var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IWalStorageProvider>();
        Assert.That(resolved, Is.SameAs(first));
    }
}
