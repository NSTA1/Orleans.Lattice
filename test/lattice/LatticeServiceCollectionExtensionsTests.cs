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
}
