using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests;

public class LatticeServiceCollectionExtensionsTests
{
    [Fact]
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

        Assert.Same(builder, capturedBuilder);
        Assert.Equal(LatticeOptions.StorageProviderName, capturedName);
    }

    [Fact]
    public void AddLattice_returns_builder_for_fluent_chaining()
    {
        var builder = Substitute.For<ISiloBuilder>();

        var result = builder.AddLattice((_, _) => { });

        Assert.Same(builder, result);
    }
}
