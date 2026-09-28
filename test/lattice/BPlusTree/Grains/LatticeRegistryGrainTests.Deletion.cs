using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeRegistryGrainTests
{
    [TestCase("logical", false)]
    [TestCase("physical", false)]
    [TestCase("logical", true)]
    public async Task Alias_mutation_refuses_a_deleted_endpoint_before_writing(string endpoint, bool remove)
    {
        var factory = Substitute.For<IGrainFactory>();
        var tree = Substitute.For<ISystemLattice>();
        factory.GetGrain<ISystemLattice>(LatticeConstants.RegistryTreeId).Returns(tree);
        var deleted = Substitute.For<ITreeDeletionGrain>();
        deleted.EnsureAliasWritableAsync().ThrowsAsync(new InvalidOperationException("deleted"));
        factory.GetGrain<ITreeDeletionGrain>(endpoint).Returns(deleted);
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        var registry = new LatticeRegistryGrain(factory, options);

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            remove ? registry.RemoveAliasAsync("logical") : registry.SetAliasAsync("logical", "physical"));

        await tree.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }
}
