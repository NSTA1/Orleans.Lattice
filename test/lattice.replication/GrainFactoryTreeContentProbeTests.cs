using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Covers <see cref="GrainFactoryTreeContentProbe"/>, the default
/// <see cref="ILatticeTreeContentProbe"/> that reads a tree's live entry count
/// under the system origin.
/// </summary>
[TestFixture]
public class GrainFactoryTreeContentProbeTests
{
    [Test]
    public void Constructor_throws_on_null_grainFactory()
    {
        Assert.That(
            () => new GrainFactoryTreeContentProbe(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task CountAsync_returns_the_tree_entry_count()
    {
        var tree = Substitute.For<ILattice>();
        tree.CountAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(42));
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>("tree-1").Returns(tree);
        var probe = new GrainFactoryTreeContentProbe(grainFactory);

        var count = await probe.CountAsync("tree-1", CancellationToken.None);

        Assert.That(count, Is.EqualTo(42));
    }

    [Test]
    public void CountAsync_throws_on_empty_treeId()
    {
        var probe = new GrainFactoryTreeContentProbe(Substitute.For<IGrainFactory>());

        Assert.That(
            async () => await probe.CountAsync(string.Empty, CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public void CountAsync_throws_on_null_treeId()
    {
        var probe = new GrainFactoryTreeContentProbe(Substitute.For<IGrainFactory>());

        Assert.That(
            async () => await probe.CountAsync(null!, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());
    }
}
