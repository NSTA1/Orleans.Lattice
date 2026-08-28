using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Covers <see cref="GrainFactoryTreeContentProbe"/>, the default
/// <see cref="ILatticeTreeContentProbe"/> that decides whether a tree holds any
/// data, under the system origin.
/// <para>
/// The probe answers existence by taking the first key from the tree's key
/// stream rather than counting: <c>ILattice.CountAsync</c> is a
/// strongly-consistent whole-tree fan-out that walks every leaf chain and
/// restarts whenever the shard map moves under it, so reducing it to a boolean
/// wasted almost all of that work. The short-circuit is asserted explicitly
/// below, because losing it would silently reintroduce the cost.
/// </para>
/// </summary>
[TestFixture]
public class GrainFactoryTreeContentProbeTests
{
    // Yields keys one at a time, recording how many the consumer actually
    // pulled, so the short-circuit can be asserted rather than assumed.
    private sealed class CountingKeyStream(int available)
    {
        public int Yielded { get; private set; }

        public async IAsyncEnumerable<string> EnumerateAsync()
        {
            for (var i = 0; i < available; i++)
            {
                Yielded++;
                yield return $"k{i}";
                await Task.Yield();
            }
        }
    }

    private static (GrainFactoryTreeContentProbe Probe, CountingKeyStream Stream) CreateProbe(int availableKeys)
    {
        var stream = new CountingKeyStream(availableKeys);
        var tree = Substitute.For<ILattice>();
        tree.KeysAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(),
                Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => stream.EnumerateAsync());

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>("tree-1").Returns(tree);
        return (new GrainFactoryTreeContentProbe(grainFactory), stream);
    }

    [Test]
    public void Constructor_throws_on_null_grainFactory()
    {
        Assert.That(
            () => new GrainFactoryTreeContentProbe(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task HasContentAsync_returns_true_for_a_tree_holding_entries()
    {
        var (probe, _) = CreateProbe(availableKeys: 5);

        Assert.That(await probe.HasContentAsync("tree-1", CancellationToken.None), Is.True);
    }

    [Test]
    public async Task HasContentAsync_returns_false_for_an_empty_tree()
    {
        var (probe, _) = CreateProbe(availableKeys: 0);

        Assert.That(await probe.HasContentAsync("tree-1", CancellationToken.None), Is.False);
    }

    [Test]
    public async Task HasContentAsync_stops_at_the_first_key_instead_of_draining_the_tree()
    {
        // The whole point of the seam returning a boolean: existence must cost
        // one row, not a walk of every row in the tree.
        var (probe, stream) = CreateProbe(availableKeys: 1000);

        await probe.HasContentAsync("tree-1", CancellationToken.None);

        Assert.That(stream.Yielded, Is.EqualTo(1),
            "the probe must abandon the key stream after the first row");
    }

    [Test]
    public void HasContentAsync_throws_on_empty_treeId()
    {
        var probe = new GrainFactoryTreeContentProbe(Substitute.For<IGrainFactory>());

        Assert.That(
            async () => await probe.HasContentAsync(string.Empty, CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public void HasContentAsync_throws_on_null_treeId()
    {
        var probe = new GrainFactoryTreeContentProbe(Substitute.For<IGrainFactory>());

        Assert.That(
            async () => await probe.HasContentAsync(null!, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());
    }
}
