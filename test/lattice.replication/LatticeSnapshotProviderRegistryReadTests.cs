using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Registry round trips a snapshot export pays. The open and close generations
/// and the tombstone and prepared passes each derive the routed copy and its
/// shard map from one <see cref="ILatticeRegistry.GetEntryAsync"/> read, because
/// <see cref="ILatticeRegistry.ResolveAsync"/> and
/// <see cref="ILatticeRegistry.GetShardMapAsync"/> are pure projections of it.
/// </summary>
[TestFixture]
public class LatticeSnapshotProviderRegistryReadTests
{
    private const string Tree = "snap-registry-reads";
    private const string Physical = Tree + "-copy";

    [Test]
    public async Task Export_derives_routing_and_shard_map_from_the_registry_entry()
    {
        var factory = Substitute.For<IGrainFactory>();
        var cursors = Substitute.For<IWalCursorRegistry>();
        cursors.GetCausalStableAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<VersionVector?>(new VersionVector()));

        var lattice = Substitute.For<ILattice>();
        lattice.EntriesAsync(
            Arg.Any<string?>(),
            Arg.Any<string?>(),
            Arg.Any<bool>(),
            Arg.Any<bool?>(),
            Arg.Any<CancellationToken>()).Returns(_ => Empty());
        factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(lattice);

        var map = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, 2);
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Tree).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { PhysicalTreeId = Physical, ShardCount = 2, ShardMap = map }));
        registry.ResolveAsync(Tree).Returns(Task.FromResult(Physical));
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var provider = new LatticeSnapshotProvider(factory, cursors, LatticeSnapshotProviderUnitTests.TestOptions());
        var stream = await provider.ExportAsync(Tree, HybridLogicalClock.Zero);
        await foreach (var _ in stream.Entries)
        {
        }

        Assert.Multiple(() =>
        {
            Assert.That(stream.OpenGeneration!.Value.PhysicalTreeId, Is.EqualTo(Physical));
            Assert.That(stream.OpenGeneration.Value.ShardMapVersion, Is.EqualTo(map.Version));
            Assert.That(stream.CloseGeneration!.Value.PhysicalTreeId, Is.EqualTo(Physical));
        });

        await registry.DidNotReceive().GetShardMapAsync(Arg.Any<string>());

        // The two remaining resolves are deliberate time-of-use reads: the
        // enumeration's snap0 routing when the drain starts, and its close-time
        // re-resolve that detects an alias move during the export.
        await registry.Received(2).ResolveAsync(Tree);
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Empty()
    {
        await Task.CompletedTask;
        yield break;
    }
}
