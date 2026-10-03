using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Routing reads a tree's physical id and shard map together, from one
/// <see cref="ILatticeRegistry.GetEntryAsync"/> read (issue #4336). Unit tests
/// that drive routing through a substitute registry configure the alias and the
/// map through <see cref="ILatticeRegistry.ResolveAsync"/> and
/// <see cref="ILatticeRegistry.GetShardMapAsync"/>; this bridge makes the
/// substitute's entry read answer from those stubs, so a test keeps expressing
/// an alias and a map the way it always has and keeps counting those calls.
/// </summary>
internal static class SubstituteRegistryRouting
{
    /// <summary>
    /// Makes <paramref name="registry"/>'s <see cref="ILatticeRegistry.GetEntryAsync"/>
    /// return <paramref name="baseEntry"/> for the requested tree (an empty entry when
    /// it returns <see langword="null"/>) with its alias and map taken from the
    /// substitute's own <see cref="ILatticeRegistry.ResolveAsync"/> and
    /// <see cref="ILatticeRegistry.GetShardMapAsync"/> answers.
    /// </summary>
    public static ILatticeRegistry RouteEntriesThroughAliasAndMapStubs(
        this ILatticeRegistry registry,
        Func<string, TreeRegistryEntry?>? baseEntry = null)
    {
        registry.GetEntryAsync(Arg.Any<string>()).Returns(async call =>
        {
            var treeId = call.Arg<string>();
            var physical = await registry.ResolveAsync(treeId);
            var map = await registry.GetShardMapAsync(treeId);
            var entry = baseEntry?.Invoke(treeId) ?? new TreeRegistryEntry();
            return (TreeRegistryEntry?)(entry with
            {
                PhysicalTreeId = string.IsNullOrEmpty(physical) || physical == treeId ? entry.PhysicalTreeId : physical,
                ShardMap = map ?? entry.ShardMap,
            });
        });
        return registry;
    }
}
