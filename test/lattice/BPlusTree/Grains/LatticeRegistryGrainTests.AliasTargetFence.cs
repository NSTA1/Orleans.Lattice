using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeRegistryGrainTests
{
    // --- Issue #6083: a slot diff for a copy its owner aliases is refused ---

    private const string AliasOwner = "fold-owner";
    private const string AliasCopy = "fold-owner-copy";

    private static void SeedEntry(ISystemLattice tree, string treeId, TreeRegistryEntry entry) =>
        tree.GetAsync(treeId).Returns(Task.FromResult<byte[]?>(
            System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(entry)));

    [Test]
    public async Task ReassignSlotsAsync_fenced_refuses_a_derived_copy_its_owner_aliases()
    {
        var (grain, tree) = CreateGrain();
        SeedEntry(tree, AliasCopy, new TreeRegistryEntry
        {
            ShardCount = 4,
            DerivedFrom = AliasOwner,
            ShardMap = ShardMap.CreateDefault(8, 4),
        });
        SeedEntry(tree, AliasOwner, new TreeRegistryEntry { ShardCount = 4, PhysicalTreeId = AliasCopy });

        var result = await grain.ReassignSlotsAsync(AliasCopy, [0], 1, ShardMap.CreateDefault(8, 4), AliasCopy);

        Assert.That(result, Is.Null);
        await tree.DidNotReceive().SetAsync(AliasCopy, Arg.Any<byte[]>());
    }

    [Test]
    public async Task ReassignSlotsAsync_fenced_admits_a_derived_copy_its_owner_no_longer_aliases()
    {
        var (grain, tree) = CreateGrain();
        SeedEntry(tree, AliasCopy, new TreeRegistryEntry
        {
            ShardCount = 4,
            DerivedFrom = AliasOwner,
            ShardMap = ShardMap.CreateDefault(8, 4),
        });
        SeedEntry(tree, AliasOwner, new TreeRegistryEntry { ShardCount = 4, PhysicalTreeId = AliasCopy + "-newer" });

        var result = await grain.ReassignSlotsAsync(AliasCopy, [0], 1, ShardMap.CreateDefault(8, 4), AliasCopy);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.Slots[0], Is.EqualTo(1));
    }
}
