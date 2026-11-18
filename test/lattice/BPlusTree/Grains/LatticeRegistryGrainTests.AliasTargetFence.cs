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

    // --- Issue #6089: the derived copy's own row follows the logical row ---

    private static TreeRegistryEntry Captured(ISystemLattice tree, string treeId)
    {
        var call = tree.ReceivedCalls().Last(c => c.GetMethodInfo().Name == nameof(ISystemLattice.SetAsync)
            && (string)c.GetArguments()[0]! == treeId);
        return System.Text.Json.JsonSerializer.Deserialize<TreeRegistryEntry>((byte[])call.GetArguments()[1]!)!;
    }

    private static void SeedAliasedDerivedCopy(ISystemLattice tree, ShardMap logicalMap, ShardMap copyMap, string? derivedFrom = AliasOwner)
    {
        SeedEntry(tree, AliasOwner, new TreeRegistryEntry { ShardCount = 4, PhysicalTreeId = AliasCopy, ShardMap = logicalMap });
        SeedEntry(tree, AliasCopy, new TreeRegistryEntry { ShardCount = 4, DerivedFrom = derivedFrom, ShardMap = copyMap });
    }

    [Test]
    public async Task ReassignSlotsAsync_fenced_mirrors_the_diff_onto_the_derived_copy_the_logical_row_aliases()
    {
        var (grain, tree) = CreateGrain();
        SeedAliasedDerivedCopy(tree, ShardMap.CreateDefault(8, 4), ShardMap.CreateDefault(8, 4));

        var result = await grain.ReassignSlotsAsync(AliasOwner, [3, 7], 2, ShardMap.CreateDefault(8, 4), AliasCopy);

        Assert.That(result, Is.Not.Null);
        Assert.That(Captured(tree, AliasCopy).ShardMap!.Slots, Is.EqualTo(result!.Slots).AsCollection);
    }

    [Test]
    public async Task ReassignSlotsAsync_fenced_empty_brings_a_lagging_copy_into_line_without_rewriting_the_logical_row()
    {
        var (grain, tree) = CreateGrain();
        var logicalMap = new ShardMap { Slots = [0, 1, 2, 2, 0, 1, 2, 2], Version = 9 };
        SeedAliasedDerivedCopy(tree, logicalMap, ShardMap.CreateDefault(8, 4));

        var result = await grain.ReassignSlotsAsync(AliasOwner, [], 0, logicalMap, AliasCopy);

        Assert.That(result!.Slots, Is.EqualTo(logicalMap.Slots).AsCollection);
        await tree.DidNotReceive().SetAsync(AliasOwner, Arg.Any<byte[]>());
        var copy = Captured(tree, AliasCopy).ShardMap!;
        Assert.That(copy.Slots, Is.EqualTo(logicalMap.Slots).AsCollection);
        Assert.That(copy.Version, Is.GreaterThan(1L));
    }

    [Test]
    public async Task ReassignSlotsAsync_fenced_empty_writes_nothing_when_the_copy_is_already_in_line()
    {
        var (grain, tree) = CreateGrain();
        SeedAliasedDerivedCopy(tree, ShardMap.CreateDefault(8, 4), ShardMap.CreateDefault(8, 4));

        await grain.ReassignSlotsAsync(AliasOwner, [], 0, ShardMap.CreateDefault(8, 4), AliasCopy);

        await tree.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [Test]
    public async Task ReassignSlotsAsync_fenced_leaves_an_independent_alias_target_row_alone()
    {
        var (grain, tree) = CreateGrain();
        SeedAliasedDerivedCopy(tree, ShardMap.CreateDefault(8, 4), ShardMap.CreateDefault(8, 4), derivedFrom: null);

        await grain.ReassignSlotsAsync(AliasOwner, [3], 2, ShardMap.CreateDefault(8, 4), AliasCopy);

        await tree.DidNotReceive().SetAsync(AliasCopy, Arg.Any<byte[]>());
    }
}