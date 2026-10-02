using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The fenced reassignment a split or fold commits through (issue #4264): the
/// diff is applied only while the logical entry still describes the physical
/// tree the coordinator drained, and the check runs inside the same exclusive
/// call as the write.
/// </summary>
public partial class LatticeRegistryGrainShardMapConcurrencyTests
{
    private const string CopyTreeId = "shard-map-concurrency-copy";

    [Test]
    public async Task Fenced_reassignment_applies_the_diff_while_the_tree_resolves_to_the_bound_tree()
    {
        var grain = CreateGrainOverBackingStore();
        await grain.SetShardMapAsync(TreeId, new ShardMap { Slots = [0, 0, 1, 1] });

        var result = await grain.ReassignSlotsAsync(TreeId, [1], 2, ShardMap.CreateDefault(4, 2), TreeId);

        Assert.That(result!.Slots, Is.EqualTo(new[] { 0, 2, 1, 1 }));
        Assert.That((await grain.GetShardMapAsync(TreeId))!.Slots, Is.EqualTo(new[] { 0, 2, 1, 1 }));
    }

    [Test]
    public async Task Fenced_reassignment_refuses_and_writes_nothing_once_the_tree_resolves_elsewhere()
    {
        var grain = CreateGrainOverBackingStore();
        var carried = new ShardMap { Slots = [0, 0, 1, 1] };
        var entry = await grain.GetEntryAsync(TreeId);
        await grain.UpdateAsync(TreeId, entry! with { PhysicalTreeId = CopyTreeId, ShardMap = carried });
        var before = (await grain.GetShardMapAsync(TreeId))!;

        var result = await grain.ReassignSlotsAsync(TreeId, [1], 2, ShardMap.CreateDefault(4, 2), TreeId);

        var after = (await grain.GetShardMapAsync(TreeId))!;
        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Null);
            Assert.That(after.Slots, Is.EqualTo(before.Slots));
            Assert.That(after.Version, Is.EqualTo(before.Version));
        });
    }

    [Test]
    public async Task Fenced_reassignment_refuses_between_a_cutovers_map_carry_and_its_alias_swap()
    {
        var grain = CreateGrainOverBackingStore();
        var entry = await grain.GetEntryAsync(TreeId);
        await grain.UpdateAsync(TreeId, entry! with
        {
            ShardMap = new ShardMap { Slots = [0, 0, 1, 1] },
            AliasCutoverTarget = CopyTreeId,
        });

        var result = await grain.ReassignSlotsAsync(TreeId, [1], 2, ShardMap.CreateDefault(4, 2), TreeId);

        Assert.That(result, Is.Null);
        Assert.That((await grain.GetShardMapAsync(TreeId))!.Slots, Is.EqualTo(new[] { 0, 0, 1, 1 }));
    }

    [Test]
    public async Task Unfenced_reassignment_is_unaffected_by_the_cutover_marker()
    {
        var grain = CreateGrainOverBackingStore();
        var entry = await grain.GetEntryAsync(TreeId);
        await grain.UpdateAsync(TreeId, entry! with
        {
            ShardMap = new ShardMap { Slots = [0, 0, 1, 1] },
            AliasCutoverTarget = CopyTreeId,
        });

        var result = await grain.ReassignSlotsAsync(TreeId, [], 0, ShardMap.CreateDefault(4, 2));

        Assert.That(result.Slots, Is.EqualTo(new[] { 0, 0, 1, 1 }));
    }

    [Test]
    public async Task SetAlias_clears_the_cutover_marker()
    {
        var grain = CreateGrainOverBackingStore();
        var entry = await grain.GetEntryAsync(TreeId);
        await grain.UpdateAsync(TreeId, entry! with { AliasCutoverTarget = CopyTreeId });

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await grain.SetAliasAsync(TreeId, CopyTreeId);
        }

        var after = await grain.GetEntryAsync(TreeId);
        Assert.Multiple(() =>
        {
            Assert.That(after!.PhysicalTreeId, Is.EqualTo(CopyTreeId));
            Assert.That(after.AliasCutoverTarget, Is.Null);
        });
    }

    [Test]
    public async Task RemoveAlias_clears_the_cutover_marker()
    {
        var grain = CreateGrainOverBackingStore();
        var entry = await grain.GetEntryAsync(TreeId);
        await grain.UpdateAsync(TreeId, entry! with { PhysicalTreeId = CopyTreeId, AliasCutoverTarget = TreeId });

        await grain.RemoveAliasAsync(TreeId);

        var after = await grain.GetEntryAsync(TreeId);
        Assert.Multiple(() =>
        {
            Assert.That(after!.PhysicalTreeId, Is.Null);
            Assert.That(after.AliasCutoverTarget, Is.Null);
        });
    }

    [Test]
    public void Fenced_reassignment_throws_when_the_bound_physical_tree_is_null()
    {
        var grain = CreateGrainOverBackingStore();

        Assert.That(
            async () => await grain.ReassignSlotsAsync(TreeId, [0], 1, ShardMap.CreateDefault(8, 2), null!),
            Throws.ArgumentNullException);
    }
}
