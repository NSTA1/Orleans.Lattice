using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Truth table for <see cref="ShardMapCommitFence"/> (issue #4264).
/// </summary>
[TestFixture]
public sealed class ShardMapCommitFenceTests
{
    private const string Logical = "fence-tree";
    private const string Copy = "fence-tree-copy";

    [Test]
    public void Admits_an_unaliased_tree_bound_to_its_own_id() =>
        Assert.That(ShardMapCommitFence.Admits(new TreeRegistryEntry(), Logical, Logical), Is.True);

    [Test]
    public void Admits_a_missing_entry_bound_to_the_logical_id() =>
        Assert.That(ShardMapCommitFence.Admits(null, Logical, Logical), Is.True);

    [Test]
    public void Admits_an_aliased_tree_bound_to_its_alias_target() =>
        Assert.That(
            ShardMapCommitFence.Admits(new TreeRegistryEntry { PhysicalTreeId = Copy }, Logical, Copy),
            Is.True);

    [Test]
    public void Refuses_once_the_alias_names_another_tree() =>
        Assert.That(
            ShardMapCommitFence.Admits(new TreeRegistryEntry { PhysicalTreeId = Copy }, Logical, Logical),
            Is.False);

    [Test]
    public void Refuses_while_a_cutover_to_another_tree_has_carried_its_map() =>
        Assert.That(
            ShardMapCommitFence.Admits(new TreeRegistryEntry { AliasCutoverTarget = Copy }, Logical, Logical),
            Is.False);

    [Test]
    public void Admits_when_the_cutover_marker_names_the_bound_tree() =>
        Assert.That(
            ShardMapCommitFence.Admits(
                new TreeRegistryEntry { PhysicalTreeId = Copy, AliasCutoverTarget = Copy }, Logical, Copy),
            Is.True);
}
