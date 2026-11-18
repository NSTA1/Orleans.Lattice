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

    // --- Issue #6083: a copy whose owner routes through it ---

    [Test]
    public void IsAliasTarget_holds_for_a_derived_copy_its_owner_aliases() =>
        Assert.That(
            ShardMapCommitFence.IsAliasTarget(
                new TreeRegistryEntry { DerivedFrom = Logical }, Copy, new TreeRegistryEntry { PhysicalTreeId = Copy }),
            Is.True);

    [Test]
    public void IsAliasTarget_holds_for_a_derived_copy_its_owner_is_cutting_over_to() =>
        Assert.That(
            ShardMapCommitFence.IsAliasTarget(
                new TreeRegistryEntry { DerivedFrom = Logical }, Copy, new TreeRegistryEntry { AliasCutoverTarget = Copy }),
            Is.True);

    [Test]
    public void IsAliasTarget_does_not_hold_once_the_owner_aliases_another_copy() =>
        Assert.That(
            ShardMapCommitFence.IsAliasTarget(
                new TreeRegistryEntry { DerivedFrom = Logical }, Copy, new TreeRegistryEntry { PhysicalTreeId = Copy + "-2" }),
            Is.False);

    [Test]
    public void IsAliasTarget_does_not_hold_for_a_tree_that_records_no_owner() =>
        Assert.That(
            ShardMapCommitFence.IsAliasTarget(
                new TreeRegistryEntry(), Copy, new TreeRegistryEntry { PhysicalTreeId = Copy }),
            Is.False);
}
