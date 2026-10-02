using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="WalFloorHolderReader.IsLeafPublishedConsumerId"/>,
/// the gate every WAL GC pin removal passes (issue #4238). It accepts exactly
/// the ids a <c>BPlusLeafGrain</c> publishes and nothing else, because a
/// removal is authorised by evidence about the parsed grain, which is evidence
/// about the pin's publisher only when the parse is exact.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class WalFloorHolderReaderTests
{
    private const string Tree = "tree-4238";

    private static readonly GrainId Leaf = GrainId.Create(
        GrainType.Create("bplusleaf"),
        GrainIdKeyExtensions.CreateGuidKey(new Guid("6262caad-0b1e-4c4e-9f00-8d1b5a4c3e21")));

    private static string Consumer(GrainId leaf, string suffix = "") =>
        $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{Tree}_{leaf}{suffix}";

    [Test]
    public void IsLeafPublishedConsumerId_accepts_the_unsuffixed_id_on_a_single_partition_tree() =>
        Assert.That(WalFloorHolderReader.IsLeafPublishedConsumerId(Tree, Consumer(Leaf), 1), Is.True);

    [TestCase(0)]
    [TestCase(7)]
    public void IsLeafPublishedConsumerId_accepts_an_in_range_suffix_on_a_partitioned_tree(int partition) =>
        Assert.That(
            WalFloorHolderReader.IsLeafPublishedConsumerId(Tree, Consumer(Leaf, "_" + partition), 8),
            Is.True);

    [TestCase("_3", 1, TestName = "a suffix on a single-partition tree")]
    [TestCase("", 8, TestName = "no suffix on a partitioned tree")]
    [TestCase("_8", 8, TestName = "a suffix out of range")]
    [TestCase("_03", 8, TestName = "a non-canonical suffix")]
    [TestCase("_+3", 8, TestName = "a signed suffix")]
    [TestCase("_3", 0, TestName = "a non-positive partition count")]
    public void IsLeafPublishedConsumerId_refuses_an_id_whose_partition_is_ambiguous(string suffix, int partitions) =>
        Assert.That(
            WalFloorHolderReader.IsLeafPublishedConsumerId(Tree, Consumer(Leaf, suffix), partitions),
            Is.False);

    [TestCase("leaf-1")]
    [TestCase("6262caad0b1e4c4e9f008d1b5a4c3e2")]
    [TestCase("6262CAAD0B1E4C4E9F008D1B5A4C3E21")]
    [TestCase("6262caad-0b1e-4c4e-9f00-8d1b5a4c3e21")]
    public void IsLeafPublishedConsumerId_refuses_a_leaf_key_that_is_not_a_canonical_guid_key(string key) =>
        Assert.That(
            WalFloorHolderReader.IsLeafPublishedConsumerId(Tree, Consumer(GrainId.Create("bplusleaf", key)), 1),
            Is.False);

    [Test]
    public void IsLeafPublishedConsumerId_refuses_another_trees_consumer_id() =>
        Assert.That(
            WalFloorHolderReader.IsLeafPublishedConsumerId(
                "other-tree", Consumer(Leaf), 1),
            Is.False);
}
