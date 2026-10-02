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

    [TestCase("_9", 8, TestName = "TryParseConsumerId refuses a suffix out of range")]
    [TestCase("_8", 8, TestName = "TryParseConsumerId refuses the first partition past the pinned count")]
    [TestCase("_03", 8, TestName = "TryParseConsumerId refuses a zero-padded suffix")]
    [TestCase("_+3", 8, TestName = "TryParseConsumerId refuses a signed suffix")]
    [TestCase("_-0", 8, TestName = "TryParseConsumerId refuses a negative-zero suffix")]
    public void TryParseConsumerId_refuses_a_suffix_that_is_not_a_canonical_in_range_partition(
        string suffix, int partitions)
    {
        // Issue #4242: the diagnostic parse used to strip any ulong-parseable
        // suffix and attribute the pin to it, so `_9` on an 8-partition tree read
        // as partition 9 and `_03` / `_+3` both read as partition 3. It now
        // shares the removal gate's suffix grammar.
        var parsed = WalFloorHolderReader.TryParseConsumerId(
            Tree, Consumer(Leaf, suffix), partitions, out _, out var partition);

        Assert.That(parsed, Is.False, $"attributed to partition {partition}");
    }

    [TestCase(0)]
    [TestCase(3)]
    [TestCase(7)]
    public void TryParseConsumerId_parses_a_canonical_in_range_suffix(int partition)
    {
        var parsed = WalFloorHolderReader.TryParseConsumerId(
            Tree, Consumer(Leaf, "_" + partition), 8, out var leaf, out var parsedPartition);

        Assert.Multiple(() =>
        {
            Assert.That(parsed, Is.True);
            Assert.That(leaf, Is.EqualTo(Leaf));
            Assert.That(parsedPartition, Is.EqualTo(partition));
        });
    }

    [Test]
    public void TryParseConsumerId_does_not_truncate_a_trailing_digit_group_on_a_single_partition_tree()
    {
        var parsed = WalFloorHolderReader.TryParseConsumerId(
            Tree, Consumer(Leaf, "_3"), 1, out var leaf, out var partition);

        Assert.Multiple(() =>
        {
            Assert.That(parsed, Is.True);
            Assert.That(leaf.ToString(), Does.EndWith("_3"), "a single-partition id carries no suffix to strip.");
            Assert.That(partition, Is.Zero);
        });
    }

    [TestCase("_3", 1, "AmbiguousPartition", TestName = "ClassifyConsumerId reads the 4238 shape as an ambiguous partition")]
    [TestCase("", 8, "AmbiguousPartition", TestName = "ClassifyConsumerId reads a missing suffix on a partitioned tree as ambiguous")]
    [TestCase("_8", 8, "AmbiguousPartition", TestName = "ClassifyConsumerId reads an out-of-range suffix as ambiguous")]
    [TestCase("_03", 8, "AmbiguousPartition", TestName = "ClassifyConsumerId reads a zero-padded suffix as ambiguous")]
    [TestCase("_3", 0, "AmbiguousPartition", TestName = "ClassifyConsumerId reads a non-positive count as ambiguous")]
    [TestCase("_3", 8, "LeafPublished", TestName = "ClassifyConsumerId accepts a leaf's own suffixed id")]
    [TestCase("", 1, "LeafPublished", TestName = "ClassifyConsumerId accepts a leaf's own unsuffixed id")]
    public void ClassifyConsumerId_names_why_an_id_is_refused(string suffix, int partitions, string expected) =>
        Assert.That(WalFloorHolderReader.ClassifyConsumerId(Tree, Consumer(Leaf, suffix), partitions), Is.EqualTo(Enum.Parse<ConsumerIdVerdict>(expected)));

    [TestCase("leaf-1", 1)]
    [TestCase("6262CAAD0B1E4C4E9F008D1B5A4C3E21", 8)]
    public void ClassifyConsumerId_reads_a_leaf_key_that_is_not_a_canonical_guid_as_malformed(string key, int partitions) =>
        Assert.That(
            WalFloorHolderReader.ClassifyConsumerId(
                Tree, Consumer(GrainId.Create("bplusleaf", key), partitions > 1 ? "_0" : string.Empty), partitions),
            Is.EqualTo(ConsumerIdVerdict.MalformedId));

    [Test]
    public void ClassifyConsumerId_reads_another_trees_id_as_malformed() =>
        Assert.That(
            WalFloorHolderReader.ClassifyConsumerId("other-tree", Consumer(Leaf), 1),
            Is.EqualTo(ConsumerIdVerdict.MalformedId));

    [Test]
    public void IsLeafPublishedConsumerId_refuses_another_trees_consumer_id() =>
        Assert.That(
            WalFloorHolderReader.IsLeafPublishedConsumerId(
                "other-tree", Consumer(Leaf), 1),
            Is.False);
}
