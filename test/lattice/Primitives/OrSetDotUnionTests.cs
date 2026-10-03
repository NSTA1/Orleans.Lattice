namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Covers <see cref="OrSetDotUnion"/>, the dot-list unions the observed-remove
/// primitives fold their merges and deltas through.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class OrSetDotUnionTests
{
    [Test]
    public void UnionInto_appends_only_absent_dots_on_the_narrow_path()
    {
        var target = new List<OrSetDot> { Dot("a", 1) };

        OrSetDotUnion.UnionInto(target, [Dot("a", 1), Dot("b", 1)]);

        Assert.That(target, Is.EqualTo(new[] { Dot("a", 1), Dot("b", 1) }));
    }

    [Test]
    public void UnionInto_appends_only_absent_dots_on_the_wide_path()
    {
        var target = new List<OrSetDot> { Dot("a", 1), Dot("a", 2) };
        var source = Enumerable.Range(1, OrSetDotUnion.LinearScanThreshold + 2).Select(i => Dot("a", i)).ToList();

        OrSetDotUnion.UnionInto(target, source);

        Assert.That(target, Is.EqualTo(source));
    }

    [Test]
    public void UnionInto_a_list_with_itself_is_the_identity()
    {
        var target = new List<OrSetDot> { Dot("a", 1) };

        OrSetDotUnion.UnionInto(target, target);

        Assert.That(target, Has.Count.EqualTo(1));
    }

    [Test]
    public void UnionDeltaDots_folds_every_collection_shape_identically()
    {
        OrSetDot[] dots = [Dot("a", 1), Dot("b", 1), Dot("a", 1)];
        var wide = Enumerable.Range(1, OrSetDotUnion.LinearScanThreshold + 2).Select(i => Dot("w", i)).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(FoldDelta(dots), Is.EqualTo(new[] { Dot("a", 1), Dot("b", 1) }), "array");
            Assert.That(FoldDelta(dots.ToList()), Is.EqualTo(new[] { Dot("a", 1), Dot("b", 1) }), "list");
            Assert.That(FoldDelta(dots.ToList().AsReadOnly()), Is.EqualTo(new[] { Dot("a", 1), Dot("b", 1) }), "unspannable");
            Assert.That(FoldDelta(wide), Is.EqualTo(wide), "wide");
            Assert.That(FoldDelta(wide.ToList().AsReadOnly()), Is.EqualTo(wide), "wide unspannable");
            Assert.That(FoldDelta(null), Is.Empty, "null");
        });
    }

    [Test]
    public void MergeDotMaps_copies_new_keys_and_unions_existing_ones()
    {
        var target = new Dictionary<string, List<OrSetDot>> { ["k"] = [Dot("a", 1)] };
        var sourceList = new List<OrSetDot> { Dot("b", 1) };
        var source = new Dictionary<string, List<OrSetDot>>
        {
            ["k"] = [Dot("a", 1), Dot("a", 2)],
            ["n"] = sourceList,
        };

        OrSetDotUnion.MergeDotMaps(target, source);

        Assert.Multiple(() =>
        {
            Assert.That(target["k"], Is.EqualTo(new[] { Dot("a", 1), Dot("a", 2) }));
            Assert.That(target["n"], Is.EqualTo(sourceList));
            Assert.That(target["n"], Is.Not.SameAs(sourceList), "a new key's list is copied, not shared");
        });
    }

    private static List<OrSetDot> FoldDelta(IReadOnlyList<OrSetDot>? source)
    {
        var target = new List<OrSetDot>();
        OrSetDotUnion.UnionDeltaDots(target, source);
        return target;
    }

    private static OrSetDot Dot(string replica, long counter) => new() { ReplicaId = replica, Counter = counter };
}
