using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Symbols;

/// <summary>
/// Unit tests for <see cref="CrossReferenceNode"/>, the reverse cross-reference
/// projection record. They pin its default identity, and that <see cref="CrossReferenceNode.Merge"/>
/// carries identity from the left (falling back to the right when unset) and folds both
/// edge sets through their CRDT union so concurrent reconcilers converge.
/// </summary>
[TestFixture]
public sealed class CrossReferenceNodeTests
{
    private static OrSet SetOf(params string[] elements)
    {
        var set = new OrSet();
        foreach (var element in elements)
        {
            var bytes = Encoding.UTF8.GetBytes(element);
            set.Add(bytes, element, counter: 0);
        }

        return set;
    }

    private static IReadOnlySet<string> Decode(OrSet set)
        => set.Elements().Select(e => Encoding.UTF8.GetString(e)).ToHashSet(StringComparer.Ordinal);

    [Test]
    public void Default_has_empty_identity_and_edge_sets()
    {
        var node = new CrossReferenceNode();

        Assert.Multiple(() =>
        {
            Assert.That(node.RepoId, Is.Empty);
            Assert.That(node.Name, Is.Empty);
            Assert.That(node.Referrers.IsEmpty, Is.True);
            Assert.That(node.Tests.IsEmpty, Is.True);
        });
    }

    [Test]
    public void Merge_unions_referrer_and_test_edges_from_both_replicas()
    {
        var left = new CrossReferenceNode
        {
            RepoId = "acme",
            Name = "Widget",
            Referrers = SetOf("N.A"),
            Tests = SetOf("N.WidgetTests"),
        };
        var right = new CrossReferenceNode
        {
            RepoId = "acme",
            Name = "Widget",
            Referrers = SetOf("N.B"),
            Tests = SetOf(),
        };

        var merged = CrossReferenceNode.Merge(left, right);

        Assert.Multiple(() =>
        {
            Assert.That(Decode(merged.Referrers), Is.EquivalentTo(new[] { "N.A", "N.B" }));
            Assert.That(Decode(merged.Tests), Is.EquivalentTo(new[] { "N.WidgetTests" }));
        });
    }

    [Test]
    public void Merge_prefers_left_identity_and_falls_back_to_right_when_unset()
    {
        var left = new CrossReferenceNode { RepoId = "acme", Name = "Widget" };
        var right = new CrossReferenceNode { RepoId = "other", Name = "Other" };

        var fromLeft = CrossReferenceNode.Merge(left, right);
        var fromRight = CrossReferenceNode.Merge(new CrossReferenceNode(), right);

        Assert.Multiple(() =>
        {
            Assert.That(fromLeft.RepoId, Is.EqualTo("acme"));
            Assert.That(fromLeft.Name, Is.EqualTo("Widget"));
            Assert.That(fromRight.RepoId, Is.EqualTo("other"));
            Assert.That(fromRight.Name, Is.EqualTo("Other"));
        });
    }

    [Test]
    public void Merge_null_throws()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => CrossReferenceNode.Merge(null!, new CrossReferenceNode()),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(() => CrossReferenceNode.Merge(new CrossReferenceNode(), null!),
                Throws.InstanceOf<ArgumentNullException>());
        });
    }
}
