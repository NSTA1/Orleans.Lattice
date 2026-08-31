using System.Linq.Expressions;
using NSubstitute;

namespace Orleans.Lattice.Tests.Predicates;

/// <summary>
/// Regression coverage for <see cref="LatticePredicateNode"/> structural
/// equality and the "at most one predicate per tree" guard that depends on it.
/// Before the fix, <see cref="LatticePredicateNode"/> inherited the
/// compiler-generated record equality, which compares its
/// <c>LatticePredicateNode[]? Children</c> array by reference, so two
/// structurally identical predicate IRs never compared equal. That made the
/// restatement branch in <c>LatticeAtomicWriteBuilder.SetWhere</c> unreachable
/// and threw on a re-stated identical guard.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticePredicateNodeEqualityRegressionTests
{
    private sealed record Doc(string Name, int Score);

    [Test]
    public void A_leaf_node_with_no_children_compares_equal()
    {
        // Control: leaf nodes carry a null Children array, so they already
        // compared structurally even before the fix. This isolates the defect
        // to the array field.
        Assert.That(
            LatticePredicateNode.Member("Score").Equals(LatticePredicateNode.Member("Score")),
            Is.True);
    }

    [Test]
    public void Two_structurally_identical_compiled_predicates_compare_equal()
    {
        Expression<Func<Doc, bool>> a = d => d.Score > 3;
        Expression<Func<Doc, bool>> b = d => d.Score > 3;

        var irA = LatticePredicatePushdown.Compile(a, JsonLatticeSerializer<Doc>.Default);
        var irB = LatticePredicatePushdown.Compile(b, JsonLatticeSerializer<Doc>.Default);

        // Regressed when Children was compared by reference.
        Assert.That(irA.Equals(irB), Is.True);
        Assert.That(irA.GetHashCode(), Is.EqualTo(irB.GetHashCode()));
    }

    [Test]
    public void Distinct_compiled_predicates_do_not_compare_equal()
    {
        Expression<Func<Doc, bool>> a = d => d.Score > 3;
        Expression<Func<Doc, bool>> b = d => d.Score > 4;

        var irA = LatticePredicatePushdown.Compile(a, JsonLatticeSerializer<Doc>.Default);
        var irB = LatticePredicatePushdown.Compile(b, JsonLatticeSerializer<Doc>.Default);

        Assert.That(irA.Equals(irB), Is.False);
    }

    [Test]
    public void SetWhere_admits_a_restatement_of_an_identical_predicate()
    {
        var factory = Substitute.For<IGrainFactory>();
        var builder = factory.BeginAtomicWrite("op")
            .ForTree("orders")
            .SetWhere<Doc>("a", new Doc("ada", 7), d => d.Score > 3);

        // Regressed with InvalidOperationException: "Tree 'orders' already has a
        // guard predicate; a cross-tree slice supports at most one."
        Assert.DoesNotThrow(
            () => builder.SetWhere<Doc>("b", new Doc("bob", 9), d => d.Score > 3));
    }

    [Test]
    public void SetWhere_still_rejects_a_conflicting_predicate_on_the_same_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        var builder = factory.BeginAtomicWrite("op")
            .ForTree("orders")
            .SetWhere<Doc>("a", new Doc("ada", 7), d => d.Score > 3);

        // The one-predicate-per-tree guard must remain enforced: a genuinely
        // different predicate for a second key still conflicts.
        Assert.Throws<InvalidOperationException>(
            () => builder.SetWhere<Doc>("b", new Doc("bob", 9), d => d.Score > 4));
    }
}
