using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Regression coverage for <see cref="LatticeValueTransform"/> structural
/// equality. Before the fix, <see cref="LatticeValueTransform"/> inherited the
/// compiler-generated record-struct equality, which compares its
/// <c>LatticeValueTransform[]? Children</c> array by reference, so two
/// structurally identical transform IRs never compared equal - and a tree that
/// round-tripped through serialization never equalled its pre-serialization
/// self. That is the same defect that was fixed for the sibling
/// <see cref="LatticePredicateNode"/>; this pins the equivalent contract for the
/// transform IR.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticeValueTransformEqualityRegressionTests
{
    private ServiceProvider _services = null!;
    private Serializer<LatticeValueTransform> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<LatticeValueTransform>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private static LatticeValueTransform Pipeline() =>
        LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("Full", LatticeValueTransform.Compute(
                LatticeComputeOperator.Concat,
                LatticeValueTransform.Member("First"),
                LatticeValueTransform.Const(LatticeConstant.Text(" ")),
                LatticeValueTransform.Member("Last"))),
            LatticeValueTransform.DropMember("First"),
            LatticeValueTransform.RenameMember("Last", "Surname"));

    [Test]
    public void A_leaf_node_with_null_children_compares_equal()
    {
        // Control: leaf nodes carry a null Children array, so they already
        // compared structurally even before the fix. This isolates the defect
        // to the array field.
        Assert.That(
            LatticeValueTransform.DropMember("Legacy").Equals(LatticeValueTransform.DropMember("Legacy")),
            Is.True);
    }

    [Test]
    public void Two_structurally_identical_transform_trees_compare_equal()
    {
        var a = Pipeline();
        var b = Pipeline();

        // Regressed when Children was compared by reference: a and b hold
        // distinct array instances, so the compiler-generated equality returned
        // false for structurally identical trees.
        Assert.That(a.Equals(b), Is.True);
        Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
    }

    [Test]
    public void Structurally_different_transform_trees_do_not_compare_equal()
    {
        var a = Pipeline();
        var b = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("Full", LatticeValueTransform.Compute(
                LatticeComputeOperator.Concat,
                LatticeValueTransform.Member("First"),
                LatticeValueTransform.Const(LatticeConstant.Text(" ")),
                LatticeValueTransform.Member("Last"))),
            LatticeValueTransform.DropMember("First"),
            LatticeValueTransform.RenameMember("Last", "FamilyName")); // differs from "Surname"

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void A_difference_in_a_deeply_nested_child_is_detected()
    {
        var a = Pipeline();
        var b = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("Full", LatticeValueTransform.Compute(
                LatticeComputeOperator.Concat,
                LatticeValueTransform.Member("First"),
                LatticeValueTransform.Const(LatticeConstant.Text("-")), // separator differs deep inside
                LatticeValueTransform.Member("Last"))),
            LatticeValueTransform.DropMember("First"),
            LatticeValueTransform.RenameMember("Last", "Surname"));

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void The_embedded_condition_predicate_participates_in_equality()
    {
        var condition = LatticePredicateNode.Compare(
            LatticeComparisonOperator.GreaterThan,
            LatticePredicateNode.Member("Age"),
            LatticePredicateNode.Const(LatticeConstant.Integer(18)));
        var otherCondition = LatticePredicateNode.Compare(
            LatticeComparisonOperator.GreaterThan,
            LatticePredicateNode.Member("Age"),
            LatticePredicateNode.Const(LatticeConstant.Integer(21)));

        var gold = LatticeValueTransform.Const(LatticeConstant.Text("gold"));
        var standard = LatticeValueTransform.Const(LatticeConstant.Text("standard"));

        Assert.That(
            LatticeValueTransform.Conditional(condition, gold, standard)
                .Equals(LatticeValueTransform.Conditional(condition, gold, standard)),
            Is.True);
        Assert.That(
            LatticeValueTransform.Conditional(condition, gold, standard)
                .Equals(LatticeValueTransform.Conditional(otherCondition, gold, standard)),
            Is.False);
    }

    [Test]
    public void A_transform_tree_equals_itself_after_a_serialization_round_trip()
    {
        var original = Pipeline();

        var bytes = _serializer.SerializeToArray(original);
        var decoded = _serializer.Deserialize(bytes);

        // The natural structural check the pre-fix IR could not support: a
        // deserialized tree carries a freshly allocated Children array, so
        // reference equality returned false even though the tree is identical.
        Assert.That(decoded.Equals(original), Is.True);
        Assert.That(decoded.GetHashCode(), Is.EqualTo(original.GetHashCode()));
    }

    [Test]
    public void Two_empty_passthrough_transforms_compare_equal()
    {
        // Passthrough() with no operations carries an empty (non-null) Children
        // array; sequence equality must treat two empty arrays as equal.
        Assert.That(
            LatticeValueTransform.Passthrough().Equals(LatticeValueTransform.Passthrough()),
            Is.True);
    }
}
