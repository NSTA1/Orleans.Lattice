using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Covers the <see cref="LatticeValueTransform"/> IR: the factory helpers build
/// the expected discriminated nodes, and every node kind (including an embedded
/// core <see cref="LatticePredicateNode"/> condition) round-trips through Orleans
/// serialization deterministically.
/// </summary>
[TestFixture]
public sealed class LatticeValueTransformTests
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

    private LatticeValueTransform RoundTrip(LatticeValueTransform transform)
    {
        var bytes = _serializer.SerializeToArray(transform);
        return _serializer.Deserialize(bytes);
    }

    // Record-struct equality compares the Children array by reference, so it is
    // not a structural round-trip check. Determinism is asserted by re-serializing
    // the decoded tree and comparing the wire bytes.
    private void AssertRoundTrips(LatticeValueTransform transform)
    {
        var bytes = _serializer.SerializeToArray(transform);
        var decoded = _serializer.Deserialize(bytes);
        var reencoded = _serializer.SerializeToArray(decoded);
        Assert.That(reencoded, Is.EqualTo(bytes));
    }

    [Test]
    public void Passthrough_builds_identity_node_with_empty_children()
    {
        var node = LatticeValueTransform.Passthrough();

        Assert.That(node.Kind, Is.EqualTo(LatticeValueTransformKind.Passthrough));
        Assert.That(node.Children, Is.Empty);
    }

    [Test]
    public void SetMember_builds_node_with_path_and_single_value_child()
    {
        var node = LatticeValueTransform.SetMember("Name", LatticeValueTransform.Const(LatticeConstant.Text("x")));

        Assert.That(node.Kind, Is.EqualTo(LatticeValueTransformKind.SetMember));
        Assert.That(node.MemberPath, Is.EqualTo("Name"));
        Assert.That(node.Children, Has.Length.EqualTo(1));
        Assert.That(node.Children![0].Kind, Is.EqualTo(LatticeValueTransformKind.Constant));
    }

    [Test]
    public void DropMember_builds_leaf_node_with_path()
    {
        var node = LatticeValueTransform.DropMember("Legacy");

        Assert.That(node.Kind, Is.EqualTo(LatticeValueTransformKind.DropMember));
        Assert.That(node.MemberPath, Is.EqualTo("Legacy"));
        Assert.That(node.Children, Is.Null);
    }

    [Test]
    public void RenameMember_builds_node_with_from_and_to_paths()
    {
        var node = LatticeValueTransform.RenameMember("Old", "New");

        Assert.That(node.Kind, Is.EqualTo(LatticeValueTransformKind.RenameMember));
        Assert.That(node.MemberPath, Is.EqualTo("Old"));
        Assert.That(node.ToPath, Is.EqualTo("New"));
    }

    [Test]
    public void Member_builds_leaf_value_expression()
    {
        var node = LatticeValueTransform.Member("Age");

        Assert.That(node.Kind, Is.EqualTo(LatticeValueTransformKind.Member));
        Assert.That(node.MemberPath, Is.EqualTo("Age"));
    }

    [Test]
    public void Const_builds_constant_value_expression()
    {
        var node = LatticeValueTransform.Const(LatticeConstant.Integer(42));

        Assert.That(node.Kind, Is.EqualTo(LatticeValueTransformKind.Constant));
        Assert.That(node.Constant.Kind, Is.EqualTo(LatticeConstantKind.Int64));
        Assert.That(node.Constant.Int64Value, Is.EqualTo(42));
    }

    [Test]
    public void Conditional_builds_node_with_condition_and_two_branches()
    {
        var condition = LatticePredicateNode.Compare(
            LatticeComparisonOperator.GreaterThan,
            LatticePredicateNode.Member("Age"),
            LatticePredicateNode.Const(LatticeConstant.Integer(18)));

        var node = LatticeValueTransform.Conditional(
            condition,
            LatticeValueTransform.Const(LatticeConstant.Text("adult")),
            LatticeValueTransform.Const(LatticeConstant.Text("minor")));

        Assert.That(node.Kind, Is.EqualTo(LatticeValueTransformKind.Conditional));
        Assert.That(node.Condition.Kind, Is.EqualTo(LatticePredicateNodeKind.Compare));
        Assert.That(node.Children, Has.Length.EqualTo(2));
    }

    [Test]
    public void Compute_builds_node_with_operator_and_operands()
    {
        var node = LatticeValueTransform.Compute(
            LatticeComputeOperator.Concat,
            LatticeValueTransform.Member("First"),
            LatticeValueTransform.Const(LatticeConstant.Text(" ")),
            LatticeValueTransform.Member("Last"));

        Assert.That(node.Kind, Is.EqualTo(LatticeValueTransformKind.Compute));
        Assert.That(node.ComputeOperator, Is.EqualTo(LatticeComputeOperator.Concat));
        Assert.That(node.Children, Has.Length.EqualTo(3));
    }

    [Test]
    public void Passthrough_pipeline_round_trips_through_serialization()
    {
        var original = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("Full", LatticeValueTransform.Compute(
                LatticeComputeOperator.Concat,
                LatticeValueTransform.Member("First"),
                LatticeValueTransform.Const(LatticeConstant.Text(" ")),
                LatticeValueTransform.Member("Last"))),
            LatticeValueTransform.DropMember("First"),
            LatticeValueTransform.RenameMember("Last", "Surname"));

        var decoded = RoundTrip(original);

        AssertRoundTrips(original);
        Assert.That(decoded.Kind, Is.EqualTo(LatticeValueTransformKind.Passthrough));
        Assert.That(decoded.Children, Has.Length.EqualTo(3));
        Assert.That(decoded.Children![0].Children![0].ComputeOperator, Is.EqualTo(LatticeComputeOperator.Concat));
        Assert.That(decoded.Children![2].ToPath, Is.EqualTo("Surname"));
    }

    [Test]
    public void Conditional_with_embedded_predicate_round_trips_through_serialization()
    {
        var condition = LatticePredicateNode.Bool(
            LatticeBooleanOperator.And,
            LatticePredicateNode.Compare(
                LatticeComparisonOperator.GreaterThanOrEqual,
                LatticePredicateNode.Member("Age"),
                LatticePredicateNode.Const(LatticeConstant.Integer(18))),
            LatticePredicateNode.StringCall(
                LatticeStringMethod.StartsWith,
                LatticePredicateNode.Member("Country"),
                LatticePredicateNode.Const(LatticeConstant.Text("U"))));

        var original = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("Tier", LatticeValueTransform.Conditional(
                condition,
                LatticeValueTransform.Const(LatticeConstant.Text("gold")),
                LatticeValueTransform.Const(LatticeConstant.Text("standard")))));

        var decoded = RoundTrip(original);

        AssertRoundTrips(original);
        var branch = decoded.Children![0].Children![0];
        Assert.That(branch.Condition.Kind, Is.EqualTo(LatticePredicateNodeKind.Boolean));
    }

    [Test]
    public void Every_constant_kind_round_trips_through_serialization()
    {
        var original = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("a", LatticeValueTransform.Const(LatticeConstant.Null())),
            LatticeValueTransform.SetMember("b", LatticeValueTransform.Const(LatticeConstant.Bool(true))),
            LatticeValueTransform.SetMember("c", LatticeValueTransform.Const(LatticeConstant.Text("s"))),
            LatticeValueTransform.SetMember("d", LatticeValueTransform.Const(LatticeConstant.Integer(-7))),
            LatticeValueTransform.SetMember("e", LatticeValueTransform.Const(LatticeConstant.Real(1.5))));

        var decoded = RoundTrip(original);

        AssertRoundTrips(original);
        Assert.That(decoded.Children, Has.Length.EqualTo(5));
        Assert.That(decoded.Children![0].Children![0].Constant.Kind, Is.EqualTo(LatticeConstantKind.Null));
        Assert.That(decoded.Children![4].Children![0].Constant.DoubleValue, Is.EqualTo(1.5));
    }
}
