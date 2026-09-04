using System.Text;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Regression tests for the projection-version encoding that gates view rebuilds.
/// The maintainer skips a rebuild when a redefined view's recomputed
/// <c>ProjectionVersion</c> equals the stored one, so two structurally different
/// filters must never hash to the same version - otherwise a redefined view keeps
/// serving results built for the old filter. The node encoding previously joined
/// the caller-controlled member-path and constant fields with a bare <c>:</c>
/// delimiter, so a value containing <c>:</c> could shift a field boundary and make
/// a structurally different node serialize identically. The fields are now
/// length-prefixed, which makes the encoding injective. This exercises all three
/// built-in projections, which share the same node encoder.
/// </summary>
[TestFixture]
public class ViewProjectionVersionAliasingRegressionTests
{
    /// <summary>
    /// The constant's <c>ToString</c> wraps its string value in fixed boilerplate;
    /// extract the exact surrounding text at runtime so the crafted collision does
    /// not depend on the record-struct <c>ToString</c> format.
    /// </summary>
    private static string ConstantPrefix()
    {
        const string sentinel = "\uE000S\uE000";
        var wrapped = LatticeConstant.Text(sentinel).ToString()!;
        var index = wrapped.IndexOf(sentinel, StringComparison.Ordinal);
        return wrapped[..index];
    }

    /// <summary>
    /// Reproduces the pre-fix leaf-node encoding (member path and constant joined
    /// with a bare <c>:</c>) so the crafted pair can be shown to have aliased under
    /// the old logic.
    /// </summary>
    private static string OldLeafEncode(LatticePredicateNode node)
    {
        var builder = new StringBuilder();
        builder.Append('(')
            .Append((int)node.Kind).Append(':')
            .Append(node.MemberPath ?? string.Empty).Append(':')
            .Append((int)node.ComparisonOperator).Append(':')
            .Append((int)node.BooleanOperator).Append(':')
            .Append((int)node.StringMethod).Append(':')
            .Append(node.Constant.ToString())
            .Append(')');
        return builder.ToString();
    }

    /// <summary>
    /// Builds two leaf filter nodes that differ structurally (their member paths
    /// and constants have different lengths) yet serialize identically under the
    /// pre-fix bare-delimiter encoding, because the same content sits either side
    /// of the member-path / constant boundary.
    /// </summary>
    private static (LatticePredicateNode A, LatticePredicateNode B) CollidingPair()
    {
        // The literal text the old encoder placed between the member-path slot and
        // the constant's string value for a leaf node whose operator fields are all
        // the default (zero) enum values: ":" + compOp + ":" + boolOp + ":" +
        // strMethod + ":" + the constant's ToString prefix.
        var between = ":0:0:0:" + ConstantPrefix();

        var a = new LatticePredicateNode
        {
            Kind = LatticePredicateNodeKind.Member,
            MemberPath = "a",
            Constant = LatticeConstant.Text("b" + between + "c"),
        };

        var b = new LatticePredicateNode
        {
            Kind = LatticePredicateNodeKind.Member,
            MemberPath = "a" + between + "b",
            Constant = LatticeConstant.Text("c"),
        };

        return (a, b);
    }

    [Test]
    public void Crafted_filters_alias_under_the_pre_fix_bare_delimiter_encoding()
    {
        var (a, b) = CollidingPair();

        // Self-check: the pair genuinely collided under the old encoding, so the
        // per-projection assertions below are true regression coverage rather than a
        // pair that trivially differs.
        Assert.That(OldLeafEncode(a), Is.EqualTo(OldLeafEncode(b)));
        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void Predicate_projection_version_distinguishes_aliasing_filters()
    {
        var (a, b) = CollidingPair();

        var versionA = new PredicateLatticeViewProjection(a).ProjectionVersion;
        var versionB = new PredicateLatticeViewProjection(b).ProjectionVersion;

        Assert.That(versionA, Is.Not.EqualTo(versionB));
    }

    [Test]
    public void Fold_projection_version_distinguishes_aliasing_filters()
    {
        var (a, b) = CollidingPair();

        var versionA = new LatticeFoldProjection(_ => "g", () => [], (acc, _, _, _) => acc, "fold-v1", a).ProjectionVersion;
        var versionB = new LatticeFoldProjection(_ => "g", () => [], (acc, _, _, _) => acc, "fold-v1", b).ProjectionVersion;

        Assert.That(versionA, Is.Not.EqualTo(versionB));
    }

    [Test]
    public void Aggregation_projection_version_distinguishes_aliasing_filters()
    {
        var (a, b) = CollidingPair();

        var versionA = new AggregationLatticeViewProjection(AggregationKind.Count, _ => "g", "sel-v1", filter: a).ProjectionVersion;
        var versionB = new AggregationLatticeViewProjection(AggregationKind.Count, _ => "g", "sel-v1", filter: b).ProjectionVersion;

        Assert.That(versionA, Is.Not.EqualTo(versionB));
    }
}
