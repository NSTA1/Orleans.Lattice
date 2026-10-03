using System.Text;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Covers <see cref="ProjectionVersionFingerprint"/>, the shared builder of the
/// built-in view projections' <c>ProjectionVersion</c>.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class ProjectionVersionFingerprintTests
{
    [Test]
    public void AppendFilter_writes_none_for_an_absent_filter()
    {
        var builder = new StringBuilder();

        ProjectionVersionFingerprint.AppendFilter(builder, null);

        Assert.That(builder.ToString(), Is.EqualTo("none"));
    }

    [Test]
    public void AppendFilter_distinguishes_filters_that_differ_only_in_a_constant()
    {
        var a = Encode(LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal, LatticePredicateNode.Member("Name"), LatticePredicateNode.Const(LatticeConstant.Text("a:b"))));
        var b = Encode(LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal, LatticePredicateNode.Member("Name"), LatticePredicateNode.Const(LatticeConstant.Text("a"))));

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void Hash_is_a_deterministic_128_bit_upper_case_hex_digest()
    {
        var first = ProjectionVersionFingerprint.Hash(new StringBuilder("v1|filter=none"));
        var second = ProjectionVersionFingerprint.Hash(new StringBuilder("v1|filter=none"));
        var other = ProjectionVersionFingerprint.Hash(new StringBuilder("v2|filter=none"));

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(second));
            Assert.That(first, Is.Not.EqualTo(other));
            Assert.That(first, Does.Match("^[0-9A-F]{32}$"));
        });
    }

    private static string Encode(LatticePredicateNode node)
    {
        var builder = new StringBuilder();
        ProjectionVersionFingerprint.AppendFilter(builder, node);
        return builder.ToString();
    }
}
