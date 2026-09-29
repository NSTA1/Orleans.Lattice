using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.Tests.UI.Design;

/// <summary>
/// The density selector's .NET names agree with the attribute
/// <c>lattice-operate.css</c> reads, so the appearance setting that writes the
/// attribute and the stylesheet that reads it cannot drift apart.
/// </summary>
[TestFixture]
public sealed class LtDensitiesTests
{
    [Test]
    [TestCase((int)LtDensity.Comfortable, "comfortable")]
    [TestCase((int)LtDensity.Compact, "compact")]
    public void Each_density_has_its_attribute_value(int density, string expected)
    {
        Assert.That(LtDensities.AttributeValue((LtDensity)density), Is.EqualTo(expected));
    }

    [Test]
    public void An_undeclared_density_is_rejected()
    {
        Assert.That(() => LtDensities.AttributeValue((LtDensity)42), Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void The_stylesheet_reads_the_compact_density_from_the_declared_attribute()
    {
        var selector = $"[{LtDensities.AttributeName}=\"{LtDensities.AttributeValue(LtDensity.Compact)}\"]";

        Assert.That(ShellStylesheets.Rules(ShellStylesheets.Operate).Select(rule => rule.Selector), Does.Contain(selector));
    }
}
