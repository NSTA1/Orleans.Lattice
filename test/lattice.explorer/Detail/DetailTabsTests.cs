using Orleans.Lattice.Explorer.Core.Detail;

namespace Orleans.Lattice.Explorer.Tests.Detail;

[TestFixture]
public class DetailTabsTests
{
    [Test]
    public void Ordered_IsMetricsTopologyData()
    {
        Assert.That(DetailTabs.Ordered, Is.EqualTo(new[]
        {
            DetailTab.Metrics,
            DetailTab.Topology,
            DetailTab.Data,
        }));
    }

    [Test]
    public void Default_IsMetrics()
    {
        Assert.That(DetailTabs.Default, Is.EqualTo(DetailTab.Metrics));
    }

    [Test]
    public void Ordered_StartsWithDefault()
    {
        Assert.That(DetailTabs.Ordered[0], Is.EqualTo(DetailTabs.Default));
    }

    [TestCase(DetailTab.Metrics, "Metrics")]
    [TestCase(DetailTab.Topology, "Topology")]
    [TestCase(DetailTab.Data, "Data")]
    public void Label_ReturnsDisplayName(DetailTab tab, string expected)
    {
        Assert.That(DetailTabs.Label(tab), Is.EqualTo(expected));
    }

    [Test]
    public void Label_UnknownTab_Throws()
    {
        Assert.That(() => DetailTabs.Label((DetailTab)99), Throws.TypeOf<ArgumentOutOfRangeException>());
    }
}
