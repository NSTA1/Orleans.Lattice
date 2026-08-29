using Orleans.Lattice.Explorer.MyTenant;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The Metrics surface's optional-section resolution, which exists so the panel
/// injects a plugin-owned type rather than a service provider it could ask for
/// anything.
/// </summary>
[TestFixture]
public sealed class MyTenantMetricsSectionAccessorTests
{
    private sealed class StubSection : IMyTenantMetricsSection
    {
        public Type ViewType => typeof(StubSection);

        public string Label => "Tenant metrics";
    }

    [Test]
    public void A_head_that_registered_no_section_gets_the_placeholder()
    {
        var accessor = new MyTenantMetricsSectionAccessor();

        Assert.Multiple(() =>
        {
            Assert.That(accessor.Section, Is.Null);
            Assert.That(accessor.HasSection, Is.False);
        });
    }

    [Test]
    public void A_registered_section_is_handed_through()
    {
        var section = new StubSection();

        var accessor = new MyTenantMetricsSectionAccessor(section);

        Assert.Multiple(() =>
        {
            Assert.That(accessor.Section, Is.SameAs(section));
            Assert.That(accessor.HasSection, Is.True);
            Assert.That(accessor.Section!.ViewType, Is.EqualTo(typeof(StubSection)));
            Assert.That(accessor.Section.Label, Is.EqualTo("Tenant metrics"));
        });
    }
}
