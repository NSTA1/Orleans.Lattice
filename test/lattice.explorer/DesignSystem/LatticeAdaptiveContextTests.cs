using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Unit tests for the shell context cascaded to every primitive, and in
/// particular for the width a measured layout sizes itself against.
/// </summary>
[TestFixture]
public sealed class LatticeAdaptiveContextTests
{
    [Test]
    public void Unmeasured_is_the_default_band_at_the_standard_density()
    {
        var context = LatticeAdaptiveContext.Unmeasured;

        Assert.Multiple(() =>
        {
            Assert.That(context.Breakpoint, Is.EqualTo(LatticeBreakpoints.Default));
            Assert.That(context.Density, Is.EqualTo(LatticeDensity.Cosy));
            Assert.That(context.IsMeasured, Is.False);
            Assert.That(context.ViewportWidth, Is.Null);
        });
    }

    [Test]
    public void A_context_without_a_width_is_still_constructible()
    {
        // The three-argument shape predates the width, and every existing
        // caller still compiles against it.
        var context = new LatticeAdaptiveContext(LatticeBreakpoint.Medium, LatticeDensity.Compact, true);

        Assert.That(context.ViewportWidth, Is.Null);
    }

    [Test]
    public void LayoutWidth_prefers_a_measured_width()
    {
        var context = new LatticeAdaptiveContext(
            LatticeBreakpoint.Compact, LatticeDensity.Cosy, IsMeasured: true, ViewportWidth: 412);

        Assert.That(context.LayoutWidth, Is.EqualTo(412));
    }

    [TestCase(LatticeBreakpoint.Compact)]
    [TestCase(LatticeBreakpoint.Medium)]
    [TestCase(LatticeBreakpoint.Expanded)]
    public void LayoutWidth_falls_back_to_the_bands_nominal_width(LatticeBreakpoint breakpoint)
    {
        var context = new LatticeAdaptiveContext(breakpoint, LatticeDensity.Cosy, IsMeasured: true);

        Assert.That(context.LayoutWidth, Is.EqualTo(LatticeBreakpoints.NominalWidth(breakpoint)));
    }

    [Test]
    public void LayoutWidth_is_never_zero_so_a_measurement_always_has_something_to_divide_by()
    {
        foreach (var breakpoint in LatticeBreakpoints.All)
        {
            var context = new LatticeAdaptiveContext(breakpoint, LatticeDensity.Cosy, IsMeasured: false);

            Assert.That(context.LayoutWidth, Is.GreaterThan(0));
        }
    }

    [Test]
    public void Two_contexts_with_the_same_values_are_equal()
    {
        Assert.That(
            new LatticeAdaptiveContext(LatticeBreakpoint.Medium, LatticeDensity.Cosy, true, 700),
            Is.EqualTo(new LatticeAdaptiveContext(LatticeBreakpoint.Medium, LatticeDensity.Cosy, true, 700)));
    }

    [Test]
    public void Two_contexts_differing_only_in_width_are_not_equal()
    {
        Assert.That(
            new LatticeAdaptiveContext(LatticeBreakpoint.Medium, LatticeDensity.Cosy, true, 700),
            Is.Not.EqualTo(new LatticeAdaptiveContext(LatticeBreakpoint.Medium, LatticeDensity.Cosy, true, 900)));
    }
}
