using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Unit tests for the viewport seam: the observable breakpoint state a head
/// drives and every adaptive primitive reads through the cascaded context.
/// </summary>
[TestFixture]
public sealed class LatticeViewportTests
{
    [Test]
    public void A_new_viewport_reports_the_default_breakpoint_and_no_measurement()
    {
        var viewport = new LatticeViewport();

        Assert.Multiple(() =>
        {
            Assert.That(viewport.Breakpoint, Is.EqualTo(LatticeBreakpoints.Default));
            Assert.That(viewport.IsMeasured, Is.False);
        });
    }

    [Test]
    public void SetBreakpoint_reports_a_change_and_raises_the_event_once()
    {
        var viewport = new LatticeViewport();
        var observed = new List<LatticeBreakpoint>();
        viewport.BreakpointChanged += observed.Add;

        var changed = viewport.SetBreakpoint(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            Assert.That(changed, Is.True);
            Assert.That(viewport.Breakpoint, Is.EqualTo(LatticeBreakpoint.Compact));
            Assert.That(viewport.IsMeasured, Is.True);
            Assert.That(observed, Is.EqualTo(new[] { LatticeBreakpoint.Compact }));
        });
    }

    [Test]
    public void SetBreakpoint_to_the_current_value_reports_no_change_and_raises_nothing()
    {
        var viewport = new LatticeViewport();
        viewport.SetBreakpoint(LatticeBreakpoint.Medium);

        var observed = new List<LatticeBreakpoint>();
        viewport.BreakpointChanged += observed.Add;

        var changed = viewport.SetBreakpoint(LatticeBreakpoint.Medium);

        Assert.Multiple(() =>
        {
            Assert.That(changed, Is.False);
            Assert.That(observed, Is.Empty);
        });
    }

    [Test]
    public void SetBreakpoint_marks_the_viewport_measured_even_when_it_matches_the_default()
    {
        var viewport = new LatticeViewport();

        var changed = viewport.SetBreakpoint(LatticeBreakpoints.Default);

        Assert.Multiple(() =>
        {
            Assert.That(changed, Is.False, "the value did not move");
            Assert.That(viewport.IsMeasured, Is.True, "but a real measurement arrived");
        });
    }

    [Test]
    public void SetBreakpoint_rejects_an_undeclared_breakpoint()
    {
        var viewport = new LatticeViewport();

        Assert.That(
            () => viewport.SetBreakpoint((LatticeBreakpoint)42),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void SetBreakpoint_leaves_the_state_untouched_when_it_rejects_a_value()
    {
        var viewport = new LatticeViewport();

        Assert.That(() => viewport.SetBreakpoint((LatticeBreakpoint)42), Throws.Exception);

        Assert.Multiple(() =>
        {
            Assert.That(viewport.Breakpoint, Is.EqualTo(LatticeBreakpoints.Default));
            Assert.That(viewport.IsMeasured, Is.False);
        });
    }

    [TestCase(320, LatticeBreakpoint.Compact)]
    [TestCase(LatticeBreakpoints.MediumMinimumWidth, LatticeBreakpoint.Medium)]
    [TestCase(LatticeBreakpoints.ExpandedMinimumWidth, LatticeBreakpoint.Expanded)]
    public void SetViewportWidth_resolves_the_width_to_a_breakpoint(int width, LatticeBreakpoint expected)
    {
        var viewport = new LatticeViewport();

        viewport.SetViewportWidth(width);

        Assert.That(viewport.Breakpoint, Is.EqualTo(expected));
    }

    [Test]
    public void SetViewportWidth_does_not_raise_when_the_resize_stays_inside_a_band()
    {
        var viewport = new LatticeViewport();
        viewport.SetViewportWidth(LatticeBreakpoints.MediumMinimumWidth);

        var raised = 0;
        viewport.BreakpointChanged += _ => raised++;

        viewport.SetViewportWidth(LatticeBreakpoints.MediumMinimumWidth + 1);
        viewport.SetViewportWidth(LatticeBreakpoints.ExpandedMinimumWidth - 1);

        Assert.Multiple(() =>
        {
            Assert.That(raised, Is.Zero);
            Assert.That(viewport.Breakpoint, Is.EqualTo(LatticeBreakpoint.Medium));
        });
    }

    [Test]
    public void SetViewportWidth_raises_once_per_boundary_crossing()
    {
        var viewport = new LatticeViewport();
        var observed = new List<LatticeBreakpoint>();
        viewport.BreakpointChanged += observed.Add;

        viewport.SetViewportWidth(320);
        viewport.SetViewportWidth(700);
        viewport.SetViewportWidth(1400);
        viewport.SetViewportWidth(320);

        Assert.That(observed, Is.EqualTo(new[]
        {
            LatticeBreakpoint.Compact,
            LatticeBreakpoint.Medium,
            LatticeBreakpoint.Expanded,
            LatticeBreakpoint.Compact,
        }));
    }

    [Test]
    public void An_unsubscribed_handler_stops_receiving_changes()
    {
        var viewport = new LatticeViewport();
        var observed = new List<LatticeBreakpoint>();
        void Handler(LatticeBreakpoint breakpoint) => observed.Add(breakpoint);

        viewport.BreakpointChanged += Handler;
        viewport.SetBreakpoint(LatticeBreakpoint.Compact);
        viewport.BreakpointChanged -= Handler;
        viewport.SetBreakpoint(LatticeBreakpoint.Medium);

        Assert.That(observed, Is.EqualTo(new[] { LatticeBreakpoint.Compact }));
    }

    [Test]
    public void A_viewport_with_no_subscribers_still_records_the_breakpoint()
    {
        ILatticeViewport viewport = new LatticeViewport();

        viewport.SetViewportWidth(320);

        Assert.That(viewport.Breakpoint, Is.EqualTo(LatticeBreakpoint.Compact));
    }
}
