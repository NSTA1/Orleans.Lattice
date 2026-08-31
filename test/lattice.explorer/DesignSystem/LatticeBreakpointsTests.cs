using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Unit tests for the .NET half of the breakpoint token layer: the canonical
/// widths, the resolution from a viewport measurement, the stable names, and
/// the inline capacities the adaptive primitives size themselves from.
/// </summary>
[TestFixture]
public sealed class LatticeBreakpointsTests
{
    [Test]
    public void Breakpoints_are_ordered_ascending_by_width()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeBreakpoint.Compact, Is.LessThan(LatticeBreakpoint.Medium));
            Assert.That(LatticeBreakpoint.Medium, Is.LessThan(LatticeBreakpoint.Expanded));
            Assert.That(LatticeBreakpoints.MediumMinimumWidth, Is.LessThan(LatticeBreakpoints.ExpandedMinimumWidth));
        });
    }

    [Test]
    public void All_lists_every_breakpoint_narrowest_first()
    {
        Assert.That(LatticeBreakpoints.All, Is.EqualTo(new[]
        {
            LatticeBreakpoint.Compact,
            LatticeBreakpoint.Medium,
            LatticeBreakpoint.Expanded,
        }));
    }

    [Test]
    public void All_covers_every_declared_enum_member()
    {
        Assert.That(LatticeBreakpoints.All, Is.EquivalentTo(Enum.GetValues<LatticeBreakpoint>()));
    }

    [Test]
    public void All_returns_the_same_instance_so_enumeration_allocates_no_array()
    {
        Assert.That(LatticeBreakpoints.All, Is.SameAs(LatticeBreakpoints.All));
    }

    [TestCase(0, LatticeBreakpoint.Compact)]
    [TestCase(320, LatticeBreakpoint.Compact)]
    [TestCase(LatticeBreakpoints.MediumMinimumWidth - 1, LatticeBreakpoint.Compact)]
    [TestCase(LatticeBreakpoints.MediumMinimumWidth, LatticeBreakpoint.Medium)]
    [TestCase(800, LatticeBreakpoint.Medium)]
    [TestCase(LatticeBreakpoints.ExpandedMinimumWidth - 1, LatticeBreakpoint.Medium)]
    [TestCase(LatticeBreakpoints.ExpandedMinimumWidth, LatticeBreakpoint.Expanded)]
    [TestCase(3840, LatticeBreakpoint.Expanded)]
    public void Resolve_maps_a_width_to_its_breakpoint(int width, LatticeBreakpoint expected)
    {
        Assert.That(LatticeBreakpoints.Resolve(width), Is.EqualTo(expected));
    }

    [Test]
    public void Resolve_degrades_a_negative_width_to_compact_rather_than_throwing()
    {
        Assert.That(LatticeBreakpoints.Resolve(-1), Is.EqualTo(LatticeBreakpoint.Compact));
        Assert.That(LatticeBreakpoints.Resolve(int.MinValue), Is.EqualTo(LatticeBreakpoint.Compact));
    }

    [Test]
    public void Resolve_maps_the_widest_representable_width_to_expanded()
    {
        Assert.That(LatticeBreakpoints.Resolve(int.MaxValue), Is.EqualTo(LatticeBreakpoint.Expanded));
    }

    [TestCase(LatticeBreakpoint.Compact, 0)]
    [TestCase(LatticeBreakpoint.Medium, LatticeBreakpoints.MediumMinimumWidth)]
    [TestCase(LatticeBreakpoint.Expanded, LatticeBreakpoints.ExpandedMinimumWidth)]
    public void MinimumWidth_returns_the_breakpoints_inclusive_lower_bound(
        LatticeBreakpoint breakpoint, int expected)
    {
        Assert.That(LatticeBreakpoints.MinimumWidth(breakpoint), Is.EqualTo(expected));
    }

    [Test]
    public void MinimumWidth_round_trips_through_resolve_for_every_breakpoint()
    {
        foreach (var breakpoint in LatticeBreakpoints.All)
        {
            Assert.That(
                LatticeBreakpoints.Resolve(LatticeBreakpoints.MinimumWidth(breakpoint)),
                Is.EqualTo(breakpoint),
                $"the minimum width of {breakpoint} must resolve back to it");
        }
    }

    [Test]
    public void MinimumWidth_rejects_an_undeclared_breakpoint()
    {
        Assert.That(
            () => LatticeBreakpoints.MinimumWidth((LatticeBreakpoint)42),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [TestCase(LatticeBreakpoint.Compact, LatticeBreakpoints.CompactNominalWidth)]
    [TestCase(LatticeBreakpoint.Medium, LatticeBreakpoints.MediumNominalWidth)]
    [TestCase(LatticeBreakpoint.Expanded, LatticeBreakpoints.ExpandedNominalWidth)]
    public void NominalWidth_returns_a_representative_width_for_the_band(
        LatticeBreakpoint breakpoint, int expected)
    {
        Assert.That(LatticeBreakpoints.NominalWidth(breakpoint), Is.EqualTo(expected));
    }

    [Test]
    public void NominalWidth_resolves_back_to_the_breakpoint_it_describes()
    {
        foreach (var breakpoint in LatticeBreakpoints.All)
        {
            Assert.That(
                LatticeBreakpoints.Resolve(LatticeBreakpoints.NominalWidth(breakpoint)),
                Is.EqualTo(breakpoint),
                "a nominal width that fell in another band would size a layout for the "
                + "wrong shape");
        }
    }

    [Test]
    public void NominalWidth_is_non_zero_for_every_breakpoint()
    {
        // The reason this exists rather than reusing MinimumWidth: compact's
        // lower bound is zero, which is useless as a width to measure against.
        foreach (var breakpoint in LatticeBreakpoints.All)
        {
            Assert.That(LatticeBreakpoints.NominalWidth(breakpoint), Is.GreaterThan(0));
        }
    }

    [Test]
    public void NominalWidth_increases_with_the_band()
    {
        Assert.That(
            LatticeBreakpoints.NominalWidth(LatticeBreakpoint.Compact),
            Is.LessThan(LatticeBreakpoints.NominalWidth(LatticeBreakpoint.Medium)));
        Assert.That(
            LatticeBreakpoints.NominalWidth(LatticeBreakpoint.Medium),
            Is.LessThan(LatticeBreakpoints.NominalWidth(LatticeBreakpoint.Expanded)));
    }

    [Test]
    public void NominalWidth_rejects_an_undeclared_breakpoint()
    {
        Assert.That(
            () => LatticeBreakpoints.NominalWidth((LatticeBreakpoint)42),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [TestCase(LatticeBreakpoint.Compact, LatticeBreakpoints.CompactName)]
    [TestCase(LatticeBreakpoint.Medium, LatticeBreakpoints.MediumName)]
    [TestCase(LatticeBreakpoint.Expanded, LatticeBreakpoints.ExpandedName)]
    public void Name_returns_the_stable_lowercase_name(LatticeBreakpoint breakpoint, string expected)
    {
        Assert.That(LatticeBreakpoints.Name(breakpoint), Is.EqualTo(expected));
    }

    [Test]
    public void Name_returns_an_interned_literal_so_a_render_path_allocates_nothing()
    {
        foreach (var breakpoint in LatticeBreakpoints.All)
        {
            Assert.That(
                LatticeBreakpoints.Name(breakpoint),
                Is.SameAs(LatticeBreakpoints.Name(breakpoint)),
                $"{breakpoint} must return the same string instance on every call");
        }
    }

    [Test]
    public void Name_rejects_an_undeclared_breakpoint()
    {
        Assert.That(
            () => LatticeBreakpoints.Name((LatticeBreakpoint)42),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void TryParseName_round_trips_every_name_produced_by_Name()
    {
        foreach (var breakpoint in LatticeBreakpoints.All)
        {
            Assert.That(
                LatticeBreakpoints.TryParseName(LatticeBreakpoints.Name(breakpoint), out var parsed),
                Is.True);
            Assert.That(parsed, Is.EqualTo(breakpoint));
        }
    }

    [TestCase("COMPACT", LatticeBreakpoint.Compact)]
    [TestCase("Medium", LatticeBreakpoint.Medium)]
    [TestCase("eXpAnDeD", LatticeBreakpoint.Expanded)]
    public void TryParseName_is_case_insensitive(string name, LatticeBreakpoint expected)
    {
        Assert.That(LatticeBreakpoints.TryParseName(name, out var parsed), Is.True);
        Assert.That(parsed, Is.EqualTo(expected));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("tiny")]
    [TestCase("compact ")]
    public void TryParseName_returns_false_and_the_default_for_an_unknown_name(string? name)
    {
        Assert.That(LatticeBreakpoints.TryParseName(name, out var parsed), Is.False);
        Assert.That(parsed, Is.EqualTo(LatticeBreakpoints.Default));
    }

    [Test]
    public void Default_is_expanded_so_an_unmeasured_head_renders_the_shipped_layout()
    {
        Assert.That(LatticeBreakpoints.Default, Is.EqualTo(LatticeBreakpoint.Expanded));
    }

    [TestCase(LatticeBreakpoint.Compact, LatticeBreakpoint.Compact, true)]
    [TestCase(LatticeBreakpoint.Compact, LatticeBreakpoint.Medium, false)]
    [TestCase(LatticeBreakpoint.Compact, LatticeBreakpoint.Expanded, false)]
    [TestCase(LatticeBreakpoint.Medium, LatticeBreakpoint.Compact, true)]
    [TestCase(LatticeBreakpoint.Medium, LatticeBreakpoint.Medium, true)]
    [TestCase(LatticeBreakpoint.Medium, LatticeBreakpoint.Expanded, false)]
    [TestCase(LatticeBreakpoint.Expanded, LatticeBreakpoint.Compact, true)]
    [TestCase(LatticeBreakpoint.Expanded, LatticeBreakpoint.Medium, true)]
    [TestCase(LatticeBreakpoint.Expanded, LatticeBreakpoint.Expanded, true)]
    public void IsAtLeast_compares_by_width_order(
        LatticeBreakpoint breakpoint, LatticeBreakpoint minimum, bool expected)
    {
        Assert.That(breakpoint.IsAtLeast(minimum), Is.EqualTo(expected));
    }

    [TestCase(LatticeBreakpoint.Compact, LatticeBreakpoints.CompactTabInlineCapacity)]
    [TestCase(LatticeBreakpoint.Medium, LatticeBreakpoints.MediumTabInlineCapacity)]
    [TestCase(LatticeBreakpoint.Expanded, LatticeBreakpoints.ExpandedTabInlineCapacity)]
    public void TabInlineCapacity_returns_the_breakpoints_token(
        LatticeBreakpoint breakpoint, int expected)
    {
        Assert.That(LatticeBreakpoints.TabInlineCapacity(breakpoint), Is.EqualTo(expected));
    }

    [Test]
    public void TabInlineCapacity_is_at_least_one_and_widens_with_the_viewport()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeBreakpoints.TabInlineCapacity(LatticeBreakpoint.Compact), Is.GreaterThanOrEqualTo(1));
            Assert.That(
                LatticeBreakpoints.TabInlineCapacity(LatticeBreakpoint.Medium),
                Is.GreaterThan(LatticeBreakpoints.TabInlineCapacity(LatticeBreakpoint.Compact)));
            Assert.That(
                LatticeBreakpoints.TabInlineCapacity(LatticeBreakpoint.Expanded),
                Is.GreaterThan(LatticeBreakpoints.TabInlineCapacity(LatticeBreakpoint.Medium)));
        });
    }

    [Test]
    public void TabInlineCapacity_rejects_an_undeclared_breakpoint()
    {
        Assert.That(
            () => LatticeBreakpoints.TabInlineCapacity((LatticeBreakpoint)42),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void NavigationInlineCapacity_bounds_only_the_compact_bar()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeBreakpoints.NavigationInlineCapacity(LatticeBreakpoint.Compact),
                Is.EqualTo(LatticeBreakpoints.CompactNavigationInlineCapacity));
            Assert.That(
                LatticeBreakpoints.NavigationInlineCapacity(LatticeBreakpoint.Medium),
                Is.EqualTo(int.MaxValue));
            Assert.That(
                LatticeBreakpoints.NavigationInlineCapacity(LatticeBreakpoint.Expanded),
                Is.EqualTo(int.MaxValue));
        });
    }

    [Test]
    public void NavigationInlineCapacity_rejects_an_undeclared_breakpoint()
    {
        Assert.That(
            () => LatticeBreakpoints.NavigationInlineCapacity((LatticeBreakpoint)42),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Custom_property_names_match_the_declared_stylesheet_convention()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeBreakpoints.MediumMinimumWidthCustomProperty,
                Is.EqualTo("--lx-breakpoint-medium-min"));
            Assert.That(
                LatticeBreakpoints.ExpandedMinimumWidthCustomProperty,
                Is.EqualTo("--lx-breakpoint-expanded-min"));
        });
    }
}
