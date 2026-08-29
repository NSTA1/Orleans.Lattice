using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Unit tests for the design system's declarative descriptors: the navigation
/// destination, the tab, the column, and the cascaded shell context.
/// </summary>
[TestFixture]
public sealed class LatticeDescriptorTests
{
    [Test]
    public void A_nav_item_is_enabled_by_default_and_has_no_description()
    {
        var item = new LatticeNavItem("backups", "Backups");

        Assert.Multiple(() =>
        {
            Assert.That(item.Id, Is.EqualTo("backups"));
            Assert.That(item.Label, Is.EqualTo("Backups"));
            Assert.That(item.IsEnabled, Is.True);
            Assert.That(item.Description, Is.Null);
            Assert.That(item.ShortLabel, Is.Null);
        });
    }

    [Test]
    public void A_nav_item_falls_back_to_its_label_in_a_space_constrained_slot()
    {
        var item = new LatticeNavItem("dead-letter", "Dead letters");

        Assert.That(item.CompactLabel, Is.EqualTo("Dead letters"));
    }

    [Test]
    public void A_nav_item_prefers_its_short_label_in_a_space_constrained_slot()
    {
        var item = new LatticeNavItem("dead-letter", "Dead letters") { ShortLabel = "DLQ" };

        Assert.That(item.CompactLabel, Is.EqualTo("DLQ"));
    }

    [Test]
    public void A_nav_item_carries_its_disabled_state_and_reason()
    {
        var item = new LatticeNavItem("access", "Access")
        {
            IsEnabled = false,
            Description = "Access is not available for your account.",
        };

        Assert.Multiple(() =>
        {
            Assert.That(item.IsEnabled, Is.False);
            Assert.That(item.Description, Is.EqualTo("Access is not available for your account."));
        });
    }

    [Test]
    public void Nav_items_with_the_same_values_are_equal()
    {
        var first = new LatticeNavItem("a", "A") { ShortLabel = "a", IsEnabled = false, Description = "why" };
        var second = new LatticeNavItem("a", "A") { ShortLabel = "a", IsEnabled = false, Description = "why" };

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(second));
            Assert.That(first.GetHashCode(), Is.EqualTo(second.GetHashCode()));
        });
    }

    [Test]
    public void A_tab_item_is_enabled_by_default_and_has_no_description()
    {
        var tab = new LatticeTabItem("metrics", "Metrics");

        Assert.Multiple(() =>
        {
            Assert.That(tab.Id, Is.EqualTo("metrics"));
            Assert.That(tab.Label, Is.EqualTo("Metrics"));
            Assert.That(tab.IsEnabled, Is.True);
            Assert.That(tab.Description, Is.Null);
        });
    }

    [Test]
    public void A_tab_item_carries_its_disabled_state_and_reason()
    {
        var tab = new LatticeTabItem("schema", "Schema")
        {
            IsEnabled = false,
            Description = "Schema administration is not installed.",
        };

        Assert.Multiple(() =>
        {
            Assert.That(tab.IsEnabled, Is.False);
            Assert.That(tab.Description, Is.EqualTo("Schema administration is not installed."));
        });
    }

    [Test]
    public void Tab_items_with_the_same_values_are_equal()
    {
        var first = new LatticeTabItem("a", "A") { IsEnabled = false, Description = "why" };
        var second = new LatticeTabItem("a", "A") { IsEnabled = false, Description = "why" };

        Assert.That(first, Is.EqualTo(second));
    }

    [Test]
    public void A_column_shows_on_compact_by_default_and_is_neither_primary_nor_code()
    {
        var column = new LatticeTableColumn<string>
        {
            Header = "Key",
            Cell = item => builder => builder.AddContent(0, item),
        };

        Assert.Multiple(() =>
        {
            Assert.That(column.Header, Is.EqualTo("Key"));
            Assert.That(column.Cell, Is.Not.Null);
            Assert.That(column.ShowOnCompact, Is.True);
            Assert.That(column.IsPrimary, Is.False);
            Assert.That(column.IsNumericOrCode, Is.False);
        });
    }

    [Test]
    public void A_column_carries_its_reflow_and_presentation_flags()
    {
        var column = new LatticeTableColumn<string>
        {
            Header = "Digest",
            Cell = item => builder => builder.AddContent(0, item),
            IsPrimary = true,
            ShowOnCompact = false,
            IsNumericOrCode = true,
        };

        Assert.Multiple(() =>
        {
            Assert.That(column.IsPrimary, Is.True);
            Assert.That(column.ShowOnCompact, Is.False);
            Assert.That(column.IsNumericOrCode, Is.True);
        });
    }

    [Test]
    public void A_columns_cell_projects_the_row_it_is_given()
    {
        var column = new LatticeTableColumn<int>
        {
            Header = "Value",
            Cell = item => builder => builder.AddContent(0, item),
        };

        RenderFragment fragment = column.Cell(7);

        Assert.That(fragment, Is.Not.Null);
    }

    [Test]
    public void The_unmeasured_context_is_the_default_breakpoint_and_the_standard_density()
    {
        var context = LatticeAdaptiveContext.Unmeasured;

        Assert.Multiple(() =>
        {
            Assert.That(context.Breakpoint, Is.EqualTo(LatticeBreakpoints.Default));
            Assert.That(context.Density, Is.EqualTo(LatticeDensity.Cosy));
            Assert.That(context.IsMeasured, Is.False);
        });
    }

    [Test]
    public void The_unmeasured_context_is_a_shared_instance()
    {
        Assert.That(LatticeAdaptiveContext.Unmeasured, Is.SameAs(LatticeAdaptiveContext.Unmeasured));
    }

    [Test]
    public void Contexts_with_the_same_values_are_equal()
    {
        var first = new LatticeAdaptiveContext(LatticeBreakpoint.Compact, LatticeDensity.Compact, true);
        var second = new LatticeAdaptiveContext(LatticeBreakpoint.Compact, LatticeDensity.Compact, true);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(second));
            Assert.That(first.GetHashCode(), Is.EqualTo(second.GetHashCode()));
        });
    }

    [Test]
    public void Contexts_that_differ_in_any_component_are_not_equal()
    {
        var baseline = new LatticeAdaptiveContext(LatticeBreakpoint.Compact, LatticeDensity.Compact, true);

        Assert.Multiple(() =>
        {
            Assert.That(baseline, Is.Not.EqualTo(baseline with { Breakpoint = LatticeBreakpoint.Medium }));
            Assert.That(baseline, Is.Not.EqualTo(baseline with { Density = LatticeDensity.Cosy }));
            Assert.That(baseline, Is.Not.EqualTo(baseline with { IsMeasured = false }));
        });
    }
}
