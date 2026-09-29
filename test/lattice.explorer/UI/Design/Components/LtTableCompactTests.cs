using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// The table's compact form below the small breakpoint: two-line list rows that
/// each open a detail sheet (focus trapped and returned to the row), the default
/// lines drawn from the columns, a "Sort by" select, and the scroll-frame
/// fallback for matrix-shaped data.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtTableCompactTests : ShellDesignTestContext
{
    private static readonly Order[] Orders =
    [
        new("order/2026-09/10231", "shipped", 129.90m, "14:02:11"),
        new("order/2026-09/10232", "open", 18.00m, "14:02:09"),
        new("order/2026-09/10233", "picking", 402.15m, "14:01:58"),
    ];

    [Test]
    public void Without_a_measured_band_the_booktabs_table_renders()
    {
        var cut = RenderOrders(breakpoint: null);

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("table.lt-table"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll(".lt-table-list"), Is.Empty);
        });
    }

    [Test]
    [TestCase(1)]
    [TestCase(2)]
    public void Medium_and_expanded_keep_the_table(int band)
    {
        var cut = RenderOrders((LtBreakpoint)band);

        Assert.That(cut.FindAll("table.lt-table"), Has.Count.EqualTo(1));
    }

    [Test]
    public void Compact_turns_every_row_into_a_two_line_list_row()
    {
        var cut = RenderOrders(LtBreakpoint.Compact);

        var rows = cut.FindAll(".lt-table-list__row");
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("table"), Is.Empty);
            Assert.That(cut.Find(".lt-table-list").GetAttribute("role"), Is.EqualTo("region"));
            Assert.That(cut.Find(".lt-table-list").GetAttribute("aria-labelledby"), Is.EqualTo(cut.Find(".lt-table__caption").Id));
            Assert.That(rows, Has.Count.EqualTo(3));
            Assert.That(rows[0].QuerySelector("button")!.GetAttribute("aria-haspopup"), Is.EqualTo("dialog"));
            Assert.That(rows[0].QuerySelector(".lt-compact-row__primary")!.TextContent, Is.EqualTo("order/2026-09/10231"));
            Assert.That(rows[0].QuerySelector(".lt-compact-row__primary")!.ClassList, Does.Contain("lt-compact-row__primary--mono"),
                "the row-header column is mono, so line one is");
            Assert.That(rows[0].QuerySelectorAll(".lt-compact-row__term").Select(term => term.TextContent),
                Is.EqualTo(new[] { "Status", "Total", "Updated" }), "line two is the next three columns");
        });
    }

    [Test]
    public void A_compact_row_template_draws_line_one_and_line_two_with_its_state()
    {
        var cut = RenderOrders(LtBreakpoint.Compact, table => table.Add(x => x.CompactRow, order => builder =>
        {
            builder.OpenComponent<LtCompactRow>(0);
            builder.AddComponentParameter(1, nameof(LtCompactRow.Mono), true);
            builder.AddComponentParameter(2, nameof(LtCompactRow.Primary), (RenderFragment)(b => b.AddContent(0, order.Key)));
            builder.AddComponentParameter(3, nameof(LtCompactRow.Secondary), (RenderFragment)(b => b.AddContent(0, order.Updated)));
            builder.AddComponentParameter(4, nameof(LtCompactRow.State), (RenderFragment)(b =>
            {
                b.OpenComponent<LtStatusPill>(0);
                b.AddComponentParameter(1, nameof(LtStatusPill.State), LtStateRole.Healthy);
                b.AddComponentParameter(2, nameof(LtStatusPill.Text), order.Status);
                b.CloseComponent();
            }));
            builder.CloseComponent();
        }));

        var secondary = cut.FindAll(".lt-compact-row__secondary")[0];
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-compact-row__primary")[0].TextContent, Is.EqualTo("order/2026-09/10231"));
            Assert.That(secondary.FirstElementChild!.ClassList, Does.Contain("lt-pill"), "the state leads line two");
            Assert.That(secondary.TextContent, Does.Contain("14:02:11"));
        });
    }

    [Test]
    public void Tapping_a_row_opens_its_detail_sheet_with_every_column_and_its_actions()
    {
        var cut = RenderOrders(LtBreakpoint.Compact, table => table
            .Add(x => x.DetailActions, order => builder =>
            {
                builder.OpenElement(0, "button");
                builder.AddAttribute(1, "data-action", "cancel");
                builder.AddContent(2, "Cancel " + order.Key);
                builder.CloseElement();
            }));

        cut.FindAll(".lt-table-list__open")[2].Click();

        var sheet = cut.Find("[role='dialog']");
        Assert.Multiple(() =>
        {
            Assert.That(sheet.ClassList, Does.Contain("lt-dialog--sheet").And.Contain("lt-dialog--end"));
            Assert.That(sheet.GetAttribute("aria-modal"), Is.EqualTo("true"));
            Assert.That(sheet.QuerySelector("h2")!.TextContent, Is.EqualTo("order/2026-09/10233"), "the row header titles the sheet");
            Assert.That(sheet.QuerySelectorAll(".lt-dl__term").Select(term => term.TextContent), Is.EqualTo(new[] { "Key", "Status", "Total", "Updated" }));
            Assert.That(sheet.QuerySelectorAll(".lt-dl__value")[1].TextContent, Is.EqualTo("picking"));
            Assert.That(sheet.QuerySelector("[data-action='cancel']")!.TextContent, Is.EqualTo("Cancel order/2026-09/10233"));
            Assert.That(cut.FindAll(".lt-dialog__sentinel"), Has.Count.EqualTo(2), "focus is trapped");
        });
    }

    [Test]
    public void Closing_the_sheet_returns_focus_to_its_row()
    {
        var cut = RenderOrders(LtBreakpoint.Compact);
        cut.FindAll(".lt-table-list__open")[1].Click();
        var before = JSInterop.Invocations.Count;

        cut.Find("[role='dialog']").KeyDown(new KeyboardEventArgs { Key = "Escape" });

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[role='dialog']"), Is.Empty);
            Assert.That(JSInterop.Invocations.Skip(before).Any(invocation => invocation.Identifier.EndsWith("focus", StringComparison.Ordinal)), Is.True);
        });
    }

    [Test]
    public void A_detail_template_and_title_replace_the_defaults()
    {
        var cut = RenderOrders(LtBreakpoint.Compact, table => table
            .Add(x => x.DetailTitle, order => "Order " + order.Key[^5..])
            .Add(x => x.Detail, order => builder => builder.AddMarkupContent(0, "<p class=\"probe\">" + order.Total + "</p>")));

        cut.FindAll(".lt-table-list__open")[0].Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[role='dialog'] h2").TextContent, Is.EqualTo("Order 10231"));
            Assert.That(cut.Find("[role='dialog'] .probe").TextContent, Is.EqualTo("129.90"));
            Assert.That(cut.FindAll("[role='dialog'] .lt-dl"), Is.Empty);
        });
    }

    [Test]
    public void The_current_row_keeps_aria_current_in_the_list()
    {
        var cut = RenderOrders(LtBreakpoint.Compact, table => table.Add(x => x.IsCurrent, order => order.Status == "picking"));

        Assert.That(cut.FindAll(".lt-table-list__row[aria-current='true']").Single().TextContent, Does.Contain("10233"));
    }

    [Test]
    public void The_list_sorts_through_a_select()
    {
        var cut = RenderOrders(LtBreakpoint.Compact);

        var select = cut.Find(".lt-table-list__sort select");
        Assert.That(select.QuerySelectorAll("option").Select(option => option.TextContent), Is.EqualTo(new[]
        {
            "Natural order", "Key, ascending", "Key, descending", "Total, ascending", "Total, descending",
        }));

        select.Change(select.QuerySelectorAll("option")[4].GetAttribute("value"));
        Assert.That(Keys(cut), Is.EqualTo(new[] { "10233", "10231", "10232" }));

        cut.Find(".lt-table-list__sort select").Change(string.Empty);
        Assert.That(Keys(cut), Is.EqualTo(new[] { "10231", "10232", "10233" }));

        cut.Find(".lt-table-list__sort select").Change("99:ascending");
        Assert.That(Keys(cut), Is.EqualTo(new[] { "10231", "10232", "10233" }), "an unknown value is the natural order");
    }

    [Test]
    public void Without_sortable_columns_there_is_no_sort_select()
    {
        var cut = Render<LtTable<Order>>(p =>
        {
            p.AddCascadingValue(LtBreakpointCascade.Name, LtBreakpoint.Compact);
            p.Add(x => x.Items, Orders).Add(x => x.Caption, "Orders");
            p.AddChildContent<LtColumn<Order>>(column => column.Add(x => x.Title, "Key").Add(x => x.Value, order => order.Key));
        });

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-table-list__sort"), Is.Empty);
            Assert.That(cut.FindAll(".lt-compact-row__primary")[0].ClassList, Does.Not.Contain("lt-compact-row__primary--mono"));
        });
    }

    [Test]
    public void An_empty_list_says_so()
    {
        var cut = RenderOrders(LtBreakpoint.Compact, rows: []);

        Assert.That(cut.Find(".lt-table-list__empty").TextContent.Trim(), Is.EqualTo("No rows."));
    }

    [Test]
    public void A_virtualised_list_renders_its_rows()
    {
        var cut = RenderOrders(LtBreakpoint.Compact, table => table.Add(x => x.Virtualize, true).Add(x => x.CompactRowHeight, 60f));

        Assert.That(cut.FindAll(".lt-table-list__row"), Is.Not.Empty);
    }

    [Test]
    public void Matrix_shaped_data_keeps_the_table_in_its_scroll_frame()
    {
        var cut = RenderOrders(LtBreakpoint.Compact, table => table.Add(x => x.Compact, LtTableCompact.ScrollFrame));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-table-frame table"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll(".lt-table-list"), Is.Empty);
        });
    }

    [Test]
    public void The_defaults_are_the_list_a_64px_compact_row_and_a_44px_row()
    {
        var table = new LtTable<Order>();

        Assert.Multiple(() =>
        {
            Assert.That(table.Compact, Is.EqualTo(LtTableCompact.List));
            Assert.That(table.CompactRowHeight, Is.EqualTo(64f));
            Assert.That(table.RowHeight, Is.EqualTo(44f));
        });
    }

    [Test]
    public void A_dialog_is_centred_unless_placed_as_a_sheet()
    {
        var centred = Render<LtDialog>(p => p.Add(x => x.Open, true).Add(x => x.Title, "Question"));
        var start = Render<LtDialog>(p => p.Add(x => x.Open, true).Add(x => x.Title, "Directory").Add(x => x.Placement, LtDialogPlacement.Start));

        Assert.Multiple(() =>
        {
            Assert.That(centred.Find(".lt-dialog").ClassName, Is.EqualTo("lt-dialog"));
            Assert.That(centred.Find(".lt-dialog-layer").ClassName, Is.EqualTo("lt-dialog-layer"));
            Assert.That(start.Find(".lt-dialog").ClassList, Does.Contain("lt-dialog--sheet").And.Contain("lt-dialog--start"));
            Assert.That(start.Find(".lt-dialog-layer").ClassList, Does.Contain("lt-dialog-layer--sheet"));
        });
    }

    private static IEnumerable<string> Keys(IRenderedComponent<LtTable<Order>> cut) =>
        cut.FindAll(".lt-compact-row__primary").Select(primary => primary.TextContent[^5..]);

    private IRenderedComponent<LtTable<Order>> RenderOrders(
        LtBreakpoint? breakpoint,
        Action<ComponentParameterCollectionBuilder<LtTable<Order>>>? configure = null,
        IReadOnlyList<Order>? rows = null) =>
        Render<LtTable<Order>>(p =>
        {
            if (breakpoint is { } band)
            {
                p.AddCascadingValue(LtBreakpointCascade.Name, band);
            }

            p.Add(x => x.Items, rows ?? Orders).Add(x => x.Caption, "Orders").Add(x => x.RowKey, order => order.Key);
            p.AddChildContent<LtColumn<Order>>(column => column
                .Add(x => x.Title, "Key").Add(x => x.Value, order => order.Key).Add(x => x.SortBy, order => order.Key)
                .Add(x => x.Mono, true).Add(x => x.RowHeader, true));
            p.AddChildContent<LtColumn<Order>>(column => column.Add(x => x.Title, "Status").Add(x => x.Value, order => order.Status));
            p.AddChildContent<LtColumn<Order>>(column => column
                .Add(x => x.Title, "Total").Add(x => x.Value, order => order.Total.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture))
                .Add(x => x.SortBy, order => order.Total).Add(x => x.Align, LtColumnAlign.End));
            p.AddChildContent<LtColumn<Order>>(column => column.Add(x => x.Title, "Updated").Add(x => x.Value, order => order.Updated));
            configure?.Invoke(p);
        });

    /// <summary>One row of the table under test.</summary>
    /// <param name="Key">The key.</param>
    /// <param name="Status">The status.</param>
    /// <param name="Total">The total.</param>
    /// <param name="Updated">When it was last updated.</param>
    public sealed record Order(string Key, string Status, decimal Total, string Updated);
}
