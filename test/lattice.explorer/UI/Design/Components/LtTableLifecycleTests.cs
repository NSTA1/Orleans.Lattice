using Bunit;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtTableLifecycleTests : ShellDesignTestContext
{
    [Test]
    public void Sort_null_keys_tie_stably_in_both_directions()
    {
        var cut = RenderTable();

        cut.Find("thead button").Click();
        Assert.That(Names(cut), Is.EqualTo(new[] { "missing-first", "missing-second", "one", "two" }));
        Assert.That(cut.Instance.DisplayedRows.Select(row => row.Name), Is.EqualTo(Names(cut)));
        cut.Find("thead button").Click();
        Assert.That(Names(cut), Is.EqualTo(new[] { "two", "one", "missing-first", "missing-second" }));
        cut.Find("thead button").Click();
        Assert.That(Names(cut), Is.EqualTo(new[] { "missing-first", "missing-second", "one", "two" }));
    }

    [Test]
    public void Removing_the_sorted_column_restores_natural_order_and_removes_sort_state()
    {
        var cut = RenderTable();
        cut.Find("thead button").Click();
        Assert.That(Names(cut), Is.EqualTo(new[] { "missing-first", "missing-second", "one", "two" }));

        cut.Render(p => p.Add(x => x.ChildContent, Columns(includeSort: false)));

        Assert.Multiple(() =>
        {
            Assert.That(Names(cut), Is.EqualTo(new[] { "two", "missing-first", "one", "missing-second" }));
            Assert.That(cut.FindAll("thead button, th[aria-sort]"), Is.Empty);
            Assert.That(cut.FindAll("thead th"), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void Removing_an_unsorted_column_preserves_the_active_sort()
    {
        var cut = RenderTable();
        cut.Find("thead button").Click();

        cut.Render(p => p.Add(x => x.ChildContent, Columns(includeSort: true, includeName: false)));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("thead th").GetAttribute("aria-sort"), Is.EqualTo("ascending"));
            Assert.That(cut.FindAll("tbody tr").Select(row => row.TextContent), Is.EqualTo(new[] { "", "", "1", "2" }));
        });
    }

    [Test]
    public void Cell_right_aligned_mono_content_preserves_both_styles_and_the_full_value()
    {
        var cut = Render<LtTable<string>>(p => p.Add(x => x.Caption, "Ids").Add(x => x.Items, new[] { "0123456789" })
            .AddChildContent<LtColumn<string>>(column => column
                .Add(x => x.Title, "Id").Add(x => x.Value, value => value)
                .Add(x => x.Mono, true).Add(x => x.Align, LtColumnAlign.End)));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("tbody td").ClassList, Does.Contain("lt-table__cell--end").And.Contain("lt-table__cell--mono"));
            Assert.That(cut.Find("tbody td").HasAttribute("title"), Is.False, "end-aligned figures are not truncated and need no tooltip");
            Assert.That(cut.Find("tbody td").TextContent, Is.EqualTo("0123456789"));
        });
    }

    [Test]
    public void Compact_null_row_can_open_a_detail_sheet_using_the_caption_as_its_title()
    {
        var cut = Render<CascadingValue<LtBreakpoint?>>(p => p
            .Add(x => x.Name, LtBreakpointCascade.Name).Add(x => x.Value, LtBreakpoint.Compact)
            .AddChildContent<LtTable<string?>>(table => table
                .Add(x => x.Caption, "Optional ids").Add(x => x.Items, new string?[] { null })
                .AddChildContent<LtColumn<string?>>(column => column
                    .Add(x => x.Title, "Id").Add(x => x.Value, value => value))));

        cut.Find(".lt-table-list__open").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[role=dialog]").TextContent, Does.Contain("Optional ids"));
            Assert.That(cut.FindAll(".lt-table-list__row"), Has.Count.EqualTo(1));
        });
    }

    private IRenderedComponent<LtTable<(string Name, int? Key)>> RenderTable() =>
        Render<LtTable<(string Name, int? Key)>>(p => p
            .Add(x => x.Caption, "Rows")
            .Add(x => x.Items, new (string Name, int? Key)[] { ("two", 2), ("missing-first", null), ("one", 1), ("missing-second", null) })
            .Add(x => x.ChildContent, Columns(includeSort: true)));

    [TestCase(false)]
    [TestCase(true)]
    public void Changing_the_sorted_columns_key_reorders_existing_rows(bool descending)
    {
        var cut = RenderTable();
        cut.Find("thead button").Click();
        if (descending)
        {
            cut.Find("thead button").Click();
        }

        cut.Render(p => p.Add(x => x.ChildContent,
            Columns(includeSort: true, sortBy: row => row.Key is { } key ? -key : null)));

        Assert.Multiple(() =>
        {
            Assert.That(Names(cut), Is.EqualTo(descending
                ? new[] { "one", "two", "missing-first", "missing-second" }
                : new[] { "missing-first", "missing-second", "two", "one" }));
            Assert.That(cut.FindAll("th[aria-sort]").Single().GetAttribute("aria-sort"),
                Is.EqualTo(descending ? "descending" : "ascending"));
        });
    }

    [Test]
    public void Clearing_the_sorted_columns_key_restores_natural_order_and_removes_sort_state()
    {
        var cut = RenderTable();
        cut.Find("thead button").Click();

        cut.Render(p => p.Add(x => x.ChildContent, Columns(includeSort: true, canSort: false)));

        Assert.Multiple(() =>
        {
            Assert.That(Names(cut), Is.EqualTo(new[] { "two", "missing-first", "one", "missing-second" }));
            Assert.That(cut.FindAll("thead button, th[aria-sort]"), Is.Empty);
        });
    }

    private static RenderFragment Columns(bool includeSort, bool includeName = true,
        Func<(string Name, int? Key), IComparable?>? sortBy = null, bool canSort = true) => builder =>
    {
        if (includeName)
        {
            builder.OpenComponent<LtColumn<(string Name, int? Key)>>(0);
            builder.AddComponentParameter(1, nameof(LtColumn<(string, int?)>.Title), "Name");
            builder.AddComponentParameter(2, nameof(LtColumn<(string, int?)>.Value), (Func<(string Name, int? Key), object?>)(row => row.Name));
            builder.CloseComponent();
        }

        if (includeSort)
        {
            builder.OpenComponent<LtColumn<(string Name, int? Key)>>(3);
            builder.AddComponentParameter(4, nameof(LtColumn<(string, int?)>.Title), "Key");
            builder.AddComponentParameter(5, nameof(LtColumn<(string, int?)>.Value), (Func<(string Name, int? Key), object?>)(row => row.Key));
            builder.AddComponentParameter(6, nameof(LtColumn<(string, int?)>.SortBy),
                canSort ? sortBy ?? (Func<(string Name, int? Key), IComparable?>)(row => row.Key) : null);
            builder.CloseComponent();
        }
    };

    private static string[] Names(IRenderedComponent<LtTable<(string Name, int? Key)>> cut) =>
        [.. cut.FindAll("tbody tr").Select(row => row.Children[0].TextContent)];
}
