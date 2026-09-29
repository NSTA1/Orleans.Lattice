using Microsoft.Extensions.DependencyInjection;
using Bunit;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// The booktabs table: a captioned, keyboard-scrollable region; headers that
/// sort from the keyboard and announce their order; row headers, mono and
/// numeric columns; a current row; an empty state; and virtualisation.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtTableTests : ShellDesignTestContext
{
    private static readonly TreeRow[] Trees =
    [
        new("orders", 1200),
        new("customers", null),
        new("audit", 40),
        new("billing", 1200),
    ];

    [Test]
    public void The_table_is_captioned_and_its_frame_is_a_named_focusable_region()
    {
        var cut = RenderTable(Trees);
        var frame = cut.Find(".lt-table-frame");
        var caption = cut.Find("caption");

        Assert.Multiple(() =>
        {
            Assert.That(caption.TextContent, Is.EqualTo("Trees"));
            Assert.That(frame.GetAttribute("role"), Is.EqualTo("region"));
            Assert.That(frame.GetAttribute("aria-labelledby"), Is.EqualTo(caption.Id));
            Assert.That(frame.GetAttribute("tabindex"), Is.EqualTo("0"), "a table wider than its column must be scrollable from the keyboard");
        });
    }

    [Test]
    public void Headers_are_column_headers_and_only_sortable_ones_are_buttons()
    {
        var cut = RenderTable(Trees);
        var headers = cut.FindAll("thead th");

        Assert.Multiple(() =>
        {
            Assert.That(headers.Select(header => header.GetAttribute("scope")), Is.EqualTo(new[] { "col", "col", "col" }));
            Assert.That(headers.Select(header => header.TextContent.Trim()), Is.EqualTo(new[] { "Tree", "Keys", "Owner" }));
            Assert.That(headers[0].QuerySelector("button"), Is.Not.Null);
            Assert.That(headers[1].QuerySelector("button"), Is.Not.Null);
            Assert.That(headers[2].QuerySelector("button"), Is.Null, "a column without a sort key is not sortable");
            Assert.That(headers.Any(header => header.HasAttribute("aria-sort")), Is.False, "nothing is sorted until asked");
        });
    }

    [Test]
    public void Cells_render_values_templates_row_headers_and_column_styles()
    {
        var cut = RenderTable(Trees);
        var firstRow = cut.FindAll("tbody tr")[0];

        Assert.Multiple(() =>
        {
            Assert.That(firstRow.Children[0].TagName, Is.EqualTo("TH"));
            Assert.That(firstRow.Children[0].GetAttribute("scope"), Is.EqualTo("row"));
            Assert.That(firstRow.Children[0].ClassName, Is.EqualTo("lt-table__cell lt-table__cell--mono"));
            Assert.That(firstRow.Children[0].TextContent, Is.EqualTo("orders"));
            Assert.That(firstRow.Children[1].ClassName, Is.EqualTo("lt-table__cell lt-table__cell--end"));
            Assert.That(firstRow.Children[1].TextContent, Is.EqualTo("1200"));
            Assert.That(firstRow.Children[2].QuerySelector("em")?.TextContent, Is.EqualTo("ops"));
        });
    }

    [Test]
    public void Pressing_a_header_sorts_ascending_and_pressing_it_again_sorts_descending()
    {
        var cut = RenderTable(Trees);

        cut.FindAll("thead th button")[0].Click();
        var ascending = Names(cut);
        var sortedHeader = cut.FindAll("thead th")[0];
        var ascendingState = (sortedHeader.GetAttribute("aria-sort"), sortedHeader.QuerySelector("button")?.GetAttribute("data-lt-sort"));

        cut.FindAll("thead th button")[0].Click();

        Assert.Multiple(() =>
        {
            Assert.That(ascending, Is.EqualTo(new[] { "audit", "billing", "customers", "orders" }));
            Assert.That(ascendingState, Is.EqualTo(("ascending", "ascending")));
            Assert.That(Names(cut), Is.EqualTo(new[] { "orders", "customers", "billing", "audit" }));
            Assert.That(cut.FindAll("thead th")[0].GetAttribute("aria-sort"), Is.EqualTo("descending"));
        });
    }

    [Test]
    public void Sorting_by_another_column_moves_aria_sort_to_it_and_is_stable()
    {
        var cut = RenderTable(Trees);
        cut.FindAll("thead th button")[0].Click();

        cut.FindAll("thead th button")[1].Click();

        Assert.Multiple(() =>
        {
            Assert.That(Names(cut), Is.EqualTo(new[] { "customers", "audit", "orders", "billing" }),
                "a missing value sorts first, and ties keep the rows' natural order");
            Assert.That(cut.FindAll("thead th")[0].HasAttribute("aria-sort"), Is.False);
            Assert.That(cut.FindAll("thead th")[1].GetAttribute("aria-sort"), Is.EqualTo("ascending"));
        });
    }

    [Test]
    public void The_current_row_is_marked_in_markup()
    {
        var cut = RenderTable(Trees, isCurrent: row => row.Name == "audit");
        var current = cut.FindAll("tbody tr[aria-current]");

        Assert.Multiple(() =>
        {
            Assert.That(current, Has.Count.EqualTo(1));
            Assert.That(current[0].GetAttribute("aria-current"), Is.EqualTo("true"));
            Assert.That(current[0].Children[0].TextContent, Is.EqualTo("audit"));
        });
    }

    [Test]
    public void An_empty_table_says_so_across_every_column()
    {
        var cut = RenderTable([]);
        var cell = cut.Find("tbody td");

        Assert.Multiple(() =>
        {
            Assert.That(cell.GetAttribute("colspan"), Is.EqualTo("3"));
            Assert.That(cell.TextContent.Trim(), Is.EqualTo("No rows."));
        });
    }

    [Test]
    public void Custom_empty_content_replaces_the_default()
    {
        var cut = RenderTable([], configure: p => p.Add(x => x.EmptyContent, "No trees match this filter."));

        Assert.That(cut.Find("tbody td").TextContent.Trim(), Is.EqualTo("No trees match this filter."));
    }

    [Test]
    public void A_hidden_caption_stays_in_the_document()
    {
        var cut = RenderTable(Trees, configure: p => p.Add(x => x.CaptionHidden, true));

        Assert.That(cut.Find("caption").ClassName, Is.EqualTo("lt-table__caption lt-visually-hidden"));
    }

    [Test]
    public void New_items_replace_the_rows_and_keep_the_sort()
    {
        var cut = RenderTable(Trees);
        cut.FindAll("thead th button")[0].Click();

        cut.Render(p => p.Add(x => x.Items, new[] { new TreeRow("zeta", 1), new TreeRow("alpha", 2) }));

        Assert.That(Names(cut), Is.EqualTo(new[] { "alpha", "zeta" }));
    }

    [Test]
    public async Task The_server_prerender_of_a_virtualised_table_renders_its_first_rows_as_plain_rows()
    {
        // The server prerender never renders interactively, so virtualisation
        // cannot measure anything there; the first paint must still list rows.
        var many = Enumerable.Range(0, 5000).Select(i => new TreeRow($"tree-{i:D4}", i)).ToArray();
        var few = Enumerable.Range(0, 3).Select(i => new TreeRow($"tree-{i:D4}", i)).ToArray();

        var longList = await PrerenderAsync(many);
        var shortList = await PrerenderAsync(few);

        Assert.Multiple(() =>
        {
            Assert.That(Count(longList, "class=\"lt-table__row"), Is.EqualTo(LtTable<TreeRow>.PrerenderedRowLimit));
            Assert.That(longList, Does.Contain("tree-0000").And.Contain("tree-0049").And.Not.Contain("tree-0050"));
            Assert.That(Count(shortList, "class=\"lt-table__row"), Is.EqualTo(3));
        });
    }

    private static async Task<string> PrerenderAsync(IReadOnlyList<TreeRow> rows)
    {
        await using var services = new ServiceCollection().AddLogging().BuildServiceProvider();
        await using var renderer = new Microsoft.AspNetCore.Components.Web.HtmlRenderer(services, services.GetRequiredService<Microsoft.Extensions.Logging.ILoggerFactory>());
        return await renderer.Dispatcher.InvokeAsync(async () =>
        {
            RenderFragment columns = builder =>
            {
                builder.OpenComponent<LtColumn<TreeRow>>(0);
                builder.AddComponentParameter(1, nameof(LtColumn<TreeRow>.Title), "Tree");
                builder.AddComponentParameter(2, nameof(LtColumn<TreeRow>.Value), (Func<TreeRow, object?>)(row => row.Name));
                builder.AddComponentParameter(3, nameof(LtColumn<TreeRow>.RowHeader), true);
                builder.CloseComponent();
            };
            var output = await renderer.RenderComponentAsync<LtTable<TreeRow>>(ParameterView.FromDictionary(new Dictionary<string, object?>
            {
                [nameof(LtTable<TreeRow>.Items)] = rows,
                [nameof(LtTable<TreeRow>.Caption)] = "Trees",
                [nameof(LtTable<TreeRow>.Virtualize)] = true,
                [nameof(LtTable<TreeRow>.ChildContent)] = columns,
            }));
            return output.ToHtmlString();
        });
    }

    private static int Count(string html, string fragment)
    {
        var count = 0;
        for (var index = html.IndexOf(fragment, StringComparison.Ordinal); index >= 0; index = html.IndexOf(fragment, index + 1, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    [Test]
    public void A_virtualised_table_renders_only_part_of_a_long_list()
    {
        var many = Enumerable.Range(0, 5000).Select(i => new TreeRow($"tree-{i:D4}", i)).ToArray();

        var cut = RenderTable(many, configure: p => p.Add(x => x.Virtualize, true));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.LessThan(many.Length));
            Assert.That(cut.FindAll("tbody > tr:not(.lt-table__row)"), Is.Not.Empty, "virtualisation reserves the unrendered rows with spacer rows");
        });
    }

    [Test]
    public void A_table_without_virtualisation_renders_every_row()
    {
        var some = Enumerable.Range(0, 60).Select(i => new TreeRow($"tree-{i:D2}", i)).ToArray();

        var cut = RenderTable(some);

        Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(60));
    }

    [Test]
    public void A_column_outside_a_table_is_rejected()
    {
        Assert.That(() => Render<LtColumn<TreeRow>>(p => p.Add(x => x.Title, "Tree")), Throws.InvalidOperationException);
    }

    private static string[] Names(IRenderedComponent<LtTable<TreeRow>> cut) =>
        cut.FindAll("tbody tr").Select(row => row.Children[0].TextContent).ToArray();

    private IRenderedComponent<LtTable<TreeRow>> RenderTable(
        IReadOnlyList<TreeRow> rows,
        Func<TreeRow, bool>? isCurrent = null,
        Action<ComponentParameterCollectionBuilder<LtTable<TreeRow>>>? configure = null)
    {
        return Render<LtTable<TreeRow>>(p =>
        {
            p.Add(x => x.Items, rows).Add(x => x.Caption, "Trees").Add(x => x.RowKey, row => row.Name);
            p.AddChildContent<LtColumn<TreeRow>>(column => column
                .Add(x => x.Title, "Tree")
                .Add(x => x.Value, row => row.Name)
                .Add(x => x.SortBy, row => row.Name)
                .Add(x => x.Mono, true)
                .Add(x => x.RowHeader, true));
            p.AddChildContent<LtColumn<TreeRow>>(column => column
                .Add(x => x.Title, "Keys")
                .Add(x => x.Value, row => row.Keys)
                .Add(x => x.SortBy, row => row.Keys)
                .Add(x => x.Align, LtColumnAlign.End));
            p.AddChildContent<LtColumn<TreeRow>>(column => column
                .Add(x => x.Title, "Owner")
                .Add(x => x.ChildContent, _ => "<em>ops</em>"));
            if (isCurrent is not null)
            {
                p.Add(x => x.IsCurrent, isCurrent);
            }

            configure?.Invoke(p);
        });
    }
    /// <summary>One row of the tables under test.</summary>
    /// <param name="Name">The tree id.</param>
    /// <param name="Keys">The live key count, or <see langword="null"/> when unknown.</param>
    public sealed record TreeRow(string Name, int? Keys);
}
