using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.DesignSystem.Components;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Component tests for the adaptive table-or-card primitive at every
/// breakpoint: a real table on wide viewports, a card list on compact, from one
/// column declaration.
/// </summary>
[TestFixture]
public sealed class LatticeAdaptiveTableTests
{
    private sealed record BackupRow(string Id, string Tree, string Size);

    private static readonly BackupRow[] TwoRows =
    [
        new("bk-1", "orders", "12 MiB"),
        new("bk-2", "customers", "4 MiB"),
    ];

    private static IReadOnlyList<LatticeTableColumn<BackupRow>> Columns(
        bool hideSizeOnCompact = false) =>
    [
        new LatticeTableColumn<BackupRow>
        {
            Header = "Backup",
            IsPrimary = true,
            IsNumericOrCode = true,
            Cell = row => builder => builder.AddContent(0, row.Id),
        },
        new LatticeTableColumn<BackupRow>
        {
            Header = "Tree",
            Cell = row => builder => builder.AddContent(0, row.Tree),
        },
        new LatticeTableColumn<BackupRow>
        {
            Header = "Size",
            IsNumericOrCode = true,
            ShowOnCompact = !hideSizeOnCompact,
            Cell = row => builder => builder.AddContent(0, row.Size),
        },
    ];

    private static Task<string> RenderAsync(
        LatticeBreakpoint breakpoint,
        IReadOnlyList<BackupRow>? rows = null,
        IReadOnlyList<LatticeTableColumn<BackupRow>>? columns = null) =>
        DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTable<BackupRow>>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = breakpoint,
                ["Items"] = rows ?? TwoRows,
                ["Columns"] = columns ?? Columns(),
                ["KeySelector"] = (Func<BackupRow, object>)(row => row.Id),
            });

    // -------------------------------------------------------------- wide table

    [TestCase(LatticeBreakpoint.Medium)]
    [TestCase(LatticeBreakpoint.Expanded)]
    public async Task Render_atWideBreakpoints_isARealTable(LatticeBreakpoint breakpoint)
    {
        var html = await RenderAsync(breakpoint);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("<table"));
            Assert.That(html, Does.Contain("<thead>"));
            Assert.That(html, Does.Contain("<tbody>"));
            Assert.That(html, Does.Contain($"data-lx-breakpoint=\"{LatticeBreakpoints.Name(breakpoint)}\""));
            Assert.That(html, Does.Not.Contain("lx-cardlist"));
        });
    }

    [Test]
    public async Task Render_atExpanded_emitsAColumnHeaderPerColumn()
    {
        var html = await RenderAsync(LatticeBreakpoint.Expanded);

        Assert.Multiple(() =>
        {
            Assert.That(DesignSystemRenderHarness.CountOccurrences(html, "scope=\"col\""), Is.EqualTo(3));
            Assert.That(html, Does.Contain(">Backup</th>"));
            Assert.That(html, Does.Contain(">Tree</th>"));
            Assert.That(html, Does.Contain(">Size</th>"));
        });
    }

    [Test]
    public async Task Render_atExpanded_marksThePrimaryColumnAsTheRowHeader()
    {
        var html = await RenderAsync(LatticeBreakpoint.Expanded);

        Assert.That(
            DesignSystemRenderHarness.CountOccurrences(html, "scope=\"row\""),
            Is.EqualTo(TwoRows.Length));
    }

    [Test]
    public async Task Render_atExpanded_rendersEveryRowAndCell()
    {
        var html = await RenderAsync(LatticeBreakpoint.Expanded);

        Assert.Multiple(() =>
        {
            Assert.That(DesignSystemRenderHarness.CountOccurrences(html, "<tr"), Is.EqualTo(TwoRows.Length + 1));
            Assert.That(html, Does.Contain("bk-1"));
            Assert.That(html, Does.Contain("customers"));
            Assert.That(html, Does.Contain("4 MiB"));
        });
    }

    [Test]
    public async Task Render_atExpanded_keepsAColumnThatIsHiddenOnCompact()
    {
        var html = await RenderAsync(LatticeBreakpoint.Expanded, columns: Columns(hideSizeOnCompact: true));

        Assert.That(html, Does.Contain(">Size</th>"), "ShowOnCompact only governs the card list");
    }

    [Test]
    public async Task Render_atExpanded_marksTechnicalColumnsForTheMonospaceFace()
    {
        var html = await RenderAsync(LatticeBreakpoint.Expanded);

        Assert.That(html, Does.Contain("lx-cell lx-cell-code"));
    }

    [Test]
    public async Task Render_atExpanded_hidesTheCaptionUnlessTheCallerAsksForIt()
    {
        var html = await RenderAsync(LatticeBreakpoint.Expanded);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("<caption"));
            Assert.That(html, Does.Contain("lx-visually-hidden"),
                "the caption still names the table for assistive technology");
        });
    }

    [Test]
    public async Task Render_atExpanded_showsTheCaptionWhenTheCallerAsksForIt()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTable<BackupRow>>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Expanded,
                ["Items"] = TwoRows,
                ["Columns"] = Columns(),
                ["Label"] = "Backup catalogue",
                ["ShowCaption"] = true,
            });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-table-caption"));
            Assert.That(html, Does.Contain("Backup catalogue"));
        });
    }

    // -------------------------------------------------------------- card list

    [Test]
    public async Task Render_atCompact_reflowsToACardList()
    {
        var html = await RenderAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-cardlist"));
            Assert.That(html, Does.Contain("data-lx-breakpoint=\"compact\""));
            Assert.That(html, Does.Not.Contain("<table"));
            Assert.That(html, Does.Not.Contain("<thead>"));
        });
    }

    [Test]
    public async Task Render_atCompact_emitsOneCardPerRow()
    {
        var html = await RenderAsync(LatticeBreakpoint.Compact);

        Assert.That(
            DesignSystemRenderHarness.CountOccurrences(html, "class=\"lx-card\""),
            Is.EqualTo(TwoRows.Length));
    }

    [Test]
    public async Task Render_atCompact_promotesThePrimaryColumnToTheCardTitleWithNoFieldLabel()
    {
        var html = await RenderAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(html, "lx-card-title"),
                Is.EqualTo(TwoRows.Length));
            Assert.That(html, Does.Not.Contain(">Backup</span>"),
                "the primary column is the title, so it needs no field label");
        });
    }

    [Test]
    public async Task Render_atCompact_labelsEveryRemainingFieldWithItsColumnHeader()
    {
        var html = await RenderAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(">Tree</span>"));
            Assert.That(html, Does.Contain(">Size</span>"));
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(html, "lx-card-field-label"),
                Is.EqualTo(TwoRows.Length * 2));
        });
    }

    [Test]
    public async Task Render_atCompact_dropsAColumnThatOptedOutOfTheReflow()
    {
        var html = await RenderAsync(LatticeBreakpoint.Compact, columns: Columns(hideSizeOnCompact: true));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Not.Contain(">Size</span>"));
            Assert.That(html, Does.Not.Contain("12 MiB"));
            Assert.That(html, Does.Contain(">Tree</span>"), "the other fields survive");
        });
    }

    [Test]
    public async Task Render_atCompact_withNoPrimaryColumn_rendersEveryColumnAsALabelledField()
    {
        IReadOnlyList<LatticeTableColumn<BackupRow>> columns =
        [
            new LatticeTableColumn<BackupRow>
            {
                Header = "Tree",
                Cell = row => builder => builder.AddContent(0, row.Tree),
            },
            new LatticeTableColumn<BackupRow>
            {
                Header = "Size",
                Cell = row => builder => builder.AddContent(0, row.Size),
            },
        ];

        var html = await RenderAsync(LatticeBreakpoint.Compact, columns: columns);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Not.Contain("lx-card-title"));
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(html, "lx-card-field-label"),
                Is.EqualTo(TwoRows.Length * 2));
        });
    }

    [Test]
    public async Task Render_withSeveralPrimaryColumns_takesTheFirstDeterministically()
    {
        IReadOnlyList<LatticeTableColumn<BackupRow>> columns =
        [
            new LatticeTableColumn<BackupRow>
            {
                Header = "Backup",
                IsPrimary = true,
                Cell = row => builder => builder.AddContent(0, row.Id),
            },
            new LatticeTableColumn<BackupRow>
            {
                Header = "Tree",
                IsPrimary = true,
                Cell = row => builder => builder.AddContent(0, row.Tree),
            },
        ];

        var html = await RenderAsync(LatticeBreakpoint.Compact, columns: columns);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(">Tree</span>"), "the second primary falls back to a field");
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(html, "lx-card-title"),
                Is.EqualTo(TwoRows.Length));
        });
    }

    // ------------------------------------------------------------ empty states

    [TestCase(LatticeBreakpoint.Compact)]
    [TestCase(LatticeBreakpoint.Medium)]
    [TestCase(LatticeBreakpoint.Expanded)]
    public async Task Render_withNoRows_showsTheDefaultEmptyMessage(LatticeBreakpoint breakpoint)
    {
        var html = await RenderAsync(breakpoint, rows: Array.Empty<BackupRow>());

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-datalist-empty"));
            Assert.That(html, Does.Contain("Nothing to show."));
            Assert.That(html, Does.Not.Contain("<table"));
            Assert.That(html, Does.Not.Contain("lx-cardlist"));
        });
    }

    [Test]
    public async Task Render_withNoRows_prefersTheCallersEmptyContent()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTable<BackupRow>>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Expanded,
                ["Items"] = Array.Empty<BackupRow>(),
                ["Columns"] = Columns(),
                ["EmptyContent"] = (RenderFragment)(builder =>
                    builder.AddMarkupContent(0, "<p>No backups yet.</p>")),
            });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("No backups yet."));
            Assert.That(html, Does.Not.Contain("Nothing to show."));
        });
    }

    [Test]
    public async Task Render_withNoRows_usesTheCallersEmptyText()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTable<BackupRow>>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Expanded,
                ["Items"] = Array.Empty<BackupRow>(),
                ["Columns"] = Columns(),
                ["EmptyText"] = "No backups match this filter.",
            });

        Assert.That(html, Does.Contain("No backups match this filter."));
    }

    [Test]
    public async Task Render_withNoColumns_showsTheEmptyStateRatherThanAnEmptyTable()
    {
        var html = await RenderAsync(
            LatticeBreakpoint.Expanded,
            columns: Array.Empty<LatticeTableColumn<BackupRow>>());

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-datalist-empty"));
            Assert.That(html, Does.Not.Contain("<table"));
        });
    }

    [Test]
    public async Task Render_withNullItemsAndColumns_doesNotThrow()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTable<BackupRow>>(
            new Dictionary<string, object?> { ["Breakpoint"] = LatticeBreakpoint.Expanded });

        Assert.That(html, Does.Contain("lx-datalist-empty"));
    }

    // -------------------------------------------------------------- defaults

    [Test]
    public async Task Render_withNoBreakpoint_fallsBackToTheDefaultPresentation()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTable<BackupRow>>(
            new Dictionary<string, object?>
            {
                ["Items"] = TwoRows,
                ["Columns"] = Columns(),
            });

        Assert.That(html, Does.Contain("<table"));
    }

    [Test]
    public async Task Render_followsTheCascadedShellContextWhenNoBreakpointIsPinned()
    {
        var html = await DesignSystemRenderHarness.RenderCascadedAsync<LatticeAdaptiveTable<BackupRow>>(
            new LatticeAdaptiveContext(LatticeBreakpoint.Compact, LatticeDensity.Cosy, IsMeasured: true),
            new Dictionary<string, object?>
            {
                ["Items"] = TwoRows,
                ["Columns"] = Columns(),
            });

        Assert.That(html, Does.Contain("lx-cardlist"));
    }

    [Test]
    public async Task Render_pinnedBreakpointWinsOverTheCascadedShellContext()
    {
        var html = await DesignSystemRenderHarness.RenderCascadedAsync<LatticeAdaptiveTable<BackupRow>>(
            new LatticeAdaptiveContext(LatticeBreakpoint.Compact, LatticeDensity.Cosy, IsMeasured: true),
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = (LatticeBreakpoint?)LatticeBreakpoint.Expanded,
                ["Items"] = TwoRows,
                ["Columns"] = Columns(),
            });

        Assert.That(html, Does.Contain("<table"));
    }

    [Test]
    public async Task Render_namesTheCardListForAssistiveTechnology()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTable<BackupRow>>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Compact,
                ["Items"] = TwoRows,
                ["Columns"] = Columns(),
                ["Label"] = "Backup catalogue",
            });

        Assert.That(html, Does.Contain("aria-label=\"Backup catalogue\""));
    }

    [TestCase(LatticeBreakpoint.Compact)]
    [TestCase(LatticeBreakpoint.Expanded)]
    public async Task Render_appendsTheCallersClass(LatticeBreakpoint breakpoint)
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTable<BackupRow>>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = breakpoint,
                ["Items"] = TwoRows,
                ["Columns"] = Columns(),
                ["Class"] = "explorer-backups-table",
            });

        Assert.That(html, Does.Contain("explorer-backups-table"));
    }

    [Test]
    public async Task Render_withoutAKeySelector_stillRendersEveryRow()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTable<BackupRow>>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Expanded,
                ["Items"] = TwoRows,
                ["Columns"] = Columns(),
            });

        Assert.That(DesignSystemRenderHarness.CountOccurrences(html, "scope=\"row\""), Is.EqualTo(TwoRows.Length));
    }
}
