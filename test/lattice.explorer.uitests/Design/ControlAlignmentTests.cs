namespace Orleans.Lattice.Explorer.UiTests.Design;

/// <summary>
/// Issue #4120: every toolbar lines its controls up on their control boxes, and every
/// field primitive and button is one control height, measured in Chromium on the
/// representative toolbars of every area - the Backups catalogue, the Data directory and
/// a tree's keys and history, Access rules and groups, Cluster trees, WAL and orphans,
/// the Apps catalogue, Schema, Replication and Tenancy - at desktop and phone widths, in
/// Paper and in Board, and in both densities. <see cref="ControlAlignment"/> says what
/// each measurement catches.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class ControlAlignmentTests : UiTestBase
{
    /// <summary>Each width and appearance the gate measures.</summary>
    public static IEnumerable<TestCaseData> Appearances() =>
        from tenancy in new[] { false, true }
        from width in new[] { Shell.LargeWidth, PhoneWidth }
        from appearance in new[]
        {
            ShellAppearanceChoice.Default,
            ShellAppearanceChoice.Default with { Board = true },
            ShellAppearanceChoice.Default with { Compact = true },
            ShellAppearanceChoice.Default with { Board = true, Compact = true },
        }
        select new TestCaseData(tenancy, width, appearance.Board, appearance.Compact)
            .SetArgDisplayNames(tenancy ? "tenancy" : "areas", width.ToString(System.Globalization.CultureInfo.InvariantCulture), appearance.Board ? "Board" : "Paper", appearance.Compact ? "compact" : "comfortable");

    /// <summary>The phone width the issue was reported at.</summary>
    internal const int PhoneWidth = 390;

    [TestCaseSource(nameof(Appearances))]
    public async Task Every_toolbar_lines_up_on_its_control_boxes_and_every_field_is_one_control_height(bool tenancy, int width, bool board, bool compact)
    {
        var appearance = ShellAppearanceChoice.Default with { Board = board, Compact = compact };
        // The tenancy pages live in a second world; each test drives one page in one world.
        var world = tenancy ? await UiHosts.TenantWorldAsync() : await UiHosts.WorldAsync();
        var page = await AppearAsync(world, appearance, width);
        var faults = await ControlAlignment.MeasureAsync(page, world, tenancy ? ControlAlignment.TenancyPages : ControlAlignment.WorldPages, placeholders: true);

        Assert.That(faults, Is.Empty,
            $"At {width}px in {appearance}, these controls break the control-alignment rule:" + Environment.NewLine + string.Join(Environment.NewLine, faults));
    }

    /// <summary>
    /// Opens a page in <paramref name="appearance"/>, chosen through the desktop header -
    /// at phone width the appearance controls sit behind the menu - then narrows the
    /// viewport to <paramref name="width"/>. The choice is remembered across navigations.
    /// </summary>
    private async Task<Microsoft.Playwright.IPage> AppearAsync(ExplorerWorld world, ShellAppearanceChoice appearance, int width)
    {
        var page = await OpenAsync(world.Head, "/", WorldIdentities.Admin);

        // The desktop header's appearance control, once the circuit has settled on the
        // desktop layout (a circuit can start from the last breakpoint it saw).
        await Microsoft.Playwright.Assertions.Expect(Shell.Banner(page).Locator("button[data-lt-command=\"appearance.menu\"]")).ToBeVisibleAsync();

        // The sign-in is restored after the circuit starts and moves the remembered
        // preferences to the signed-in identity, re-applying what it remembers; an
        // appearance chosen before that would be replaced, so choose it after.
        await Shell.ExpectSignedInAsync(page, WorldIdentities.Admin);
        await Shell.SetAppearanceAsync(page, appearance);
        await page.SetViewportSizeAsync(width, Shell.Height);
        return page;
    }
}