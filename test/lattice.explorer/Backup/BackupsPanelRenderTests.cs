using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Backup.Components;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Tests.Backup;

/// <summary>
/// The Backups plugin's rendered surface: the decomposed regions, the catalogue
/// reflow the design system's adaptive table produces, and the keyboard and
/// ARIA semantics the conversion had to preserve.
/// </summary>
/// <remarks>
/// Every render is driven by a stubbed domain supplied up front, so no test
/// here depends on a clock, an ordering, or a background task.
/// </remarks>
[TestFixture]
public sealed class BackupsPanelRenderTests
{
    [Test]
    public async Task The_panel_renders_the_area_frame_and_both_sub_tabs()
    {
        var html = await BackupsRenderHarness.RenderPanelAsync(StubBackupsDomain.Create());

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("class=\"lx-backups\""));
            Assert.That(html, Does.Contain("aria-label=\"Backups\""));
            Assert.That(html, Does.Contain("role=\"tablist\""));
            Assert.That(html, Does.Contain("New Backup"));
            Assert.That(html, Does.Contain("Existing Backups"));
        });
    }

    [Test]
    public async Task The_catalogue_renders_a_table_at_expanded()
    {
        var html = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(new[] { SampleBackup.Manifest("backup-1") }),
            breakpoint: LatticeBreakpoint.Expanded);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("<table"));
            Assert.That(html, Does.Contain("lx-table"));
            Assert.That(html, Does.Not.Contain("lx-cardlist"));
        });
    }

    [Test]
    public async Task The_catalogue_reflows_to_a_card_list_at_compact()
    {
        var html = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(new[] { SampleBackup.Manifest("backup-1") }),
            breakpoint: LatticeBreakpoint.Compact);

        // The wide catalogue must stop scrolling sideways off a phone screen.
        // The reflow is the design system's, driven by the breakpoint name and
        // never by a media query in this plugin (epic decision D7).
        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-cardlist"));
            Assert.That(html, Does.Contain("lx-card-title"));
            Assert.That(html, Does.Not.Contain("<table"));
        });
    }

    [Test]
    public async Task Row_selection_is_a_real_toggle_button_in_both_presentations()
    {
        var expanded = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(new[] { SampleBackup.Manifest("backup-1") }),
            breakpoint: LatticeBreakpoint.Expanded);
        var compact = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(new[] { SampleBackup.Manifest("backup-1") }),
            breakpoint: LatticeBreakpoint.Compact);

        // The retired surface selected a row by clicking the <tr>, which no
        // keyboard could reach and which announced nothing. A button in the
        // primary cell is reachable, announces its pressed state, and survives
        // the reflow into the card title.
        Assert.Multiple(() =>
        {
            Assert.That(expanded, Does.Contain("lx-backups-rowbutton"));
            Assert.That(expanded, Does.Contain("aria-pressed"));
            Assert.That(compact, Does.Contain("lx-backups-rowbutton"));
            Assert.That(compact, Does.Contain("aria-pressed"));
        });
    }

    [Test]
    public async Task The_catalogue_omits_the_health_column_when_the_server_reports_it_unavailable()
    {
        var html = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(
                new[] { SampleBackup.Manifest("backup-1") },
                healthAvailable: false));

        Assert.That(html, Does.Not.Contain("lx-backups-health"));
    }

    [Test]
    public async Task The_catalogue_shows_an_unverified_indicator_when_health_monitoring_is_available()
    {
        var html = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(
                new[] { SampleBackup.Manifest("backup-1") },
                healthAvailable: true));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Health"));
            Assert.That(html, Does.Contain("lx-backups-health-unknown"));
        });
    }

    [Test]
    public async Task The_filter_bar_renders_a_labelled_control_per_facet()
    {
        var html = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(new[] { SampleBackup.Manifest("backup-1") }));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("aria-label=\"Filter backups\""));
            Assert.That(html, Does.Contain("aria-label=\"Filter by kind\""));
            Assert.That(html, Does.Contain("aria-label=\"Filter by scope\""));
            Assert.That(
                BackupsRenderHarness.CountOccurrences(html, "lx-backups-filter-label"),
                Is.EqualTo(4),
                "name, kind, scope and created each keep their own label after the filter row left the table head");
        });
    }

    [Test]
    public async Task The_pager_renders_previous_and_next_with_the_page_number()
    {
        var html = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(new[] { SampleBackup.Manifest("backup-1") }));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-backups-pager"));
            Assert.That(html, Does.Contain("Page 1"));
            Assert.That(html, Does.Contain(">Prev<"));
            Assert.That(html, Does.Contain(">Next<"));
        });
    }

    [Test]
    public async Task An_empty_catalogue_reports_that_nothing_is_visible()
    {
        var html = await BackupsRenderHarness.RenderPanelAsync(StubBackupsDomain.Create());

        Assert.That(html, Does.Contain("No backups are visible."));
    }

    [Test]
    public async Task A_denied_listing_says_so_instead_of_rendering_an_empty_catalogue()
    {
        var html = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(status: BackupOperationStatus.Denied));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("You are not permitted to list backups here."));
            Assert.That(html, Does.Not.Contain("lx-backups-pager"));
        });
    }

    [Test]
    public async Task A_failed_listing_surfaces_the_readers_message()
    {
        var html = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(
                status: BackupOperationStatus.Failed,
                message: "the endpoint is unreachable"));

        Assert.That(html, Does.Contain("the endpoint is unreachable"));
    }

    [Test]
    public async Task The_capture_form_offers_every_visible_tree_with_its_shadows_grouped()
    {
        var html = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(trees: new[]
            {
                new BackupTreeOption("orders", null),
                new BackupTreeOption("orders-shadow", "orders"),
            }),
            subTab: BackupsSubTab.New);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-backups-new"));
            Assert.That(html, Does.Contain(">orders<"));
            Assert.That(html, Does.Contain(">orders-shadow<"));
            Assert.That(html, Does.Contain("(restore shadows)"));
            Assert.That(html, Does.Contain("role=\"listbox\""));
        });
    }

    [Test]
    public async Task The_capture_form_names_every_control_it_renders()
    {
        var html = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(trees: new[] { new BackupTreeOption("orders", null) }),
            subTab: BackupsSubTab.New);

        // The retired form inferred each control's name from an adjacent layout
        // table cell, which announced nothing. Every control now carries one.
        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("aria-labelledby=\"lx-backups-name-label\""));
            Assert.That(html, Does.Contain("aria-label=\"Backup kind\""));
            Assert.That(html, Does.Contain("aria-label=\"Recurring schedule\""));
            Assert.That(html, Does.Contain("aria-label=\"Interval hours\""));
            Assert.That(html, Does.Contain("aria-label=\"Interval minutes\""));
            Assert.That(html, Does.Not.Contain("<table"), "the capture form is a wrapping flow, not a layout table");
        });
    }

    [Test]
    public async Task The_capture_form_reports_an_empty_tree_list_rather_than_rendering_nothing()
    {
        var html = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(),
            subTab: BackupsSubTab.New);

        Assert.That(html, Does.Contain("No trees are visible."));
    }

    [Test]
    public async Task No_dialog_renders_until_one_is_requested()
    {
        var html = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(new[] { SampleBackup.Manifest("backup-1") }));

        Assert.That(html, Does.Not.Contain("explorer-modal-backdrop"));
    }
}
