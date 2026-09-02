using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Backup.Components;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Tests.Plugins;

using Orleans.Lattice.Explorer.Core.Vocabulary;

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
            Assert.That(html, Does.Contain("New backup"));
            Assert.That(html, Does.Contain("Existing backups"));
        });
    }

    [Test]
    public async Task The_sub_tab_strip_states_selection_on_every_tab_not_just_the_active_one()
    {
        // aria-selected is enumerated, not boolean. Handing Blazor a bool makes
        // the active tab emit aria-selected="" and the inactive one omit the
        // attribute entirely, so a screen-reader user cannot tell which tab is
        // selected. Counting is what catches it: a spot check for
        // "the active tab has aria-selected" passes on the broken form.
        var html = await BackupsRenderHarness.RenderPanelAsync(StubBackupsDomain.Create());
        var aria = PluginAriaMarkup.TallyAriaSelected(html);

        Assert.Multiple(() =>
        {
            Assert.That(aria.Invalid, Is.Zero, "a bare or empty aria-selected is not a valid enumerated value");
            Assert.That(aria.Valid, Is.EqualTo(aria.Total), "every occurrence must read true or false");
            Assert.That(aria.True, Is.EqualTo(1), "exactly one sub-tab is selected");
            Assert.That(
                aria.False,
                Is.EqualTo(1),
                "the unselected sub-tab must say so rather than omit the attribute");
        });
    }

    [Test]
    public async Task Before_the_surface_is_restored_the_body_says_so_and_the_strip_stays_navigable()
    {
        // The retained surface has not resolved - browser storage is
        // unreachable, as during a prerender or with script disabled - so
        // neither body is the right one to render. Two things must hold.
        //
        // The panel says what is happening instead of rendering an empty block:
        // a caller who never gets past this is looking at a real state.
        //
        // And the strip still reports exactly one selected tab. A tab strip
        // whose every tab reads unselected has no tab to put in the tab
        // sequence, so it cannot be reached by keyboard at all - which is why
        // the shared primitive always keeps one, and why the "nothing selected
        // yet" state belongs in the panel rather than in the strip.
        var html = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(),
            preferencesLoaded: false);
        var aria = PluginAriaMarkup.TallyAriaSelected(html);

        Assert.Multiple(() =>
        {
            Assert.That(aria.Invalid, Is.Zero);
            Assert.That(aria.True, Is.EqualTo(1), "the strip keeps a tab in the tab sequence");
            Assert.That(aria.False, Is.EqualTo(1));
            Assert.That(html, Does.Contain("aria-busy=\"true\""));
            Assert.That(
                html,
                Does.Contain(ExplorerStateCopy.Loading(ExplorerSubjects.Backups).Headline),
                "an unresolved surface is a state with words, not an empty panel");
        });
    }

    [Test]
    public async Task The_tree_picker_states_selection_on_every_option()
    {
        // role="option" carries the same enumerated requirement as role="tab":
        // one option reads "true", every other reads "false", and none may carry
        // the empty value.
        var html = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(
                trees:
                [
                    new BackupTreeOption("orders", null),
                    new BackupTreeOption("invoices", null),
                    new BackupTreeOption("orders-restore", "orders"),
                ]),
            subTab: BackupsSubTab.New);
        var aria = PluginAriaMarkup.TallyAriaSelected(html);

        Assert.Multiple(() =>
        {
            Assert.That(
                PluginAriaMarkup.Count(html, "role=\"option\""),
                Is.EqualTo(3),
                "the picker lists both ordinary trees and the restore shadow");
            Assert.That(aria.Invalid, Is.Zero, "a bare or empty aria-selected is not a valid enumerated value");
            Assert.That(aria.Valid, Is.EqualTo(aria.Total));

            // Two tabs plus three options, and no tree is selected until one is
            // picked, so every option states "false" rather than omitting.
            Assert.That(aria.True, Is.EqualTo(1), "only the active sub-tab is selected");
            Assert.That(aria.False, Is.EqualTo(4));
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
            Assert.That(
                html,
                Does.Contain(ExplorerStateCopy.NotPermitted(ExplorerSubjects.Backups, "Backup").Explanation),
                "the refusal comes from the shared vocabulary, not a second wording");
            Assert.That(
                html,
                Does.Contain("Backup"),
                "a denial names the missing permission, not the label the caller can already see");
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

        Assert.That(html, Does.Not.Contain("lx-modal-backdrop"));
    }

    [Test]
    public async Task The_sub_tab_strip_is_the_design_systems_tab_strip()
    {
        var html = await BackupsRenderHarness.RenderPanelAsync(StubBackupsDomain.Create());

        // Backups was the last surface still declaring its own tab strip, in
        // the app.css monolith. Composing the shared primitive is what gives it
        // the same focus, disabled and compact-target behaviour as every other
        // strip - and it is not something a build can check (issue #1770).
        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("class=\"lx-tabstrip\""));
            Assert.That(html, Does.Contain("lx-tab"));
            Assert.That(html, Does.Not.Contain("explorer-tabstrip"));
        });
    }

    [Test]
    public async Task The_tree_picker_is_the_design_systems_navigation_list()
    {
        var html = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(trees: new[] { new BackupTreeOption("orders", null) }),
            subTab: BackupsSubTab.New);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("class=\"lx-nav-list\""));
            Assert.That(html, Does.Contain("lx-nav-item"));
            Assert.That(html, Does.Contain("lx-nav-item-id"));
        });
    }

    [Test]
    public async Task The_row_actions_render_the_design_systems_button_primitives()
    {
        var html = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(new[] { SampleBackup.Manifest("backup-1") }),
            afterFirstRender: panel => panel.SelectRowAsync(panel.Rows[0].Row));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-btn"));
            Assert.That(html, Does.Contain("lx-btn-danger"), "delete is the outlined destructive variant");
        });
    }

    [Test]
    public async Task The_capture_form_renders_the_design_systems_button_primitives()
    {
        var html = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(trees: new[] { new BackupTreeOption("orders", null) }),
            subTab: BackupsSubTab.New,
            afterFirstRender: panel => panel.AddTreeAsync("orders"));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-btn-primary"), "capture is the form's one affirmative action");
            Assert.That(html, Does.Contain("lx-btn-icon"), "the per-tree remove control is the compact variant");
        });
    }

    [Test]
    public async Task The_delete_dialog_renders_the_design_systems_modal_primitives()
    {
        // The modal family had no rule outside app.css, so deleting the file
        // without migrating it would have left all four Backups dialogs as
        // unstyled blocks in the page flow - no backdrop, no centring, no
        // surface. Nothing but a render assertion catches that (issue #1770).
        var html = await BackupsRenderHarness.RenderPanelAsync(
            StubBackupsDomain.Create(new[] { SampleBackup.Manifest("backup-1") }),
            afterFirstRender: async panel =>
            {
                await panel.SelectRowAsync(panel.Rows[0].Row);
                panel.RequestDelete(panel.Rows[0].Row);
            });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-modal-backdrop"));
            Assert.That(html, Does.Contain("class=\"lx-modal\""));
            Assert.That(html, Does.Contain("lx-modal-actions"));
            Assert.That(html, Does.Contain("role=\"alertdialog\""));
            Assert.That(html, Does.Contain("lx-btn-danger"));
        });
    }
}
