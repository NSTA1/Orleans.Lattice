using Orleans.Lattice.Explorer.Access.Views;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// The Access plugin's rendered surface: the reflow the design system's adaptive
/// table produces, the adaptive-root fallback's yielding behaviour, and the
/// keyboard and ARIA semantics the conversion had to preserve.
/// </summary>
/// <remarks>
/// Every render is driven by a stubbed domain supplied up front, so no test here
/// depends on a clock, an ordering, or a background task.
/// </remarks>
[TestFixture]
public sealed class AccessPanelRenderTests
{
    [Test]
    public async Task The_panel_renders_the_area_frame_and_all_three_sub_surfaces()
    {
        var html = await AccessRenderHarness.RenderPanelAsync(StubAccessDomain.Create());

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("class=\"lxa-panel\""));
            Assert.That(html, Does.Contain("aria-label=\"Access\""));
            Assert.That(html, Does.Contain("role=\"tablist\""));
            Assert.That(html, Does.Contain(">Groups<"));
            Assert.That(html, Does.Contain(">Policies<"));
            Assert.That(html, Does.Contain(">Explain<"));
        });
    }

    [Test]
    public async Task The_sub_surface_strip_keeps_the_wai_aria_tabs_pattern()
    {
        var html = await AccessRenderHarness.RenderPanelAsync(StubAccessDomain.Create());

        // The retired AccessTab strip was three bare buttons with no roving
        // tabindex and no panel association. Rendering through the design
        // system's primitive restores the whole pattern.
        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("role=\"tab\""));
            Assert.That(html, Does.Contain("role=\"tabpanel\""));
            Assert.That(html, Does.Contain("aria-selected=\"true\""));
            Assert.That(html, Does.Contain("aria-controls="));
            Assert.That(html, Does.Contain("tabindex=\"0\""));
            Assert.That(
                html,
                Does.Contain("tabindex=\"-1\""),
                "a roving tabindex makes the strip a single tab stop");
        });
    }

    [Test]
    public async Task The_rule_table_renders_a_real_table_at_expanded()
    {
        var workspace = await AccessRenderHarness.CreateWorkspaceAsync(
            StubAccessDomain.Create(trees: ["orders"], rules: [StubAccessDomain.Rule("r1")]),
            AccessSurfaces.Policies,
            selectedTreeId: "orders");

        var html = await AccessRenderHarness.RenderViewAsync<AccessPolicyView>(
            workspace,
            LatticeBreakpoint.Expanded);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("<table"));
            Assert.That(html, Does.Contain("lx-table"));
            Assert.That(html, Does.Contain("r1"));
            Assert.That(html, Does.Not.Contain("lx-cardlist"));
        });
    }

    [Test]
    public async Task The_rule_table_reflows_to_a_card_list_at_compact()
    {
        var workspace = await AccessRenderHarness.CreateWorkspaceAsync(
            StubAccessDomain.Create(trees: ["orders"], rules: [StubAccessDomain.Rule("r1")]),
            AccessSurfaces.Policies,
            selectedTreeId: "orders");

        var html = await AccessRenderHarness.RenderViewAsync<AccessPolicyView>(
            workspace,
            LatticeBreakpoint.Compact);

        // The six-column rule table must stop scrolling sideways off a phone
        // screen. The reflow is the design system's, driven by the breakpoint
        // name and never by a media query in this plugin (epic decision D7).
        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-cardlist"));
            Assert.That(html, Does.Contain("lx-card-title"));
            Assert.That(html, Does.Not.Contain("<table"));
        });
    }

    [Test]
    public async Task Every_rule_keeps_its_subject_scope_operations_and_effect_after_the_reflow()
    {
        var workspace = await AccessRenderHarness.CreateWorkspaceAsync(
            StubAccessDomain.Create(trees: ["orders"], rules: [StubAccessDomain.Rule("r1")]),
            AccessSurfaces.Policies,
            selectedTreeId: "orders");

        var html = await AccessRenderHarness.RenderViewAsync<AccessPolicyView>(
            workspace,
            LatticeBreakpoint.Compact);

        // A card that dropped a column would be a silent data loss, so the
        // labelled fields are asserted individually rather than by count.
        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Subject"));
            Assert.That(html, Does.Contain("Scope"));
            Assert.That(html, Does.Contain("Ops"));
            Assert.That(html, Does.Contain("Effect"));
            Assert.That(html, Does.Contain("user:alice"));
            Assert.That(html, Does.Contain("Read"));
        });
    }

    [Test]
    public async Task The_per_rule_edit_action_survives_the_reflow_into_a_card()
    {
        var workspace = await AccessRenderHarness.CreateWorkspaceAsync(
            StubAccessDomain.Create(trees: ["orders"], rules: [StubAccessDomain.Rule("r1")]),
            AccessSurfaces.Policies,
            selectedTreeId: "orders");

        var expanded = await AccessRenderHarness.RenderViewAsync<AccessPolicyView>(
            workspace,
            LatticeBreakpoint.Expanded);
        var compact = await AccessRenderHarness.RenderViewAsync<AccessPolicyView>(
            workspace,
            LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            Assert.That(expanded, Does.Contain(">Edit<"));
            Assert.That(compact, Does.Contain(">Edit<"), "a card whose row action vanished would strand the operator");
        });
    }

    [Test]
    public async Task An_empty_rule_set_says_so_rather_than_rendering_an_empty_table()
    {
        var workspace = await AccessRenderHarness.CreateWorkspaceAsync(
            StubAccessDomain.Create(trees: ["orders"]),
            AccessSurfaces.Policies,
            selectedTreeId: "orders");

        var html = await AccessRenderHarness.RenderViewAsync<AccessPolicyView>(workspace);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("No rules to show."));
            Assert.That(html, Does.Not.Contain("<table"));
        });
    }

    [Test]
    public async Task A_shell_supplied_cascade_leaves_the_panel_hosting_no_root_of_its_own()
    {
        // MainLayout now hosts LatticeAdaptiveRoot, so the panel must yield to
        // it. Two roots in one tree would mean two viewport owners.
        var html = await AccessRenderHarness.RenderPanelAsync(
            StubAccessDomain.Create(),
            LatticeBreakpoint.Expanded);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Not.Contain("lx-root"));
            Assert.That(html, Does.Not.Contain("lxa-root"));
            Assert.That(html, Does.Contain("lxa-panel"), "the panel itself still renders");
        });
    }

    [Test]
    public async Task Without_a_cascade_the_panel_supplies_its_own_root_so_a_bare_head_still_adapts()
    {
        // The defence for a head that composes the plugin without the shared
        // MainLayout: with no ambient context the plugin owns the viewport
        // itself rather than silently pinning to the default breakpoint.
        var html = await AccessRenderHarness.RenderPanelAsync(
            StubAccessDomain.Create(),
            breakpoint: null);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-root"));
            Assert.That(html, Does.Contain("lxa-root"));
            Assert.That(html, Does.Contain("lxa-panel"));
        });
    }

    [Test]
    public async Task A_denied_gate_greys_the_surface_out_and_says_why()
    {
        var html = await AccessRenderHarness.RenderPanelAsync(
            StubAccessDomain.Create(),
            access: ExplorerPluginAccess.Denied);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Access administration is not permitted for your account"));
            Assert.That(html, Does.Contain("role=\"status\""));
            Assert.That(
                html,
                Does.Not.Contain("role=\"alert\""),
                "a settled denial is not the recoverable sign-in state");
        });
    }

    [Test]
    public async Task An_authentication_required_gate_prompts_a_sign_in_rather_than_greying_out()
    {
        var html = await AccessRenderHarness.RenderPanelAsync(
            StubAccessDomain.Create(),
            access: ExplorerPluginAccess.AuthenticationRequired);

        // The one behaviour the issue's acceptance criteria call out by name.
        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("sign in to administer access"));
            Assert.That(
                html,
                Does.Contain("role=\"alert\""),
                "a recoverable state is announced, not silently greyed out");
            Assert.That(html, Does.Not.Contain("Access administration is not permitted for your account"));
        });
    }

    [Test]
    public async Task The_tree_selector_renders_a_listbox_of_the_catalog_trees()
    {
        var workspace = await AccessRenderHarness.CreateWorkspaceAsync(
            StubAccessDomain.Create(trees: ["orders", "audit"]),
            AccessSurfaces.Policies);

        var html = await AccessRenderHarness.RenderViewAsync<AccessPolicyView>(workspace);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("role=\"listbox\""));
            Assert.That(html, Does.Contain("aria-label=\"Trees\""));
            Assert.That(html, Does.Contain(">orders<"));
            Assert.That(html, Does.Contain(">audit<"));
        });
    }

    [Test]
    public async Task The_groups_surface_lists_its_groups_by_display_name()
    {
        var html = await AccessRenderHarness.RenderPanelAsync(
            StubAccessDomain.Create(groups:
            [
                new Api.Auth.AuthGroup { GroupId = "admins", DisplayName = "Administrators" },
            ]));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Administrators"));
            Assert.That(html, Does.Contain("aria-label=\"Groups\""));
        });
    }
}
