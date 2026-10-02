using System.Text.RegularExpressions;
using Grpc.Core;
using Microsoft.Playwright;
using Orleans.Lattice.Api.TenantAdmin;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Epic #4154 (K1, issue #4173): delegated tenant access administration end to end,
/// over the Explorer's own gRPC transport. A tenant administrator who is not a
/// platform operator keeps the tenant's groups, members and rules; Explain shows the
/// Platform layer deciding over a shadowed tenant rule; nothing of another tenant is
/// visible or reachable; and with the feature off the tenant pages say so and the
/// facade refuses.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class DelegatedTenantAccessJourneyTests : UiTestBase
{
    private const string Group = "floor-team";
    private const string RuleId = "floor-team-read-orders";

    [Test]
    public async Task A_tenant_admin_who_is_not_an_operator_grants_a_new_group_read_and_its_member_reads_the_tree()
    {
        var world = await UiHosts.DelegatedWorldAsync();
        await world.RemoveGlobexGroupAsync(Group);
        var page = await OpenAsync(world.Head, $"/t/{ExplorerWorld.Globex}/access/groups", WorldIdentities.GlobexAdmin);
        var content = Shell.Content(page);
        await Expect(content.Locator("[data-lt-tenant-view=groups]")).ToBeVisibleAsync();

        // Create the group.
        await content.GetByRole(AriaRole.Button, new() { Name = "New group", Exact = true }).ClickAsync();
        var dialog = page.GetByRole(AriaRole.Dialog, new() { Name = "New group" });
        await dialog.GetByRole(AriaRole.Textbox, new() { Name = "Group name", Exact = true }).FillAsync(Group);
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Create group" }).ClickAsync();
        await Expect(page).ToHaveURLAsync(new Regex($"/t/{ExplorerWorld.Globex}/access/groups/{Group}$"));
        await Expect(page.GetByRole(AriaRole.Heading, new() { Name = Group, Level = 1 })).ToBeVisibleAsync();
        await Expect(content.Locator("[data-lt-tenant-view=group]")).ToHaveAttributeAsync("data-lt-caller", "tenant-admin");

        // Add a user to it.
        // A tenant administrator cannot read the cluster's identity directory, so a user is named as typed.
        await content.GetByRole(AriaRole.Combobox, new() { Name = "Member kind", Exact = true }).SelectOptionAsync("user");
        await content.GetByRole(AriaRole.Combobox, new() { Name = "Member", Exact = true }).FillAsync(WorldIdentities.Dave);
        await content.GetByRole(AriaRole.Button, new() { Name = "Add member", Exact = true }).ClickAsync();
        await Expect(content.Locator("table tbody")).ToContainTextAsync(WorldIdentities.Dave);

        // Add the group to the tenant's members.
        await Shell.GotoAsync(page, world.Head, $"/t/{ExplorerWorld.Globex}/access/members");
        await Expect(content.Locator("[data-lt-tenant-view=members]")).ToBeVisibleAsync();
        await AddSubjectAsync(page, "Member", "tenant-group", Group, "Add member");
        await Expect(content.Locator("table tbody")).ToContainTextAsync(Group);

        // Grant the group Read on the tenant's orders.
        await Shell.GotoAsync(page, world.Head, $"/t/{ExplorerWorld.Globex}/access/rules?new=true");
        var editor = page.Locator($"form[data-lt-tenant-rule-editor={ExplorerWorld.Globex}]");
        await Expect(editor).ToBeVisibleAsync();
        await editor.GetByRole(AriaRole.Textbox, new() { Name = "Rule id", Exact = true }).FillAsync(RuleId);
        await AddSubjectAsync(page, "Subject", "tenant-group", Group, submit: null, scope: editor);
        await PickAsync(page, editor.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true }), ExplorerWorld.GlobexOrdersTree);
        await editor.GetByLabel("Range read", new() { Exact = true }).CheckAsync();
        await editor.GetByRole(AriaRole.Button, new() { Name = "Save rule" }).ClickAsync();
        await Expect(content.Locator("[data-lt-rule-layer=tenant]")).ToContainTextAsync(RuleId);

        // The new member reads the tree.
        var reader = await OpenAsync(world.Head, $"/t/{ExplorerWorld.Globex}/data/{ExplorerWorld.GlobexOrdersTree}", WorldIdentities.Dave);
        await Expect(Shell.Content(reader)).ToContainTextAsync("order-001");
        await Expect(Shell.Content(reader)).ToContainTextAsync("order-003");
    }

    [Test]
    public async Task Explain_shows_the_platform_layer_deciding_over_a_shadowed_tenant_rule()
    {
        var world = await UiHosts.DelegatedWorldAsync();
        var page = await OpenAsync(
            world.Head,
            $"/t/{ExplorerWorld.Globex}/access/explain?subject={WorldIdentities.Alice}&kind=user&tree={ExplorerWorld.GlobexInvoicesTree}",
            WorldIdentities.GlobexAdmin);
        var form = page.Locator($"form[data-lt-tenant-explain={ExplorerWorld.Globex}]");
        await Expect(form).ToBeVisibleAsync();

        await form.GetByRole(AriaRole.Button, new() { Name = "Explain", Exact = true }).ClickAsync();

        var content = Shell.Content(page);
        await Expect(content.Locator("[data-lt-explanation]")).ToHaveAttributeAsync("data-lt-explanation", "denied");
        var layers = content.Locator("ol.lt-access-layers");
        await Expect(layers).ToHaveAttributeAsync("data-lt-deciding-layer", "platform");
        await Expect(layers.Locator("[data-lt-layer=platform]")).ToHaveAttributeAsync("data-lt-layer-state", "deciding");
        await Expect(layers.Locator("[data-lt-layer=tenant]")).ToHaveAttributeAsync("data-lt-layer-state", "losing");
        await Expect(layers.Locator($"[data-lt-rule-id=\"{ExplorerWorld.GlobexPlatformRuleId}\"]")).ToBeVisibleAsync();
        await Expect(layers.Locator($"[data-lt-rule-id=\"{ExplorerWorld.GlobexReadersRuleId}\"]")).ToBeVisibleAsync();
        await Expect(content.Locator($"[data-lt-shadowed-by=\"{ExplorerWorld.GlobexPlatformRuleId}\"]")).ToBeVisibleAsync();
    }

    [Test]
    public async Task A_tenant_admin_sees_and_reaches_nothing_of_another_tenant()
    {
        var world = await UiHosts.DelegatedWorldAsync();

        // The facade refuses globex's administrator for acme, asserted or not.
        var foreign = world.TenantDirectory(WorldIdentities.GlobexAdmin, ExplorerWorld.Acme);
        var listing = Assert.ThrowsAsync<RpcException>(() => foreign.ListGroupsAsync(ExplorerWorld.Acme, new TenantAccessPageRequest()));
        Assert.That(listing!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
        var crossing = Assert.ThrowsAsync<RpcException>(
            () => world.TenantDirectory(WorldIdentities.GlobexAdmin, ExplorerWorld.Globex).ListGroupsAsync(ExplorerWorld.Acme, new TenantAccessPageRequest()));
        Assert.That(crossing!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
        var posture = Assert.ThrowsAsync<RpcException>(
            () => world.TenantPolicy(WorldIdentities.GlobexAdmin, ExplorerWorld.Acme).GetPostureAsync(ExplorerWorld.Acme));
        Assert.That(posture!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));

        // Globex's own pages list only globex's groups.
        var page = await OpenAsync(world.Head, $"/t/{ExplorerWorld.Globex}/access/groups", WorldIdentities.GlobexAdmin);
        var content = Shell.Content(page);
        await Expect(content.Locator("[data-lt-tenant-view=groups] table tbody")).ToContainTextAsync(ExplorerWorld.GlobexReadersGroup);
        await Expect(content).Not.ToContainTextAsync(ExplorerWorld.AcmeGroup);

        // Acme's addresses, followed directly, show none of acme's access.
        foreach (var path in new[] { "groups", "members", "rules", "explain" })
        {
            // A tenant administrator is not a platform operator, so another tenant's
            // address is never honoured: it lands on globex's own page.
            await Shell.GotoAsync(page, world.Head, $"/t/{ExplorerWorld.Acme}/access/{path}");
            await Expect(Shell.Heading(page)).ToBeVisibleAsync();
            await Expect(page).Not.ToHaveURLAsync(new Regex($"/t/{ExplorerWorld.Acme}/"));
            await Expect(content.Locator($"[data-lt-tenant-view][aria-label$=\"tenant {ExplorerWorld.Acme}\"]")).ToHaveCountAsync(0);
            await Expect(content).Not.ToContainTextAsync(ExplorerWorld.AcmeGroup);
        }

        await Shell.GotoAsync(page, world.Head, $"/t/{ExplorerWorld.Acme}/access/groups/{ExplorerWorld.AcmeGroup}");
        await Expect(Shell.Heading(page)).ToHaveTextAsync(ExplorerAreas.NotFoundHeading);
        await Expect(content.Locator("[data-lt-tenant-view]")).ToHaveCountAsync(0);
    }

    [Test]
    public async Task With_the_feature_off_the_tenant_groups_page_says_so_and_the_routes_refuse()
    {
        var world = await UiHosts.TenantWorldAsync();

        var page = await OpenAsync(world.Head, $"/t/{ExplorerWorld.Globex}/access/groups", WorldIdentities.Admin);
        var content = Shell.Content(page);
        var caveat = content.Locator("[data-lt-cluster-wide-groups]");
        await Expect(caveat).ToHaveAttributeAsync("data-lt-tenant-access", "off");
        await Expect(caveat).ToContainTextAsync("Delegated tenant access administration is off");
        await Expect(content.Locator("[data-lt-tenant-view]")).ToHaveCountAsync(0);

        await Shell.GotoAsync(page, world.Head, $"/t/{ExplorerWorld.Globex}/access/members");
        await Expect(content.Locator("[data-lt-tenant-access]")).ToHaveAttributeAsync("data-lt-tenant-access", "off");
        await Expect(content.Locator("[data-lt-tenant-view]")).ToHaveCountAsync(0);

        // The facade itself refuses, naming the switch.
        var refusal = Assert.ThrowsAsync<RpcException>(
            () => world.TenantDirectory(WorldIdentities.Admin, ExplorerWorld.Globex).ListGroupsAsync(ExplorerWorld.Globex, new TenantAccessPageRequest()));
        Assert.Multiple(() =>
        {
            Assert.That(refusal!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
            Assert.That(refusal.Status.Detail, Is.EqualTo(new TenantAccessAdministrationDisabledException(ExplorerWorld.Globex).Message));
        });
    }

    /// <summary>
    /// Chooses a subject in an <c>AccessSubjectPicker</c> labelled <paramref name="label"/>:
    /// its kind, then the subject itself, and submits with <paramref name="submit"/> when given.
    /// </summary>
    private static async Task AddSubjectAsync(IPage page, string label, string kind, string subject, string? submit, ILocator? scope = null)
    {
        var within = scope ?? Shell.Content(page);
        await within.GetByRole(AriaRole.Combobox, new() { Name = $"{label} kind", Exact = true }).SelectOptionAsync(kind);
        await PickAsync(page, within.GetByRole(AriaRole.Combobox, new() { Name = label, Exact = true }), subject);
        if (submit is not null)
        {
            await within.GetByRole(AriaRole.Button, new() { Name = submit, Exact = true }).ClickAsync();
        }
    }

    /// <summary>Types <paramref name="value"/> into a picker and chooses the suggestion that names it exactly.</summary>
    private static async Task PickAsync(IPage page, ILocator box, string value)
    {
        await box.FillAsync(value);
        var option = page.Locator(".lt-combobox__list [role=option]").Filter(new()
        {
            Has = page.Locator(".lt-combobox__value", new() { HasTextRegex = new Regex($"^\\s*{Regex.Escape(value)}\\s*$") }),
        });
        await option.First.ClickAsync();
        await Expect(box).ToHaveValueAsync(value);
    }
}
