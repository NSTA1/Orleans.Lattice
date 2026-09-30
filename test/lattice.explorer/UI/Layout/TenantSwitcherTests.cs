using Bunit;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Session;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Session;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// Issue #3962: the top-bar tenant switcher. It is absent unless a signed-in
/// caller may switch between two or more tenants; it lists every one of them,
/// including the default tenant an operator reaches; choosing one re-roots the
/// address through the same fail-closed switch as the address line; it is
/// keyboard-complete; the palette opens it; and at phone width it lives in the
/// directory sheet.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenantSwitcherTests : ShellLayoutTestContext
{
    private static readonly string[] Reachable = ["acme", "default", "globex"];

    private FakeAuthSession Auth => (FakeAuthSession)Services.GetRequiredService<IExplorerAuthSession>();

    [Test]
    public void It_is_absent_with_tenancy_off()
    {
        AddArea(new FakeArea("data", "Data"));
        var cut = RenderSignedIn("data");

        cut.WaitUntil(() => Assert.That(cut.FindAll("#page-body"), Has.Count.EqualTo(1)));
        Assert.That(Toggles(cut), Is.Empty);
    }

    [Test]
    public void It_is_absent_with_a_single_reachable_tenant()
    {
        Operator(allowSwitch: true, "acme");
        var cut = RenderSignedIn("t/acme/data/orders");

        cut.WaitUntil(() => Assert.That(cut.FindAll("#page-body"), Has.Count.EqualTo(1)));
        Assert.That(Toggles(cut), Is.Empty);
    }

    [Test]
    public void It_is_absent_for_a_caller_who_may_not_switch_even_with_several_tenants()
    {
        UseTenancy("acme", allowSwitch: false, Reachable);
        AddAreas();
        var cut = RenderSignedIn("t/acme/data/orders");

        cut.WaitUntil(() => Assert.That(cut.FindAll("#page-body"), Has.Count.EqualTo(1)));
        Assert.That(Toggles(cut), Is.Empty);
    }

    [Test]
    public void It_is_shown_with_two_or_more_tenants_naming_the_active_one()
    {
        Operator(allowSwitch: true, Reachable);
        var cut = RenderSignedIn("t/acme/data/orders");

        cut.WaitUntil(() => Assert.That(Toggles(cut), Has.Count.EqualTo(1)));
        var toggle = Toggle(cut);
        Assert.Multiple(() =>
        {
            Assert.That(toggle.TextContent, Does.Contain("Tenant").And.Contain("acme"));
            Assert.That(toggle.GetAttribute("aria-expanded"), Is.EqualTo("false"));
            Assert.That(toggle.GetAttribute("aria-controls"), Is.Not.Empty);
        });
        ExplorerCommandControls.AssertVisibleControl(cut, new ExplorerCommand(ChromeCommands.TenantSwitchId, "Switch tenant"));
    }

    [Test]
    public void It_lists_every_reachable_tenant_including_default_for_an_operator()
    {
        Operator(allowSwitch: true, Reachable);
        var cut = RenderSignedIn("t/acme/data/orders");
        cut.WaitUntil(() => Assert.That(Toggles(cut), Has.Count.EqualTo(1)));

        Toggle(cut).Click();
        Field(cut).KeyDown(new KeyboardEventArgs { Key = "ArrowDown" });

        cut.WaitUntil(() => Assert.That(Options(cut), Is.EqualTo(new[] { "acme", "default", "globex" })));
        Assert.Multiple(() =>
        {
            Assert.That(Toggle(cut).GetAttribute("aria-expanded"), Is.EqualTo("true"));
            Assert.That(cut.Find(".lt-shell-tenant__panel").GetAttribute("role"), Is.EqualTo("group"));
            Assert.That(cut.Find(".lt-shell-tenant__panel .lt-combobox__option .lt-combobox__detail").TextContent, Is.EqualTo(TenantSwitchChoices.ActiveDetail));
        });
    }

    [Test]
    public void Choosing_a_tenant_by_keyboard_re_roots_the_address_and_the_switch_is_announced()
    {
        Operator(allowSwitch: true, Reachable);
        var cut = RenderSignedIn("t/acme/data/orders");
        cut.WaitUntil(() => Assert.That(Toggles(cut), Has.Count.EqualTo(1)));

        Toggle(cut).Click();
        Field(cut).KeyDown(new KeyboardEventArgs { Key = "ArrowDown" });
        cut.WaitUntil(() => Assert.That(Options(cut), Has.Length.EqualTo(3)));
        Field(cut).KeyDown(new KeyboardEventArgs { Key = "ArrowDown" });
        Field(cut).KeyDown(new KeyboardEventArgs { Key = "ArrowDown" });
        Assert.That(Field(cut).GetAttribute("aria-activedescendant"), Is.EqualTo(cut.FindAll(".lt-shell-tenant__panel [role=option]")[2].Id));
        Field(cut).KeyDown(new KeyboardEventArgs { Key = "Enter" });

        cut.WaitUntil(() => Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri + "t/globex/data/orders")));
        RenderAgain(cut);

        cut.WaitUntil(() =>
        {
            Assert.That(Switcher!.ReceivedCalls().Any(call => call.GetMethodInfo().Name == nameof(IExplorerTenantSwitcher.SwitchTenantAsync)), Is.True);
            Assert.That(cut.Find(".lt-toasts").TextContent, Does.Contain(ExplorerNavigator.SwitchedNotice("globex")));
            Assert.That(Toggle(cut).TextContent, Does.Contain("globex"));
            Assert.That(cut.FindAll(".lt-shell-tenant__panel"), Is.Empty, "the panel closes on a choice");
        });
    }

    [Test]
    public void A_refused_switch_stays_in_the_active_tenant_and_shows_the_notice()
    {
        Operator(allowSwitch: false, Reachable);
        var cut = RenderSignedIn("t/acme/data/orders");
        cut.WaitUntil(() => Assert.That(Toggles(cut), Has.Count.EqualTo(1)));

        Toggle(cut).Click();
        Field(cut).Input("globex");
        cut.WaitUntil(() => Assert.That(Options(cut), Is.EqualTo(new[] { "globex" })));
        Field(cut).KeyDown(new KeyboardEventArgs { Key = "ArrowDown" });
        Field(cut).KeyDown(new KeyboardEventArgs { Key = "Enter" });
        cut.WaitUntil(() => Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri + "t/globex/data/orders")));
        RenderAgain(cut);

        cut.WaitUntil(() =>
        {
            Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri + "t/acme/data/orders"));
            Assert.That(cut.Find(".lt-toasts").TextContent, Does.Contain(ExplorerNavigator.RefusedNotice("globex", "acme")));
        });
    }

    [Test]
    public void At_a_cluster_wide_address_the_caller_stays_and_only_the_tenant_changes()
    {
        Operator(allowSwitch: true, Reachable);
        var cut = RenderSignedIn("cluster");
        cut.WaitUntil(() => Assert.That(Toggles(cut), Has.Count.EqualTo(1)));

        Toggle(cut).Click();
        Field(cut).Input("globex");
        cut.WaitUntil(() => Assert.That(Options(cut), Is.EqualTo(new[] { "globex" })));
        Field(cut).KeyDown(new KeyboardEventArgs { Key = "ArrowDown" });
        Field(cut).KeyDown(new KeyboardEventArgs { Key = "Enter" });
        RenderAgain(cut);

        cut.WaitUntil(() =>
        {
            Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri + "cluster"));
            Assert.That(cut.Find(".lt-toasts").TextContent, Does.Contain(ExplorerNavigator.SwitchedNotice("globex")));
            Assert.That(Toggle(cut).TextContent, Does.Contain("globex"));
        });
    }

    [Test]
    public void Escape_closes_the_panel_and_returns_focus_to_the_button()
    {
        Operator(allowSwitch: true, Reachable);
        var cut = RenderSignedIn("t/acme/data/orders");
        cut.WaitUntil(() => Assert.That(Toggles(cut), Has.Count.EqualTo(1)));
        Toggle(cut).Click();
        var focusesBefore = Module.Invocations["focusElement"].Count;

        Field(cut).KeyDown(new KeyboardEventArgs { Key = "ArrowDown" });
        cut.WaitUntil(() => Assert.That(Options(cut), Has.Length.EqualTo(3)));
        Field(cut).KeyDown(new KeyboardEventArgs { Key = "Escape" });
        Assert.That(cut.FindAll(".lt-shell-tenant__panel"), Has.Count.EqualTo(1), "the first Escape closes only the list");
        Assert.That(Field(cut).GetAttribute("aria-expanded"), Is.EqualTo("false"));

        Field(cut).KeyDown(new KeyboardEventArgs { Key = "Escape" });

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-shell-tenant__panel"), Is.Empty);
            Assert.That(Toggle(cut).GetAttribute("aria-expanded"), Is.EqualTo("false"));
            Assert.That(Module.Invocations["focusElement"], Has.Count.GreaterThan(focusesBefore), "focus goes back to the button through the chrome's safe route");
        });
    }

    [Test]
    public void Opening_the_panel_lists_every_tenant_at_once_with_the_active_one_marked()
    {
        // Issue #3986: it is a dropdown, not an empty text field that lists only after Down.
        Operator(allowSwitch: true, Reachable);
        var cut = RenderSignedIn("t/acme/data/orders");
        cut.WaitUntil(() => Assert.That(Toggles(cut), Has.Count.EqualTo(1)));

        Toggle(cut).Click();
        Field(cut).Focus();

        cut.WaitUntil(() => Assert.That(Options(cut), Is.EqualTo(new[] { "acme", "default", "globex" })));
        var active = cut.FindAll(".lt-shell-tenant__panel [role=option]")[0];
        Assert.Multiple(() =>
        {
            Assert.That(Field(cut).GetAttribute("aria-expanded"), Is.EqualTo("true"));
            Assert.That(Field(cut).GetAttribute("placeholder"), Is.EqualTo(TenantSwitcher.FilterPlaceholder));
            Assert.That(active.QuerySelector(".lt-node")!.ClassList, Does.Contain("lt-node--join"), "the active tenant carries the you-are-here node");
            Assert.That(active.QuerySelector(".lt-combobox__detail")!.TextContent, Is.EqualTo(TenantSwitchChoices.ActiveDetail));
            Assert.That(cut.FindAll(".lt-shell-tenant__panel .lt-node--join"), Has.Count.EqualTo(1));
        });

        Field(cut).Input("glo");
        cut.WaitUntil(() => Assert.That(Options(cut), Is.EqualTo(new[] { "globex" }), "typing filters the list"));
    }

    [Test]
    public async Task At_phone_width_focusing_the_stacked_field_lists_every_tenant()
    {
        Operator(allowSwitch: true, Reachable);
        var cut = RenderSignedIn("t/acme/data/orders");
        cut.WaitUntil(() => Assert.That(Toggles(cut), Has.Count.EqualTo(1)));
        await SetBandAsync(cut, 0);
        cut.FindAll(".lt-shell-header button").Single(button => button.TextContent.Trim() == "Directory").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-shell-tenant--stacked input"), Has.Count.EqualTo(1)));

        cut.Find(".lt-shell-tenant--stacked input").Focus();

        cut.WaitUntil(() => Assert.That(
            cut.FindAll(".lt-shell-tenant--stacked [role=option] .lt-combobox__value").Select(value => value.TextContent),
            Is.EqualTo(Reachable)));
    }

    [Test]
    public void Opening_the_switcher_closes_the_appearance_menu_and_opening_appearance_closes_the_switcher()
    {
        // Issue #3986: header panels close each other.
        Operator(allowSwitch: true, Reachable);
        var cut = RenderSignedIn("t/acme/data/orders");
        cut.WaitUntil(() => Assert.That(Toggles(cut), Has.Count.EqualTo(1)));
        var appearance = $".lt-shell-header button[data-lt-command=\"{ChromeCommands.AppearanceMenuId}\"]";

        cut.Find(appearance).Click();
        Assert.That(AppearancePanels(cut), Has.Count.EqualTo(1));
        Toggle(cut).Click();

        cut.WaitUntil(() =>
        {
            Assert.That(AppearancePanels(cut), Is.Empty);
            Assert.That(cut.Find(appearance).GetAttribute("aria-expanded"), Is.EqualTo("false"));
            Assert.That(cut.FindAll(".lt-shell-tenant__panel"), Has.Count.EqualTo(1));
        });

        cut.Find(appearance).Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-shell-tenant__panel"), Is.Empty);
            Assert.That(Toggle(cut).GetAttribute("aria-expanded"), Is.EqualTo("false"));
            Assert.That(AppearancePanels(cut), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void Opening_the_connection_settings_closes_an_open_header_panel()
    {
        Operator(allowSwitch: true, Reachable);
        var cut = RenderSignedIn("t/acme/data/orders");
        cut.WaitUntil(() => Assert.That(Toggles(cut), Has.Count.EqualTo(1)));
        Toggle(cut).Click();
        Assert.That(cut.FindAll(".lt-shell-tenant__panel"), Has.Count.EqualTo(1));

        cut.InvokeAsync(() => Services.GetRequiredService<SessionChromeState>().OpenConfiguration());

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-shell-tenant__panel"), Is.Empty);
            Assert.That(Toggle(cut).GetAttribute("aria-expanded"), Is.EqualTo("false"));
        });
    }

    [Test]
    public void It_appears_on_sign_in_and_disappears_on_sign_out()
    {
        Operator(allowSwitch: true, Reachable);
        Navigation.NavigateTo("t/acme/data/orders");
        var cut = RenderLayout();
        cut.WaitUntil(() => Assert.That(cut.FindAll("#page-body"), Has.Count.EqualTo(1)));
        Assert.That(Toggles(cut), Is.Empty, "a signed-out caller never sees it");

        cut.InvokeAsync(() => Auth.SignIn("dana"));
        cut.WaitUntil(() => Assert.That(Toggles(cut), Has.Count.EqualTo(1)));

        cut.InvokeAsync(() => Auth.LogoutAsync());
        cut.WaitUntil(() => Assert.That(Toggles(cut), Is.Empty));
    }

    [Test]
    public async Task The_palettes_switch_tenant_command_opens_it()
    {
        Operator(allowSwitch: true, Reachable);
        var cut = RenderSignedIn("t/acme/data/orders");
        cut.WaitUntil(() => Assert.That(Toggles(cut), Has.Count.EqualTo(1)));

        await PressShortcutAsync(cut);
        cut.Find(".lt-shell-address input").Input(">switch tenant");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-shell-address [role=listbox]").TextContent, Does.Contain("Switch tenant")));
        cut.Find(".lt-shell-address input").KeyDown(new KeyboardEventArgs { Key = "Enter" });

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-shell-tenant__panel"), Has.Count.EqualTo(1));
            Assert.That(Toggle(cut).GetAttribute("aria-expanded"), Is.EqualTo("true"));
        });
        Assert.That(Services.GetRequiredService<ExplorerTenantSwitch>().IsOpenRequested, Is.False, "the header's switcher took the request");
    }

    [Test]
    public async Task The_palette_offers_no_switch_tenant_command_when_it_is_absent()
    {
        Operator(allowSwitch: true, "acme");
        var cut = RenderSignedIn("t/acme/data/orders");
        cut.WaitUntil(() => Assert.That(cut.FindAll("#page-body"), Has.Count.EqualTo(1)));

        await PressShortcutAsync(cut);
        cut.Find(".lt-shell-address input").Input(">theme");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-shell-address [role=option]"), Is.Not.Empty));
        cut.Find(".lt-shell-address input").Input(">switch tenant");

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-shell-address [role=option]"), Is.Empty));
    }

    [Test]
    public async Task At_phone_width_it_lives_in_the_directory_sheet_and_the_palette_opens_the_sheet()
    {
        Operator(allowSwitch: true, Reachable);
        var cut = RenderSignedIn("t/acme/data/orders");
        cut.WaitUntil(() => Assert.That(Toggles(cut), Has.Count.EqualTo(1)));

        await SetBandAsync(cut, 0);
        Assert.That(Toggles(cut), Is.Empty, "the compact header keeps only the directory, the mark and the menu");

        await PressShortcutAsync(cut);
        cut.Find(".lt-shell-address input").Input(">switch tenant");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-shell-address [role=listbox]").TextContent, Does.Contain("Switch tenant")));
        cut.Find(".lt-shell-address input").KeyDown(new KeyboardEventArgs { Key = "Enter" });

        cut.WaitUntil(() =>
        {
            var sheet = cut.Find(".lt-dialog--sheet");
            Assert.That(sheet.QuerySelector(".lt-shell-tenant--stacked input[role=combobox]"), Is.Not.Null);
            Assert.That(sheet.QuerySelector(".lt-shell-directory, nav"), Is.Not.Null);
            Assert.That(Services.GetRequiredService<ExplorerTenantSwitch>().IsOpenRequested, Is.False, "the stacked switcher took the request");
        });
        var control = cut.FindAll($"[data-lt-command=\"{ChromeCommands.TenantSwitchId}\"]").Single();
        Assert.Multiple(() =>
        {
            Assert.That(control.LocalName, Is.EqualTo("input"), "the stacked field itself is the command's visible control");
            Assert.That(cut.Find($"label[for=\"{control.Id}\"]").TextContent, Is.EqualTo("Switch tenant"));
        });

        var field = cut.Find(".lt-shell-tenant--stacked input");
        field.KeyDown(new KeyboardEventArgs { Key = "ArrowDown" });
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-shell-tenant--stacked [role=option]").Select(option => option.QuerySelector(".lt-combobox__value")!.TextContent), Is.EqualTo(Reachable)));
    }

    private void Operator(bool allowSwitch, params string[] reachable)
    {
        UseTenancy("acme", allowSwitch, reachable);
        Switcher!.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(true));
        AddAreas();
    }

    private void AddAreas()
    {
        AddArea(new FakeArea("data", "Data", 1));
        AddArea(new FakeArea("cluster", "Cluster", 2) { IsTenantScoped = false });
    }

    private IRenderedComponent<ShellLayout> RenderSignedIn(string relative)
    {
        Auth.SignIn("dana");
        Navigation.NavigateTo(relative);
        return RenderLayout();
    }

    // The router renders the layout again after every navigation.
    private static void RenderAgain(IRenderedComponent<ShellLayout> cut) =>
        cut.Render(parameters => parameters.Add(layout => layout.Body, PageBody));

    private static IReadOnlyList<AngleSharp.Dom.IElement> Toggles(IRenderedComponent<ShellLayout> cut) =>
        cut.FindAll($".lt-shell-header button[data-lt-command=\"{ChromeCommands.TenantSwitchId}\"]");

    private static AngleSharp.Dom.IElement Toggle(IRenderedComponent<ShellLayout> cut) => Toggles(cut).Single();

    private static AngleSharp.Dom.IElement Field(IRenderedComponent<ShellLayout> cut) => cut.Find(".lt-shell-tenant__panel input[role=combobox]");

    private static string[] Options(IRenderedComponent<ShellLayout> cut) =>
        [.. cut.FindAll(".lt-shell-tenant__panel [role=option] .lt-combobox__value").Select(value => value.TextContent)];

    private static IReadOnlyList<AngleSharp.Dom.IElement> AppearancePanels(IRenderedComponent<ShellLayout> cut) =>
        cut.FindAll(".lt-shell-header .lt-shell-menu[aria-label=\"Appearance\"]");
}
