using Bunit;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.JSInterop;
using NSubstitute;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// The layout: skip links, landmarks, the session slots, the gate on the main
/// landmark, tenancy redirects, and the global shortcut.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ShellLayoutTests : ShellLayoutTestContext
{
    [Test]
    public void The_skip_links_come_first_and_name_directory_address_and_content()
    {
        var cut = RenderLayout();

        var focusable = cut.FindAll("a[href], button, input, select, textarea, [tabindex]")
            .Where(element => element.GetAttribute("tabindex") != "-1")
            .Take(3)
            .Select(element => (element.TextContent.Trim(), element.GetAttribute("href")))
            .ToArray();

        Assert.That(focusable, Is.EqualTo(new[]
        {
            ("Skip to directory", "#lt-shell-directory"),
            ("Skip to address", "#lt-shell-address"),
            ("Skip to content", "#lt-shell-content"),
        }));
    }

    [Test]
    public void The_frame_has_a_banner_an_address_a_directory_and_a_main_landmark()
    {
        var cut = RenderLayout();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("header.lt-shell-header"), Has.Count.EqualTo(1));
            Assert.That(cut.Find("nav[aria-label='Address']"), Is.Not.Null);
            Assert.That(cut.Find("#lt-shell-directory nav").GetAttribute("aria-labelledby"), Is.Not.Null.And.Not.Empty);
            Assert.That(cut.Find("main#lt-shell-content").GetAttribute("tabindex"), Is.EqualTo("-1"));
            Assert.That(cut.Find("main #page-body").TextContent, Is.EqualTo("The page"));
            Assert.That(cut.Find(".lt-shell-brand").TextContent, Does.Contain("Lattice Explorer"));
            Assert.That(cut.Markup, Does.Not.Contain("Shell"), "the product name never says Shell");
        });
    }

    [Test]
    public void The_root_is_the_breakpoint_container()
    {
        var cut = RenderLayout();

        Assert.That(cut.Find(".lt-shell").ClassList, Does.Contain("lt-viewport"));
    }

    [Test]
    public void The_header_renders_the_session_slots_in_order_and_the_overlay_at_the_root()
    {
        AddSlotProbes();

        var cut = RenderLayout();

        Assert.Multiple(() =>
        {
            Assert.That(
                cut.FindAll(".lt-shell-header [data-probe]").Select(element => element.GetAttribute("data-probe")),
                Is.EqualTo(new[] { "connection", "identity" }));
            Assert.That(cut.FindAll(".lt-shell > [data-probe='overlay']"), Has.Count.EqualTo(1));
            Assert.That(cut.Find(".lt-shell-header").TextContent, Does.Contain("Appearance"));
        });
    }

    [Test]
    public void Empty_slots_render_nothing()
    {
        var cut = RenderLayout();

        Assert.That(cut.FindAll("[data-probe]"), Is.Empty);
    }

    [Test]
    public void Skip_links_move_focus_to_their_regions()
    {
        var cut = RenderLayout();

        cut.FindAll(".lt-shell-skip__link")[2].Click();
        cut.FindAll(".lt-shell-skip__link")[0].Click();
        cut.FindAll(".lt-shell-skip__link")[1].Click();

        // Every skip link focuses through the chrome module, which focuses only an element
        // still in the document.
        Assert.That(JSInterop.Invocations.Count(invocation => invocation.Identifier == "focusElement"), Is.GreaterThanOrEqualTo(3));
    }

    [Test]
    public void A_refused_skip_link_focus_leaves_the_layout_alive()
    {
        var refused = new JSException("Unable to focus an invalid element.");
        JSInterop.SetupVoid("Blazor._internal.domWrapper.focus", _ => true).SetException(refused);
        JSInterop.SetupModule(ShellChromeAssets.ModuleSpecifier).SetupVoid("focusElement", _ => true).SetException(refused);
        var cut = RenderLayout();

        foreach (var index in new[] { 0, 1, 2 })
        {
            cut.FindAll(".lt-shell-skip__link")[index].Click();
        }

        // Still answering: the address line still opens.
        cut.Find(".lt-shell-address-line__edit").Click();
        Assert.That(cut.FindAll("input[role='combobox']"), Has.Count.EqualTo(1));
    }

    [Test]
    public void A_visible_areas_page_renders_once_its_availability_is_known()
    {
        var gate = new TaskCompletionSource<AreaAvailability>();
        AddArea(new FakeArea("data", "Data") { Availability = _ => new ValueTask<AreaAvailability>(gate.Task) });
        Navigation.NavigateTo("data/orders");

        var cut = RenderLayout();
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("#page-body"), Is.Empty, "an area's page never renders before its availability is known");
            Assert.That(cut.FindAll("main .lt-skeleton"), Has.Count.EqualTo(1));
        });

        gate.SetResult(AreaAvailability.Visible);

        cut.WaitUntil(() => Assert.That(cut.FindAll("main #page-body"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void A_hidden_areas_address_renders_not_found()
    {
        AddArea(new FakeArea("data", "Data") { Availability = _ => ValueTask.FromResult(AreaAvailability.Hidden) });
        Navigation.NavigateTo("data/orders");

        var cut = RenderLayout();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("#page-body"), Is.Empty);
            Assert.That(cut.Find("main h1").TextContent, Is.EqualTo("Nothing lives at this address"));
        });
    }

    [Test]
    public void An_unavailable_areas_address_says_why()
    {
        AddArea(new FakeArea("backups", "Backups")
        {
            Availability = _ => ValueTask.FromResult(AreaAvailability.Unavailable("Sign in to see backups.")),
        });
        Navigation.NavigateTo("backups");

        var cut = RenderLayout();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("#page-body"), Is.Empty);
            Assert.That(cut.Find("main h1").TextContent, Is.EqualTo("Backups"));
            Assert.That(cut.Find("main .lt-empty").TextContent, Does.Contain("Sign in to see backups."));
        });
    }

    [Test]
    public void An_address_outside_every_area_renders_the_page_for_the_router_to_decide()
    {
        Navigation.NavigateTo("reset-view");

        var cut = RenderLayout();

        Assert.That(cut.FindAll("main #page-body"), Has.Count.EqualTo(1));
    }

    [Test]
    public void With_tenancy_off_a_tenant_address_is_replaced_by_its_plain_form()
    {
        AddArea(new FakeArea("data", "Data"));
        Navigation.NavigateTo("t/acme/data/orders");

        RenderLayout();

        Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri + "data/orders"));
    }

    [Test]
    public void A_default_tenant_non_operator_is_redirected_to_the_plain_address()
    {
        UseTenancy("default");
        Switcher!.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(false));
        AddArea(new FakeArea("data", "Data"));
        Navigation.NavigateTo("t/default/data");

        RenderLayout();

        Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri + "data"));
    }

    [Test]
    public void A_default_tenant_operator_keeps_the_tenant_root()
    {
        UseTenancy("default");
        Switcher!.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(true));
        AddArea(new FakeArea("data", "Data"));
        Navigation.NavigateTo("data");

        RenderLayout();

        Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri + "t/default/data"));
    }

    [Test]
    public void With_tenancy_on_a_refused_switch_redirects_to_the_active_tenant_and_is_announced()
    {
        UseTenancy("acme");
        AddArea(new FakeArea("data", "Data"));
        Navigation.NavigateTo("t/globex/data");

        var cut = RenderLayout();

        Assert.Multiple(() =>
        {
            Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri + "t/acme/data"));
            Assert.That(cut.Find(".lt-toasts").TextContent, Does.Contain("You can't scope to tenant globex"));
        });
    }

    [Test]
    public void With_tenancy_on_the_address_and_links_are_rooted_at_the_active_tenant()
    {
        UseTenancy("acme");
        AddArea(new FakeArea("data", "Data"));
        Navigation.NavigateTo("t/acme/data");

        var cut = RenderLayout();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-shell-brand").GetAttribute("href"), Is.EqualTo("t/acme"));
            Assert.That(cut.FindAll(".lt-chain__text").Select(node => node.TextContent), Is.EqualTo(new[] { "t/acme", "data" }));
            Assert.That(cut.Find("[data-lt-command='go.data']").GetAttribute("href"), Is.EqualTo("t/acme/data"));
        });
    }

    [Test]
    public void A_navigation_re_resolves_the_location()
    {
        AddArea(new FakeArea("data", "Data"));
        var cut = RenderLayout();

        NavigateAndRender(cut, "data/a/crm");

        cut.WaitUntil(() =>
            Assert.That(cut.FindAll(".lt-chain__text").Select(node => node.TextContent), Is.EqualTo(new[] { "Home", "data", "a", "crm" })));
    }

    [Test]
    public void Invalidating_the_directory_asks_the_areas_again()
    {
        var data = AddArea(new FakeArea("data", "Data"));
        var cut = RenderLayout();
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-command='go.data']"), Has.Count.EqualTo(1)));
        var before = data.AvailabilityCalls;

        data.Availability = _ => ValueTask.FromResult(AreaAvailability.Hidden);
        cut.InvokeAsync(() => Services.GetRequiredService<ExplorerAreaDirectory>().Invalidate());

        cut.WaitUntil(() =>
        {
            Assert.That(data.AvailabilityCalls, Is.GreaterThan(before));
            Assert.That(cut.FindAll("[data-lt-command='go.data']"), Is.Empty);
        });
    }

    [Test]
    public async Task The_global_shortcut_opens_the_address_line()
    {
        var cut = RenderLayout();

        await PressShortcutAsync(cut);

        Assert.That(cut.Find("input[role='combobox']").GetAttribute("value"), Is.EqualTo("/"));
    }

    [Test]
    public void The_layout_registers_the_shortcut_and_the_width_observer_and_applies_the_appearance()
    {
        RenderLayout();

        Assert.Multiple(() =>
        {
            Assert.That(Module.Invocations["registerShortcuts"], Has.Count.EqualTo(1));
            Assert.That(Module.Invocations["observeViewport"].Single().Arguments[2], Is.EqualTo(new[] { LtBreakpoints.MediumMinimumWidth, LtBreakpoints.ExpandedMinimumWidth }));
            Assert.That(Applier.Applied, Is.Not.Empty, "the remembered appearance is applied after the first render");
        });
    }

    [Test]
    public async Task Disposing_the_layout_is_safe()
    {
        var cut = RenderLayout();

        await cut.Instance.DisposeAsync();

        Assert.Pass("disposed without a fault");
    }

    [Test]
    public void Escape_in_the_address_input_restores_the_chain()
    {
        var cut = RenderLayout();
        cut.Find(".lt-shell-address-line__edit").Click();

        cut.Find("input[role='combobox']").KeyDown(new KeyboardEventArgs { Key = "Escape" });

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("input[role='combobox']"), Is.Empty);
            Assert.That(cut.FindAll("nav[aria-label='Address']"), Has.Count.EqualTo(1));
        });
    }
}
