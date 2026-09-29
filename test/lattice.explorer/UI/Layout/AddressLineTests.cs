using Bunit;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.JSInterop;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Layout.Appearance;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// The address line: the chain, and the combobox it turns into - typed prefixes,
/// keyboard operation, the command palette, and completions from every visible
/// area arriving in parallel with a per-source timeout.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed partial class AddressLineTests : ShellChromeTestContext
{
    [Test]
    public void The_address_is_a_mono_chain_whose_ancestors_are_links_and_whose_current_node_is_the_marker()
    {
        var cut = RenderLine(Location("/data/a/crm/Orders?key=k-1"));

        var nodes = cut.FindAll(".lt-chain__text");
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("nav").GetAttribute("aria-label"), Is.EqualTo("Address"));
            Assert.That(cut.Find(".lt-chain").ClassList, Does.Contain("lt-chain--mono"));
            Assert.That(nodes.Select(node => node.TextContent), Is.EqualTo(new[] { "Home", "data", "a", "crm", "Orders", "?key=k-1" }));
            Assert.That(nodes.Take(5).All(node => node.LocalName == "a"), Is.True);
            Assert.That(nodes[4].GetAttribute("href"), Is.EqualTo("data/a/crm/%4Frders"));
            Assert.That(nodes[5].GetAttribute("aria-current"), Is.EqualTo("page"));
            Assert.That(cut.FindAll(".lt-node--join"), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void With_a_tenant_root_the_tenant_is_the_first_node()
    {
        var cut = RenderLine(Location("/t/acme/data"));

        Assert.That(cut.FindAll(".lt-chain__text").Select(node => node.TextContent), Is.EqualTo(new[] { "t/acme", "data" }));
    }

    [Test]
    public void The_edit_control_announces_its_shortcuts()
    {
        var cut = RenderLine(Location("/"));

        var edit = cut.Find(".lt-shell-address-line__edit");
        Assert.Multiple(() =>
        {
            Assert.That(edit.GetAttribute("aria-keyshortcuts"), Is.EqualTo("/ Control+K"));
            Assert.That(edit.TextContent, Does.Contain("Go to an address"));
            Assert.That(cut.FindAll("kbd").Select(key => key.TextContent), Is.EqualTo(new[] { "/", "Ctrl K" }));
        });
    }

    [Test]
    public void A_click_turns_the_line_into_a_combobox_holding_the_current_address_selected()
    {
        var cut = RenderLine(Location("/data/orders"));

        cut.Find(".lt-shell-address-line__edit").Click();

        var input = cut.Find("input");
        Assert.Multiple(() =>
        {
            Assert.That(input.GetAttribute("role"), Is.EqualTo("combobox"));
            Assert.That(input.GetAttribute("value"), Is.EqualTo("/data/orders"));
            Assert.That(input.GetAttribute("aria-expanded"), Is.EqualTo("false"));
            Assert.That(input.GetAttribute("aria-autocomplete"), Is.EqualTo("list"));
            Assert.That(cut.Find("label").GetAttribute("for"), Is.EqualTo(input.Id));
            Assert.That(cut.Find("[role='search']"), Is.Not.Null);
            Assert.That(cut.FindAll("[role='status']"), Has.Count.EqualTo(1));
            Assert.That(JSInterop.Invocations.Any(invocation => invocation.Identifier == "focusAndSelect"), Is.True);
        });
    }

    [Test]
    public void Escape_restores_the_chain_and_returns_focus_to_the_edit_control()
    {
        var cut = RenderLine(Location("/data/orders"));
        cut.Find(".lt-shell-address-line__edit").Click();
        cut.Find("input").Input("/apps");

        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "Escape" });

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("input"), Is.Empty);
            Assert.That(cut.FindAll(".lt-chain__text").Select(node => node.TextContent), Is.EqualTo(new[] { "Home", "data", "orders" }));
            Assert.That(JSInterop.Invocations.Any(invocation => invocation.Identifier == "focusElement"), Is.True);
        });
    }

    [Test]
    public void A_focus_that_lands_on_a_vanished_element_never_ends_the_circuit()
    {
        // The browser refuses a focus whose element has already gone - Blazor's own
        // "Unable to focus an invalid element". On a loaded server that happens when the
        // edit control is re-rendered away before its focus request lands. It must cost
        // nothing but the focus: escaping OnAfterRenderAsync, it ends the circuit, and the
        // console stops answering (the red UI leg on #3943).
        var refused = new JSException("Unable to focus an invalid element.");
        JSInterop.SetupVoid("Blazor._internal.domWrapper.focus", _ => true).SetException(refused);
        var module = JSInterop.SetupModule(ShellChromeAssets.ModuleSpecifier);
        module.SetupVoid("focusElement", _ => true).SetException(refused);
        module.SetupVoid("focusAndSelect", _ => true).SetException(refused);

        var cut = RenderLine(Location("/data/orders"));
        cut.Find(".lt-shell-address-line__edit").Click();
        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "Escape" });
        cut.InvokeAsync(() => cut.Instance.FocusAsync().AsTask()).GetAwaiter().GetResult();

        // Still answering: it opens, takes typing and closes again.
        cut.Find(".lt-shell-address-line__edit").Click();
        cut.Find("input").Input("/apps");
        Assert.That(cut.Find("input").GetAttribute("value"), Is.EqualTo("/apps"));
        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "Escape" });
        Assert.That(cut.FindAll("input"), Is.Empty);
    }

    [Test]
    public void Leaving_the_input_closes_it()
    {
        var cut = RenderLine(Location("/"));
        cut.Find(".lt-shell-address-line__edit").Click();

        cut.Find("input").Blur();

        Assert.That(cut.FindAll("input"), Is.Empty);
    }

    [Test]
    public void A_literal_address_offers_to_go_there_and_Enter_navigates()
    {
        AddArea(new FakeArea("data", "Data"));
        var cut = RenderLine(Location("/"));
        cut.Find(".lt-shell-address-line__edit").Click();

        cut.Find("input").Input("/data/Orders");

        Assert.That(cut.Find("[role='option']").TextContent, Does.Contain("Go to /data/%4Frders"));

        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "Enter" });

        Assert.Multiple(() =>
        {
            Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri + "data/%4Frders"));
            Assert.That(cut.FindAll("input"), Is.Empty);
        });
    }

    [Test]
    public void A_search_that_names_an_area_path_offers_to_go_there()
    {
        AddArea(new FakeArea("data", "Data"));
        var cut = RenderLine(Location("/"));
        cut.Find(".lt-shell-address-line__edit").Click();

        cut.Find("input").Input("data/a/crm");

        Assert.That(cut.FindAll("[role='option']").First().TextContent, Does.Contain("Go to /data/a/crm"));
    }

    [Test]
    public void Enter_with_nothing_to_choose_says_so()
    {
        var cut = RenderLine(Location("/"));
        cut.Find(".lt-shell-address-line__edit").Click();
        cut.Find("input").Input("zzz");

        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "Enter" });

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[role='status']").TextContent, Is.EqualTo("Nothing matches zzz."));
            Assert.That(cut.FindAll("input"), Has.Count.EqualTo(1), "the input stays open");
        });
    }

    [Test]
    public void An_empty_search_offers_nothing_and_Enter_prompts()
    {
        var cut = RenderLine(Location("/"));
        cut.Find(".lt-shell-address-line__edit").Click();
        cut.Find("input").Input(string.Empty);

        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "Enter" });

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[role='listbox']"), Is.Empty);
            Assert.That(cut.Find("[role='status']").TextContent, Does.StartWith("Type an address"));
        });
    }

    [Test]
    public void Arrow_keys_move_the_active_option_and_wrap()
    {
        var cut = RenderLine(Location("/"));
        cut.Find(".lt-shell-address-line__edit").Click();
        cut.Find("input").Input(">theme");
        var options = cut.FindAll("[role='option']");
        Assert.That(options, Has.Count.EqualTo(3), "the three theme commands");

        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "ArrowDown" });
        Assert.That(ActiveOption(cut), Does.Contain("system theme"));

        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "ArrowDown" });
        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "ArrowDown" });
        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "ArrowDown" });
        Assert.That(ActiveOption(cut), Does.Contain("system theme"), "down from the last wraps to the first");

        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "ArrowUp" });
        Assert.Multiple(() =>
        {
            Assert.That(ActiveOption(cut), Does.Contain("Board theme"), "up from the first wraps to the last");
            Assert.That(cut.Find("input").GetAttribute("aria-activedescendant"), Is.EqualTo(cut.Find("[aria-selected='true']").Id));
            Assert.That(cut.Find("[role='status']").TextContent, Does.Contain("Board theme"));
        });
    }

    [Test]
    public void Arrow_keys_with_no_options_do_nothing()
    {
        var cut = RenderLine(Location("/"));
        cut.Find(".lt-shell-address-line__edit").Click();

        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "ArrowDown" });

        Assert.That(cut.FindAll("[aria-selected='true']"), Is.Empty);
    }

    [Test]
    public void The_palette_runs_the_active_command()
    {
        var appearance = Services.GetRequiredService<ShellAppearance>();
        var cut = RenderLine(Location("/"));
        cut.Find(".lt-shell-address-line__edit").Click();
        cut.Find("input").Input("> board");

        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "Enter" });

        Assert.Multiple(() =>
        {
            Assert.That(appearance.Theme, Is.EqualTo(ShellTheme.Board));
            Assert.That(Applier.Applied.Last().Theme, Is.EqualTo(ShellTheme.Board));
            Assert.That(cut.FindAll("input"), Is.Empty);
        });
    }

    [Test]
    public void The_palette_lists_area_commands_and_a_navigating_command_navigates()
    {
        var invoked = 0;
        var data = new FakeArea("data", "Data")
        {
            Commands =
            [
                new ExplorerCommand("data.create-tree", "Create a tree")
                {
                    Target = ExplorerAddress.ForArea("data"),
                    InvokeAsync = _ =>
                    {
                        invoked++;
                        return ValueTask.CompletedTask;
                    },
                },
            ],
        };
        AddArea(data);
        var cut = RenderLine(Location("/", Visible(data)));
        cut.Find(".lt-shell-address-line__edit").Click();
        cut.Find("input").Input(">create");

        cut.Find("[role='option']").Click();

        Assert.Multiple(() =>
        {
            Assert.That(invoked, Is.EqualTo(1));
            Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri + "data"));
        });
    }

    [Test]
    public void The_palette_announces_how_many_commands_match()
    {
        var cut = RenderLine(Location("/"));
        cut.Find(".lt-shell-address-line__edit").Click();

        cut.Find("input").Input(">contrast");

        Assert.That(cut.Find("[role='status']").TextContent, Is.EqualTo("3 suggestions."));
    }

    [Test]
    public void An_unavailable_areas_commands_are_not_offered()
    {
        var backups = new FakeArea("backups", "Backups") { Commands = [new ExplorerCommand("backups.capture", "Capture a backup")] };
        var cut = RenderLine(Location("/", new ExplorerAreaEntry(backups, AreaAvailability.Unavailable("No grant."))));
        cut.Find(".lt-shell-address-line__edit").Click();

        cut.Find("input").Input(">capture");

        Assert.That(cut.FindAll("[role='option']"), Is.Empty);
    }

    [Test]
    public void The_tenant_prefix_explains_that_tenancy_is_off()
    {
        var cut = RenderLine(Location("/"));
        cut.Find(".lt-shell-address-line__edit").Click();

        cut.Find("input").Input("t/");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-shell-combobox__note").TextContent, Is.EqualTo("Tenancy is off, so there is no tenant to choose."));
            Assert.That(cut.FindAll("[role='listbox']"), Is.Empty);
        });
    }

    [Test]
    public void The_tenant_prefix_completes_reachable_tenants_and_re_roots_the_address()
    {
        UseTenancy("acme", allowSwitch: true, "acme", "globex");
        var cut = RenderLine(Location("/t/acme/data/orders"));
        cut.Find(".lt-shell-address-line__edit").Click();

        cut.Find("input").Input("t/glo");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[role='option']"), Has.Count.EqualTo(1)));
        cut.Find("[role='option']").Click();

        Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri + "t/globex/data/orders"));
    }

    private static string ActiveOption(IRenderedComponent<AddressLine> cut) => cut.Find("[aria-selected='true']").TextContent;

    private static ExplorerAreaEntry Visible(FakeArea area) => new(area, AreaAvailability.Visible);

    private static ExplorerLocation Location(string address, params ExplorerAreaEntry[] entries) =>
        new(ExplorerAddress.Parse(address), entries, EntriesLoaded: true, TenancyActive: false);

    private IRenderedComponent<AddressLine> RenderLine(ExplorerLocation location, bool compact = false)
    {
        JSInterop.SetupModule(ShellChromeAssets.ModuleSpecifier).Mode = JSRuntimeMode.Loose;
        return Render<AddressLine>(parameters =>
        {
            parameters.AddCascadingValue(location);
            if (compact)
            {
                parameters.AddCascadingValue(
                    Orleans.Lattice.Explorer.UI.Design.Components.LtBreakpointCascade.Name,
                    Orleans.Lattice.Explorer.UI.Design.Tokens.LtBreakpoint.Compact);
            }
        });
    }
}
