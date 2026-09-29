using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.JSInterop;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// The tabs: the WAI-ARIA tabs pattern with automatic activation - roles and
/// relationships, a roving tab stop, arrow, Home and End keys that move focus
/// and activation together, and disabled tabs that are skipped.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtTabsTests : ShellDesignTestContext
{
    [Test]
    public void The_tab_list_is_named_and_the_first_tab_is_active_by_default()
    {
        var cut = RenderTabs();
        var tabs = cut.FindAll("[role=tab]");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[role=tablist]").GetAttribute("aria-label"), Is.EqualTo("Tree views"));
            Assert.That(tabs.Select(tab => tab.TextContent), Is.EqualTo(new[] { "Keys", "History", "Views", "Shards" }));
            Assert.That(tabs.Select(tab => tab.GetAttribute("aria-selected")), Is.EqualTo(new[] { "true", "false", "false", "false" }));
            Assert.That(tabs.Select(tab => tab.GetAttribute("tabindex")), Is.EqualTo(new[] { "0", "-1", "-1", "-1" }),
                "only the active tab is in the tab order");
        });
    }

    [Test]
    public void The_active_tab_controls_the_one_panel_that_is_labelled_by_it()
    {
        var cut = RenderTabs(active: "history");
        var active = cut.Find("[role=tab][aria-selected=true]");
        var panel = cut.Find("[role=tabpanel]");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[role=tabpanel]"), Has.Count.EqualTo(1));
            Assert.That(active.GetAttribute("aria-controls"), Is.EqualTo(panel.Id));
            Assert.That(panel.GetAttribute("aria-labelledby"), Is.EqualTo(active.Id));
            Assert.That(panel.GetAttribute("tabindex"), Is.EqualTo("0"));
            Assert.That(panel.TextContent, Is.EqualTo("History panel"));
        });
    }

    [Test]
    public void Clicking_a_tab_activates_it_and_reports_it()
    {
        var observed = new List<string>();
        var cut = RenderTabs(onChanged: observed.Add);

        cut.FindAll("[role=tab]")[2].Click();

        Assert.Multiple(() =>
        {
            Assert.That(observed, Is.EqualTo(new[] { "views" }));
            Assert.That(cut.Find("[role=tabpanel]").TextContent, Is.EqualTo("Views panel"));
        });
    }

    [Test]
    public void A_refused_focus_leaves_the_tabs_answering_the_keyboard()
    {
        JSInterop.SetupVoid("Blazor._internal.domWrapper.focus", _ => true)
            .SetException(new JSException("Unable to focus an invalid element."));
        var cut = RenderTabs();

        cut.Find("[role=tablist]").KeyDown(new KeyboardEventArgs { Key = "ArrowRight" });
        cut.Find("[role=tablist]").KeyDown(new KeyboardEventArgs { Key = "ArrowRight" });

        Assert.That(cut.Find("[role=tab][aria-selected=true]").TextContent, Is.EqualTo("Views"));
    }

    [Test]
    public void ArrowRight_moves_activation_and_focus_to_the_next_tab()
    {
        var cut = RenderTabs();
        var history = cut.FindAll("[role=tab]")[1].GetAttribute("blazor:elementreference");

        cut.Find("[role=tablist]").KeyDown(new KeyboardEventArgs { Key = "ArrowRight" });

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[role=tab][aria-selected=true]").TextContent, Is.EqualTo("History"));
            Assert.That(history, Is.Not.Empty);
            Assert.That(FocusedElementId(), Is.EqualTo(history), "focus must follow the activation to the new tab");
        });
    }

    [Test]
    public void ArrowLeft_from_the_first_tab_wraps_to_the_last_enabled_tab()
    {
        var cut = RenderTabs();

        cut.Find("[role=tablist]").KeyDown(new KeyboardEventArgs { Key = "ArrowLeft" });

        Assert.That(cut.Find("[role=tab][aria-selected=true]").TextContent, Is.EqualTo("Views"),
            "Shards is disabled, so the last enabled tab is Views");
    }

    [Test]
    public void Arrow_keys_skip_a_disabled_tab()
    {
        var cut = RenderTabs(active: "views");

        cut.Find("[role=tablist]").KeyDown(new KeyboardEventArgs { Key = "ArrowRight" });

        Assert.That(cut.Find("[role=tab][aria-selected=true]").TextContent, Is.EqualTo("Keys"));
    }

    [Test]
    [TestCase("Home", "Keys")]
    [TestCase("End", "Views")]
    public void Home_and_End_jump_to_the_first_and_last_enabled_tabs(string key, string expected)
    {
        var cut = RenderTabs(active: "history");

        cut.Find("[role=tablist]").KeyDown(new KeyboardEventArgs { Key = key });

        Assert.That(cut.Find("[role=tab][aria-selected=true]").TextContent, Is.EqualTo(expected));
    }

    [Test]
    public void Another_key_changes_nothing()
    {
        var observed = new List<string>();
        var cut = RenderTabs(onChanged: observed.Add);

        cut.Find("[role=tablist]").KeyDown(new KeyboardEventArgs { Key = "ArrowDown" });

        Assert.Multiple(() =>
        {
            Assert.That(observed, Is.Empty);
            Assert.That(cut.Find("[role=tab][aria-selected=true]").TextContent, Is.EqualTo("Keys"));
        });
    }

    [Test]
    public void A_disabled_tab_is_disabled_and_cannot_be_activated()
    {
        var observed = new List<string>();
        var cut = RenderTabs(onChanged: observed.Add);
        var shards = cut.FindAll("[role=tab]")[3];

        shards.Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[role=tab]")[3].HasAttribute("disabled"), Is.True);
            Assert.That(observed, Is.Empty);
        });
    }

    [Test]
    public void An_active_id_naming_a_disabled_tab_falls_back_to_the_first_enabled_tab()
    {
        var cut = RenderTabs(active: "shards");

        Assert.That(cut.Find("[role=tab][aria-selected=true]").TextContent, Is.EqualTo("Keys"));
    }

    [Test]
    public void Two_tabs_with_one_id_are_rejected()
    {
        Assert.That(
            () => Render<LtTabs>(p =>
            {
                p.Add(x => x.Label, "Views");
                AddTab(p, "keys", "Keys", disabled: false);
                AddTab(p, "keys", "Keys again", disabled: false);
            }),
            Throws.InvalidOperationException);
    }

    [Test]
    public void A_tab_outside_a_tab_set_is_rejected()
    {
        Assert.That(
            () => Render<LtTab>(p => p.Add(x => x.Id, "keys").Add(x => x.Title, "Keys")),
            Throws.InvalidOperationException);
    }

    private IRenderedComponent<LtTabs> RenderTabs(string? active = null, Action<string>? onChanged = null)
    {
        return Render<LtTabs>(p =>
        {
            p.Add(x => x.Label, "Tree views").Add(x => x.ActiveId, active);
            AddTab(p, "keys", "Keys", disabled: false);
            AddTab(p, "history", "History", disabled: false);
            AddTab(p, "views", "Views", disabled: false);
            AddTab(p, "shards", "Shards", disabled: true);
            if (onChanged is not null)
            {
                p.Add(x => x.ActiveIdChanged, onChanged);
            }
        });
    }

    private static void AddTab(ComponentParameterCollectionBuilder<LtTabs> parameters, string id, string title, bool disabled) =>
        parameters.AddChildContent<LtTab>(tab => tab
            .Add(x => x.Id, id)
            .Add(x => x.Title, title)
            .Add(x => x.Disabled, disabled)
            .AddChildContent(title + " panel"));
    private string? FocusedElementId()
    {
        var invocation = JSInterop.VerifyFocusAsyncInvoke();
        return ((ElementReference)invocation.Arguments[0]!).Id;
    }
}
