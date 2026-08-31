using Bunit;
using Microsoft.AspNetCore.Components;
using NUnit.Framework.Internal;
using Orleans.Lattice.Explorer.DesignSystem.Components;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tests.Bunit;
using Orleans.Lattice.Explorer.Tests.Plugins;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// The shell-level accessibility structure the browser lane measures: one
/// heading outline, one main landmark on every area, and every tab bound to a
/// real panel.
/// </summary>
/// <remarks>
/// <para>
/// These are the browserless half of the epic's accessibility gate. The lane in
/// <c>test/lattice.explorer.uitests</c> asserts the same guarantees against a
/// real browser and a real axe run, but it is advisory and slow; these run in
/// the required build check, on every change, in milliseconds. A regression
/// here fails a pull request rather than waiting for a nightly sweep.
/// </para>
/// <para>
/// bUnit's context locks its service collection after the first render, and
/// NUnit reuses one fixture instance across a fixture's tests by default, so a
/// fixture with more than one case must ask for an instance per case.
/// </para>
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AppShellStructureBunitTests : LatticeComponentTestContext
{
    [Test]
    public void The_shell_renders_exactly_one_level_one_heading_naming_the_surface()
    {
        ConfigureShellServices(AreaPlugin("a", "Alpha", 100));

        var cut = RenderShell();

        var headings = cut.FindAll("h1");

        Assert.Multiple(() =>
        {
            Assert.That(headings, Has.Count.EqualTo(1), "a document with two h1s has no single title");
            Assert.That(
                headings[0].TextContent.Trim(),
                Is.EqualTo("Explore"),
                "and it names what the caller is looking at, not the product");
        });
    }

    [Test]
    public void The_heading_outline_never_skips_a_level()
    {
        ConfigureShellServices();

        var cut = RenderShell();

        var levels = cut.FindAll("h1, h2, h3, h4, h5, h6")
            .Select(element => int.Parse(element.TagName[1..], System.Globalization.CultureInfo.InvariantCulture))
            .ToArray();

        var previous = 0;
        var skips = new List<string>();
        for (var i = 0; i < levels.Length; i++)
        {
            if (previous != 0 && levels[i] > previous + 1)
            {
                skips.Add($"level {levels[i]} follows level {previous}");
            }

            previous = levels[i];
        }

        Assert.Multiple(() =>
        {
            Assert.That(levels, Is.Not.Empty, "an outline with no headings can only be read linearly");
            Assert.That(levels[0], Is.EqualTo(1), "the outline starts at its root");
            Assert.That(skips, Is.Empty);
        });
    }

    [Test]
    public void The_shell_exposes_one_main_landmark_and_a_navigation_landmark()
    {
        ConfigureShellServices();

        var cut = RenderShell();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("main"), Has.Count.EqualTo(1));
            Assert.That(
                cut.FindAll("nav"),
                Has.Count.GreaterThanOrEqualTo(1),
                "the rail is a navigation landmark of its own");
        });
    }

    [Test]
    public void The_main_landmark_survives_an_area_plugin_taking_over_the_surface()
    {
        // The measured defect: the shell wrapped only its own home surface, so
        // the moment a plugin area replaced the working surface the document had
        // no main landmark and a skip link had nowhere to land. The landmark
        // belongs to the frame, not to the surface inside it.
        ConfigureShellServices(AreaPlugin("orleans.lattice.alpha", "Alpha", 100));

        var cut = RenderShell();
        cut.Find("[role=tab][aria-selected=false]").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("main"), Has.Count.EqualTo(1));
            Assert.That(cut.Find("h1").TextContent.Trim(), Is.EqualTo("Alpha"));
        });
    }

    [Test]
    public void The_skip_links_target_is_the_main_landmark_and_is_focusable()
    {
        ConfigureShellServices();

        var cut = RenderShell();
        var main = cut.Find("main");

        Assert.Multiple(() =>
        {
            Assert.That(main.Id, Is.EqualTo(ExplorerShellRegions.Main));
            Assert.That(
                main.GetAttribute("tabindex"),
                Is.EqualTo("-1"),
                "an anchor whose target cannot take focus performs no bypass");
        });
    }

    [Test]
    public void Every_rail_tab_is_bound_to_a_real_tab_panel_that_names_it_back()
    {
        ConfigureShellServices(AreaPlugin("a", "Alpha", 100));

        var cut = RenderShell();

        var tabs = cut.FindAll("[role=tab]");
        var panel = cut.Find("#" + ExplorerShellRegions.AreaContent);

        Assert.Multiple(() =>
        {
            Assert.That(tabs, Has.Count.EqualTo(2));
            foreach (var tab in tabs)
            {
                Assert.That(
                    tab.GetAttribute("aria-controls"),
                    Is.EqualTo(ExplorerShellRegions.AreaContent),
                    "a tab that controls nothing leaves a screen-reader caller with nothing to move into");
            }

            Assert.That(panel.GetAttribute("role"), Is.EqualTo("tabpanel"));
            Assert.That(
                panel.GetAttribute("aria-labelledby"),
                Is.EqualTo(ExplorerShellRegions.AreaTabElementId("explore")),
                "and the panel names the tab that selected it");
        });
    }

    [Test]
    public void The_rail_is_the_only_tab_list_the_shell_frame_declares()
    {
        // After this issue no hand-rolled role=tablist remains in the shell: the
        // rail, the catalog kind and the detail surfaces all run on the one
        // primitive. The frame itself contributes exactly one strip.
        ConfigureShellServices(AreaPlugin("a", "Alpha", 100));

        var cut = RenderShell();

        var strips = cut.FindAll("[role=tablist]");

        Assert.Multiple(() =>
        {
            Assert.That(strips, Has.Count.EqualTo(1));
            Assert.That(strips[0].GetAttribute("aria-label"), Is.EqualTo("Application areas"));
            Assert.That(
                strips[0].GetAttribute("aria-orientation"),
                Is.EqualTo("vertical"),
                "which is what binds Up and Down and leaves Left and Right to the page");
        });
    }

    [Test]
    public void The_rail_keeps_exactly_one_tab_in_the_documents_tab_sequence()
    {
        ConfigureShellServices(
            AreaPlugin("a", "Alpha", 100),
            AreaPlugin("b", "Bravo", 200));

        var cut = RenderShell();

        var tabs = cut.FindAll("[role=tab]");

        Assert.Multiple(() =>
        {
            Assert.That(
                tabs.Count(tab => tab.GetAttribute("tabindex") == "0"),
                Is.EqualTo(1),
                "a roving tabindex is what stops a caller tabbing through the whole rail to pass it");
            Assert.That(
                tabs.All(tab => tab.HasAttribute("tabindex")),
                Is.True,
                "a tab with no explicit tabindex defaults back into the sequence");
        });
    }

    [Test]
    public void A_denied_area_is_explained_through_the_help_primitive_and_never_a_title()
    {
        ConfigureShellServices(DeniedAreaPlugin("a", "Alpha"));

        var cut = RenderShell();

        var label = cut.Find("." + "lx-shell-rail-demoted-label");
        var explanationId = label.GetAttribute("aria-describedby");

        Assert.That(explanationId, Is.Not.Null.And.Not.Empty);
        var explanation = cut.Find("#" + explanationId);

        Assert.Multiple(() =>
        {
            Assert.That(label.HasAttribute("title"), Is.False, "a title is invisible on touch and to a keyboard");
            Assert.That(explanation.TextContent, Does.Contain("Alpha"));
            Assert.That(
                explanation.TextContent,
                Does.Contain("Ask a platform administrator"),
                "an aria-describedby target contributes its text even while hidden, "
                + "so the remedy holds whether or not the disclosure is open");
        });
    }

    [Test]
    public void A_capability_the_cluster_does_not_have_is_explained_once_rather_than_repeated()
    {
        // An unavailable area contributes no entry - there is nothing to sign in
        // for and nothing to be granted - so the absence is answered in one
        // affordance instead of a row of dead names.
        ConfigureShellServices(
            GatedAreaPlugin("a", "Telemetry", ExplorerPluginAccessGates.Unavailable),
            AreaPlugin("b", "Bravo", 200));

        var cut = RenderShell();

        var capabilities = cut.Find("#" + LatticeHelp.ExplanationElementId(
            ExplorerShellRegions.CapabilitiesHelp));

        Assert.Multiple(() =>
        {
            Assert.That(
                cut.FindAll("[role=tab]").Select(tab => tab.TextContent.Trim()),
                Is.EqualTo(new[] { "Explore", "Bravo" }));
            Assert.That(cut.FindAll(".lx-shell-rail-demoted-label"), Is.Empty);
            Assert.That(
                capabilities.TextContent,
                Does.Contain("Telemetry"),
                "which is what answers 'why do I not see Telemetry?'");
        });
    }

    private IRenderedComponent<AppShell> RenderShell()
    {
        var catalog = (RenderFragment)(builder =>
        {
            builder.OpenElement(0, "nav");
            builder.AddAttribute(1, "aria-label", "catalog");
            builder.CloseElement();
        });

        var detail = (RenderFragment)(builder =>
        {
            builder.OpenElement(0, "section");
            builder.AddContent(1, "detail-surface");
            builder.CloseElement();
        });

        return Render<AppShell>(parameters => parameters
            .AddCascadingValue(AdaptiveContext(LatticeBreakpoint.Expanded))
            .Add(shell => shell.Catalog, catalog)
            .Add(shell => shell.ChildContent, detail));
    }

    private static IExplorerPlugin DeniedAreaPlugin(string id, string label) =>
        GatedAreaPlugin(id, label, ExplorerPluginAccessGates.Denied);

    private static IExplorerPlugin GatedAreaPlugin(string id, string label, IExplorerPluginAccessGate gate) =>
        new FakeExplorerPlugin(
            id,
            ExplorerPluginSurface.Area,
            100,
            label,
            gate,
            domainContract: null,
            typeof(StubDeniedAreaView));

    /// <summary>A stand-in area view for a plugin behind a gate.</summary>
    private sealed class StubDeniedAreaView : ComponentBase
    {
    }
}
