using Bunit;
using Orleans.Lattice.Api.Telemetry;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NUnit.Framework.Internal;
using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.DesignSystem.Components;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Telemetry;
using Orleans.Lattice.Explorer.Tests.Bunit;
using Orleans.Lattice.Explorer.Tests.Plugins;
using Orleans.Lattice.Explorer.Tests.Telemetry;

using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// The acceptance criterion "a user can discover why Telemetry is absent when
/// the cluster has no telemetry backend", walked end to end: the real gate
/// answering a real cluster with no backend, through the visibility policy, to
/// the affordance the rail renders.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why Telemetry is the case that matters.</b> It is the area most often
/// unavailable, because telemetry is an add-on and most clusters are configured
/// without a backend. <c>Unavailable</c> renders <em>no rail entry at all</em>,
/// which is the correct call - a caller cannot be given a grant for a capability
/// nobody installed - but it means the area vanishes with no trace. Without the
/// affordance the honest answer ("this cluster does not run telemetry") is
/// unreachable, and the user is left to conclude the Explorer is broken or that
/// they lack a permission.
/// </para>
/// <para>
/// Each step is asserted on the real collaborator rather than a stand-in for it:
/// the gate is <see cref="TelemetryAvailability"/> driven by a fake facade with
/// an empty catalogue, the policy is the shell's own, and the last case renders
/// the shell. So the chain is proved joined up, not merely proved to exist in
/// pieces.
/// </para>
/// <para>
/// bUnit's context locks its service collection after the first render, and
/// NUnit reuses one fixture instance across a fixture's tests by default, so a
/// fixture with more than one case must ask for an instance per case.
/// </para>
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TelemetryAbsenceDiscoveryTests : LatticeComponentTestContext
{
    private static readonly IExplorerPluginHostContext Context =
        PluginTestHost.Context(TelemetryPluginKeys.PluginId);

    [Test]
    public async Task A_cluster_with_no_telemetry_backend_reports_the_area_unavailable()
    {
        // Step one. The facade makes "no backend configured" and "entitled to
        // nothing" deliberately indistinguishable, so the gate reports the fact
        // that is true either way: there is nothing here to render.
        var client = new FakeTelemetryQueryClient { CatalogResult = TelemetryQueryCatalog.Empty };
        var gate = new TelemetryAvailability(new TelemetryQueryService(client));

        var access = await gate.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(
                access.State,
                Is.Not.EqualTo(ExplorerPluginAccessState.Denied),
                "an absent capability must not be reported as something the caller lacks a grant for");
        });
    }

    [Test]
    public async Task An_unavailable_area_is_withdrawn_from_the_rail_and_collected_for_the_affordance()
    {
        // Step two. Hiding it is right - there is no grant to ask for - but the
        // policy also reports that this is a cluster-capability absence, which
        // is what puts the label in front of the caller instead of losing it.
        var client = new FakeTelemetryQueryClient { CatalogResult = TelemetryQueryCatalog.Empty };
        var gate = new TelemetryAvailability(new TelemetryQueryService(client));

        var access = await gate.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerAreaVisibilityPolicy.Decide(access.State, hideInaccessible: false),
                Is.EqualTo(ExplorerAreaEntryPresentation.Hidden));
            Assert.That(ExplorerAreaVisibilityPolicy.IsUnavailableOnCluster(access.State), Is.True);
            Assert.That(ExplorerAreaVisibilityPolicy.IsActivable(access.State), Is.False);
        });
    }

    [Test]
    public void The_affordance_names_telemetry_and_borrows_the_shared_remedy()
    {
        // Step three. The rail composes the list, because only it knows which
        // areas withdrew; the remedy comes from the shared vocabulary, so an
        // absent capability is answered with the same sentence wherever it is
        // met rather than with a second wording invented here.
        var message = ExplorerRailCopy.MissingAreas([ExplorerVocabulary.TelemetryArea]);
        var shared = ExplorerAccessCopy.Unavailable(ExplorerVocabulary.TelemetryArea);

        Assert.Multiple(() =>
        {
            Assert.That(message.Kind, Is.EqualTo(ExplorerStateKind.Unavailable));
            Assert.That(message.Explanation, Does.Contain(ExplorerVocabulary.TelemetryArea));
            Assert.That(message.Remedy, Is.EqualTo(shared.Remedy));
            Assert.That(
                message.IsDenial,
                Is.False,
                "nothing is being withheld from the caller, so it must not read as a refusal");
        });
    }

    [Test]
    public void The_rail_offers_the_answer_as_a_keyboard_reachable_disclosure()
    {
        // Step four, and the one that makes the criterion true rather than
        // merely satisfiable: the answer is on screen, in a real button a
        // keyboard and a touch caller can both open, asking the question the
        // user would actually ask.
        ConfigureShellServices(UnavailableTelemetryPlugin());

        var cut = RenderShell();
        var trigger = cut.Find(".lx-shell-rail-footer button.lx-help-trigger");
        var explanation = cut.Find(
            "#" + LatticeHelp.ExplanationElementId(ExplorerShellRegions.CapabilitiesHelp));

        Assert.Multiple(() =>
        {
            Assert.That(
                cut.FindAll("[role=tab]").Select(tab => tab.TextContent.Trim()),
                Does.Not.Contain(ExplorerVocabulary.TelemetryArea),
                "an area the cluster does not run offers no entry to click");
            Assert.That(trigger.TextContent, Does.Contain(ExplorerRailCopy.MissingAreasTriggerText));
            Assert.That(trigger.HasAttribute("aria-expanded"), Is.True, "a disclosure, not a hover tooltip");
            Assert.That(
                explanation.TextContent,
                Does.Contain(ExplorerVocabulary.TelemetryArea),
                "the answer names the area that went missing");
        });
    }

    [Test]
    public void An_area_the_cluster_does_serve_is_not_reported_missing()
    {
        // The converse, so the affordance is proved to be reading the gates
        // rather than always naming Telemetry: with the area allowed it appears
        // in the rail and the answer says nothing is missing.
        ConfigureShellServices(AllowedTelemetryPlugin());

        var cut = RenderShell();
        var explanation = cut.Find(
            "#" + LatticeHelp.ExplanationElementId(ExplorerShellRegions.CapabilitiesHelp));

        Assert.Multiple(() =>
        {
            Assert.That(
                cut.FindAll("[role=tab]").Select(tab => tab.TextContent.Trim()),
                Does.Contain(ExplorerVocabulary.TelemetryArea),
                "an area the cluster serves is offered");
            Assert.That(explanation.TextContent, Does.Not.Contain(ExplorerVocabulary.TelemetryArea));
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

    private static IExplorerPlugin UnavailableTelemetryPlugin() =>
        TelemetryPlugin(ExplorerPluginAccessGates.Unavailable);

    private static IExplorerPlugin AllowedTelemetryPlugin() =>
        TelemetryPlugin(ExplorerPluginAccessGates.Allowed);

    private static IExplorerPlugin TelemetryPlugin(IExplorerPluginAccessGate gate) =>
        new FakeExplorerPlugin(
            TelemetryPluginKeys.PluginId,
            ExplorerPluginSurface.Area,
            100,
            ExplorerVocabulary.TelemetryArea,
            gate,
            domainContract: null,
            typeof(StubTelemetryAreaView));

    /// <summary>A stand-in area view, so an activated plugin has something to render.</summary>
    private sealed class StubTelemetryAreaView : ComponentBase
    {
    }
}
