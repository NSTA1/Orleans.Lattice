using Microsoft.Extensions.DependencyInjection;
using Microsoft.JSInterop;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.Core.History;
using Orleans.Lattice.Explorer.Core.Topology;
using Orleans.Lattice.Explorer.Plugins.DeadLetter;
using Orleans.Lattice.Explorer.Plugins.History;
using Orleans.Lattice.Explorer.Plugins.Metrics;
using Orleans.Lattice.Explorer.Plugins.TagIndex;
using Orleans.Lattice.Explorer.Plugins.Topology;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// Every settled state a per-selection surface can rest in offers at least one
/// keyboard-reachable control (issue #1855).
/// </summary>
/// <remarks>
/// <para>
/// The detail body that hosts these surfaces is a scrolling region. A scrolling
/// region whose content is not keyboard reachable cannot be scrolled from the
/// keyboard at all, which axe reports as <c>scrollable-region-focusable</c> and
/// WCAG as an SC 2.1.1 failure. The systemic remedy is a <c>tabindex</c> on the
/// scroll container, which belongs to the shell; what belongs here is the other
/// half - a surface that can offer a way out of a state must actually offer it,
/// as a control rather than as a sentence.
/// </para>
/// <para>
/// The scope is deliberately "a state the surface itself can resolve", not
/// "every state", and the difference is worth stating because it is where an
/// honest guard and a vacuous one part company:
/// </para>
/// <list type="bullet">
///   <item><description>A load in flight has nothing to offer, and a button
///   that cancels nothing would be worse than none. It is transient by
///   definition.</description></item>
///   <item><description>Two settled states are resolved somewhere else on the
///   page rather than inside the surface: the timeline with no key chosen (the
///   key is chosen on the Data surface) and a selection that is not a tag index
///   (a different selection is chosen in the catalog). Neither destination is
///   reachable from a per-selection surface - there is no seam to switch the
///   active surface for the same selection, and the catalog belongs to the
///   shell. Those two are covered below by asserting they name their
///   destination in words, which is the strongest thing this package can
///   promise; the scroll container's <c>tabindex</c> is what covers them
///   mechanically.</description></item>
/// </list>
/// <para>
/// Each case drives a real surface into a real state through its own domain
/// contract, so nothing here waits on a clock, a delay or a background task.
/// </para>
/// </remarks>
[TestFixture]
public sealed class SelectionSurfaceRemedyReachabilityTests
{
    private const string Boom = "the endpoint is unreachable";

    [Test]
    public async Task A_failed_tag_index_read_offers_a_control()
    {
        var surface = Substitute.For<ITagIndexSurface>();
        surface.ListCoveredTreesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException(Boom));

        var html = await SelectionViewRenderHarness.RenderAsync<TagIndexDetailTab, ITagIndexSurface>(
            surface,
            SelectionViewRenderHarness.TagIndex());

        AssertOffersAControl(html, "the tag-index surface's failed read");
    }

    [Test]
    public async Task An_unrenderable_tag_index_selection_names_its_destination()
    {
        var surface = Substitute.For<ITagIndexSurface>();

        var html = await SelectionViewRenderHarness.RenderAsync<TagIndexDetailTab, ITagIndexSurface>(
            surface,
            SelectionViewRenderHarness.Tree());

        // Resolved in the shell's catalog, which this package cannot reach, so
        // the promise it can keep is that the way out is stated rather than left
        // for the reader to infer from an empty pane.
        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Choose a tag index from the catalog"));
            Assert.That(html, Does.Contain("lx-selection-message-remedy"));
        });
    }

    [Test]
    public async Task A_failed_dead_letter_read_offers_a_control()
    {
        var surface = Substitute.For<IDeadLetterSurface>();
        surface
            .ListAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException(Boom));

        var html = await SelectionViewRenderHarness.RenderAsync<DeadLetterTab, IDeadLetterSurface>(
            surface,
            SelectionViewRenderHarness.Tree());

        AssertOffersAControl(html, "the dead-letter surface's failed read");
    }

    [Test]
    public async Task A_silent_metrics_cluster_offers_a_control()
    {
        var surface = Substitute.For<IMetricsSurface>();
        surface.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<TreeMetrics?>(null));

        var html = await SelectionViewRenderHarness.RenderAsync<MetricsTab, IMetricsSurface>(
            surface,
            SelectionViewRenderHarness.Tree());

        AssertOffersAControl(html, "the metrics surface's no-metrics state");
    }

    [Test]
    public async Task A_silent_topology_cluster_offers_a_control()
    {
        var surface = Substitute.For<ITopologySurface>();
        surface.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TopologyFetch { Roots = [] }));

        var html = await SelectionViewRenderHarness.RenderAsync<TopologyTab, ITopologySurface>(
            surface,
            SelectionViewRenderHarness.Tree(),
            configure: services => services.AddSingleton(Substitute.For<IJSRuntime>()));

        AssertOffersAControl(html, "the topology surface's no-structure state");
    }

    [Test]
    public async Task A_history_timeline_with_no_key_names_its_destination()
    {
        var surface = Substitute.For<IHistorySurface>();
        surface.InspectedKey(Arg.Any<string>()).Returns((string?)null);

        var html = await SelectionViewRenderHarness.RenderAsync<HistoryTab, IHistorySurface>(
            surface,
            SelectionViewRenderHarness.Tree());

        // The key is chosen on a sibling surface for the same selection, and no
        // seam switches the active surface without changing the selection, so
        // naming the destination is the strongest promise available here.
        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Open the Data surface"));
            Assert.That(html, Does.Contain("lx-selection-message-remedy"));
        });
    }

    [Test]
    public async Task An_expandable_topology_node_is_a_keyboard_reachable_button()
    {
        var surface = Substitute.For<ITopologySurface>();
        surface.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TopologyFetch
            {
                Roots =
                [
                    new TopologyNode
                    {
                        NodeId = "s0",
                        ShardIndex = 0,
                        Kind = NodeKind.ShardRoot,
                        HasMoreChildren = true,
                    },
                ],
            }));

        var html = await SelectionViewRenderHarness.RenderAsync<TopologyTab, ITopologySurface>(
            surface,
            SelectionViewRenderHarness.Tree(),
            configure: services => services.AddSingleton(Substitute.For<IJSRuntime>()));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("role=\"button\""),
                "expanding a node is an action, so the node is a control");
            Assert.That(html, Does.Contain("tabindex=\"0\""),
                "the expansion path used to be reachable only with a pointer (WCAG SC 2.1.1)");
            Assert.That(html, Does.Contain("aria-label=\"shard root"),
                "the node carries its own name rather than being hidden inside a role=img graphic");
            Assert.That(html, Does.Not.Contain("role=\"img\""),
                "an image exposes no descendants, so it would hide every node control inside it");
        });
    }

    private static void AssertOffersAControl(string html, string what) =>
        Assert.That(
            html.Contains("<button", StringComparison.Ordinal)
            || html.Contains("<a href", StringComparison.Ordinal),
            Is.True,
            $"{what} must come to rest offering at least one keyboard-reachable control: the "
            + "detail body scrolls, and a region with no focusable content cannot be scrolled "
            + "from the keyboard, quite apart from the remedy being unperformable.");
}
