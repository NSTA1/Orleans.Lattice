using Microsoft.Extensions.DependencyInjection;
using Microsoft.JSInterop;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Explorer.Plugins.Data;
using Orleans.Lattice.Explorer.Plugins.DeadLetter;
using Orleans.Lattice.Explorer.Plugins.Metrics;
using Orleans.Lattice.Explorer.Plugins.TagIndex;
using Orleans.Lattice.Explorer.Plugins.Topology;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// Render-tree assertions for the shared UI primitives that moved out of the
/// deleted <c>app.css</c> monolith and into the design system (issue #1770).
/// </summary>
/// <remarks>
/// <para>
/// This fixture exists because of a specific blind spot. A class whose only
/// rule lived in the deleted monolith still compiles, still renders, and still
/// logs nothing - the element is simply unstyled. No build, and no test that
/// asserts on behaviour rather than markup, catches it. The per-selection tabs
/// were where that blind spot was widest: five of them had no component render
/// test at all, which is precisely why the breakage was invisible until the
/// class usage was measured directly.
/// </para>
/// <para>
/// So each test below renders the real component over a controlled domain
/// surface and asserts the migrated class name is in the markup the user would
/// receive. Driving a tab into its error state is deliberate: the retry control
/// is the <c>lx-btn-link</c> call site, and that variant is the one shared by
/// the most surfaces, so it is the one most worth pinning.
/// </para>
/// </remarks>
[TestFixture]
public sealed class MigratedPrimitiveRenderTests
{
    private const string Boom = "the endpoint is unreachable";

    [Test]
    public async Task The_metrics_tab_renders_the_design_systems_button_primitives()
    {
        var surface = Substitute.For<IMetricsSurface>();
        surface.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException(Boom));

        var html = await SelectionViewRenderHarness.RenderAsync<MetricsTab, IMetricsSurface>(
            surface,
            SelectionViewRenderHarness.Tree());

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-btn"), "the refresh control is the design system's button");
            Assert.That(html, Does.Contain("lx-btn-link"), "the retry control is the link-styled variant");
            Assert.That(html, Does.Contain(Boom));
        });
    }

    [Test]
    public async Task The_topology_tab_renders_the_design_systems_button_primitives()
    {
        var surface = Substitute.For<ITopologySurface>();
        surface.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException(Boom));

        var html = await SelectionViewRenderHarness.RenderAsync<TopologyTab, ITopologySurface>(
            surface,
            SelectionViewRenderHarness.Tree(),
            configure: services => services.AddSingleton(Substitute.For<IJSRuntime>()));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-btn"));
            Assert.That(html, Does.Contain("lx-btn-link"));
        });
    }

    [Test]
    public async Task The_dead_letter_tab_renders_the_design_systems_button_primitives()
    {
        var surface = Substitute.For<IDeadLetterSurface>();
        surface
            .ListAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException(Boom));

        var html = await SelectionViewRenderHarness.RenderAsync<DeadLetterTab, IDeadLetterSurface>(
            surface,
            SelectionViewRenderHarness.Tree());

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-btn"));
            Assert.That(html, Does.Contain("lx-btn-link"));
        });
    }

    [Test]
    public async Task The_data_entry_detail_renders_the_design_systems_link_button()
    {
        // Rendered from its parameters alone: this child resolves no domain
        // contract, so the error state it is asked for is exactly the state a
        // failed entry read puts it in.
        var html = await SelectionViewRenderHarness.RenderComponentAsync<DataEntryDetail>(
            new Dictionary<string, object?>
            {
                ["SelectedKey"] = "orders/1",
                ["Error"] = Boom,
            });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-btn-link"));
            Assert.That(html, Does.Contain(Boom));
        });
    }

    [Test]
    public async Task The_tag_index_tab_renders_the_design_systems_link_button()
    {
        var surface = Substitute.For<ITagIndexSurface>();
        surface.ListCoveredTreesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException(Boom));

        var html = await SelectionViewRenderHarness.RenderAsync<TagIndexDetailTab, ITagIndexSurface>(
            surface,
            SelectionViewRenderHarness.TagIndex());

        Assert.That(html, Does.Contain("lx-btn-link"));
    }

    [Test]
    public async Task The_tag_index_tab_renders_the_design_systems_badge_and_button()
    {
        var surface = Substitute.For<ITagIndexSurface>();
        surface.ListCoveredTreesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<string>>(new[] { "orders" }));
        surface.ListTagsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<string>>(new[] { "eu-west" }));

        var html = await SelectionViewRenderHarness.RenderAsync<TagIndexDetailTab, ITagIndexSurface>(
            surface,
            SelectionViewRenderHarness.TagIndex());

        // `lx-badge` is the one badge call site outside the shell, so nothing
        // else would notice if the primitive stopped being emitted here.
        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-badge"));
            Assert.That(html, Does.Contain("lx-btn"));
        });
    }
}
