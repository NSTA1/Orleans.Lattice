using Microsoft.AspNetCore.Components;
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
/// receive. Driving a tab into its error state is deliberate: it is where a
/// surface offers its recovery control, and that control is the shared button
/// primitive.
/// </para>
/// <para>
/// Issue #1855 moved the recovery control from the inline <c>lx-btn-link</c>
/// variant onto the shared state block, where the action is the block's own
/// button (<c>lx-btn</c>) rather than a link beside a sentence. The link
/// variant is still the right shape for a retry that sits inline in a list, so
/// it keeps a call site and a test of its own below - dropping that assertion
/// entirely would have left the variant unguarded, which is the exact failure
/// this fixture was written to prevent.
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
            Assert.That(html, Does.Contain("lx-selection-message-action"), "the retry control is the state block's action");
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
            Assert.That(html, Does.Contain("lx-selection-message-action"));
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
            Assert.That(html, Does.Contain("lx-selection-message-action"));
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
                ["OnRetry"] = EventCallback.Empty,
            });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-btn"));
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

        Assert.That(html, Does.Contain("lx-selection-message-action"));
    }

    [Test]
    public async Task The_tag_index_member_list_renders_the_design_systems_link_button()
    {
        // The inline retry inside a list is still the link-styled variant, so
        // this is what keeps `lx-btn-link` guarded now that the surfaces' block
        // states use the full button.
        var html = await SelectionViewRenderHarness.RenderComponentAsync<TagIndexMembers>(
            new Dictionary<string, object?>
            {
                ["Tag"] = "eu-west",
                ["Error"] = Boom,
            });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-btn-link"));
            Assert.That(html, Does.Contain(Boom));
        });
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
