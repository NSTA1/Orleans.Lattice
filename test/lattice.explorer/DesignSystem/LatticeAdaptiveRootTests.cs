using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.DesignSystem;
using Orleans.Lattice.Explorer.DesignSystem.Components;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Component tests for the adaptive root: the element that owns viewport
/// observation and cascades the shell context every primitive reads.
/// </summary>
[TestFixture]
public sealed class LatticeAdaptiveRootTests
{
    private static Task<string> RenderAsync(
        Action<IServiceCollection>? configureServices = null,
        IDictionary<string, object?>? parameters = null) =>
        DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveRoot>(
            parameters ?? new Dictionary<string, object?>(),
            configureServices);

    [Test]
    public async Task Render_withNoViewportRegistered_fallsBackToTheDefaultBreakpoint()
    {
        var html = await RenderAsync();

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("class=\"lx-root"));
            Assert.That(
                html,
                Does.Contain($"data-lx-breakpoint=\"{LatticeBreakpoints.Name(LatticeBreakpoints.Default)}\""));
            Assert.That(html, Does.Contain("data-lx-measured=\"false\""));
        });
    }

    [Test]
    public async Task Render_publishesTheRegisteredViewportsBreakpoint()
    {
        var viewport = new LatticeViewport();
        viewport.SetViewportWidth(320);

        var html = await RenderAsync(services => services.AddSingleton<ILatticeViewport>(viewport));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("data-lx-breakpoint=\"compact\""));
            Assert.That(html, Does.Contain("data-lx-measured=\"true\""));
        });
    }

    [Test]
    public async Task Render_publishesTheStandardDensityByDefault()
    {
        var html = await RenderAsync();

        Assert.That(html, Does.Contain($"data-lx-density=\"{LatticeDensities.Name(LatticeDensity.Cosy)}\""));
    }

    [Test]
    public async Task Render_publishesTheCallersDensity()
    {
        var html = await RenderAsync(
            parameters: new Dictionary<string, object?> { ["Density"] = LatticeDensity.Compact });

        Assert.That(html, Does.Contain("data-lx-density=\"compact\""));
    }

    [Test]
    public async Task Render_appendsTheCallersClassToTheRootElement()
    {
        var html = await RenderAsync(
            parameters: new Dictionary<string, object?> { ["Class"] = "explorer-shell" });

        Assert.That(html, Does.Contain("lx-root explorer-shell"));
    }

    [Test]
    public async Task Render_rendersItsChildContent()
    {
        var html = await RenderAsync(parameters: new Dictionary<string, object?>
        {
            ["ChildContent"] = (RenderFragment)(builder => builder.AddMarkupContent(0, "<p>shell</p>")),
        });

        Assert.That(html, Does.Contain("<p>shell</p>"));
    }

    [Test]
    public async Task Render_cascadesTheShellContextToADescendantPrimitive()
    {
        var viewport = new LatticeViewport();
        viewport.SetViewportWidth(320);

        LatticeNavItem[] items = [new("explore", "Explore"), new("backups", "Backups")];

        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveRoot>(
            new Dictionary<string, object?>
            {
                ["ChildContent"] = (RenderFragment)(builder =>
                {
                    builder.OpenComponent<LatticeAdaptiveNav>(0);
                    builder.AddComponentParameter(1, nameof(LatticeAdaptiveNav.Items), items);
                    builder.CloseComponent();
                }),
            },
            services => services.AddSingleton<ILatticeViewport>(viewport));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-nav-bottom"), "the nav adopted the cascaded compact breakpoint");
            Assert.That(html, Does.Not.Contain("lx-nav-sidebar"));
        });
    }

    [Test]
    public async Task Render_aDescendantMayStillPinItsOwnBreakpoint()
    {
        var viewport = new LatticeViewport();
        viewport.SetViewportWidth(320);

        LatticeNavItem[] items = [new("explore", "Explore")];

        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveRoot>(
            new Dictionary<string, object?>
            {
                ["ChildContent"] = (RenderFragment)(builder =>
                {
                    builder.OpenComponent<LatticeAdaptiveNav>(0);
                    builder.AddComponentParameter(1, nameof(LatticeAdaptiveNav.Items), items);
                    builder.AddComponentParameter(
                        2,
                        nameof(LatticeAdaptiveNav.Breakpoint),
                        (LatticeBreakpoint?)LatticeBreakpoint.Expanded);
                    builder.CloseComponent();
                }),
            },
            services => services.AddSingleton<ILatticeViewport>(viewport));

        Assert.That(html, Does.Contain("lx-nav-sidebar"));
    }

    [Test]
    public async Task Render_withoutAJsRuntime_stillProducesAUsableShell()
    {
        // The harness registers no IJSRuntime, which is exactly the static and
        // prerendered case: the root must render rather than fault.
        var html = await RenderAsync(services => services.AddLatticeExplorerDesignSystem());

        Assert.That(html, Does.Contain("class=\"lx-root"));
    }

    [Test]
    public void OnBreakpointChanged_ignoresAnUnknownBreakpointName()
    {
        var root = new LatticeAdaptiveRoot();

        Assert.That(() => root.OnBreakpointChanged("gigantic"), Throws.Nothing);
    }

    [Test]
    public void OnBreakpointChanged_acceptsAKnownBreakpointNameOutsideARenderedCircuit()
    {
        var root = new LatticeAdaptiveRoot();

        Assert.That(() => root.OnBreakpointChanged(LatticeBreakpoints.CompactName), Throws.Nothing);
    }

    [Test]
    public async Task DisposeAsync_isSafeWhenNothingWasEverObserved()
    {
        var root = new LatticeAdaptiveRoot();

        await root.DisposeAsync();

        Assert.That(async () => await root.DisposeAsync(), Throws.Nothing);
    }
}
