using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Telemetry;
using Orleans.Lattice.Explorer.Plugins.Telemetry.Views;
using Orleans.Lattice.Explorer.Tests.DesignSystem;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// The two mount points, rendered exactly as the shell mounts them: the
/// Telemetry area panel, and the tenant-metrics section of My Tenant.
/// </summary>
/// <remarks>
/// <para>
/// This is the end-to-end proof that both mounts resolve their one declared
/// domain contract from a host context bound to the telemetry plugin's id, and
/// that they render the same board. The harness registers exactly the two
/// services a panel reaches the host through, which is the controlled
/// domain-model seam under test as much as it is plumbing.
/// </para>
/// <para>
/// Rendering is driven entirely by the fake domain and the access decision
/// supplied here, so no test waits on a clock, a timer, a network call, or a
/// background task.
/// </para>
/// </remarks>
[TestFixture]
public sealed class TelemetryMountRenderTests
{
    [Test]
    public async Task The_area_panel_mounts_and_renders_the_board()
    {
        var (html, _) = await RenderAsync<TelemetryPanel>();

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("aria-label=\"Telemetry\""));
            Assert.That(html, Does.Contain("Write throughput"));
            Assert.That(html, Does.Contain("<svg"));
        });
    }

    [Test]
    public async Task The_area_panel_supplies_its_own_adaptive_root_when_no_shell_cascades_one()
    {
        var (withShell, _) = await RenderAsync<TelemetryPanel>(breakpoint: LatticeBreakpoint.Expanded);
        var (without, _) = await RenderAsync<TelemetryPanel>(breakpoint: null);

        Assert.Multiple(() =>
        {
            Assert.That(without, Does.Contain("lxt-root"), "the plugin stays usable on its own");
            Assert.That(
                withShell,
                Does.Not.Contain("lxt-root"),
                "a shell-provided root must not be competed with by a second one");
        });
    }

    [Test]
    public async Task The_area_panel_asks_for_whatever_visibility_the_shell_is_requesting()
    {
        var (_, domain) = await RenderAsync<TelemetryPanel>(
            configure: fake => fake.RequestedVisibility = ExplorerTelemetryVisibility.AllTenants);

        Assert.That(
            domain.LastRequest!.RequestedVisibility,
            Is.EqualTo(ExplorerTelemetryVisibility.AllTenants));
    }

    [Test]
    public async Task The_my_tenant_section_mounts_and_renders_the_same_board()
    {
        var (panel, _) = await RenderAsync<TelemetryPanel>();
        var (section, _) = await RenderAsync<TelemetryTenantSection>();

        Assert.Multiple(() =>
        {
            Assert.That(section, Does.Contain("lxt-section"));
            Assert.That(section, Does.Contain("Write throughput"));
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(section, "<polyline"),
                Is.EqualTo(DesignSystemRenderHarness.CountOccurrences(panel, "<polyline")),
                "the section is the same board, not a second implementation");
        });
    }

    [Test]
    public async Task The_my_tenant_section_asks_only_for_the_callers_own_tenant_however_the_shell_is_set()
    {
        // A platform operator with the shell switched to a cross-tenant view is
        // still looking at a section headed "your tenant".
        var (_, domain) = await RenderAsync<TelemetryTenantSection>(
            configure: fake => fake.RequestedVisibility = ExplorerTelemetryVisibility.AllTenants);

        Assert.Multiple(() =>
        {
            Assert.That(domain.Requests, Is.Not.Empty);
            Assert.That(
                domain.Requests.Select(request => request.RequestedVisibility),
                Is.All.EqualTo(ExplorerTelemetryVisibility.ActiveTenant));
            Assert.That(domain.Requests.Select(request => request.RequestedTenantId), Is.All.Null);
        });
    }

    [Test]
    public async Task The_my_tenant_section_renders_the_scope_the_facade_pinned()
    {
        var (html, _) = await RenderAsync<TelemetryTenantSection>(
            configure: fake => fake.Result = ExplorerTelemetrySample.Result(
                ExplorerTelemetrySample.ActiveScope()));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(ExplorerTelemetrySample.TenantId));
            Assert.That(html, Does.Not.Contain("Narrower than you asked for"));
        });
    }

    [Test]
    public async Task The_my_tenant_section_still_reports_a_degrade_the_facade_declared()
    {
        // The pin is a request, not a grant.
        var (html, _) = await RenderAsync<TelemetryTenantSection>(
            configure: fake => fake.Result = ExplorerTelemetrySample.Result(
                ExplorerTelemetrySample.DowngradedScope()));

        Assert.That(html, Does.Contain("Narrower than you asked for"));
    }

    [Test]
    public async Task The_my_tenant_section_renders_nothing_about_quota_or_usage()
    {
        // Quota lives on the My Tenant Quota surface. Two answers to one
        // question in one area is worse than one.
        var (html, _) = await RenderAsync<TelemetryTenantSection>();

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Not.Contain("quota").IgnoreCase);
            Assert.That(html, Does.Not.Contain("ceiling").IgnoreCase);
            Assert.That(html, Does.Not.Contain("overage").IgnoreCase);
        });
    }

    [Test]
    public async Task An_unavailable_cluster_renders_an_empty_body_at_both_mounts()
    {
        var (panel, _) = await RenderAsync<TelemetryPanel>(
            access: ExplorerPluginAccess.ReportUnavailable("no telemetry facade here"));
        var (section, _) = await RenderAsync<TelemetryTenantSection>(
            access: ExplorerPluginAccess.ReportUnavailable("no telemetry facade here"));

        Assert.Multiple(() =>
        {
            Assert.That(panel, Does.Not.Contain("no telemetry facade here"));
            Assert.That(panel, Does.Not.Contain("<svg"));
            Assert.That(section, Does.Not.Contain("<svg"));
        });
    }

    [Test]
    public async Task Both_mounts_key_their_gate_on_the_telemetry_plugins_own_id()
    {
        // The section lives inside the My Tenant area but its availability is a
        // telemetry question; reading My Tenant's decision would report the
        // wrong surface's state.
        var (panel, _) = await RenderAsync<TelemetryPanel>(access: ExplorerPluginAccess.Deny("telemetry denied"));
        var (section, _) = await RenderAsync<TelemetryTenantSection>(
            access: ExplorerPluginAccess.Deny("telemetry denied"));

        Assert.Multiple(() =>
        {
            Assert.That(panel, Does.Contain("telemetry denied"));
            Assert.That(section, Does.Contain("telemetry denied"));
        });
    }

    private static async Task<(string Html, FakeExplorerTelemetryDomain Domain)> RenderAsync<TComponent>(
        Action<FakeExplorerTelemetryDomain>? configure = null,
        ExplorerPluginAccess? access = null,
        LatticeBreakpoint? breakpoint = LatticeBreakpoint.Expanded)
        where TComponent : IComponent
    {
        var domain = new FakeExplorerTelemetryDomain();
        configure?.Invoke(domain);

        var store = new ExplorerPluginAccessStore();
        store.Set(TelemetryPluginKeys.PluginId, access ?? ExplorerPluginAccess.Allowed);

        var context = Substitute.For<IExplorerPluginHostContext>();
        context.PluginId.Returns(TelemetryPluginKeys.PluginId);
        context.GetDomain<ITelemetryDomain>().Returns(domain);

        var factory = Substitute.For<IExplorerPluginHostContextFactory>();
        factory.Create(TelemetryPluginKeys.PluginId).Returns(context);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(factory);
        services.AddSingleton<IExplorerPluginAccessStore>(store);

        await using var provider = services.BuildServiceProvider();
        var loggerFactory = provider.GetRequiredService<ILoggerFactory>();
        await using var renderer = new HtmlRenderer(provider, loggerFactory);

        var html = await renderer.Dispatcher.InvokeAsync(async () =>
        {
            if (breakpoint is not { } value)
            {
                var bare = await renderer.RenderComponentAsync<TComponent>(ParameterView.Empty);
                return bare.ToHtmlString();
            }

            var parameters = ParameterView.FromDictionary(new Dictionary<string, object?>
            {
                ["Value"] = new LatticeAdaptiveContext(value, LatticeDensity.Cosy, IsMeasured: true),
                ["ChildContent"] = (RenderFragment)(builder =>
                {
                    builder.OpenComponent<TComponent>(0);
                    builder.CloseComponent();
                }),
            });

            var cascaded = await renderer
                .RenderComponentAsync<CascadingValue<LatticeAdaptiveContext>>(parameters);
            return cascaded.ToHtmlString();
        });

        return (html, domain);
    }
}
