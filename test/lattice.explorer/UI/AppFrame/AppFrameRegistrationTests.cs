using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.UI;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Framing;
using Orleans.Lattice.Explorer.UI.Framing.Broker;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Explorer.Tests.UI.Design;
using Orleans.Lattice.Explorer.Tests.UI.Session;
using Orleans.Lattice.Explorer.Tests.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Framing;

/// <summary>
/// The frame host's registration (per-circuit lifetimes, optional collaborators), its host
/// context seam, its outbound messages and its responsive stylesheet.
/// </summary>
[TestFixture]
public sealed class AppFrameRegistrationTests
{
    [Test]
    public void The_loader_broker_and_host_context_are_per_circuit_and_only_the_cache_is_shared()
    {
        var services = new ServiceCollection().AddLatticeExplorerShell();

        ServiceLifetime Lifetime<T>() => services.Single(descriptor => descriptor.ServiceType == typeof(T)).Lifetime;

        Assert.Multiple(() =>
        {
            Assert.That(Lifetime<AppBridgeBroker>(), Is.EqualTo(ServiceLifetime.Scoped));
            Assert.That(Lifetime<AppFrameBundleLoader>(), Is.EqualTo(ServiceLifetime.Scoped));
            Assert.That(Lifetime<IAppFrameHostContext>(), Is.EqualTo(ServiceLifetime.Scoped));
            Assert.That(Lifetime<AppFrameBundleCache>(), Is.EqualTo(ServiceLifetime.Singleton));
            Assert.That(Lifetime<TimeProvider>(), Is.EqualTo(ServiceLifetime.Singleton));
        });
    }

    [Test]
    public void Each_circuit_gets_its_own_broker_and_loader()
    {
        using var provider = CircuitServices().BuildServiceProvider(validateScopes: true);
        using var first = provider.CreateScope();
        using var second = provider.CreateScope();

        Assert.Multiple(() =>
        {
            Assert.That(first.ServiceProvider.GetRequiredService<AppBridgeBroker>(), Is.Not.SameAs(second.ServiceProvider.GetRequiredService<AppBridgeBroker>()));
            Assert.That(first.ServiceProvider.GetRequiredService<AppFrameBundleLoader>(), Is.Not.SameAs(second.ServiceProvider.GetRequiredService<AppFrameBundleLoader>()));
            Assert.That(first.ServiceProvider.GetRequiredService<AppFrameBundleCache>(), Is.SameAs(second.ServiceProvider.GetRequiredService<AppFrameBundleCache>()));
        });
    }

    [Test]
    public void The_cache_captures_nothing_but_its_bound()
    {
        var parameters = typeof(AppFrameBundleCache).GetConstructors().SelectMany(constructor => constructor.GetParameters());
        Assert.That(parameters.Select(parameter => parameter.ParameterType), Is.All.EqualTo(typeof(long)));
    }

    [Test]
    public async Task Without_a_transport_every_launch_is_refused()
    {
        // The Shell's transport always registers a workspace, so model a host that
        // serves none by removing it: the loader then receives no workspace at all.
        var services = CircuitServices();
        services.RemoveAll<ILatticeAppWorkspace>();
        using var provider = services.BuildServiceProvider(validateScopes: true);
        using var scope = provider.CreateScope();

        var result = await scope.ServiceProvider.GetRequiredService<AppFrameBundleLoader>().AuthorizeAsync(AppFrameTestData.Slug);

        Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.NoGrant));
    }

    [Test]
    public async Task A_transport_with_no_configured_endpoint_refuses_every_launch()
    {
        using var provider = CircuitServices().BuildServiceProvider(validateScopes: true);
        using var scope = provider.CreateScope();

        var result = await scope.ServiceProvider.GetRequiredService<AppFrameBundleLoader>().AuthorizeAsync(AppFrameTestData.Slug);

        Assert.Multiple(() =>
        {
            Assert.That(scope.ServiceProvider.GetRequiredService<ILatticeAppWorkspace>(), Is.InstanceOf<ShellAppWorkspaceTransport>());
            Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.Unavailable));
        });
    }

    [Test]
    public void A_registered_transport_and_toast_queue_are_the_circuits_own()
    {
        var workspace = new FakeAppWorkspace();
        using var provider = new ServiceCollection()
            .AddLogging()
            .AddScoped<ILatticeAppWorkspace>(_ => workspace)
            .AddScoped<ILatticeAppBridge, FakeAppBridge>()
            .AddLatticeExplorerShell()
            .BuildServiceProvider(validateScopes: true);
        using var scope = provider.CreateScope();

        Assert.Multiple(() =>
        {
            Assert.That(scope.ServiceProvider.GetRequiredService<AppBridgeBroker>(), Is.Not.Null);
            Assert.That(scope.ServiceProvider.GetRequiredService<LtToastService>(), Is.Not.Null);
            Assert.That(scope.ServiceProvider.GetRequiredService<IAppFrameHostContext>(), Is.InstanceOf<DefaultAppFrameHostContext>());
        });
    }

    [Test]
    public void The_default_host_context_discloses_nothing()
    {
        var context = new DefaultAppFrameHostContext();

        Assert.Multiple(() =>
        {
            Assert.That(context.Appearance, Is.SameAs(AppFrameAppearance.Default));
            Assert.That(context.TenantDisplayName, Is.Null);
            Assert.That(context.UserDisplayName, Is.Null);
        });
    }

    [Test]
    public void Appearance_sanitises_to_the_closed_sets_and_keeps_a_valid_instance()
    {
        var valid = new AppFrameAppearance("board", "more", "compact", true);
        var hostile = new AppFrameAppearance("\"}", "x", "y", true).Sanitise();

        Assert.Multiple(() =>
        {
            Assert.That(valid.Sanitise(), Is.SameAs(valid));
            Assert.That(hostile, Is.EqualTo(new AppFrameAppearance("paper", "standard", "comfortable", true)));
        });
    }

    [Test]
    public void Nav_changed_and_context_changed_carry_only_their_data()
    {
        var nav = JsonDocument.Parse(AppFrameMessages.NavChanged("/a\"b")).RootElement;
        var context = JsonDocument.Parse(AppFrameMessages.ContextChanged(new AppFrameAppearance("board", "?", "compact", false))).RootElement;

        Assert.Multiple(() =>
        {
            Assert.That(nav.GetProperty("type").GetString(), Is.EqualTo("nav.changed"));
            Assert.That(nav.GetProperty("data").GetProperty("path").GetString(), Is.EqualTo("/a\"b"));
            Assert.That(context.GetProperty("type").GetString(), Is.EqualTo("context.changed"));
            Assert.That(context.GetProperty("data").GetProperty("contrast").GetString(), Is.EqualTo("standard"));
            Assert.That(() => AppFrameMessages.NavChanged(null!), Throws.ArgumentNullException);
            Assert.That(() => AppFrameMessages.ContextChanged(null!), Throws.ArgumentNullException);
            Assert.That(() => AppFrameMessages.Bundle(null!, AppFrameAppearance.Default), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_host_asset_paths_come_from_the_shells_content_base()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppFrameAssets.HostModule, Is.EqualTo("_content/Orleans.Lattice.Explorer.UI/appframe/host.mjs"));
            Assert.That(AppFrameAssets.Stylesheet, Is.EqualTo("_content/Orleans.Lattice.Explorer.UI/appframe/appframe.css"));
            Assert.That(AppFrameAssets.HostModuleImport, Is.EqualTo("./" + AppFrameAssets.HostModule));
            Assert.That(File.Exists(ShellStylesheets.Absolute("src/lattice.explorer/UI/wwwroot/appframe/host.mjs")), Is.True);
        });
    }

    /// <summary>
    /// The responsive contract (epic #3807, point 8): the frame fills the region at every width
    /// with no width query, and the "Leave app" controls meet the 44px touch target in
    /// comfortable density and never drop below 24px in compact density.
    /// </summary>
    [Test]
    public void The_stylesheet_fills_the_region_and_sizes_the_leave_controls_for_touch()
    {
        var rules = ShellStylesheets.Parse(ShellStylesheets.WithoutComments("src/lattice.explorer/UI/wwwroot/appframe/appframe.css"));
        string Body(string selector) => rules.Single(rule => rule.Selector == selector && rule.AtRule.Length == 0).Body;

        Assert.Multiple(() =>
        {
            Assert.That(rules.Select(rule => rule.AtRule), Is.All.Empty, "no media or container query");
            Assert.That(Body(".appframe"), Does.Contain("min-inline-size: 0;"));
            Assert.That(Body(".appframe__viewport"), Does.Contain("flex: 1 1 auto;").And.Contain("min-inline-size: 0;"));
            Assert.That(Body(".appframe__viewport > iframe"), Does.Contain("inline-size: 100%;").And.Contain("border: 0;"));
            Assert.That(Body(".appframe__bar:first-child"), Does.Contain("position: sticky;"));
            Assert.That(ShellStylesheets.Pixels(Value(Body(".appframe__bar > .lt-btn"), "min-block-size")), Is.GreaterThanOrEqualTo(44));
            Assert.That(ShellStylesheets.Pixels(Value(Body(".appframe__bar > .lt-btn"), "min-inline-size")), Is.GreaterThanOrEqualTo(44));
            Assert.That(ShellStylesheets.Pixels(Value(Body("[data-lt-density=\"compact\"] .appframe__bar > .lt-btn"), "min-block-size")), Is.GreaterThanOrEqualTo(24));
            Assert.That(Regex.IsMatch(string.Concat(rules.Select(rule => rule.Body)), @"#[0-9a-fA-F]{3,8}\b|rgb\(|hsl\("), Is.False, "no colour literal");
        });
    }

    private static string Value(string body, string property) =>
        Regex.Match(body, property + @"\s*:\s*([^;]+);").Groups[1].Value.Trim();

    /// <summary>
    /// A circuit's container as the web head builds it: the Core session and auth session the
    /// Shell's transport reads (unconfigured and signed out), then the Shell.
    /// </summary>
    private static IServiceCollection CircuitServices() =>
        new ServiceCollection()
            .AddLogging()
            .AddScoped<IExplorerSession>(_ => new FakeExplorerSession(new FakeStateConnection()))
            .AddScoped<IExplorerAuthSession, FakeAuthSession>()
            .AddShellTransportTestHead()
            .AddLatticeExplorerShell();
}
