using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.JSInterop;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Shell;
using Orleans.Lattice.Explorer.Shell.Areas.Apps.App;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Tests.Shell.Design;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;
using Orleans.Lattice.Explorer.Tests.Shell.Session;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Apps.App;

/// <summary>
/// The app pages' registration and assets: one scoped loader per circuit over whichever app
/// facades the host registered, no area and no completion source of their own (the Apps
/// area owns both), and a stylesheet that exists where the page links it.
/// </summary>
[TestFixture]
public sealed class AppPagesRegistrationTests
{
    [Test]
    public async Task The_loader_is_scoped_and_reads_the_circuits_facades()
    {
        var workspace = new FakeAppPagesWorkspace().Grant(AppPageTestData.Workspace());
        var services = HeadServices();
        services.AddLatticeExplorerShell();
        services.AddSingleton<ILatticeAppWorkspace>(workspace);
        services.AddSingleton<ILatticeAppsControl>(new FakeAppPagesControl());

        var descriptor = services.Single(candidate => candidate.ServiceType == typeof(AppPageLoader));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        await using var first = provider.CreateAsyncScope();
        await using var second = provider.CreateAsyncScope();
        var loader = first.ServiceProvider.GetRequiredService<AppPageLoader>();

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
            Assert.That(second.ServiceProvider.GetRequiredService<AppPageLoader>(), Is.Not.SameAs(loader));
        });

        var load = await loader.LoadAsync(AppPageTestData.Slug, CancellationToken.None);
        Assert.That(load.Kind, Is.EqualTo(AppPageLoadKind.Loaded));
    }

    [Test]
    public async Task A_host_without_the_app_facades_still_resolves_and_answers_not_found()
    {
        var services = HeadServices();
        services.AddLatticeExplorerShell();
        services.RemoveAll<ILatticeAppWorkspace>();
        services.RemoveAll<ILatticeAppsControl>();
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        await using var scope = provider.CreateAsyncScope();

        var load = await scope.ServiceProvider.GetRequiredService<AppPageLoader>().LoadAsync(AppPageTestData.Slug, CancellationToken.None);

        Assert.That(load, Is.SameAs(AppPageLoad.NotFound));
    }

    /// <summary>
    /// The services a head provides per circuit that the rest of the Shell reads - the
    /// navigation manager, the JavaScript runtime and Core's session seams - so a container
    /// that registers the whole Shell validates on build.
    /// </summary>
    private static ServiceCollection HeadServices()
    {
        var services = new ServiceCollection();
        services.AddScoped<NavigationManager>(_ => new TestNavigationManager());
        services.AddScoped(_ => NSubstitute.Substitute.For<IJSRuntime>());
        services.AddSingleton<IExplorerSession>(new FakeExplorerSession(new FakeStateConnection()));
        services.AddSingleton<IExplorerAuthSession>(new FakeAuthSession());
        return services;
    }

    [Test]
    public void The_app_pages_register_no_area_and_no_completion_source()
    {
        var services = new ServiceCollection().AddLatticeExplorerShell();
        var fromThisFolder = services
            .Where(descriptor => descriptor.ServiceType == typeof(IExplorerArea) || descriptor.ServiceType == typeof(IAddressCompletionSource))
            .Where(descriptor => descriptor.ImplementationType?.Namespace == typeof(AppPage).Namespace);

        Assert.Multiple(() =>
        {
            Assert.That(fromThisFolder, Is.Empty);
            Assert.That(typeof(AppPage).Assembly.GetTypes()
                .Where(type => type.Namespace == typeof(AppPage).Namespace)
                .Where(type => typeof(IExplorerArea).IsAssignableFrom(type) || typeof(IAddressCompletionSource).IsAssignableFrom(type)),
                Is.Empty);
        });
    }

    [Test]
    public void Only_the_page_is_public()
    {
        var exported = typeof(AppPage).Assembly.GetExportedTypes().Where(type => type.Namespace == typeof(AppPage).Namespace);

        Assert.That(exported, Is.EquivalentTo(new[] { typeof(AppPage) }));
    }

    [Test]
    public void The_stylesheet_is_served_from_the_shells_apps_folder_and_exists()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppPagesAssets.BasePath, Is.EqualTo("_content/Orleans.Lattice.Explorer.Shell/apps/"));
            Assert.That(AppPagesAssets.Stylesheet, Is.EqualTo("_content/Orleans.Lattice.Explorer.Shell/apps/app.css"));
            Assert.That(File.Exists(ShellStylesheets.Absolute("src/lattice.explorer/Shell/wwwroot/apps/app.css")), Is.True);
        });
    }

    [Test]
    public void The_stylesheet_names_only_app_page_classes_and_raises_every_control_to_the_comfortable_target()
    {
        var rules = ShellStylesheets.Rules("src/lattice.explorer/Shell/wwwroot/apps/app.css");
        var ownClasses = rules
            .SelectMany(rule => System.Text.RegularExpressions.Regex.Matches(rule.Selector, @"\.(lt-[a-z0-9_-]+)").Select(match => match.Groups[1].Value))
            .Where(name => !name.StartsWith("lt-tabs", StringComparison.Ordinal) && !name.StartsWith("lt-btn", StringComparison.Ordinal));

        Assert.Multiple(() =>
        {
            Assert.That(ownClasses, Is.Not.Empty);
            Assert.That(ownClasses, Has.All.StartWith("lt-app-"));
            Assert.That(rules.Single(rule => rule.Selector == ".lt-app-sections .lt-tabs__tab").Body, Does.Contain("min-height: 2.75rem;"));
            Assert.That(rules.Single(rule => rule.Selector == ".lt-app-actions > .lt-btn").Body, Does.Contain("min-height: 2.75rem;"));
        });
    }
}
