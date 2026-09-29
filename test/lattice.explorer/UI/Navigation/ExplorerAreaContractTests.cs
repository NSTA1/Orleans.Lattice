using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Layout.Appearance;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Navigation.Completion;

namespace Orleans.Lattice.Explorer.Tests.UI.Navigation;

/// <summary>
/// The area contract's boundary (epic decision E2): nothing public lets another
/// assembly register an area or a completion source, and the one internal way in
/// registers idempotently beside the rest of the chrome.
/// </summary>
[TestFixture]
public sealed class ExplorerAreaContractTests
{
    private static readonly Type[] Contract =
    [
        typeof(IExplorerArea),
        typeof(IAddressCompletionSource),
        typeof(AddressQuery),
        typeof(AddressCompletion),
        typeof(ExplorerCommand),
        typeof(AreaAvailability),
        typeof(ExplorerAreaDirectory),
        typeof(ExplorerAreaServiceCollectionExtensions),
    ];

    [Test]
    public void The_area_contract_is_internal()
    {
        Assert.That(Contract.Where(type => type.IsVisible).Select(type => type.FullName), Is.Empty);
    }

    [Test]
    public void No_public_type_lets_another_assembly_register_an_area_or_a_completion_source()
    {
        var assembly = typeof(IExplorerArea).Assembly;
        var violations = new List<string>();

        foreach (var type in assembly.GetExportedTypes())
        {
            if (typeof(IExplorerArea).IsAssignableFrom(type) || typeof(IAddressCompletionSource).IsAssignableFrom(type))
            {
                violations.Add($"{type.FullName} is public and implements the area contract, so it could be subclassed or registered from outside");
            }

            foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance | BindingFlags.DeclaredOnly))
            {
                if (!(method.IsPublic || method.IsFamily || method.IsFamilyOrAssembly))
                {
                    continue;
                }

                var mentions = method.GetParameters().Select(parameter => parameter.ParameterType)
                    .Concat(method.IsGenericMethodDefinition ? method.GetGenericArguments().SelectMany(argument => argument.GetGenericParameterConstraints()) : [])
                    .Append(method.ReturnType);

                if (mentions.Any(MentionsContract))
                {
                    violations.Add($"{type.FullName}.{method.Name} exposes the area contract");
                }
            }

            if (type.Name.Contains("Register", StringComparison.Ordinal) || type.GetMethods().Any(method => method.Name.Contains("ExplorerArea", StringComparison.Ordinal)))
            {
                violations.Add($"{type.FullName} looks like a public area registration surface");
            }
        }

        Assert.That(violations, Is.Empty, string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void The_scan_sees_the_public_chrome()
    {
        // Battery test: the sweep above must actually reach public types, or it
        // passes vacuously.
        Assert.That(typeof(IExplorerArea).Assembly.GetExportedTypes(), Does.Contain(typeof(ShellLayout)));
    }

    [Test]
    public void AddExplorerArea_registers_a_scoped_area_once()
    {
        var services = new ServiceCollection();

        services.AddExplorerArea<ProbeArea>().AddExplorerArea<ProbeArea>();

        var descriptor = services.Single(candidate => candidate.ServiceType == typeof(IExplorerArea));
        Assert.Multiple(() =>
        {
            Assert.That(descriptor.ImplementationType, Is.EqualTo(typeof(ProbeArea)));
            Assert.That(descriptor.Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
            Assert.That(() => ((IServiceCollection)null!).AddExplorerArea<ProbeArea>(), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task The_chrome_registers_everything_it_needs_and_resolves_in_a_scope()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Microsoft.AspNetCore.Components.NavigationManager>(new TestNavigationManager());
        services.AddSingleton<Microsoft.JSInterop.IJSRuntime>(NSubstitute.Substitute.For<Microsoft.JSInterop.IJSRuntime>());
        services.AddSingleton<Orleans.Lattice.Explorer.Core.Configuration.IExplorerSession>(
            new Orleans.Lattice.Explorer.Tests.UI.Session.FakeExplorerSession(new Orleans.Lattice.Explorer.Tests.UI.Session.FakeStateConnection()));
        services.AddSingleton<Orleans.Lattice.Explorer.Core.Authentication.IExplorerAuthSession>(new Orleans.Lattice.Explorer.Tests.UI.Session.FakeAuthSession());
        services.AddLatticeExplorerShell();
        services.AddExplorerArea<ProbeArea>();

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        await using var scope = provider.CreateAsyncScope();

        Assert.Multiple(() =>
        {
            Assert.That(scope.ServiceProvider.GetRequiredService<ExplorerAreaDirectory>().Areas.OfType<ProbeArea>().Count(), Is.EqualTo(1));
            Assert.That(scope.ServiceProvider.GetRequiredService<ExplorerNavigator>(), Is.Not.Null);
            Assert.That(scope.ServiceProvider.GetRequiredService<ExplorerTenancy>().IsActive, Is.False);
            Assert.That(scope.ServiceProvider.GetRequiredService<AddressCompletionFanOut>(), Is.Not.Null);
            Assert.That(scope.ServiceProvider.GetRequiredService<TenantCompletionSource>(), Is.Not.Null);
            Assert.That(scope.ServiceProvider.GetRequiredService<IShellAppearanceApplier>(), Is.InstanceOf<JsShellAppearanceApplier>());
            Assert.That(scope.ServiceProvider.GetRequiredService<ShellAppearance>(), Is.Not.Null);
            Assert.That(scope.ServiceProvider.GetRequiredService<ShellChromeInterop>(), Is.Not.Null);
            Assert.That(provider.GetRequiredService<TimeProvider>(), Is.SameAs(TimeProvider.System));
            Assert.That(provider.GetRequiredService<ExplorerChromeOptions>().CompletionTimeout, Is.EqualTo(TimeSpan.FromSeconds(2)));
        });
    }

    [Test]
    public void A_command_needs_a_dotted_lower_case_id_and_a_title()
    {
        var command = new ExplorerCommand("data.create-tree", "Create a tree") { Detail = "d", Target = ExplorerAddress.ForArea("data") };

        Assert.Multiple(() =>
        {
            Assert.That(command.Id, Is.EqualTo("data.create-tree"));
            Assert.That(command.Title, Is.EqualTo("Create a tree"));
            Assert.That(ExplorerCommand.ControlAttribute, Is.EqualTo("data-lt-command"));
            Assert.That(ExplorerCommand.IsValidId("go.home"), Is.True);
            Assert.That(ExplorerCommand.IsValidId("Go.Home"), Is.False);
            Assert.That(ExplorerCommand.IsValidId("go..home"), Is.False);
            Assert.That(ExplorerCommand.IsValidId(null), Is.False);
            Assert.That(() => new ExplorerCommand("Bad", "Title"), Throws.ArgumentException);
            Assert.That(() => new ExplorerCommand("ok", " "), Throws.ArgumentException);
        });
    }

    [Test]
    public void The_chrome_routes_are_lower_case_and_literal_first()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerRoutes.Home, Is.EqualTo("/"));
            Assert.That(ExplorerRoutes.TenantHome, Is.EqualTo("/t/{tenant}"));
            Assert.That(ExplorerRoutes.NotFound, Is.EqualTo("/not-found"));
            Assert.That(RouteTemplates(typeof(Orleans.Lattice.Explorer.UI.Pages.HomePage)), Is.EquivalentTo(new[] { ExplorerRoutes.Home, ExplorerRoutes.TenantHome }));
            Assert.That(RouteTemplates(typeof(Orleans.Lattice.Explorer.UI.Pages.NotFoundPage)), Is.EquivalentTo(new[] { ExplorerRoutes.NotFound }));
        });

        foreach (var template in new[] { ExplorerRoutes.Home, ExplorerRoutes.TenantHome, ExplorerRoutes.NotFound })
        {
            foreach (var segment in template.Split('/', StringSplitOptions.RemoveEmptyEntries).Where(segment => !segment.StartsWith('{')))
            {
                Assert.That(segment, Is.EqualTo(segment.ToLowerInvariant()), template);
            }
        }
    }

    [Test]
    public void The_location_knows_its_current_entry()
    {
        var data = new ExplorerAreaEntry(new FakeArea("data", "Data"), AreaAvailability.Visible, "3");
        var location = new ExplorerLocation(ExplorerAddress.Parse("/data/x"), [data], EntriesLoaded: true, TenancyActive: false);

        Assert.Multiple(() =>
        {
            Assert.That(location.CurrentEntry, Is.SameAs(data));
            Assert.That(location with { Address = ExplorerAddress.Home }, Has.Property(nameof(ExplorerLocation.CurrentEntry)).Null);
            Assert.That(ExplorerLocation.Initial.EntriesLoaded, Is.False);
            Assert.That(data.Badge, Is.EqualTo("3"));
        });
    }

    private static IEnumerable<string> RouteTemplates(Type page) =>
        page.GetCustomAttributes<Microsoft.AspNetCore.Components.RouteAttribute>().Select(route => route.Template);

    private static bool MentionsContract(Type type) =>
        Contract.Any(contract => contract == type)
        || (type.IsGenericType && type.GetGenericArguments().Any(MentionsContract))
        || (type.HasElementType && MentionsContract(type.GetElementType()!));

    /// <summary>An area with no behaviour, for registration tests.</summary>
    private sealed class ProbeArea : IExplorerArea
    {
        public string Key => "probe";

        public string DisplayName => "Probe";

        public int DirectoryOrder => 0;

        public ValueTask<AreaAvailability> GetAvailabilityAsync(CancellationToken cancellationToken) => ValueTask.FromResult(AreaAvailability.Visible);
    }
}
