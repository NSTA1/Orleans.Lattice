using System.Reflection;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Explorer.Tests.Hygiene;

/// <summary>
/// Epic #3807, E1 and E2: the Explorer has no extension point. The plugin model is
/// removed with no successor, and the native areas are compiled in behind an
/// internal contract, because an area registration API is a plugin API by another
/// name. This gate proves it by reflection over every public type of every
/// Orleans.Lattice.Explorer assembly the web head ships.
/// </summary>
[TestFixture]
public sealed class ExplorerExtensionPointAbsenceTests
{
    /// <summary>A name that reads as a plugin or area registration surface.</summary>
    private static readonly Regex ExtensionName = new(
        "Plugin|(Add|Register|Map|Use)\\w*Area\\b|(Add|Register)\\w*Areas\\b|ExplorerArea\\b|CompletionSource",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The public service-collection extensions the Explorer assemblies may expose:
    /// the web head's entry point and Core's own service layer. A new public
    /// registration must be added here deliberately, and it must not register an
    /// area or anything a plugin could implement.
    /// </summary>
    private static readonly string[] AllowedServiceCollectionExtensions =
    [
        "AddLatticeExplorerWeb",
        "AddExplorerAuth",
        "AddExplorerCatalog",
        "AddExplorerConfiguration",
        "AddExplorerData",
        "AddExplorerDeadLetter",
        "AddExplorerEnvironmentBootstrap",
        "AddExplorerHistory",
        "AddExplorerMetrics",
        "AddExplorerNavigation",
        "AddExplorerSession",
        "AddExplorerTenantView",
        "AddLatticeStateConnection",
    ];

    [Test]
    public void The_gate_covers_every_explorer_assembly_the_web_head_ships()
    {
        Assert.That(
            ExplorerAssemblies().Select(assembly => assembly.GetName().Name),
            Is.SupersetOf(new[]
            {
                "Orleans.Lattice.Explorer.Core",
                "Orleans.Lattice.Explorer.AppKit",
                "Orleans.Lattice.Explorer.Shell",
                "Orleans.Lattice.Explorer.Web",
            }));
    }

    [Test]
    public void No_public_explorer_type_or_member_is_named_as_a_plugin_or_area_registration()
    {
        var offenders = new List<string>();
        foreach (var type in PublicTypes())
        {
            if (ExtensionName.IsMatch(type.Name))
            {
                offenders.Add(type.FullName!);
            }

            foreach (var member in type.GetMembers(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly))
            {
                if (ExtensionName.IsMatch(member.Name))
                {
                    offenders.Add($"{type.FullName}.{member.Name}");
                }
            }
        }

        Assert.That(offenders, Is.Empty, "the Explorer exposes no plugin or area registration API (epic #3807, E1 and E2)");
    }

    [Test]
    public void The_area_and_completion_contracts_are_internal()
    {
        var shell = typeof(Orleans.Lattice.Explorer.Shell.Layout.ShellLayout).Assembly;

        Assert.Multiple(() =>
        {
            foreach (var name in new[]
            {
                "Orleans.Lattice.Explorer.Shell.Navigation.IExplorerArea",
                "Orleans.Lattice.Explorer.Shell.Navigation.IAddressCompletionSource",
                "Orleans.Lattice.Explorer.Shell.Navigation.ExplorerAreaServiceCollectionExtensions",
                "Orleans.Lattice.Explorer.Shell.ShellServiceCollectionExtensions",
            })
            {
                var type = shell.GetType(name, throwOnError: true)!;
                Assert.That(type.IsVisible, Is.False, name);
            }
        });
    }

    [Test]
    public void No_public_member_accepts_or_returns_an_area_or_completion_contract()
    {
        var shell = typeof(Orleans.Lattice.Explorer.Shell.Layout.ShellLayout).Assembly;
        var contracts = new[]
        {
            shell.GetType("Orleans.Lattice.Explorer.Shell.Navigation.IExplorerArea", throwOnError: true)!,
            shell.GetType("Orleans.Lattice.Explorer.Shell.Navigation.IAddressCompletionSource", throwOnError: true)!,
        };

        var offenders = new List<string>();
        foreach (var type in PublicTypes())
        {
            foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly))
            {
                var signature = method.GetParameters().Select(parameter => parameter.ParameterType).Append(method.ReturnType);
                if (method.IsGenericMethodDefinition)
                {
                    signature = signature.Concat(method.GetGenericArguments().SelectMany(argument => argument.GetGenericParameterConstraints()));
                }

                if (signature.Any(candidate => contracts.Any(contract => Mentions(candidate, contract))))
                {
                    offenders.Add($"{type.FullName}.{method.Name}");
                }
            }
        }

        Assert.That(offenders, Is.Empty);
    }

    [Test]
    public void Every_public_service_collection_extension_is_an_allowed_non_extension_registration()
    {
        var registrations = PublicTypes()
            .Where(type => type is { IsAbstract: true, IsSealed: true })
            .SelectMany(type => type.GetMethods(BindingFlags.Public | BindingFlags.Static))
            .Where(method => method.IsDefined(typeof(System.Runtime.CompilerServices.ExtensionAttribute), inherit: false)
                && method.GetParameters() is [{ ParameterType: var target }, ..]
                && target == typeof(IServiceCollection))
            .Select(method => method.Name)
            .Distinct()
            .Order(StringComparer.Ordinal)
            .ToArray();

        Assert.That(registrations, Is.SubsetOf(AllowedServiceCollectionExtensions));
    }

    private static bool Mentions(Type candidate, Type contract)
    {
        if (candidate == contract || contract.IsAssignableFrom(candidate))
        {
            return true;
        }

        if (candidate.HasElementType && Mentions(candidate.GetElementType()!, contract))
        {
            return true;
        }

        return candidate.IsGenericType && candidate.GetGenericArguments().Any(argument => Mentions(argument, contract));
    }

    private static IEnumerable<Type> PublicTypes() =>
        ExplorerAssemblies().SelectMany(assembly => assembly.GetExportedTypes());

    private static IReadOnlyList<Assembly> ExplorerAssemblies()
    {
        // Touch one type from each shipped assembly so it is loaded before the sweep.
        Assembly[] shipped =
        [
            typeof(Orleans.Lattice.Explorer.Core.Session.ExplorerPreferenceKey).Assembly,
            typeof(Orleans.Lattice.Explorer.AppKit.AppKitProtocol).Assembly,
            typeof(Orleans.Lattice.Explorer.Shell.Layout.ShellLayout).Assembly,
            typeof(Orleans.Lattice.Explorer.Web.LatticeExplorerWebOptions).Assembly,
        ];

        return AppDomain.CurrentDomain.GetAssemblies()
            .Concat(shipped)
            .Where(assembly => assembly.GetName().Name is { } name
                && name.StartsWith("Orleans.Lattice.Explorer", StringComparison.Ordinal)
                && !name.EndsWith(".Tests", StringComparison.Ordinal)
                && !name.EndsWith(".UiTests", StringComparison.Ordinal))
            .Distinct()
            .ToArray();
    }
}
