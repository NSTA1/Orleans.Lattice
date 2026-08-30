using System.Reflection;
using System.Xml.Linq;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// Architectural guards over the plugin contract assembly. These encode the
/// epic's binding decisions rather than a single type's behaviour: a plugin is
/// identified by a string and never an enum, it operates only against a
/// controlled domain model and never the cluster connection, and the contract
/// carries none of the closed navigation types it replaces.
/// </summary>
[TestFixture]
public sealed class ExplorerPluginContractSurfaceTests
{
    private static readonly Assembly Contract = typeof(IExplorerPlugin).Assembly;

    /// <summary>
    /// Type names a plugin must never be handed: the cluster connection, its
    /// transport, and the container itself (which would make the contract a
    /// service locator and defeat the declared-domain seam).
    /// </summary>
    private static readonly string[] ForbiddenTypeNames =
    [
        "ILatticeStateConnection",
        "ILatticeStateClient",
        "GrpcChannel",
        "CallInvoker",
        "HttpClient",
        "IServiceProvider",
    ];

    [Test]
    public void Host_context_exposes_no_cluster_connection_or_service_locator()
    {
        var offenders = new List<string>();

        foreach (var member in typeof(IExplorerPluginHostContext).GetMembers(
            BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
        {
            foreach (var type in MemberTypes(member))
            {
                if (IsForbidden(type))
                {
                    offenders.Add($"{member.Name} -> {type.FullName}");
                }
            }
        }

        Assert.That(offenders, Is.Empty, "the host context must not hand a plugin the cluster connection");
    }

    [Test]
    public void No_public_contract_member_exposes_a_cluster_connection_type()
    {
        var offenders = new List<string>();

        foreach (var type in Contract.GetExportedTypes())
        {
            foreach (var member in type.GetMembers(
                BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly))
            {
                foreach (var referenced in MemberTypes(member))
                {
                    // The domain resolver and the host-context factory are the
                    // host's own composition seam and are legitimately built
                    // over IServiceProvider; a plugin never sees them through
                    // its context, which the previous test pins.
                    if (IsForbidden(referenced)
                        && type != typeof(ExplorerPluginDomainResolver))
                    {
                        offenders.Add($"{type.Name}.{member.Name} -> {referenced.FullName}");
                    }
                }
            }
        }

        Assert.That(offenders, Is.Empty);
    }

    [Test]
    public void Contract_assembly_does_not_reference_the_explorer_core()
    {
        var referenced = Contract.GetReferencedAssemblies().Select(a => a.Name).ToArray();

        Assert.That(referenced, Does.Not.Contain("Orleans.Lattice.Explorer.Core"));
    }

    [Test]
    public void Contract_assembly_references_no_lattice_assembly_at_all()
    {
        var latticeReferences = Contract.GetReferencedAssemblies()
            .Select(a => a.Name)
            .Where(name => name is not null && name.StartsWith("Orleans.Lattice", StringComparison.Ordinal))
            .ToArray();

        Assert.That(latticeReferences, Is.Empty, "the plugin contract must stand alone");
    }

    [Test]
    public void Contract_names_none_of_the_closed_navigation_types_it_replaces()
    {
        string[] retired = ["AppArea", "DetailTab", "ExplorerCapabilities"];

        var offenders = Contract.GetExportedTypes()
            .Where(t => retired.Any(r => t.Name.Contains(r, StringComparison.Ordinal)))
            .Select(t => t.FullName!)
            .ToArray();

        Assert.That(offenders, Is.Empty);
    }

    [Test]
    public void Plugin_identity_is_a_string_and_never_an_enum()
    {
        Assert.That(
            typeof(ExplorerPluginDescriptor).GetProperty(nameof(ExplorerPluginDescriptor.PluginId))!.PropertyType,
            Is.EqualTo(typeof(string)));
    }

    [Test]
    public void A_plugin_exposes_its_view_gate_and_declared_contract()
    {
        IExplorerPlugin plugin = new FakeExplorerPlugin(
            "sample",
            gate: ExplorerPluginAccessGates.Allowed,
            domainContract: typeof(IDisposable),
            viewType: typeof(ExplorerPluginContractSurfaceTests));

        Assert.Multiple(() =>
        {
            Assert.That(plugin.Descriptor.PluginId, Is.EqualTo("sample"));
            Assert.That(plugin.ViewType, Is.EqualTo(typeof(ExplorerPluginContractSurfaceTests)));
            Assert.That(plugin.DomainContract, Is.EqualTo(typeof(IDisposable)));
            Assert.That(plugin.AccessGate, Is.SameAs(ExplorerPluginAccessGates.Allowed));
        });
    }

    [Test]
    public void The_view_type_is_a_plain_type_so_the_contract_carries_no_ui_framework_dependency()
    {
        var viewType = typeof(IExplorerPlugin).GetProperty(nameof(IExplorerPlugin.ViewType))!.PropertyType;

        Assert.Multiple(() =>
        {
            Assert.That(viewType, Is.EqualTo(typeof(Type)));
            Assert.That(
                Contract.GetReferencedAssemblies().Select(a => a.Name),
                Does.Not.Contain("Microsoft.AspNetCore.Components"));
        });
    }

    [Test]
    public void Every_exported_type_lives_in_the_plugin_namespace()
    {
        var offenders = Contract.GetExportedTypes()
            .Where(t => t.Namespace != "Orleans.Lattice.Explorer.Plugins")
            .Select(t => t.FullName!)
            .ToArray();

        Assert.That(offenders, Is.Empty);
    }

    [Test]
    public void Every_exported_type_is_documented()
    {
        // The XML documentation file ships in the NuGet package, so a missing
        // summary is a shipped defect rather than a style nit.
        var xmlPath = Path.ChangeExtension(Contract.Location, ".xml");
        Assert.That(File.Exists(xmlPath), Is.True, $"expected generated XML docs beside {Contract.Location}");

        var documented = XDocument.Load(xmlPath)
            .Descendants("member")
            .Where(m => m.Element("summary") is not null)
            .Select(m => (string?)m.Attribute("name"))
            .Where(name => name is not null)
            .ToHashSet(StringComparer.Ordinal);

        var undocumented = Contract.GetExportedTypes()
            .Select(t => $"T:{t.FullName}")
            .Where(name => !documented.Contains(name))
            .ToArray();

        Assert.That(undocumented, Is.Empty);
    }

    private static IEnumerable<Type> MemberTypes(MemberInfo member)
    {
        switch (member)
        {
            case PropertyInfo property:
                yield return property.PropertyType;
                break;
            case FieldInfo field:
                yield return field.FieldType;
                break;
            case MethodInfo method:
                yield return method.ReturnType;
                foreach (var parameter in method.GetParameters())
                {
                    yield return parameter.ParameterType;
                }

                break;
            case ConstructorInfo constructor:
                foreach (var parameter in constructor.GetParameters())
                {
                    yield return parameter.ParameterType;
                }

                break;
            case EventInfo @event when @event.EventHandlerType is not null:
                yield return @event.EventHandlerType;
                break;
        }
    }

    private static bool IsForbidden(Type type)
    {
        foreach (var candidate in Unwrap(type))
        {
            if (ForbiddenTypeNames.Contains(candidate.Name, StringComparer.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static IEnumerable<Type> Unwrap(Type type)
    {
        yield return type;

        if (type.HasElementType && type.GetElementType() is { } element)
        {
            yield return element;
        }

        if (!type.IsGenericType)
        {
            yield break;
        }

        foreach (var argument in type.GetGenericArguments())
        {
            foreach (var nested in Unwrap(argument))
            {
                yield return nested;
            }
        }
    }
}
