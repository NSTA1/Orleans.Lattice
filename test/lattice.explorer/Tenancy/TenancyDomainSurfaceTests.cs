using System.Reflection;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// The D3 guard for tenancy: nothing a tenancy plugin can reach through its
/// declared domain contract names a control-API or transport type.
/// <para>
/// The test walks the whole transitive public surface of
/// <see cref="ITenancyDomain"/> - every property type, method return type,
/// parameter type, and generic argument, recursively - and fails on any type
/// that comes from the control-API contract assembly, its gRPC binding, the gRPC
/// libraries, or the Orleans serialization stack. A future edit that returns a
/// wire record straight out of the domain model therefore breaks this test
/// rather than quietly widening what a plugin reaches.
/// </para>
/// </summary>
[TestFixture]
public class TenancyDomainSurfaceTests
{
    /// <summary>
    /// The assemblies whose types are wire or transport vocabulary. A type from
    /// any of them is exactly what the seam exists to keep out of a plugin.
    /// </summary>
    private static readonly string[] ForbiddenAssemblies =
    [
        "Orleans.Lattice.Api.Abstractions",
        "Orleans.Lattice.Api.TenantAdmin.Grpc",
        "Grpc.Core",
        "Grpc.Core.Api",
        "Grpc.Net.Client",
        "Orleans.Serialization",
        "Orleans.Serialization.Abstractions",
        "Orleans.Core.Abstractions",
    ];

    [Test]
    public void No_wire_type_is_reachable_from_the_plugin_facing_domain_model()
    {
        var leaks = FindForbiddenTypes(typeof(ITenancyDomain));

        Assert.That(
            leaks,
            Is.Empty,
            $"a control-API or transport type is reachable from {nameof(ITenancyDomain)}: "
            + string.Join(", ", leaks));
    }

    [Test]
    public void No_wire_type_is_reachable_from_the_operations_surface()
    {
        var leaks = FindForbiddenTypes(typeof(ITenantAdminService));

        Assert.That(
            leaks,
            Is.Empty,
            $"a control-API or transport type is reachable from {nameof(ITenantAdminService)}: "
            + string.Join(", ", leaks));
    }

    [Test]
    public void No_wire_type_is_reachable_from_the_narrowed_plugin_facing_domain_model()
    {
        // The tenant-administrator half of the seam is a domain contract in its
        // own right, so it carries the same obligation as the wide one.
        var leaks = FindForbiddenTypes(typeof(IMyTenantDomain));

        Assert.That(
            leaks,
            Is.Empty,
            $"a control-API or transport type is reachable from {nameof(IMyTenantDomain)}: "
            + string.Join(", ", leaks));
    }

    [Test]
    public void No_wire_type_is_reachable_from_the_narrowed_operations_surface()
    {
        var leaks = FindForbiddenTypes(typeof(ITenantSelfAdminService));

        Assert.That(
            leaks,
            Is.Empty,
            $"a control-API or transport type is reachable from {nameof(ITenantSelfAdminService)}: "
            + string.Join(", ", leaks));
    }

    [Test]
    public void The_transport_seam_is_not_reachable_from_the_domain_model()
    {
        var reachable = Reachable(typeof(ITenancyDomain));

        Assert.Multiple(() =>
        {
            Assert.That(reachable, Has.No.Member(typeof(ITenantAdminClient)));
            Assert.That(reachable, Has.No.Member(typeof(GrpcTenantAdminClient)));
        });
    }

    [Test]
    public void The_transport_seam_does_name_wire_types_so_the_boundary_is_real()
    {
        // The mirror of the guard above: the wire vocabulary genuinely exists and
        // is genuinely used - it just stops at the client. Without this, the
        // guard could pass simply because nothing anywhere touched the wire.
        var wireTypes = FindForbiddenTypes(typeof(ITenantAdminClient));

        Assert.That(
            wireTypes,
            Is.Not.Empty,
            "the transport seam is expected to speak the control API's own types");
    }

    [Test]
    public void Every_tenancy_domain_member_is_covered_by_the_walk()
    {
        // Guards the guard: if the domain contract grows a member shape the walk
        // does not inspect, the closure would silently stop covering it.
        var reachable = Reachable(typeof(ITenancyDomain));

        Assert.Multiple(() =>
        {
            Assert.That(reachable, Has.Member(typeof(ITenantAdminService)));
            Assert.That(reachable, Has.Member(typeof(ExplorerTenantQuotaUsage)));
            Assert.That(reachable, Has.Member(typeof(ExplorerTenantQuotaDimension)));
            Assert.That(reachable, Has.Member(typeof(ExplorerTenantGrant)));
            Assert.That(reachable, Has.Member(typeof(ExplorerTenantGrantState)));
            Assert.That(reachable, Has.Member(typeof(ExplorerTenantRegion)));
            Assert.That(reachable, Has.Member(typeof(TenantOperationStatus)));
        });
    }

    private static IReadOnlyList<string> FindForbiddenTypes(Type root) =>
    [
        .. Reachable(root)
            .Where(IsForbidden)
            .Select(type => type.FullName ?? type.Name)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal),
    ];

    private static bool IsForbidden(Type type)
    {
        var assembly = type.Assembly.GetName().Name;
        return assembly is not null && ForbiddenAssemblies.Contains(assembly, StringComparer.Ordinal);
    }

    /// <summary>
    /// The transitive closure of types reachable from <paramref name="root"/>'s
    /// public surface. Recursion continues through types the Explorer itself
    /// owns; every encountered type is recorded either way, so a wire type is
    /// caught even though the walk does not descend into it.
    /// </summary>
    private static HashSet<Type> Reachable(Type root)
    {
        var seen = new HashSet<Type>();
        var pending = new Stack<Type>();
        pending.Push(root);

        while (pending.Count > 0)
        {
            var type = Normalize(pending.Pop());
            if (type is null || !seen.Add(type))
            {
                continue;
            }

            foreach (var argument in type.GetGenericArguments())
            {
                pending.Push(argument);
            }

            // Only descend through the Explorer's own vocabulary: the framework
            // types in between (Task, IReadOnlyList, string) have surfaces of
            // their own that say nothing about this seam.
            if (!IsExplorerOwned(type))
            {
                continue;
            }

            const BindingFlags Flags = BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static
                | BindingFlags.DeclaredOnly;

            foreach (var property in type.GetProperties(Flags))
            {
                pending.Push(property.PropertyType);
                foreach (var parameter in property.GetIndexParameters())
                {
                    pending.Push(parameter.ParameterType);
                }
            }

            foreach (var field in type.GetFields(Flags))
            {
                pending.Push(field.FieldType);
            }

            foreach (var method in type.GetMethods(Flags))
            {
                pending.Push(method.ReturnType);
                foreach (var parameter in method.GetParameters())
                {
                    pending.Push(parameter.ParameterType);
                }
            }

            if (type.BaseType is { } baseType)
            {
                pending.Push(baseType);
            }

            // An interface's base interfaces are part of its surface, and
            // reflection does not fold them into GetMethods/GetProperties the way
            // class inheritance is folded in. Without this the walk would stop at
            // the first interface in a hierarchy - and since the tenancy seam
            // split into ITenancyDomain : IMyTenantDomain and
            // ITenantAdminService : ITenantSelfAdminService, that would silently
            // exclude most of the operations surface from the D3 guard.
            foreach (var contract in type.GetInterfaces())
            {
                pending.Push(contract);
            }
        }

        return seen;
    }

    /// <summary>
    /// Unwraps arrays, by-ref and pointer types, and nullable value types down to
    /// the type that actually carries a surface, and drops the ones that carry
    /// none (generic parameters, void, primitives, string).
    /// </summary>
    private static Type? Normalize(Type type)
    {
        while (type.IsArray || type.IsByRef || type.IsPointer)
        {
            var element = type.GetElementType();
            if (element is null)
            {
                return null;
            }

            type = element;
        }

        if (type.IsGenericParameter)
        {
            return null;
        }

        var underlying = Nullable.GetUnderlyingType(type);
        if (underlying is not null)
        {
            type = underlying;
        }

        if (type == typeof(void) || type.IsPrimitive || type == typeof(string) || type == typeof(object))
        {
            return null;
        }

        return type;
    }

    private static bool IsExplorerOwned(Type type)
    {
        var assembly = type.Assembly.GetName().Name;
        return assembly is not null
            && assembly.StartsWith("Orleans.Lattice.Explorer", StringComparison.Ordinal);
    }

    [Test]
    public void The_forbidden_set_actually_matches_a_known_wire_type() =>
        // Guards the guard the other way: a typo in an assembly name would make
        // every check above pass vacuously.
        Assert.That(IsForbidden(typeof(TenantQuotaUsageReport)), Is.True);
}
