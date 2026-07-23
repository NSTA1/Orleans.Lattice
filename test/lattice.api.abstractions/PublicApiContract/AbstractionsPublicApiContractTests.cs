using System.Reflection;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Api.Abstractions.Tests.PublicApiContract;

/// <summary>
/// Guards the public-surface contract the abstractions package exists to provide.
/// The contract-extraction refactor promoted the facade service interfaces
/// (and the state observer/metrics seams) from <c>internal</c> to <c>public</c>
/// and relocated them - together with their DTOs - into
/// <c>Orleans.Lattice.Api.Abstractions</c> while deliberately preserving their
/// original <c>Orleans.Lattice.Api.{State,Data,Auth,Backup,Schema,Replication}</c> namespaces
/// so existing consumers keep compiling unchanged. This fixture fails if any of
/// those interfaces silently reverts to internal, or if a type strays outside
/// the contracted namespaces.
/// </summary>
[TestFixture]
public class AbstractionsPublicApiContractTests
{
    private static readonly Assembly AbstractionsAssembly = typeof(ILatticeStateQuery).Assembly;

    private static readonly IReadOnlyList<Type> ServiceInterfaces = new[]
    {
        typeof(ILatticeStateQuery),
        typeof(ILatticeStateObserver),
        typeof(ILatticeStateMetricsObserver),
        typeof(ILatticeDataApi),
        typeof(ILatticeAuthAdmin),
        typeof(ILatticeBackupControl),
        typeof(ILatticeSchemaControl),
        typeof(ILatticeReplicationControl),
    };

    private static readonly IReadOnlyList<string> ContractNamespaces = new[]
    {
        "Orleans.Lattice.Api.State",
        "Orleans.Lattice.Api.Data",
        "Orleans.Lattice.Api.Auth",
        "Orleans.Lattice.Api.Backup",
        "Orleans.Lattice.Api.Schema",
        "Orleans.Lattice.Api.Replication",
    };

    [TestCaseSource(nameof(ServiceInterfaces))]
    public void Service_interface_is_a_public_interface_in_the_abstractions_assembly(Type contract)
    {
        Assert.Multiple(() =>
        {
            Assert.That(contract.IsInterface, Is.True, $"{contract.FullName} must be an interface.");
            Assert.That(contract.IsPublic, Is.True,
                $"{contract.FullName} must be public so bindings and the MCP server can consume the "
                + "contract without an InternalsVisibleTo grant.");
            Assert.That(contract.Assembly, Is.EqualTo(AbstractionsAssembly),
                $"{contract.FullName} must live in the abstractions assembly.");
        });
    }

    [Test]
    public void Every_public_type_lives_in_a_contracted_api_namespace()
    {
        var strays = AbstractionsAssembly.GetExportedTypes()
            .Where(t => t.Namespace is null
                || !t.Namespace.StartsWith("OrleansCodeGen", StringComparison.Ordinal))
            .Where(t => t.Namespace is null || !ContractNamespaces.Contains(t.Namespace))
            .Select(t => t.FullName)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.That(strays, Is.Empty,
            "Every public type in the abstractions assembly must live in one of the contracted "
            + $"namespaces ({string.Join(", ", ContractNamespaces)}) so existing consumers compile "
            + "unchanged after the move. Offending types: " + string.Join(", ", strays));
    }

    // The identity-directory and access-model surface added by issue #1248: the
    // three new facade operations and the four public wire types they exchange.
    private static readonly IReadOnlyList<string> AuthAdminDirectoryMembers = new[]
    {
        nameof(ILatticeAuthAdmin.SearchDirectoryAsync),
        nameof(ILatticeAuthAdmin.ResolveDirectoryPrincipalAsync),
        nameof(ILatticeAuthAdmin.GetAccessModelAsync),
    };

    private static readonly IReadOnlyList<Type> AuthAdminDirectoryTypes = new[]
    {
        typeof(DirectorySearchRequest),
        typeof(DirectorySearchResult),
        typeof(DirectoryPrincipalDescriptor),
        typeof(AccessModelDescriptor),
        typeof(AccessAuthenticationMode),
    };

    [TestCaseSource(nameof(AuthAdminDirectoryMembers))]
    public void Auth_admin_exposes_the_identity_directory_operation(string memberName)
    {
        var method = typeof(ILatticeAuthAdmin).GetMethod(memberName);

        Assert.That(method, Is.Not.Null,
            $"ILatticeAuthAdmin must declare a public {memberName} operation so bindings can adapt it.");
    }

    [TestCaseSource(nameof(AuthAdminDirectoryTypes))]
    public void Auth_admin_directory_type_is_public_in_the_abstractions_assembly(Type type)
    {
        Assert.Multiple(() =>
        {
            Assert.That(type.IsPublic, Is.True,
                $"{type.FullName} must be public so bindings and the MCP server can consume it.");
            Assert.That(type.Assembly, Is.EqualTo(AbstractionsAssembly),
                $"{type.FullName} must live in the abstractions assembly.");
            Assert.That(ContractNamespaces.Contains(type.Namespace), Is.True,
                $"{type.FullName} must live in a contracted namespace.");
        });
    }
}
