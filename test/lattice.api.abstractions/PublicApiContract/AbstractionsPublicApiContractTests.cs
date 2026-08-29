using System.Reflection;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Api.TreeAdmin;

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
        typeof(ILatticeTreeAdmin),
        typeof(ILatticeTelemetry),
    };

    private static readonly IReadOnlyList<string> ContractNamespaces = new[]
    {
        "Orleans.Lattice.Api.State",
        "Orleans.Lattice.Api.Data",
        "Orleans.Lattice.Api.Auth",
        "Orleans.Lattice.Api.Backup",
        "Orleans.Lattice.Api.Schema",
        "Orleans.Lattice.Api.Replication",
        "Orleans.Lattice.Api.Region",
        "Orleans.Lattice.Api.TenantAdmin",
        "Orleans.Lattice.Api.Telemetry",
        "Orleans.Lattice.Api.TreeAdmin",
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

    // The region-discovery surface added by issue #1364: the transport-agnostic
    // catalog contract and the two wire types it exchanges, so a future Explorer
    // gRPC/facade binding can consume the same region model the MCP tool does.
    private static readonly IReadOnlyList<Type> RegionContractTypes = new[]
    {
        typeof(Region.ILatticeRegionCatalog),
        typeof(Region.LatticeRegionDescriptor),
        typeof(Region.LatticeRegionGroupReachability),
    };

    [TestCaseSource(nameof(RegionContractTypes))]
    public void Region_contract_type_is_public_in_the_abstractions_assembly(Type type)
    {
        Assert.Multiple(() =>
        {
            Assert.That(type.IsPublic, Is.True,
                $"{type.FullName} must be public so a future Explorer binding can consume the region "
                + "contract without an InternalsVisibleTo grant.");
            Assert.That(type.Assembly, Is.EqualTo(AbstractionsAssembly),
                $"{type.FullName} must live in the abstractions assembly.");
            Assert.That(type.Namespace, Is.EqualTo("Orleans.Lattice.Api.Region"),
                $"{type.FullName} must live in the region contract namespace.");
        });
    }

    // The batch and typed-CRDT surface added by issues #1366 and #1361: the
    // non-atomic multi-key write and the eight typed-CRDT verb families, kept on
    // the transport-agnostic data facade so the same operations can be surfaced
    // by a gRPC binding, the MCP tool, and a future Explorer binding alike.
    private static readonly IReadOnlyList<string> DataApiCrdtMembers = new[]
    {
        nameof(ILatticeDataApi.SetManyAsync),
        nameof(ILatticeDataApi.DeleteRangeAsync),
        nameof(ILatticeDataApi.CounterIncrementAsync),
        nameof(ILatticeDataApi.CounterDecrementAsync),
        nameof(ILatticeDataApi.CounterGetAsync),
        nameof(ILatticeDataApi.SetAddAsync),
        nameof(ILatticeDataApi.SetRemoveAsync),
        nameof(ILatticeDataApi.SetGetAsync),
        nameof(ILatticeDataApi.OrFlagEnableAsync),
        nameof(ILatticeDataApi.OrFlagDisableAsync),
        nameof(ILatticeDataApi.OrFlagGetAsync),
        nameof(ILatticeDataApi.RwFlagEnableAsync),
        nameof(ILatticeDataApi.RwFlagDisableAsync),
        nameof(ILatticeDataApi.RwFlagGetAsync),
        nameof(ILatticeDataApi.GCounterIncrementAsync),
        nameof(ILatticeDataApi.GCounterGetAsync),
        nameof(ILatticeDataApi.RwSetAddAsync),
        nameof(ILatticeDataApi.RwSetRemoveAsync),
        nameof(ILatticeDataApi.RwSetGetAsync),
        nameof(ILatticeDataApi.VersionVectorTickAsync),
        nameof(ILatticeDataApi.VersionVectorGetAsync),
        nameof(ILatticeDataApi.RegisterSetAsync),
        nameof(ILatticeDataApi.RegisterGetAsync),
        nameof(ILatticeDataApi.SequenceInsertAtAsync),
        nameof(ILatticeDataApi.SequenceRemoveAtAsync),
        nameof(ILatticeDataApi.SequenceGetAsync),
        nameof(ILatticeDataApi.MapSetAsync),
        nameof(ILatticeDataApi.MapRemoveAsync),
        nameof(ILatticeDataApi.MapGetAsync),
    };

    [TestCaseSource(nameof(DataApiCrdtMembers))]
    public void Data_api_exposes_the_batch_and_typed_crdt_operation(string memberName)
    {
        var method = typeof(ILatticeDataApi).GetMethod(memberName);

        Assert.That(method, Is.Not.Null,
            $"ILatticeDataApi must declare a public {memberName} operation so the gRPC binding, the "
            + "MCP tool, and a future Explorer binding can adapt it from one transport-agnostic seam.");
    }
}
