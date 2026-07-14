using System.Reflection;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Api.Abstractions.Tests.PublicApiContract;

/// <summary>
/// Guards the public-surface contract the abstractions package exists to provide.
/// The contract-extraction refactor promoted the four facade service interfaces
/// (and the state observer/metrics seams) from <c>internal</c> to <c>public</c>
/// and relocated them - together with their DTOs - into
/// <c>Orleans.Lattice.Api.Abstractions</c> while deliberately preserving their
/// original <c>Orleans.Lattice.Api.{State,Data,Auth,Backup}</c> namespaces so
/// existing consumers keep compiling unchanged. This fixture fails if any of
/// those interfaces silently reverts to internal, or if a type strays outside
/// the four contracted namespaces.
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
    };

    private static readonly IReadOnlyList<string> ContractNamespaces = new[]
    {
        "Orleans.Lattice.Api.State",
        "Orleans.Lattice.Api.Data",
        "Orleans.Lattice.Api.Auth",
        "Orleans.Lattice.Api.Backup",
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
}
