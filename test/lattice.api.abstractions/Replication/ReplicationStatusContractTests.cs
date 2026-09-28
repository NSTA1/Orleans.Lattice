using System.Reflection;
using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Api.Abstractions.Tests.Replication;

/// <summary>
/// Pins the shape of the replication facade contracts. The peer-status read
/// contract was added as a <b>separate</b> interface precisely so that
/// <see cref="ILatticeReplicationControl"/> stays byte-identical; this fixture
/// fails if a member is added to, removed from, or re-signed on the control
/// interface, and if the status interface drifts from its one read verb.
/// </summary>
[TestFixture]
public sealed class ReplicationStatusContractTests
{
    [Test]
    public void ILatticeReplicationControl_is_unchanged()
    {
        Assert.That(Signatures(typeof(ILatticeReplicationControl)), Is.EqualTo(new[]
        {
            "Task<ReplicationConfigReport> GetReplicationConfigAsync(CancellationToken cancellationToken = default)",
            "Task<ReplicationDisableResult> DisableReplicationAsync(String treeId, CancellationToken cancellationToken = default)",
            "Task<ReplicationEnableResult> EnableReplicationAsync(String treeId, LatticeMergeMode mode, String bootstrapSourceClusterId = default, CancellationToken cancellationToken = default)",
        }));
    }

    [Test]
    public void ILatticeReplicationStatus_exposes_exactly_the_peer_status_read()
    {
        Assert.That(Signatures(typeof(ILatticeReplicationStatus)), Is.EqualTo(new[]
        {
            "Task<ReplicationPeerStatusPage> GetPeerStatusAsync(ReplicationPeerStatusQuery query, CancellationToken cancellationToken = default)",
        }));
    }

    [Test]
    public void ILatticeReplicationStatus_is_a_public_interface_separate_from_the_control_facade()
    {
        var status = typeof(ILatticeReplicationStatus);

        Assert.Multiple(() =>
        {
            Assert.That(status.IsInterface, Is.True);
            Assert.That(status.IsPublic, Is.True);
            Assert.That(status.Namespace, Is.EqualTo("Orleans.Lattice.Api.Replication"));
            Assert.That(status.Assembly, Is.EqualTo(typeof(ILatticeReplicationControl).Assembly));
            Assert.That(status.GetInterfaces(), Is.Empty, "the status contract must not extend another facade");
            Assert.That(typeof(ILatticeReplicationControl).IsAssignableFrom(status), Is.False);
            Assert.That(status.IsAssignableFrom(typeof(ILatticeReplicationControl)), Is.False);
        });
    }

    private static string[] Signatures(Type contract) =>
        contract.GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly)
            .Select(Describe)
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToArray();

    private static string Describe(MethodInfo method)
    {
        var parameters = method.GetParameters().Select(p =>
            $"{TypeName(p.ParameterType)} {p.Name}{(p.HasDefaultValue ? " = default" : string.Empty)}");
        return $"{TypeName(method.ReturnType)} {method.Name}({string.Join(", ", parameters)})";
    }

    private static string TypeName(Type type)
    {
        if (!type.IsGenericType)
        {
            return type.Name;
        }

        var name = type.Name[..type.Name.IndexOf('`')];
        return $"{name}<{string.Join(", ", type.GetGenericArguments().Select(TypeName))}>";
    }
}
