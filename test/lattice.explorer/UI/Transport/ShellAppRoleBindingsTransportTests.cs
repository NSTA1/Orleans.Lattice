using Grpc.Core;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>The Shell's <see cref="ILatticeAppRoleBindings"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellAppRoleBindingsTransportTests : ShellTransportAdapterContractTests<ILatticeAppRoleBindings>
{
    private const string Service = "/orleans.lattice.api.apps/";

    private static readonly AppRoleBindingsUpdate Update = new()
    {
        Slug = "crm",
        Version = "1.0.0",
        RoleBindings = [new AppRoleBindingDescriptor { RoleName = "reader", GroupId = "g-readers" }],
    };

    internal override IEnumerable<ShellTransportCall<ILatticeAppRoleBindings>> Calls() =>
    [
        new("UpdateRoleBindingsAsync", Service + "UpdateRoleBindings", (f, ct) => f.UpdateRoleBindingsAsync(Update, ct)),
    ];

    [Test]
    public void A_cluster_that_does_not_serve_rebinding_maps_to_not_supported()
    {
        using var circuit = new ShellTransportCircuit();
        var bindings = circuit.Resolve<ILatticeAppRoleBindings>();
        circuit.Peer.AnswerWith(StatusCode.Unimplemented, "App role re-binding is not served by this host.");

        Assert.That(() => bindings.UpdateRoleBindingsAsync(Update), Throws.InstanceOf<NotSupportedException>());
    }

    [Test]
    public void A_failed_precondition_maps_to_invalid_operation()
    {
        using var circuit = new ShellTransportCircuit();
        var bindings = circuit.Resolve<ILatticeAppRoleBindings>();
        circuit.Peer.AnswerWith(StatusCode.FailedPrecondition, "The app-control precondition was not met.");

        Assert.That(() => bindings.UpdateRoleBindingsAsync(Update), Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void The_argument_guard_runs_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var bindings = circuit.Resolve<ILatticeAppRoleBindings>();

        Assert.That(() => bindings.UpdateRoleBindingsAsync(null!), Throws.ArgumentNullException);
        Assert.That(circuit.Peer.Requests, Is.Empty);
    }
}
