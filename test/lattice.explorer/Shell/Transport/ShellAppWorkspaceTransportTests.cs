using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Tests.Shell.Transport;

/// <summary>The Shell's <see cref="ILatticeAppWorkspace"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellAppWorkspaceTransportTests : ShellTransportAdapterContractTests<ILatticeAppWorkspace>
{
    private const string Service = "/orleans.lattice.api.apps.workspace/";

    internal override IEnumerable<ShellTransportCall<ILatticeAppWorkspace>> Calls() =>
    [
        new("ListMyAppsAsync", Service + "ListMyApps", (f, ct) => f.ListMyAppsAsync(ct)),
        new("DescribeMyAppAsync", Service + "DescribeMyApp", (f, ct) => f.DescribeMyAppAsync("crm", ct)),
        new("GetIconAsync", Service + "GetIcon", (f, ct) => f.GetIconAsync("crm", ct)),
        new("GetUiAssetAsync", Service + "GetUiAsset", (f, ct) => f.GetUiAssetAsync("crm", "ui/index.js", ct)),
    ];

    [Test]
    public void An_app_the_caller_may_not_open_maps_to_the_facade_denial()
    {
        using var circuit = new ShellTransportCircuit();
        var workspace = circuit.Resolve<ILatticeAppWorkspace>();
        circuit.Peer.AnswerWith(Grpc.Core.StatusCode.PermissionDenied, "not granted");

        Assert.That(
            () => workspace.GetUiAssetAsync("crm", "ui/index.js"),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>().With.Message.EqualTo("not granted"));
    }

    [Test]
    public void An_unreachable_cluster_maps_to_a_transient_transport_fault()
    {
        using var circuit = new ShellTransportCircuit();
        var workspace = circuit.Resolve<ILatticeAppWorkspace>();
        circuit.Peer.AnswerWith(Grpc.Core.StatusCode.Unavailable);

        var ex = Assert.ThrowsAsync<Orleans.Lattice.Explorer.Shell.Transport.ShellTransportException>(() => workspace.ListMyAppsAsync());

        Assert.That(ex!.IsTransient, Is.True);
    }

    [Test]
    public void Argument_guards_run_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var workspace = circuit.Resolve<ILatticeAppWorkspace>();

        Assert.Multiple(() =>
        {
            Assert.That(() => workspace.DescribeMyAppAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => workspace.GetIconAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => workspace.GetUiAssetAsync(string.Empty, "ui/index.js"), Throws.ArgumentException);
            Assert.That(() => workspace.GetUiAssetAsync("crm", string.Empty), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }
}
