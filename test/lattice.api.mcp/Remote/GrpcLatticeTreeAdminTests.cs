using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="GrpcLatticeTreeAdmin"/>, the remote-host adapter that
/// fronts <see cref="ILatticeTreeAdmin"/> over the tree-administration-API gRPC
/// client. At this scaffolding stage the facade exposes the capability probe, so the
/// adapter is proven to forward its request and unwrap the response, plus the
/// argument guards. Deterministic over a <see cref="FakeCallInvoker"/>.
/// </summary>
[TestFixture]
public sealed class GrpcLatticeTreeAdminTests
{
    private static GrpcLatticeTreeAdmin Adapter(FakeCallInvoker invoker)
        => new(RemoteTestSupport.TreeAdminClient(invoker));

    private static LatticeTreeAdminCapabilities Caps(string tree) => new()
    {
        TreeId = tree,
        CanAdministerTree = false,
        Schema = new LatticeSchemaCapabilities { TreeId = tree },
    };

    [Test]
    public void Constructor_null_client_throws()
        => Assert.That(() => new GrpcLatticeTreeAdmin(null!), Throws.ArgumentNullException);

    [Test]
    public async Task ProbeCapabilitiesAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => Caps("orders"));

        var result = await Adapter(invoker).ProbeCapabilitiesAsync("orders");

        var sent = (TreeAdminTreeRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(result.TreeId, Is.EqualTo("orders"));
            Assert.That(result.CanAdministerTree, Is.False);
            Assert.That(result.Schema.TreeId, Is.EqualTo("orders"));
        });
    }

    [Test]
    public void ProbeCapabilitiesAsync_empty_tree_throws()
        => Assert.ThrowsAsync<ArgumentException>(
            async () => await Adapter(new FakeCallInvoker(_ => Caps("x"))).ProbeCapabilitiesAsync(""));
}
