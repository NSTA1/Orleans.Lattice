using Grpc.Core;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit coverage for <see cref="GrpcLatticeWalReclamation"/>, the remote-mode adapter
/// that forwards the WAL reclamation read to a tree-administration control-API
/// endpoint over gRPC (#4237). Deterministic over a <see cref="FakeCallInvoker"/>.
/// </summary>
[TestFixture]
public sealed class GrpcLatticeWalReclamationTests
{
    [Test]
    public async Task The_read_forwards_the_tree_and_returns_the_wire_report()
    {
        var report = new TreeWalReclamationReport
        {
            TreeId = "orders",
            PinStoreReadable = true,
            PinCount = 1,
            FloorHolder = new TreeWalFloorHolder
            {
                ConsumerId = "c",
                PinOffset = 5,
                PersistedCheckpoint = -1,
                State = TreeWalFloorHolderState.NeverCheckpointed,
            },
        };
        var invoker = new FakeCallInvoker(_ => report);

        var read = await new GrpcLatticeWalReclamation(RemoteTestSupport.TreeAdminClient(invoker)).GetWalReclamationAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminTreeRequest)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
            Assert.That(read.IsWedged, Is.True);
            Assert.That(read.FloorHolder!.PinOffset, Is.EqualTo(5));
        });
    }

    [Test]
    public void A_cluster_without_the_read_faults_the_call()
    {
        var invoker = new FakeCallInvoker(_ => new RpcException(new Status(StatusCode.Unimplemented, "no reclamation")));
        var adapter = new GrpcLatticeWalReclamation(RemoteTestSupport.TreeAdminClient(invoker));

        Assert.That(
            async () => await adapter.GetWalReclamationAsync("orders"),
            Throws.InstanceOf<RpcException>().With.Property(nameof(RpcException.StatusCode)).EqualTo(StatusCode.Unimplemented));
    }

    [Test]
    public void An_empty_tree_id_is_rejected_before_the_wire()
    {
        var invoker = new FakeCallInvoker(_ => throw new AssertionException("must not reach the wire"));
        var adapter = new GrpcLatticeWalReclamation(RemoteTestSupport.TreeAdminClient(invoker));

        Assert.That(async () => await adapter.GetWalReclamationAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
        Assert.That(invoker.CallCount, Is.Zero);
    }

    [Test]
    public void The_adapter_rejects_a_null_client() =>
        Assert.That(() => new GrpcLatticeWalReclamation(null!), Throws.ArgumentNullException);
}
