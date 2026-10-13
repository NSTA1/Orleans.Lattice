using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

namespace Orleans.Lattice.Replication.Grpc.Tests;

public partial class LatticeRemoteSnapshotGrpcServiceTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Tenant_export_requires_authenticated_requester_before_provider(bool stream)
    {
        var sender = Substitute.For<ISnapshotProvider>();
        var gate = Substitute.For<IReplicationTenantIsolationGate>();
        gate.IsActive.Returns(true);
        var inner = new LatticeRemoteSnapshotService(sender, new AllEnrolledContext(),
            NullLogger<LatticeRemoteSnapshotService>.Instance, gate);
        var service = new LatticeRemoteSnapshotGrpcService(CreateMethods(), inner,
            NullLogger<LatticeRemoteSnapshotGrpcService>.Instance);
        var context = new FakeServerCallContext();
        context.RequestHeaders.Add(LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader, "allowed-peer");
        var request = new RemoteSnapshotMetadataRequestBox
        {
            Value = new RemoteSnapshotMetadataRequest { TreeName = "t/acme/orders", SourceClusterId = "allowed-peer" },
        };
        var writer = new CapturingStreamWriter<RemoteSnapshotStreamItemBox>();

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            if (stream) await service.RequestSnapshot(request, writer, context);
            else await service.GetMetadata(request, context);
        });

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
        Assert.That(writer.Written, Is.Empty);
        await sender.DidNotReceiveWithAnyArgs().ExportAsync(default!, default, default);
        await gate.DidNotReceiveWithAnyArgs().AuthorizeExportAsync(default!, default!, default);
    }
}
