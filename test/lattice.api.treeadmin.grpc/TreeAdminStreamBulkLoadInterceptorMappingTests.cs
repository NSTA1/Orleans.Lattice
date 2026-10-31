using Orleans.Serialization;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// Pins the transport authorizer's description of the streamed bulk-load RPC (issue #5365):
/// it names its own operation, never <see cref="LatticeTreeAdminApiOperation.Unknown"/>,
/// and a request frame names its tree.
/// </summary>
[TestFixture]
public sealed class TreeAdminStreamBulkLoadInterceptorMappingTests
{
    [Test]
    public void DescribeCall_maps_StreamBulkLoad_to_its_operation()
    {
        var (operation, target) = LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall<TreeAdminBulkLoadStreamRequest>(
            "/" + LatticeTreeAdminGrpcMethods.ServiceName + "/" + LatticeTreeAdminGrpcMethods.StreamBulkLoadMethodName,
            default!);

        Assert.Multiple(() =>
        {
            Assert.That(operation, Is.EqualTo(LatticeTreeAdminApiOperation.StreamBulkLoad));
            Assert.That(target, Is.Null, "a streamed call is admitted before its first frame is read");
        });
    }

    [Test]
    public void DescribeCall_reads_the_tree_from_a_stream_frame()
    {
        var (_, target) = LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
            "/" + LatticeTreeAdminGrpcMethods.ServiceName + "/" + LatticeTreeAdminGrpcMethods.StreamBulkLoadMethodName,
            new TreeAdminBulkLoadStreamRequest { TreeId = "orders", OperationId = "op" });

        Assert.That(target, Is.EqualTo("orders"));
    }

    [Test]
    public void The_StreamBulkLoad_method_is_a_duplex_stream()
    {
        using var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();

        var methods = LatticeTreeAdminGrpcMethods.FromServiceProvider(services);

        Assert.That(methods.StreamBulkLoad.Type, Is.EqualTo(global::Grpc.Core.MethodType.DuplexStreaming));
    }
}
