namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// Pins the transport authorizer's description of the streamed set-many RPC (issue #5365):
/// it names its own operation, never <see cref="LatticeDataApiOperation.Unknown"/>, and a
/// request frame names its tree.
/// </summary>
[TestFixture]
public sealed class StreamSetManyInterceptorMappingTests
{
    [Test]
    public void DescribeCall_maps_StreamSetMany_to_its_operation()
    {
        var (operation, target) = LatticeDataApiGrpcAuthInterceptor.DescribeCall<DataSetManyStreamRequest>(
            "/" + LatticeDataApiGrpcMethods.ServiceName + "/" + LatticeDataApiGrpcMethods.StreamSetManyMethodName,
            default!);

        Assert.Multiple(() =>
        {
            Assert.That(operation, Is.EqualTo(LatticeDataApiOperation.StreamSetMany));
            Assert.That(target, Is.Null, "a streamed call is admitted before its first frame is read");
        });
    }

    [Test]
    public void DescribeCall_reads_the_tree_from_a_stream_frame()
    {
        var (_, target) = LatticeDataApiGrpcAuthInterceptor.DescribeCall(
            "/" + LatticeDataApiGrpcMethods.ServiceName + "/" + LatticeDataApiGrpcMethods.StreamSetManyMethodName,
            new DataSetManyStreamRequest { TreeId = "orders" });

        Assert.That(target, Is.EqualTo("orders"));
    }

    [Test]
    public void StreamSetMany_is_appended_after_every_earlier_operation()
    {
        // The enum has implicit ordinals; a member inserted earlier would renumber the rest.
        Assert.That((int)LatticeDataApiOperation.StreamSetMany, Is.EqualTo((int)LatticeDataApiOperation.ProbeCapabilities + 1));
    }
}
