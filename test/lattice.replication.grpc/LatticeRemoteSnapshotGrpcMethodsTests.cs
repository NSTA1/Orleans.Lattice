using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Unit tests for the gRPC method holder. Pins the service-name and
/// method-name slots that the receiver-side auth interceptor reads,
/// and confirms construction validates its serialiser inputs.
/// </summary>
[TestFixture]
public class LatticeRemoteSnapshotGrpcMethodsTests
{
    private static LatticeRemoteSnapshotGrpcMethods Create()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddSerializer();
        var sp = services.BuildServiceProvider();
        return new LatticeRemoteSnapshotGrpcMethods(
            sp.GetRequiredService<Serializer<RemoteSnapshotMetadataRequest>>(),
            sp.GetRequiredService<Serializer<RemoteSnapshotMetadata>>(),
            sp.GetRequiredService<Serializer<RemoteSnapshotStreamItem>>());
    }

    [Test]
    public void Service_name_constant_matches_documented_value()
    {
        Assert.That(LatticeRemoteSnapshotGrpcMethods.ServiceName,
            Is.EqualTo("orleans.lattice.replication.LatticeRemoteSnapshot"));
    }

    [Test]
    public void GetMetadata_method_carries_unary_shape()
    {
        var methods = Create();

        Assert.Multiple(() =>
        {
            Assert.That(methods.GetMetadata.Type, Is.EqualTo(MethodType.Unary));
            Assert.That(methods.GetMetadata.ServiceName, Is.EqualTo(LatticeRemoteSnapshotGrpcMethods.ServiceName));
            Assert.That(methods.GetMetadata.Name, Is.EqualTo(LatticeRemoteSnapshotGrpcMethods.GetMetadataMethodName));
        });
    }

    [Test]
    public void RequestSnapshot_method_carries_server_streaming_shape()
    {
        var methods = Create();

        Assert.Multiple(() =>
        {
            Assert.That(methods.RequestSnapshot.Type, Is.EqualTo(MethodType.ServerStreaming));
            Assert.That(methods.RequestSnapshot.ServiceName, Is.EqualTo(LatticeRemoteSnapshotGrpcMethods.ServiceName));
            Assert.That(methods.RequestSnapshot.Name, Is.EqualTo(LatticeRemoteSnapshotGrpcMethods.RequestSnapshotMethodName));
        });
    }

    [Test]
    public void Constructor_throws_on_null_request_serializer()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddSerializer();
        var sp = services.BuildServiceProvider();
        Assert.That(() => new LatticeRemoteSnapshotGrpcMethods(
                null!,
                sp.GetRequiredService<Serializer<RemoteSnapshotMetadata>>(),
                sp.GetRequiredService<Serializer<RemoteSnapshotStreamItem>>()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_metadata_serializer()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddSerializer();
        var sp = services.BuildServiceProvider();
        Assert.That(() => new LatticeRemoteSnapshotGrpcMethods(
                sp.GetRequiredService<Serializer<RemoteSnapshotMetadataRequest>>(),
                null!,
                sp.GetRequiredService<Serializer<RemoteSnapshotStreamItem>>()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_stream_item_serializer()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddSerializer();
        var sp = services.BuildServiceProvider();
        Assert.That(() => new LatticeRemoteSnapshotGrpcMethods(
                sp.GetRequiredService<Serializer<RemoteSnapshotMetadataRequest>>(),
                sp.GetRequiredService<Serializer<RemoteSnapshotMetadata>>(),
                null!),
            Throws.ArgumentNullException);
    }
}