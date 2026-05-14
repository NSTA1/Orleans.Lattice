using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;

namespace Orleans.Lattice.Replication.Grpc.Tests.PublicApiContract;

/// <summary>
/// Failure-mode contract tests for the public gRPC transport API: the
/// HTTPS gate refuses plaintext peers by default, an unconfigured peer
/// throws, and cancellation flows through to the wire call.
/// </summary>
public partial class GrpcPublicApiContractTests
{
    [Test]
    public void SendAsync_refuses_plaintext_endpoint_by_default()
    {
        // Override the fixture's plaintext opt-in so the package's
        // hardened default is exercised. The receiver is still
        // http:// (TestServer is plaintext), so the gate must fail
        // closed.
        using var sender = _fixture.BuildSenderServices(opts =>
        {
            opts.AllowPlaintextEndpoints = false;
        });
        var transport = sender.GetRequiredService<IReplicationTransport>();

        Assert.That(
            async () => await transport.SendAsync(
                GrpcPublicApiContractFixture.BuildBatch(Array.Empty<WalRecord>()),
                CancellationToken.None),
            Throws.InvalidOperationException
                .With.Message.Contains("https").IgnoreCase);
    }

    [Test]
    public void SendAsync_throws_when_target_cluster_has_no_endpoint()
    {
        using var sender = _fixture.BuildSenderServices();
        var transport = sender.GetRequiredService<IReplicationTransport>();

        var batch = new ReplicationBatch
        {
            TargetClusterId = "site-unknown",
            TreeName = "contract-tree",
            OriginClusterId = GrpcPublicApiContractFixture.SenderClusterId,
            Payload = ReadOnlyMemory<byte>.Empty,
        };

        Assert.That(
            async () => await transport.SendAsync(batch, CancellationToken.None),
            Throws.InvalidOperationException
                .With.Message.Contains("site-unknown"));
    }

    [Test]
    public void SendAsync_propagates_cancellation_to_the_caller()
    {
        using var sender = _fixture.BuildSenderServices();
        var transport = sender.GetRequiredService<IReplicationTransport>();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // gRPC surfaces a cancelled call as RpcException(StatusCode.Cancelled)
        // with the originating OperationCanceledException attached as the
        // debug-exception. Either shape is an acceptable cancellation
        // signal; the contract here is that the call does not silently
        // succeed.
        Assert.That(
            async () => await transport.SendAsync(
                GrpcPublicApiContractFixture.BuildBatch(Array.Empty<WalRecord>()),
                cts.Token),
            Throws.InstanceOf<OperationCanceledException>()
                .Or.InstanceOf<global::Grpc.Core.RpcException>()
                    .With.Property("StatusCode").EqualTo(global::Grpc.Core.StatusCode.Cancelled));
    }

    [Test]
    public void SendAsync_throws_object_disposed_after_transport_dispose()
    {
        var sender = _fixture.BuildSenderServices();
        try
        {
            var transport = sender.GetRequiredService<IReplicationTransport>();
            ((IDisposable)transport).Dispose();

            Assert.That(
                async () => await transport.SendAsync(
                    GrpcPublicApiContractFixture.BuildBatch(Array.Empty<WalRecord>()),
                    CancellationToken.None),
                Throws.TypeOf<ObjectDisposedException>());
        }
        finally
        {
            sender.Dispose();
        }
    }
}
