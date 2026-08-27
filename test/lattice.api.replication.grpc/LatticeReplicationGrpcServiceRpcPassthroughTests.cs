using Microsoft.Extensions.Options;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Serialization;
using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests;

/// <summary>
/// Unit test for the RpcException pass-through in
/// <see cref="LatticeReplicationGrpcService"/>'s shared invoke path: an
/// <see cref="RpcException"/> raised by the underlying facade already carries a
/// deliberate gRPC status, so it is rethrown unchanged rather than being
/// remapped to an internal fault.
/// </summary>
public sealed class LatticeReplicationGrpcServiceRpcPassthroughTests
{
    private ServiceProvider _services = null!;
    private LatticeReplicationGrpcMethods _methods = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _methods = LatticeReplicationGrpcMethods.FromServiceProvider(_services);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [Test]
    public void InvokeAsync_rethrows_an_RpcException_from_the_facade_unchanged()
    {
        var thrown = new RpcException(new Status(StatusCode.NotFound, "no such tree"));
        var control = Substitute.For<ILatticeReplicationControl>();
        control.DisableReplicationAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<ReplicationDisableResult>(_ => throw thrown);

        var bridge = Substitute.For<ILatticeReplicationApiCredentialBridge>();
        bridge.Resolve(Arg.Any<ServerCallContext>()).Returns((LatticeCredential?)null);
        var authSchemeSource = Substitute.For<ILatticeReplicationApiAuthSchemeSource>();
        authSchemeSource.GetAdvertisement().Returns(new AuthSchemeAdvertisement());

        var service = new LatticeReplicationGrpcService(
            _methods,
            control,
            bridge,
            authSchemeSource,
            Options.Create(new LatticeReplicationApiGrpcOptions()),
            NullLogger<LatticeReplicationGrpcService>.Instance);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.DisableReplication(
            new ReplicationDisableRequestMessage { TreeId = "orders" },
            new FakeServerCallContext("/orleans.lattice.api.replication/DisableReplication")));

        Assert.Multiple(() =>
        {
            Assert.That(ex, Is.SameAs(thrown));
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.NotFound));
        });
    }
}
