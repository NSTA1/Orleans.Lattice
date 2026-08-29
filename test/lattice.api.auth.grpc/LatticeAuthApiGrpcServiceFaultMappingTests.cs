using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Auth.Grpc.Tests;

/// <summary>
/// Unit coverage for the fault-mapping tail of
/// <c>LatticeAuthApiGrpcService.InvokeAsync</c> - the shared adapter every unary
/// RPC funnels through. Each catch arm is proved in isolation against a fake
/// <see cref="ILatticeAuthAdmin"/> that throws, without standing up a silo or a
/// gRPC server.
///
/// The load-bearing property is that only the two expected control-plane
/// outcomes carry detail to the caller: an <see cref="ArgumentException"/>
/// surfaces its own message as <see cref="StatusCode.InvalidArgument"/>, and an
/// already-shaped <see cref="RpcException"/> passes through untouched. Every
/// other fault collapses to an opaque <see cref="StatusCode.Internal"/> whose
/// message never echoes the underlying exception, so an unexpected server-side
/// failure cannot leak internal detail over the wire.
/// </summary>
[TestFixture]
public sealed class LatticeAuthApiGrpcServiceFaultMappingTests
{
    private ServiceProvider _services = null!;
    private LatticeAuthApiGrpcMethods _methods = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _methods = LatticeAuthApiGrpcMethods.FromServiceProvider(_services);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    /// <summary>
    /// Builds the service over an admin facade whose <c>GetGroupAsync</c> throws
    /// <paramref name="fault"/>. <c>GetGroup</c> is an arbitrary representative
    /// of the surface: every RPC shares the same <c>InvokeAsync</c> body, so one
    /// method exercises the mapping for all of them.
    /// </summary>
    private LatticeAuthApiGrpcService ServiceThrowing(Exception fault)
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.GetGroupAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<Task<AuthGroup?>>(_ => throw fault);

        var bridge = Substitute.For<ILatticeAuthApiCredentialBridge>();
        bridge.Resolve(Arg.Any<ServerCallContext>()).Returns((LatticeCredential?)null);

        return new LatticeAuthApiGrpcService(
            _methods,
            admin,
            bridge,
            NullLogger<LatticeAuthApiGrpcService>.Instance);
    }

    private static LoopbackServerCallContext Context() =>
        new($"/{LatticeAuthApiGrpcMethods.ServiceName}/{LatticeAuthApiGrpcMethods.GetGroupMethodName}");

    [Test]
    public void InvokeAsync_rethrows_an_RpcException_unchanged()
    {
        var original = new RpcException(new Status(StatusCode.ResourceExhausted, "quota"));
        var service = ServiceThrowing(original);

        var thrown = Assert.ThrowsAsync<RpcException>(
            () => service.GetGroup(new AuthGroupRef { GroupId = "g" }, Context()));

        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.SameAs(original),
                "An already-shaped RpcException must pass through the adapter untouched.");
            Assert.That(thrown!.StatusCode, Is.EqualTo(StatusCode.ResourceExhausted));
        });
    }

    [Test]
    public void InvokeAsync_maps_cancellation_to_Cancelled()
    {
        var service = ServiceThrowing(new OperationCanceledException());

        var thrown = Assert.ThrowsAsync<RpcException>(
            () => service.GetGroup(new AuthGroupRef { GroupId = "g" }, Context()));

        Assert.Multiple(() =>
        {
            Assert.That(thrown!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
            Assert.That(thrown.Status.Detail, Does.Contain("cancelled"));
        });
    }

    [Test]
    public void InvokeAsync_maps_ArgumentException_to_InvalidArgument_with_its_message()
    {
        var service = ServiceThrowing(new ArgumentException("groupId must not be blank"));

        var thrown = Assert.ThrowsAsync<RpcException>(
            () => service.GetGroup(new AuthGroupRef { GroupId = "g" }, Context()));

        Assert.Multiple(() =>
        {
            Assert.That(thrown!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
            Assert.That(thrown.Status.Detail, Does.Contain("groupId must not be blank"),
                "A caller-fault message is safe to surface and is what makes the error actionable.");
        });
    }

    [Test]
    public void InvokeAsync_maps_ArgumentNullException_to_InvalidArgument()
    {
        var service = ServiceThrowing(new ArgumentNullException("groupId"));

        var thrown = Assert.ThrowsAsync<RpcException>(
            () => service.GetGroup(new AuthGroupRef { GroupId = "g" }, Context()));

        Assert.That(thrown!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument),
            "ArgumentNullException derives from ArgumentException and must take the same arm.");
    }

    [Test]
    public void InvokeAsync_maps_an_unexpected_fault_to_an_opaque_Internal()
    {
        var service = ServiceThrowing(new InvalidOperationException("connection string is s3cret"));

        var thrown = Assert.ThrowsAsync<RpcException>(
            () => service.GetGroup(new AuthGroupRef { GroupId = "g" }, Context()));

        Assert.Multiple(() =>
        {
            Assert.That(thrown!.StatusCode, Is.EqualTo(StatusCode.Internal));
            Assert.That(thrown.Status.Detail, Is.EqualTo("The auth-API request failed."));
            Assert.That(thrown.Status.Detail, Does.Not.Contain("s3cret"),
                "An unexpected server-side fault must never echo its message to the caller.");
        });
    }

    [Test]
    public void InvokeAsync_throws_on_a_null_request()
    {
        var service = ServiceThrowing(new InvalidOperationException("unreached"));

        Assert.ThrowsAsync<ArgumentNullException>(
            () => service.GetGroup(null!, Context()));
    }

    [Test]
    public void InvokeAsync_throws_on_a_null_context()
    {
        var service = ServiceThrowing(new InvalidOperationException("unreached"));

        Assert.ThrowsAsync<ArgumentNullException>(
            () => service.GetGroup(new AuthGroupRef { GroupId = "g" }, null!));
    }
}
