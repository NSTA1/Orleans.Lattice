using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Serialization;
using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests;

/// <summary>
/// In-memory round-trip test for the unauthenticated
/// <see cref="LatticeReplicationApiGrpcClient.GetAuthSchemeAsync"/> discovery RPC:
/// the client relays the request over the wire contract to the service, which
/// returns the endpoint's advertised schemes.
/// </summary>
public sealed class LatticeReplicationApiGrpcClientAuthSchemeTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [Test]
    public async Task GetAuthSchemeAsync_round_trips_the_advertised_schemes()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        var bridge = Substitute.For<ILatticeReplicationApiCredentialBridge>();
        bridge.Resolve(Arg.Any<ServerCallContext>()).Returns((LatticeCredential?)null);
        var authSchemeSource = Substitute.For<ILatticeReplicationApiAuthSchemeSource>();
        authSchemeSource.GetAdvertisement().Returns(new AuthSchemeAdvertisement
        {
            Schemes = new[] { new AuthSchemeDescriptor { SchemeId = "entra", DisplayName = "Entra" } },
        });

        var service = new LatticeReplicationGrpcService(
            LatticeReplicationGrpcMethods.FromServiceProvider(_services),
            control,
            bridge,
            authSchemeSource,
            NullLogger<LatticeReplicationGrpcService>.Instance);

        var invoker = new LoopbackCallInvoker(service, _services);
        var client = LatticeReplicationApiGrpcClient.Create(invoker, _services);

        var advertisement = await client.GetAuthSchemeAsync(new AuthSchemeAdvertisementRequest());

        Assert.That(advertisement.Schemes.Single().SchemeId, Is.EqualTo("entra"));
    }
}
