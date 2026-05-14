using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;

namespace Orleans.Lattice.Replication.Grpc.Tests.PublicApiContract;

/// <summary>
/// End-to-end integration suite that pins the public
/// <c>Orleans.Lattice.Replication.Grpc</c> API contract. Every public
/// type the package exposes - <see cref="GrpcPushTransportOptions"/>,
/// <see cref="LatticeReplicationGrpcServiceCollectionExtensions"/> -
/// is exercised via a shared
/// <see cref="GrpcPublicApiContractFixture"/> so any silent change to
/// the wire shape, channel hardening defaults, options binding, or DI
/// registration surfaces as a test failure.
/// <para>
/// Tests are split across partial files by concern. This main file
/// holds the fixture wiring and the smoke test that the receiver host
/// is up and the sender transport composes with it correctly.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
[Category("API")]
public partial class GrpcPublicApiContractTests
{
    private GrpcPublicApiContractFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrpcPublicApiContractFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [TearDown]
    public void TearDown()
    {
        _fixture.ResetApplier();
    }

    [Test]
    public async Task Smoke_sender_transport_can_push_an_empty_batch_to_the_receiver()
    {
        await using var senderServices = _fixture.BuildSenderServices();
        var transport = senderServices.GetRequiredService<IReplicationTransport>();

        var ack = await transport.SendAsync(
            GrpcPublicApiContractFixture.BuildBatch(Array.Empty<WalRecord>()),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ack.Accepted, Is.True);
            Assert.That(ack.HighestAppliedHlc, Is.EqualTo(HybridLogicalClock.Zero));
        });
    }
}
