using Microsoft.Extensions.Options;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Serialization;
using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests;

/// <summary>
/// End-to-end (in-memory) round-trip tests across the
/// <see cref="LatticeReplicationApiGrpcClient"/> and
/// <see cref="LatticeReplicationGrpcService"/> over a
/// <see cref="LoopbackCallInvoker"/>: every message is serialized and
/// deserialized on the way through, so these assert that the client's
/// request-building and response-mapping agree with the service's mapping across
/// the wire contract.
/// </summary>
[TestFixture]
public sealed class LatticeReplicationApiGrpcClientRoundTripTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private (LatticeReplicationApiGrpcClient Client, ILatticeReplicationControl Control) CreateClient()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        var bridge = Substitute.For<ILatticeReplicationApiCredentialBridge>();
        bridge.Resolve(Arg.Any<ServerCallContext>()).Returns((LatticeCredential?)null);
        var authSchemeSource = Substitute.For<ILatticeReplicationApiAuthSchemeSource>();
        authSchemeSource.GetAdvertisement().Returns(new AuthSchemeAdvertisement());

        var service = new LatticeReplicationGrpcService(
            LatticeReplicationGrpcMethods.FromServiceProvider(_services),
            control,
            bridge,
            authSchemeSource,
            Options.Create(new LatticeReplicationApiGrpcOptions()),
            NullLogger<LatticeReplicationGrpcService>.Instance);

        var invoker = new LoopbackCallInvoker(service, _services);
        var client = LatticeReplicationApiGrpcClient.Create(invoker, _services);
        return (client, control);
    }

    [Test]
    public async Task EnableReplicationAsync_round_trips_request_and_result()
    {
        var (client, control) = CreateClient();
        control.EnableReplicationAsync("orders", LatticeMergeMode.RwFlag, "cluster-b", Arg.Any<CancellationToken>())
            .Returns(new ReplicationEnableResult("orders", LatticeMergeMode.RwFlag, alreadyEnabled: true, bootstrapRequested: true));

        var result = await client.EnableReplicationAsync("orders", LatticeMergeMode.RwFlag, "cluster-b");

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo("orders"));
            Assert.That(result.Mode, Is.EqualTo(LatticeMergeMode.RwFlag));
            Assert.That(result.AlreadyEnabled, Is.True);
            Assert.That(result.BootstrapRequested, Is.True);
        });
        await control.Received(1).EnableReplicationAsync("orders", LatticeMergeMode.RwFlag, "cluster-b", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DisableReplicationAsync_round_trips_request_and_result()
    {
        var (client, control) = CreateClient();
        control.DisableReplicationAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new ReplicationDisableResult("orders", alreadyDisabled: true));

        var result = await client.DisableReplicationAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo("orders"));
            Assert.That(result.AlreadyDisabled, Is.True);
        });
    }

    [Test]
    public async Task GetReplicationConfigAsync_round_trips_entries_and_nullable_mode()
    {
        var (client, control) = CreateClient();
        control.GetReplicationConfigAsync(Arg.Any<CancellationToken>())
            .Returns(new ReplicationConfigReport(new[]
            {
                new ReplicationTreeConfigEntry("orders", enabled: true, mode: LatticeMergeMode.RwFlag, ambiguous: false),
                new ReplicationTreeConfigEntry("customers", enabled: false, mode: null, ambiguous: true),
            }));

        var report = await client.GetReplicationConfigAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.Trees, Has.Count.EqualTo(2));
            Assert.That(report.Trees[0].TreeId, Is.EqualTo("orders"));
            Assert.That(report.Trees[0].Mode, Is.EqualTo(LatticeMergeMode.RwFlag));
            Assert.That(report.Trees[1].Mode, Is.Null);
            Assert.That(report.Trees[1].Ambiguous, Is.True);
        });
    }

    [Test]
    public async Task GetReplicationConfigAsync_round_trips_the_enrollment_source()
    {
        var (client, control) = CreateClient();
        control.GetReplicationConfigAsync(Arg.Any<CancellationToken>())
            .Returns(new ReplicationConfigReport(new[]
            {
                new ReplicationTreeConfigEntry("orders", enabled: true, LatticeMergeMode.RwFlag, ambiguous: false)
                {
                    Source = ReplicationEnrollmentSource.Static,
                },
                new ReplicationTreeConfigEntry(
                    "customers", enabled: true, LatticeMergeMode.OrSet, ambiguous: false)
                {
                    Source = ReplicationEnrollmentSource.RuntimeAndStatic,
                },
                new ReplicationTreeConfigEntry("audit", enabled: true, LatticeMergeMode.OrMap, ambiguous: false),
            }));

        var report = await client.GetReplicationConfigAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.Trees[0].Source, Is.EqualTo(ReplicationEnrollmentSource.Static));
            Assert.That(report.Trees[1].Source, Is.EqualTo(ReplicationEnrollmentSource.RuntimeAndStatic));
            Assert.That(
                report.Trees[2].Source,
                Is.EqualTo(ReplicationEnrollmentSource.Runtime),
                "an unset source round-trips as the runtime default");
        });
    }

    [Test]
    public async Task EnableReplicationAsync_rejects_empty_tree_id()
    {
        var (client, _) = CreateClient();

        Assert.ThrowsAsync<ArgumentException>(async () =>
            await client.EnableReplicationAsync(string.Empty, LatticeMergeMode.RwFlag));
        await Task.CompletedTask;
    }
}
