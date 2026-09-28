using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests.PeerStatus;

/// <summary>
/// Round-trip tests across <see cref="LatticeReplicationStatusGrpcClient"/> and
/// <see cref="LatticeReplicationStatusGrpcService"/> over an in-memory loopback:
/// every query and page field survives the wire, the client is itself an
/// <see cref="ILatticeReplicationStatus"/>, and the service maps facade failures
/// onto gRPC status codes.
/// </summary>
[TestFixture]
public sealed class LatticeReplicationStatusGrpcClientRoundTripTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private LatticeReplicationStatusGrpcService CreateService(ILatticeReplicationStatus status) =>
        new(
            LatticeReplicationStatusGrpcMethods.FromServiceProvider(_services),
            status,
            NoCredentialBridge(),
            Options.Create(new LatticeReplicationApiGrpcOptions()),
            NullLogger<LatticeReplicationStatusGrpcService>.Instance);

    private static ILatticeReplicationApiCredentialBridge NoCredentialBridge()
    {
        var bridge = Substitute.For<ILatticeReplicationApiCredentialBridge>();
        bridge.Resolve(Arg.Any<ServerCallContext>()).Returns((LatticeCredential?)null);
        return bridge;
    }

    private (LatticeReplicationStatusGrpcClient Client, ILatticeReplicationStatus Status, StatusLoopbackCallInvoker Invoker) CreateClient()
    {
        var status = Substitute.For<ILatticeReplicationStatus>();
        var invoker = new StatusLoopbackCallInvoker(CreateService(status), _services);
        return (LatticeReplicationStatusGrpcClient.Create(invoker, _services), status, invoker);
    }

    [Test]
    public void The_client_implements_the_status_contract_directly()
    {
        var (client, _, _) = CreateClient();

        Assert.Multiple(() =>
        {
            Assert.That(client, Is.InstanceOf<ILatticeReplicationStatus>());
            Assert.That(typeof(ILatticeReplicationStatus).IsAssignableFrom(typeof(LatticeReplicationStatusGrpcClient)), Is.True);
        });
    }

    [Test]
    public async Task GetPeerStatusAsync_round_trips_the_query_and_every_page_field()
    {
        var (client, status, _) = CreateClient();
        ReplicationPeerStatusQuery? received = null;
        status.GetPeerStatusAsync(Arg.Do<ReplicationPeerStatusQuery>(q => received = q), Arg.Any<CancellationToken>())
            .Returns(new ReplicationPeerStatusPage(
                "west",
                new[]
                {
                    new ReplicationPeerStatusEntry(
                        "a/crm/contacts", "east", ReplicationLinkDirection.Outbound,
                        entriesBehind: 12, bytesBehind: 3400, consecutiveErrors: 1,
                        timeSinceLastContact: TimeSpan.FromMilliseconds(7_250), inFlight: 2,
                        health: ReplicationLinkHealth.Lagging),
                    new ReplicationPeerStatusEntry(
                        "orders", "north", ReplicationLinkDirection.Inbound,
                        0, 0, 0, timeSinceLastContact: null, 0, ReplicationLinkHealth.Unknown),
                },
                continuationToken: "1.token"));

        var page = await client.GetPeerStatusAsync(new ReplicationPeerStatusQuery
        {
            TreeId = "a/crm/contacts",
            PeerRegionId = "east",
            PageSize = 25,
            ContinuationToken = "1.previous",
        });

        Assert.Multiple(() =>
        {
            Assert.That(received, Is.EqualTo(new ReplicationPeerStatusQuery
            {
                TreeId = "a/crm/contacts",
                PeerRegionId = "east",
                PageSize = 25,
                ContinuationToken = "1.previous",
            }));
            Assert.That(page.LocalRegionId, Is.EqualTo("west"));
            Assert.That(page.ContinuationToken, Is.EqualTo("1.token"));
            Assert.That(page.Peers, Has.Count.EqualTo(2));

            var first = page.Peers[0];
            Assert.That(first.TreeId, Is.EqualTo("a/crm/contacts"));
            Assert.That(first.PeerRegionId, Is.EqualTo("east"));
            Assert.That(first.Direction, Is.EqualTo(ReplicationLinkDirection.Outbound));
            Assert.That(first.EntriesBehind, Is.EqualTo(12));
            Assert.That(first.BytesBehind, Is.EqualTo(3400));
            Assert.That(first.ConsecutiveErrors, Is.EqualTo(1));
            Assert.That(first.TimeSinceLastContact, Is.EqualTo(TimeSpan.FromMilliseconds(7_250)));
            Assert.That(first.InFlight, Is.EqualTo(2));
            Assert.That(first.Health, Is.EqualTo(ReplicationLinkHealth.Lagging));

            var second = page.Peers[1];
            Assert.That(second.Direction, Is.EqualTo(ReplicationLinkDirection.Inbound));
            Assert.That(second.TimeSinceLastContact, Is.Null);
            Assert.That(second.Health, Is.EqualTo(ReplicationLinkHealth.Unknown));
        });
    }

    [Test]
    public async Task GetPeerStatusAsync_round_trips_an_empty_final_page()
    {
        var (client, status, _) = CreateClient();
        status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), Arg.Any<CancellationToken>())
            .Returns(ReplicationPeerStatusPage.Empty("west"));

        var page = await client.GetPeerStatusAsync(ReplicationPeerStatusQuery.All);

        Assert.Multiple(() =>
        {
            Assert.That(page.Peers, Is.Empty);
            Assert.That(page.ContinuationToken, Is.Null);
            Assert.That(page.LocalRegionId, Is.EqualTo("west"));
        });
    }

    [Test]
    public async Task GetPeerStatusAsync_flows_the_callers_cancellation_token()
    {
        var (client, status, invoker) = CreateClient();
        status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), Arg.Any<CancellationToken>())
            .Returns(ReplicationPeerStatusPage.Empty("west"));
        using var cts = new CancellationTokenSource();

        await client.GetPeerStatusAsync(ReplicationPeerStatusQuery.All, cts.Token);

        Assert.That(invoker.LastOptions.CancellationToken, Is.EqualTo(cts.Token));
    }

    [Test]
    public void GetPeerStatusAsync_null_query_throws()
    {
        var (client, _, _) = CreateClient();

        Assert.That(async () => await client.GetPeerStatusAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Create_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => LatticeReplicationStatusGrpcClient.Create(null!, _services), Throws.ArgumentNullException);
            Assert.That(
                () => LatticeReplicationStatusGrpcClient.Create(Substitute.For<CallInvoker>(), null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Internal_constructor_rejects_null_arguments()
    {
        var methods = LatticeReplicationStatusGrpcMethods.FromServiceProvider(_services);

        Assert.Multiple(() =>
        {
            Assert.That(() => new LatticeReplicationStatusGrpcClient(null!, methods), Throws.ArgumentNullException);
            Assert.That(
                () => new LatticeReplicationStatusGrpcClient(Substitute.For<CallInvoker>(), null!),
                Throws.ArgumentNullException);
        });
    }

    private static IEnumerable<TestCaseData> FailureMappings()
    {
        yield return new TestCaseData(new LatticeAuthorizationDeniedException("denied"), StatusCode.PermissionDenied)
            .SetName("GetPeerStatus_maps_an_authorization_denial_to_PermissionDenied");
        yield return new TestCaseData(new LatticeTenantAccessDeniedException(), StatusCode.PermissionDenied)
            .SetName("GetPeerStatus_maps_a_tenant_denial_to_PermissionDenied");
        yield return new TestCaseData(new ArgumentException("bad token", "query"), StatusCode.InvalidArgument)
            .SetName("GetPeerStatus_maps_an_argument_failure_to_InvalidArgument");
        yield return new TestCaseData(new ArgumentOutOfRangeException("PageSize"), StatusCode.InvalidArgument)
            .SetName("GetPeerStatus_maps_a_negative_page_size_to_InvalidArgument");
        yield return new TestCaseData(new OperationCanceledException(), StatusCode.Cancelled)
            .SetName("GetPeerStatus_maps_cancellation_to_Cancelled");
        yield return new TestCaseData(new InvalidOperationException("t/acme/a/crm/contacts exploded"), StatusCode.Internal)
            .SetName("GetPeerStatus_maps_an_unexpected_failure_to_a_generic_Internal");
        yield return new TestCaseData(new RpcException(new Status(StatusCode.Unavailable, "down")), StatusCode.Unavailable)
            .SetName("GetPeerStatus_passes_an_RpcException_through");
    }

    [TestCaseSource(nameof(FailureMappings))]
    public void GetPeerStatus_maps_facade_failures_to_status_codes(Exception failure, StatusCode expected)
    {
        var status = Substitute.For<ILatticeReplicationStatus>();
        status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), Arg.Any<CancellationToken>()).ThrowsAsync(failure);
        var service = CreateService(status);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.GetPeerStatus(
            ReplicationPeerStatusQuery.All,
            new FakeServerCallContext("/" + LatticeReplicationStatusGrpcMethods.ServiceName + "/GetPeerStatus")));

        Assert.That(ex!.StatusCode, Is.EqualTo(expected));
        if (expected == StatusCode.Internal)
        {
            Assert.That(ex.Status.Detail, Does.Not.Contain("t/acme"), "an internal failure must not echo its message");
        }
    }

    [Test]
    public void GetPeerStatus_rejects_null_arguments()
    {
        var service = CreateService(Substitute.For<ILatticeReplicationStatus>());

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await service.GetPeerStatus(null!, new FakeServerCallContext("/x/y")),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await service.GetPeerStatus(ReplicationPeerStatusQuery.All, null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task GetPeerStatus_bridges_the_callers_credential_for_the_facade()
    {
        LatticeCredential? seen = null;
        var status = Substitute.For<ILatticeReplicationStatus>();
        status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                seen = LatticeCredentialContext.Current;
                return ReplicationPeerStatusPage.Empty("west");
            });
        var credential = new LatticeCredential("token-123", "Bearer");
        var bridge = Substitute.For<ILatticeReplicationApiCredentialBridge>();
        bridge.Resolve(Arg.Any<ServerCallContext>()).Returns(credential);
        var service = new LatticeReplicationStatusGrpcService(
            LatticeReplicationStatusGrpcMethods.FromServiceProvider(_services),
            status,
            bridge,
            Options.Create(new LatticeReplicationApiGrpcOptions()),
            NullLogger<LatticeReplicationStatusGrpcService>.Instance);

        await service.GetPeerStatus(ReplicationPeerStatusQuery.All, new FakeServerCallContext("/x/GetPeerStatus"));

        Assert.Multiple(() =>
        {
            Assert.That(seen, Is.EqualTo(credential));
            Assert.That(LatticeCredentialContext.Current, Is.Null, "the credential scope must end with the call");
        });
    }

    [Test]
    public void Service_constructor_rejects_null_dependencies()
    {
        var methods = LatticeReplicationStatusGrpcMethods.FromServiceProvider(_services);
        var status = Substitute.For<ILatticeReplicationStatus>();
        var bridge = NoCredentialBridge();
        var options = Options.Create(new LatticeReplicationApiGrpcOptions());
        var logger = NullLogger<LatticeReplicationStatusGrpcService>.Instance;

        Assert.Multiple(() =>
        {
            Assert.That(() => new LatticeReplicationStatusGrpcService(null!, status, bridge, options, logger), Throws.ArgumentNullException);
            Assert.That(() => new LatticeReplicationStatusGrpcService(methods, null!, bridge, options, logger), Throws.ArgumentNullException);
            Assert.That(() => new LatticeReplicationStatusGrpcService(methods, status, null!, options, logger), Throws.ArgumentNullException);
            Assert.That(() => new LatticeReplicationStatusGrpcService(methods, status, bridge, null!, logger), Throws.ArgumentNullException);
            Assert.That(() => new LatticeReplicationStatusGrpcService(methods, status, bridge, options, null!), Throws.ArgumentNullException);
        });
    }
}
