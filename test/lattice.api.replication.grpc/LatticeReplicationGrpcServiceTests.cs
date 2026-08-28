using Microsoft.Extensions.Options;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Serialization;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeReplicationGrpcService"/>: it delegates each
/// RPC to a substituted <see cref="ILatticeReplicationControl"/>, maps the
/// facade's result records onto the wire responses (including the nullable
/// merge-mode to <c>HasMode</c> + <c>Mode</c> split), returns the configured
/// advertisement from the unauthenticated <c>GetAuthScheme</c> RPC, and maps
/// engine / authorization / argument failures onto the right gRPC status codes.
/// </summary>
[TestFixture]
public sealed class LatticeReplicationGrpcServiceTests
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

    private LatticeReplicationGrpcService CreateService(
        ILatticeReplicationControl control,
        ILatticeReplicationApiAuthSchemeSource? authSchemeSource = null)
    {
        var bridge = Substitute.For<ILatticeReplicationApiCredentialBridge>();
        bridge.Resolve(Arg.Any<ServerCallContext>()).Returns((LatticeCredential?)null);

        var source = authSchemeSource ?? Substitute.For<ILatticeReplicationApiAuthSchemeSource>();
        if (authSchemeSource is null)
        {
            source.GetAdvertisement().Returns(new AuthSchemeAdvertisement());
        }

        return new LatticeReplicationGrpcService(
            _methods,
            control,
            bridge,
            source,
            Options.Create(new LatticeReplicationApiGrpcOptions()),
            NullLogger<LatticeReplicationGrpcService>.Instance);
    }

    private static FakeServerCallContext Context(string methodName) =>
        new("/orleans.lattice.api.replication/" + methodName);

    [Test]
    public async Task EnableReplication_delegates_and_maps_the_result()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        control.EnableReplicationAsync("orders", LatticeMergeMode.RwFlag, "cluster-b", Arg.Any<CancellationToken>())
            .Returns(new ReplicationEnableResult("orders", LatticeMergeMode.RwFlag, alreadyEnabled: true, bootstrapRequested: true));
        var service = CreateService(control);

        var response = await service.EnableReplication(
            new ReplicationEnableRequestMessage { TreeId = "orders", Mode = LatticeMergeMode.RwFlag, BootstrapSourceClusterId = "cluster-b" },
            Context("EnableReplication"));

        Assert.Multiple(() =>
        {
            Assert.That(response.TreeId, Is.EqualTo("orders"));
            Assert.That(response.Mode, Is.EqualTo(LatticeMergeMode.RwFlag));
            Assert.That(response.AlreadyEnabled, Is.True);
            Assert.That(response.BootstrapRequested, Is.True);
        });
    }

    [Test]
    public async Task EnableReplication_passes_null_bootstrap_when_empty()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        control.EnableReplicationAsync("orders", LatticeMergeMode.LwwRegister, null, Arg.Any<CancellationToken>())
            .Returns(new ReplicationEnableResult("orders", LatticeMergeMode.LwwRegister, alreadyEnabled: false, bootstrapRequested: false));
        var service = CreateService(control);

        await service.EnableReplication(
            new ReplicationEnableRequestMessage { TreeId = "orders", Mode = LatticeMergeMode.LwwRegister, BootstrapSourceClusterId = string.Empty },
            Context("EnableReplication"));

        await control.Received(1).EnableReplicationAsync("orders", LatticeMergeMode.LwwRegister, null, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DisableReplication_delegates_and_maps_the_result()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        control.DisableReplicationAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new ReplicationDisableResult("orders", alreadyDisabled: true));
        var service = CreateService(control);

        var response = await service.DisableReplication(
            new ReplicationDisableRequestMessage { TreeId = "orders" },
            Context("DisableReplication"));

        Assert.Multiple(() =>
        {
            Assert.That(response.TreeId, Is.EqualTo("orders"));
            Assert.That(response.AlreadyDisabled, Is.True);
        });
    }

    [Test]
    public async Task GetReplicationConfig_maps_entries_and_nullable_mode()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        control.GetReplicationConfigAsync(Arg.Any<CancellationToken>())
            .Returns(new ReplicationConfigReport(new[]
            {
                new ReplicationTreeConfigEntry("orders", enabled: true, mode: LatticeMergeMode.RwFlag, ambiguous: false),
                new ReplicationTreeConfigEntry("customers", enabled: false, mode: null, ambiguous: true),
            }));
        var service = CreateService(control);

        var response = await service.GetReplicationConfig(new ReplicationGetConfigRequest(), Context("GetReplicationConfig"));

        Assert.Multiple(() =>
        {
            Assert.That(response.Trees, Has.Count.EqualTo(2));
            Assert.That(response.Trees[0].TreeId, Is.EqualTo("orders"));
            Assert.That(response.Trees[0].Enabled, Is.True);
            Assert.That(response.Trees[0].HasMode, Is.True);
            Assert.That(response.Trees[0].Mode, Is.EqualTo(LatticeMergeMode.RwFlag));
            Assert.That(response.Trees[1].TreeId, Is.EqualTo("customers"));
            Assert.That(response.Trees[1].HasMode, Is.False);
            Assert.That(response.Trees[1].Ambiguous, Is.True);
        });
    }

    [Test]
    public async Task GetAuthScheme_returns_the_configured_advertisement()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        var source = Substitute.For<ILatticeReplicationApiAuthSchemeSource>();
        var advertisement = new AuthSchemeAdvertisement
        {
            Schemes = new[] { new AuthSchemeDescriptor { SchemeId = "entra", DisplayName = "Entra" } },
        };
        source.GetAdvertisement().Returns(advertisement);
        var service = CreateService(control, source);

        var response = await service.GetAuthScheme(new AuthSchemeAdvertisementRequest(), Context("GetAuthScheme"));

        Assert.That(response.Schemes.Single().SchemeId, Is.EqualTo("entra"));
    }

    [Test]
    public void Authorization_denial_maps_to_permission_denied()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        control.EnableReplicationAsync(Arg.Any<string>(), Arg.Any<LatticeMergeMode>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns<ReplicationEnableResult>(_ => throw new LatticeAuthorizationDeniedException("denied"));
        var service = CreateService(control);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.EnableReplication(
            new ReplicationEnableRequestMessage { TreeId = "orders", Mode = LatticeMergeMode.RwFlag },
            Context("EnableReplication")));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void Precondition_failure_maps_to_failed_precondition()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        control.EnableReplicationAsync(Arg.Any<string>(), Arg.Any<LatticeMergeMode>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns<ReplicationEnableResult>(_ => throw new LatticeReplicationPreconditionFailedException("no local replica id"));
        var service = CreateService(control);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.EnableReplication(
            new ReplicationEnableRequestMessage { TreeId = "orders", Mode = LatticeMergeMode.RwFlag },
            Context("EnableReplication")));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
    }

    [Test]
    public void Mode_change_rejection_maps_to_failed_precondition()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        control.EnableReplicationAsync(Arg.Any<string>(), Arg.Any<LatticeMergeMode>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns<ReplicationEnableResult>(_ => throw new LatticeReplicationModeChangeRejectedException("disable then re-enable"));
        var service = CreateService(control);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.EnableReplication(
            new ReplicationEnableRequestMessage { TreeId = "orders", Mode = LatticeMergeMode.RwFlag },
            Context("EnableReplication")));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
    }

    [Test]
    public void Argument_failure_maps_to_invalid_argument()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        control.DisableReplicationAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<ReplicationDisableResult>(_ => throw new ArgumentException("treeId"));
        var service = CreateService(control);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.DisableReplication(
            new ReplicationDisableRequestMessage { TreeId = "orders" },
            Context("DisableReplication")));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public void Cancellation_maps_to_cancelled()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        control.GetReplicationConfigAsync(Arg.Any<CancellationToken>())
            .Returns<ReplicationConfigReport>(_ => throw new OperationCanceledException());
        var service = CreateService(control);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.GetReplicationConfig(
            new ReplicationGetConfigRequest(),
            Context("GetReplicationConfig")));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
    }

    [Test]
    public void Unexpected_failure_maps_to_internal()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        control.DisableReplicationAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<ReplicationDisableResult>(_ => throw new InvalidOperationException("boom"));
        var service = CreateService(control);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.DisableReplication(
            new ReplicationDisableRequestMessage { TreeId = "orders" },
            Context("DisableReplication")));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Internal));
    }
}
