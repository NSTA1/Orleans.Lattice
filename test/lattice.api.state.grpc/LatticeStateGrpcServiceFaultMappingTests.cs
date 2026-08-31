using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeStateGrpcService"/> driven directly against
/// substituted query / observer facades - no live server and no cluster. Pins the
/// three independent fault-to-status mappings the service ships (the shared unary
/// path, the change-subscription stream, and the metrics stream / poll), each of
/// which must keep a typed failure's own status rather than collapsing it into the
/// opaque <see cref="StatusCode.Internal"/> fallback, plus the streaming
/// happy paths and the unauthenticated auth-scheme discovery RPC.
/// </summary>
[TestFixture]
public sealed class LatticeStateGrpcServiceFaultMappingTests
{
    private ServiceProvider _services = null!;
    private LatticeStateGrpcMethods _methods = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _methods = LatticeStateGrpcMethods.FromServiceProvider(_services);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private LatticeStateGrpcService CreateService(
        ILatticeStateQuery? query = null,
        ILatticeStateObserver? observer = null,
        ILatticeStateMetricsObserver? metricsObserver = null,
        ILatticeStateApiCredentialBridge? credentialBridge = null,
        ILatticeStateApiAuthSchemeSource? authSchemeSource = null) =>
        new(
            _methods,
            query ?? Substitute.For<ILatticeStateQuery>(),
            observer ?? Substitute.For<ILatticeStateObserver>(),
            metricsObserver ?? Substitute.For<ILatticeStateMetricsObserver>(),
            credentialBridge ?? Substitute.For<ILatticeStateApiCredentialBridge>(),
            authSchemeSource ?? Substitute.For<ILatticeStateApiAuthSchemeSource>(),
            Options.Create(new LatticeStateApiGrpcOptions()),
            NullLogger<LatticeStateGrpcService>.Instance);

    private static ClusterInfoRequest ClusterRequest => new();

    private static async IAsyncEnumerable<T> Throwing<T>(Exception thrown)
    {
        await Task.Yield();
        throw thrown;
#pragma warning disable CS0162 // unreachable - the compiler needs a yield to make this an iterator
        yield break;
#pragma warning restore CS0162
    }

    private static async IAsyncEnumerable<T> Sequence<T>(params T[] items)
    {
        foreach (var item in items)
        {
            await Task.Yield();
            yield return item;
        }
    }

    // ----- Shared unary fault mapping (InvokeAsync) -----

    /// <summary>Drives one unary RPC whose facade call throws and returns the mapped fault.</summary>
    private RpcException MapUnaryFault(Exception thrown)
    {
        var query = Substitute.For<ILatticeStateQuery>();
        query.GetClusterInfoAsync(Arg.Any<CancellationToken>()).ThrowsAsync(thrown);
        var service = CreateService(query);

        return Assert.ThrowsAsync<RpcException>(async () => await service.GetClusterInfo(
            ClusterRequest, StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetClusterInfoMethodName)))!;
    }

    [Test]
    public void InvokeAsync_rethrows_an_RpcException_unchanged()
    {
        var original = new RpcException(new Status(StatusCode.Unauthenticated, "no credential"));
        var query = Substitute.For<ILatticeStateQuery>();
        query.GetClusterInfoAsync(Arg.Any<CancellationToken>()).ThrowsAsync(original);
        var service = CreateService(query);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.GetClusterInfo(
            ClusterRequest, StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetClusterInfoMethodName)));

        Assert.That(ex, Is.SameAs(original));
    }

    [Test]
    public void InvokeAsync_maps_cancellation_to_Cancelled()
    {
        var ex = MapUnaryFault(new OperationCanceledException());

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.Cancelled));
            Assert.That(ex.Status.Detail, Does.Contain("cancelled"));
        });
    }

    [Test]
    public void InvokeAsync_maps_a_bad_argument_to_InvalidArgument()
    {
        var ex = MapUnaryFault(new ArgumentException("pageSize must be positive"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
            Assert.That(ex.Status.Detail, Does.Contain("pageSize"));
        });
    }

    [Test]
    public void InvokeAsync_maps_storage_back_pressure_to_ResourceExhausted()
    {
        // A WAL-saturated tree sheds the operation. It must surface as the
        // canonical retry-later code, never as an opaque Internal, so a client
        // backs off instead of treating it as a hard failure.
        var ex = MapUnaryFault(new LatticeSaturatedException("orders"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.ResourceExhausted));
            Assert.That(ex.Status.Detail, Does.Contain("busy"));
        });
    }

    [Test]
    public void InvokeAsync_maps_a_tenant_access_denial_to_PermissionDenied()
    {
        var ex = MapUnaryFault(new LatticeTenantAccessDeniedException("tenant-a is not in scope"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(ex.Status.Detail, Does.Contain("tenant-a"));
        });
    }

    [Test]
    public void InvokeAsync_maps_an_unexpected_fault_to_a_non_revealing_Internal()
    {
        var ex = MapUnaryFault(new BadImageFormatException("secret internal detail"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.Internal));
            Assert.That(ex.Status.Detail, Does.Not.Contain("secret internal detail"));
        });
    }

    [Test]
    public void InvokeAsync_rejects_a_null_request()
    {
        var service = CreateService();

        Assert.ThrowsAsync<ArgumentNullException>(async () => await service.GetClusterInfo(
            null!, StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetClusterInfoMethodName)));
    }

    [Test]
    public void InvokeAsync_rejects_a_null_context()
    {
        var service = CreateService();

        Assert.ThrowsAsync<ArgumentNullException>(async () => await service.GetClusterInfo(ClusterRequest, null!));
    }

    // ----- Unary handler shapes -----

    [Test]
    public async Task GetClusterInfo_forwards_to_the_query_facade()
    {
        var info = new ClusterInfo { ClusterId = "c1", ServiceId = "s1" };
        var query = Substitute.For<ILatticeStateQuery>();
        query.GetClusterInfoAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(info));
        var service = CreateService(query);

        var result = await service.GetClusterInfo(
            ClusterRequest, StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetClusterInfoMethodName));

        Assert.That(result, Is.SameAs(info));
    }

    [Test]
    public async Task GetDeadLetterCount_wraps_the_scalar_count_with_its_tree_id()
    {
        var query = Substitute.For<ILatticeStateQuery>();
        query.GetDeadLetterCountAsync("orders", Arg.Any<CancellationToken>()).Returns(Task.FromResult(7));
        var service = CreateService(query);

        var result = await service.GetDeadLetterCount(
            new DeadLetterCountRequest { TreeId = "orders" },
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetDeadLetterCountMethodName));

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo("orders"));
            Assert.That(result.Count, Is.EqualTo(7));
        });
    }

    [Test]
    public async Task ListDeadLetters_forwards_the_request_verbatim()
    {
        var page = new DeadLetterQueuePage { NextPageToken = "next" };
        var request = new DeadLetterQueueRequest { TreeId = "orders" };
        var query = Substitute.For<ILatticeStateQuery>();
        query.ListDeadLettersAsync(request, Arg.Any<CancellationToken>()).Returns(Task.FromResult(page));
        var service = CreateService(query);

        var result = await service.ListDeadLetters(
            request, StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ListDeadLettersMethodName));

        Assert.That(result, Is.SameAs(page));
    }

    // ----- Change subscription stream -----

    private static (LatticeStateGrpcService Service, RecordingServerStreamWriter<StateChangeNotification> Writer)
        ObserveChangesRig(Func<IAsyncEnumerable<StateChangeNotification>> stream, LatticeStateGrpcServiceFaultMappingTests owner)
    {
        var observer = Substitute.For<ILatticeStateObserver>();
        observer.ObserveAsync(Arg.Any<StateObserveRequest>(), Arg.Any<CancellationToken>()).Returns(_ => stream());
        return (owner.CreateService(observer: observer), new RecordingServerStreamWriter<StateChangeNotification>());
    }

    private RpcException MapObserveChangesFault(Exception thrown)
    {
        var (service, writer) = ObserveChangesRig(() => Throwing<StateChangeNotification>(thrown), this);

        return Assert.ThrowsAsync<RpcException>(async () => await service.ObserveChanges(
            new StateObserveRequest { TreeId = "orders" },
            writer,
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveChangesMethodName)))!;
    }

    [Test]
    public async Task ObserveChanges_writes_every_notification_the_observer_yields()
    {
        var first = new StateChangeNotification { TreeId = "orders", Key = "k1", Position = "p1" };
        var second = new StateChangeNotification { TreeId = "orders", Key = "k2", Position = "p2" };
        var (service, writer) = ObserveChangesRig(() => Sequence(first, second), this);

        await service.ObserveChanges(
            new StateObserveRequest { TreeId = "orders" },
            writer,
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveChangesMethodName));

        Assert.That(writer.Written, Is.EqualTo(new[] { first, second }));
    }

    [Test]
    public async Task ObserveChanges_ends_the_stream_cleanly_when_the_client_tears_the_subscription_down()
    {
        // A cancelled subscription is a normal client teardown, not a fault: the
        // RPC must return rather than surface an error to the peer.
        var (service, writer) = ObserveChangesRig(
            () => Throwing<StateChangeNotification>(new OperationCanceledException()), this);

        await service.ObserveChanges(
            new StateObserveRequest { TreeId = "orders" },
            writer,
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveChangesMethodName));

        Assert.That(writer.Written, Is.Empty);
    }

    [Test]
    public void ObserveChanges_rethrows_an_RpcException_unchanged()
    {
        var original = new RpcException(new Status(StatusCode.Unavailable, "silo down"));
        var (service, writer) = ObserveChangesRig(() => Throwing<StateChangeNotification>(original), this);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.ObserveChanges(
            new StateObserveRequest { TreeId = "orders" },
            writer,
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveChangesMethodName)));

        Assert.That(ex, Is.SameAs(original));
    }

    [Test]
    public void ObserveChanges_maps_an_expired_cursor_to_FailedPrecondition()
    {
        var ex = MapObserveChangesFault(new LatticeStateCursorExpiredException());

        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
    }

    [Test]
    public void ObserveChanges_maps_a_missing_tree_to_NotFound()
    {
        var ex = MapObserveChangesFault(new KeyNotFoundException("no such tree"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.NotFound));
            Assert.That(ex.Status.Detail, Does.Contain("no such tree"));
        });
    }

    [Test]
    public void ObserveChanges_maps_a_bad_argument_to_InvalidArgument()
    {
        var ex = MapObserveChangesFault(new ArgumentException("range is inverted"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
            Assert.That(ex.Status.Detail, Does.Contain("range is inverted"));
        });
    }

    [Test]
    public void ObserveChanges_maps_a_tenant_access_denial_to_PermissionDenied()
    {
        var ex = MapObserveChangesFault(new LatticeTenantAccessDeniedException("tenant-a is not in scope"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(ex.Status.Detail, Does.Contain("tenant-a"));
        });
    }

    [Test]
    public void ObserveChanges_maps_an_unexpected_fault_to_a_non_revealing_Internal()
    {
        var ex = MapObserveChangesFault(new BadImageFormatException("secret internal detail"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.Internal));
            Assert.That(ex.Status.Detail, Does.Not.Contain("secret internal detail"));
        });
    }

    [Test]
    public void ObserveChanges_rejects_a_null_request()
    {
        var service = CreateService();

        Assert.ThrowsAsync<ArgumentNullException>(async () => await service.ObserveChanges(
            null!,
            new RecordingServerStreamWriter<StateChangeNotification>(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveChangesMethodName)));
    }

    [Test]
    public void ObserveChanges_rejects_a_null_response_stream()
    {
        var service = CreateService();

        Assert.ThrowsAsync<ArgumentNullException>(async () => await service.ObserveChanges(
            new StateObserveRequest { TreeId = "orders" },
            null!,
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveChangesMethodName)));
    }

    [Test]
    public void ObserveChanges_rejects_a_null_context()
    {
        var service = CreateService();

        Assert.ThrowsAsync<ArgumentNullException>(async () => await service.ObserveChanges(
            new StateObserveRequest { TreeId = "orders" },
            new RecordingServerStreamWriter<StateChangeNotification>(),
            null!));
    }

    // ----- Metrics subscription stream -----

    private (LatticeStateGrpcService Service, RecordingServerStreamWriter<TreeMetricsSnapshot> Writer)
        ObserveMetricsRig(Func<IAsyncEnumerable<TreeMetricsSnapshot>> stream)
    {
        var metricsObserver = Substitute.For<ILatticeStateMetricsObserver>();
        metricsObserver.ObserveAsync(Arg.Any<TreeMetricsRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => stream());
        return (CreateService(metricsObserver: metricsObserver), new RecordingServerStreamWriter<TreeMetricsSnapshot>());
    }

    private RpcException MapObserveMetricsFault(Exception thrown)
    {
        var (service, writer) = ObserveMetricsRig(() => Throwing<TreeMetricsSnapshot>(thrown));

        return Assert.ThrowsAsync<RpcException>(async () => await service.ObserveMetrics(
            new TreeMetricsRequest(),
            writer,
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveMetricsMethodName)))!;
    }

    [Test]
    public async Task ObserveMetrics_writes_every_snapshot_the_observer_yields()
    {
        var snapshot = new TreeMetricsSnapshot { IsInitial = true };
        var (service, writer) = ObserveMetricsRig(() => Sequence(snapshot));

        await service.ObserveMetrics(
            new TreeMetricsRequest(),
            writer,
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveMetricsMethodName));

        Assert.That(writer.Written, Is.EqualTo(new[] { snapshot }));
    }

    [Test]
    public async Task ObserveMetrics_ends_the_stream_cleanly_on_client_teardown()
    {
        var (service, writer) = ObserveMetricsRig(
            () => Throwing<TreeMetricsSnapshot>(new OperationCanceledException()));

        await service.ObserveMetrics(
            new TreeMetricsRequest(),
            writer,
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveMetricsMethodName));

        Assert.That(writer.Written, Is.Empty);
    }

    [Test]
    public void ObserveMetrics_rethrows_an_RpcException_unchanged()
    {
        var original = new RpcException(new Status(StatusCode.Unavailable, "silo down"));
        var (service, writer) = ObserveMetricsRig(() => Throwing<TreeMetricsSnapshot>(original));

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.ObserveMetrics(
            new TreeMetricsRequest(),
            writer,
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveMetricsMethodName)));

        Assert.That(ex, Is.SameAs(original));
    }

    [Test]
    public void ObserveMetrics_maps_a_bad_argument_to_InvalidArgument()
    {
        var ex = MapObserveMetricsFault(new ArgumentException("sample interval must be positive"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
            Assert.That(ex.Status.Detail, Does.Contain("sample interval"));
        });
    }

    [Test]
    public void ObserveMetrics_maps_a_tenant_access_denial_to_PermissionDenied()
    {
        var ex = MapObserveMetricsFault(new LatticeTenantAccessDeniedException("tenant-a is not in scope"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(ex.Status.Detail, Does.Contain("tenant-a"));
        });
    }

    [Test]
    public void ObserveMetrics_maps_an_unexpected_fault_to_a_non_revealing_Internal()
    {
        var ex = MapObserveMetricsFault(new BadImageFormatException("secret internal detail"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.Internal));
            Assert.That(ex.Status.Detail, Does.Not.Contain("secret internal detail"));
        });
    }

    [Test]
    public void ObserveMetrics_rejects_a_null_request()
    {
        var service = CreateService();

        Assert.ThrowsAsync<ArgumentNullException>(async () => await service.ObserveMetrics(
            null!,
            new RecordingServerStreamWriter<TreeMetricsSnapshot>(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveMetricsMethodName)));
    }

    [Test]
    public void ObserveMetrics_rejects_a_null_response_stream()
    {
        var service = CreateService();

        Assert.ThrowsAsync<ArgumentNullException>(async () => await service.ObserveMetrics(
            new TreeMetricsRequest(),
            null!,
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveMetricsMethodName)));
    }

    [Test]
    public void ObserveMetrics_rejects_a_null_context()
    {
        var service = CreateService();

        Assert.ThrowsAsync<ArgumentNullException>(async () => await service.ObserveMetrics(
            new TreeMetricsRequest(),
            new RecordingServerStreamWriter<TreeMetricsSnapshot>(),
            null!));
    }

    // ----- Metrics poll -----

    private RpcException MapMetricsSnapshotFault(Exception thrown)
    {
        var metricsObserver = Substitute.For<ILatticeStateMetricsObserver>();
        metricsObserver.SampleAsync(Arg.Any<TreeMetricsRequest>(), Arg.Any<CancellationToken>()).ThrowsAsync(thrown);
        var service = CreateService(metricsObserver: metricsObserver);

        return Assert.ThrowsAsync<RpcException>(async () => await service.GetMetricsSnapshot(
            new TreeMetricsRequest(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetMetricsSnapshotMethodName)))!;
    }

    [Test]
    public async Task GetMetricsSnapshot_forwards_to_the_metrics_observer()
    {
        var snapshot = new TreeMetricsSnapshot { IsInitial = true };
        var metricsObserver = Substitute.For<ILatticeStateMetricsObserver>();
        metricsObserver.SampleAsync(Arg.Any<TreeMetricsRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(snapshot));
        var service = CreateService(metricsObserver: metricsObserver);

        var result = await service.GetMetricsSnapshot(
            new TreeMetricsRequest(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetMetricsSnapshotMethodName));

        Assert.That(result, Is.SameAs(snapshot));
    }

    [Test]
    public void GetMetricsSnapshot_rethrows_an_RpcException_unchanged()
    {
        var original = new RpcException(new Status(StatusCode.Unavailable, "silo down"));
        var metricsObserver = Substitute.For<ILatticeStateMetricsObserver>();
        metricsObserver.SampleAsync(Arg.Any<TreeMetricsRequest>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(original);
        var service = CreateService(metricsObserver: metricsObserver);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.GetMetricsSnapshot(
            new TreeMetricsRequest(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetMetricsSnapshotMethodName)));

        Assert.That(ex, Is.SameAs(original));
    }

    [Test]
    public void GetMetricsSnapshot_maps_cancellation_to_Cancelled()
    {
        var ex = MapMetricsSnapshotFault(new OperationCanceledException());

        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.Cancelled));
    }

    [Test]
    public void GetMetricsSnapshot_maps_a_bad_argument_to_InvalidArgument()
    {
        var ex = MapMetricsSnapshotFault(new ArgumentException("treeIds must not be empty"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
            Assert.That(ex.Status.Detail, Does.Contain("treeIds"));
        });
    }

    [Test]
    public void GetMetricsSnapshot_maps_a_tenant_access_denial_to_PermissionDenied()
    {
        var ex = MapMetricsSnapshotFault(new LatticeTenantAccessDeniedException("tenant-a is not in scope"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(ex.Status.Detail, Does.Contain("tenant-a"));
        });
    }

    [Test]
    public void GetMetricsSnapshot_maps_an_unexpected_fault_to_a_non_revealing_Internal()
    {
        var ex = MapMetricsSnapshotFault(new BadImageFormatException("secret internal detail"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.Internal));
            Assert.That(ex.Status.Detail, Does.Not.Contain("secret internal detail"));
        });
    }

    [Test]
    public void GetMetricsSnapshot_rejects_a_null_request()
    {
        var service = CreateService();

        Assert.ThrowsAsync<ArgumentNullException>(async () => await service.GetMetricsSnapshot(
            null!, StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetMetricsSnapshotMethodName)));
    }

    [Test]
    public void GetMetricsSnapshot_rejects_a_null_context()
    {
        var service = CreateService();

        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await service.GetMetricsSnapshot(new TreeMetricsRequest(), null!));
    }

    // ----- Auth-scheme discovery -----

    [Test]
    public async Task GetAuthScheme_returns_the_source_advertisement_without_bridging_a_credential()
    {
        var advertisement = new AuthSchemeAdvertisement
        {
            Schemes = [new AuthSchemeDescriptor { SchemeId = "entra" }],
        };
        var source = Substitute.For<ILatticeStateApiAuthSchemeSource>();
        source.GetAdvertisement().Returns(advertisement);
        var bridge = Substitute.For<ILatticeStateApiCredentialBridge>();
        var service = CreateService(credentialBridge: bridge, authSchemeSource: source);

        var result = await service.GetAuthScheme(
            new AuthSchemeAdvertisementRequest(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetAuthSchemeMethodName));

        Assert.That(result, Is.SameAs(advertisement));
        bridge.DidNotReceive().Resolve(Arg.Any<ServerCallContext>());
    }

    [Test]
    public void GetAuthScheme_rejects_a_null_request()
    {
        var service = CreateService();

        Assert.ThrowsAsync<ArgumentNullException>(async () => await service.GetAuthScheme(
            null!, StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetAuthSchemeMethodName)));
    }

    [Test]
    public void GetAuthScheme_rejects_a_null_context()
    {
        var service = CreateService();

        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await service.GetAuthScheme(new AuthSchemeAdvertisementRequest(), null!));
    }

    // ----- Credential bridging -----

    [Test]
    public async Task InvokeAsync_stamps_a_bridged_credential_for_the_duration_of_the_call()
    {
        var bridge = Substitute.For<ILatticeStateApiCredentialBridge>();
        bridge.Resolve(Arg.Any<ServerCallContext>()).Returns(new LatticeCredential("opaque-token", "Bearer"));
        LatticeCredential? observed = null;
        var query = Substitute.For<ILatticeStateQuery>();
        query.GetClusterInfoAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            observed = LatticeCredentialContext.Current;
            return Task.FromResult(new ClusterInfo());
        });
        var service = CreateService(query, credentialBridge: bridge);

        await service.GetClusterInfo(
            ClusterRequest, StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetClusterInfoMethodName));

        Assert.That(observed, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(observed!.Value.Token, Is.EqualTo("opaque-token"));
            Assert.That(LatticeCredentialContext.Current, Is.Null, "the scope must not outlive the call");
        });
    }
}
