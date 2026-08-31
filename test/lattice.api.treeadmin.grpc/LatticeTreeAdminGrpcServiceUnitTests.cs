using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeTreeAdminGrpcService"/> driven directly
/// against a substituted <see cref="ILatticeTreeAdmin"/> facade - no live server
/// and no cluster. Pins the shared fault-to-status mapping every RPC funnels
/// through (each typed facade failure must keep its own status rather than
/// collapsing into the opaque <see cref="StatusCode.Internal"/> fallback), the
/// credential-bridge stamping, and the handler shapes whose facade call the
/// end-to-end fixture does not reach.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminGrpcServiceUnitTests
{
    private ServiceProvider _services = null!;
    private LatticeTreeAdminGrpcMethods _methods = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _methods = LatticeTreeAdminGrpcMethods.FromServiceProvider(_services);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private LatticeTreeAdminGrpcService CreateService(
        ILatticeTreeAdmin control,
        ILatticeTreeAdminApiCredentialBridge? credentialBridge = null,
        ILatticeTreeAdminApiAuthSchemeSource? authSchemeSource = null,
        LatticeTreeAdminApiGrpcOptions? options = null)
    {
        credentialBridge ??= Substitute.For<ILatticeTreeAdminApiCredentialBridge>();
        authSchemeSource ??= Substitute.For<ILatticeTreeAdminApiAuthSchemeSource>();
        return new LatticeTreeAdminGrpcService(
            _methods,
            control,
            credentialBridge,
            authSchemeSource,
            Options.Create(options ?? new LatticeTreeAdminApiGrpcOptions()),
            NullLogger<LatticeTreeAdminGrpcService>.Instance);
    }

    private static FakeServerCallContext Context(string methodName = "CheckTreeExists") =>
        new($"/{LatticeTreeAdminGrpcMethods.ServiceName}/{methodName}");

    private static TreeAdminTreeRequest TreeRequest => new() { TreeId = "orders" };

    /// <summary>
    /// Drives one RPC whose facade call is arranged to throw <paramref name="thrown"/>
    /// and returns the resulting <see cref="RpcException"/>.
    /// </summary>
    private RpcException MapFault(Exception thrown)
    {
        var control = Substitute.For<ILatticeTreeAdmin>();
        control.CheckTreeExistsAsync("orders", Arg.Any<CancellationToken>()).ThrowsAsync(thrown);
        var service = CreateService(control);

        return Assert.ThrowsAsync<RpcException>(async () =>
            await service.CheckTreeExists(TreeRequest, Context()))!;
    }

    // ----- Fault mapping -----

    [Test]
    public void InvokeAsync_rethrows_an_RpcException_unchanged()
    {
        var original = new RpcException(new Status(StatusCode.ResourceExhausted, "quota"));
        var control = Substitute.For<ILatticeTreeAdmin>();
        control.CheckTreeExistsAsync("orders", Arg.Any<CancellationToken>()).ThrowsAsync(original);
        var service = CreateService(control);

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await service.CheckTreeExists(TreeRequest, Context()));

        Assert.That(ex, Is.SameAs(original));
    }

    [Test]
    public void InvokeAsync_maps_cancellation_to_Cancelled()
    {
        var ex = MapFault(new OperationCanceledException());

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.Cancelled));
            Assert.That(ex.Status.Detail, Does.Contain("cancelled"));
        });
    }

    [Test]
    public void InvokeAsync_maps_an_authorization_denial_to_PermissionDenied()
    {
        var ex = MapFault(new LatticeAuthorizationDeniedException("denied for orders"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(ex.Status.Detail, Does.Contain("denied for orders"));
        });
    }

    [Test]
    public void InvokeAsync_maps_a_missing_key_to_NotFound()
    {
        var ex = MapFault(new KeyNotFoundException("no such tree"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.NotFound));
            Assert.That(ex.Status.Detail, Does.Contain("no such tree"));
        });
    }

    [Test]
    public void InvokeAsync_maps_a_non_empty_bulk_load_target_to_FailedPrecondition()
    {
        var ex = MapFault(new TreeNotEmptyException("orders"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
            Assert.That(ex.Status.Detail, Does.Contain("orders"));
        });
    }

    [Test]
    public void InvokeAsync_maps_an_out_of_order_bulk_load_chunk_to_InvalidArgument()
    {
        var ex = MapFault(new BulkLoadOrderException("orders", 7, "b", "c"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
            Assert.That(ex.Status.Detail, Does.Contain("chunk 7"));
        });
    }

    [Test]
    public void InvokeAsync_maps_a_precondition_failure_to_FailedPrecondition()
    {
        var ex = MapFault(new InvalidOperationException("the view subsystem is not enabled"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
            Assert.That(ex.Status.Detail, Does.Contain("view subsystem"));
        });
    }

    [Test]
    public void InvokeAsync_maps_a_bad_argument_to_InvalidArgument()
    {
        var ex = MapFault(new ArgumentException("treeId must not be empty"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
            Assert.That(ex.Status.Detail, Does.Contain("treeId"));
        });
    }

    [Test]
    public void InvokeAsync_maps_a_tenant_access_denial_to_PermissionDenied()
    {
        // A fail-closed tenant resolution is an authorization outcome, so it must
        // keep its actionable reason rather than collapsing into Internal, which
        // would invite the client to retry a decision that never changes.
        var ex = MapFault(new LatticeTenantAccessDeniedException("tenant-a is not in scope"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(ex.Status.Detail, Does.Contain("tenant-a"));
        });
    }

    [Test]
    public void InvokeAsync_maps_an_unexpected_fault_to_a_non_revealing_Internal()
    {
        var ex = MapFault(new BadImageFormatException("secret internal detail"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.Internal));
            Assert.That(ex.Status.Detail, Does.Not.Contain("secret internal detail"));
            Assert.That(ex.Status.Detail, Does.Contain("tree-administration control-API request failed"));
        });
    }

    // ----- Argument guards -----

    [Test]
    public void InvokeAsync_rejects_a_null_request()
    {
        var service = CreateService(Substitute.For<ILatticeTreeAdmin>());

        Assert.ThrowsAsync<ArgumentNullException>(async () => await service.CheckTreeExists(null!, Context()));
    }

    [Test]
    public void InvokeAsync_rejects_a_null_context()
    {
        var service = CreateService(Substitute.For<ILatticeTreeAdmin>());

        Assert.ThrowsAsync<ArgumentNullException>(async () => await service.CheckTreeExists(TreeRequest, null!));
    }

    // ----- Credential bridging -----

    [Test]
    public async Task InvokeAsync_stamps_a_bridged_credential_for_the_duration_of_the_call()
    {
        var bridge = Substitute.For<ILatticeTreeAdminApiCredentialBridge>();
        bridge.Resolve(Arg.Any<ServerCallContext>()).Returns(new LatticeCredential("opaque-token", "Bearer"));
        LatticeCredential? observed = null;
        var control = Substitute.For<ILatticeTreeAdmin>();
        control.CheckTreeExistsAsync("orders", Arg.Any<CancellationToken>()).Returns(_ =>
        {
            observed = LatticeCredentialContext.Current;
            return Task.FromResult(new TreeExistenceResult { TreeId = "orders", Exists = true });
        });
        var service = CreateService(control, bridge);

        await service.CheckTreeExists(TreeRequest, Context());

        Assert.That(observed, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(observed!.Value.Token, Is.EqualTo("opaque-token"));
            Assert.That(LatticeCredentialContext.Current, Is.Null, "the scope must not outlive the call");
        });
    }

    [Test]
    public async Task InvokeAsync_leaves_the_caller_anonymous_when_the_bridge_resolves_nothing()
    {
        var bridge = Substitute.For<ILatticeTreeAdminApiCredentialBridge>();
        bridge.Resolve(Arg.Any<ServerCallContext>()).Returns((LatticeCredential?)null);
        LatticeCredential? observed = new LatticeCredential("sentinel", null);
        var control = Substitute.For<ILatticeTreeAdmin>();
        control.CheckTreeExistsAsync("orders", Arg.Any<CancellationToken>()).Returns(_ =>
        {
            observed = LatticeCredentialContext.Current;
            return Task.FromResult(new TreeExistenceResult { TreeId = "orders" });
        });
        var service = CreateService(control, bridge);

        await service.CheckTreeExists(TreeRequest, Context());

        Assert.That(observed, Is.Null);
    }

    // ----- Auth-scheme discovery -----

    [Test]
    public async Task GetAuthScheme_returns_the_source_advertisement_without_bridging_a_credential()
    {
        var advertisement = new AuthSchemeAdvertisement
        {
            Schemes = [new AuthSchemeDescriptor { SchemeId = "entra" }],
        };
        var source = Substitute.For<ILatticeTreeAdminApiAuthSchemeSource>();
        source.GetAdvertisement().Returns(advertisement);
        var bridge = Substitute.For<ILatticeTreeAdminApiCredentialBridge>();
        var service = CreateService(Substitute.For<ILatticeTreeAdmin>(), bridge, source);

        var result = await service.GetAuthScheme(new AuthSchemeAdvertisementRequest(), Context("GetAuthScheme"));

        Assert.That(result, Is.SameAs(advertisement));
        bridge.DidNotReceive().Resolve(Arg.Any<ServerCallContext>());
    }

    [Test]
    public void GetAuthScheme_rejects_a_null_request()
    {
        var service = CreateService(Substitute.For<ILatticeTreeAdmin>());

        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await service.GetAuthScheme(null!, Context("GetAuthScheme")));
    }

    [Test]
    public void GetAuthScheme_rejects_a_null_context()
    {
        var service = CreateService(Substitute.For<ILatticeTreeAdmin>());

        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await service.GetAuthScheme(new AuthSchemeAdvertisementRequest(), null!));
    }

    // ----- Handler shapes -----

    [Test]
    public async Task RestoreTreeSet_wraps_the_facade_member_results_in_the_set_response()
    {
        var member = new TreeRestoreResult
        {
            BackupId = "bk-1",
            TargetTreeId = "orders",
            Mode = TreeRestoreMode.InPlace,
            OperationId = "op-1",
            ManifestChain = ["m-1"],
            EntriesApplied = 2,
        };
        var control = Substitute.For<ILatticeTreeAdmin>();
        control.RestoreTreeSetAsync("nightly-set", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<TreeRestoreResult>>([member]));
        var service = CreateService(control);

        var result = await service.RestoreTreeSet(
            new TreeAdminRestoreSetRequest { SetId = "nightly-set" }, Context("RestoreTreeSet"));

        Assert.That(result.Results, Is.EqualTo(new[] { member }));
    }

    [Test]
    public async Task RevertTreeRestore_echoes_the_request_back_as_the_completion_ack()
    {
        // The facade revert is void, so the unary RPC has to synthesise a typed
        // response; echoing the request keeps the call shape uniform.
        var request = new TreeRestoreResult
        {
            BackupId = "bk-1",
            TargetTreeId = "orders",
            Mode = TreeRestoreMode.ShadowCutover,
            OperationId = "op-1",
            ManifestChain = [],
            EntriesApplied = 0,
        };
        var control = Substitute.For<ILatticeTreeAdmin>();
        var service = CreateService(control);

        var result = await service.RevertTreeRestore(request, Context("RevertTreeRestore"));

        Assert.That(result, Is.SameAs(request));
        await control.Received(1).RevertTreeRestoreAsync(request, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DropView_echoes_the_request_back_as_the_completion_ack()
    {
        var control = Substitute.For<ILatticeTreeAdmin>();
        var service = CreateService(control);
        var request = new TreeAdminViewRequest { ViewName = "by-region" };

        var result = await service.DropView(request, Context("DropView"));

        Assert.That(result, Is.SameAs(request));
        await control.Received(1).DropViewAsync("by-region", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PlanWalMove_forwards_the_tree_partition_and_target_provider()
    {
        var plan = new TreeWalMovePlan { TreeId = "orders", Partition = 3 };
        var control = Substitute.For<ILatticeTreeAdmin>();
        control.PlanWalMoveAsync("orders", 3, "azure-table", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(plan));
        var service = CreateService(control);

        var result = await service.PlanWalMove(
            new TreeAdminWalMovePlanRequest { TreeId = "orders", Partition = 3, TargetProviderKey = "azure-table" },
            Context("PlanWalMove"));

        Assert.That(result, Is.SameAs(plan));
    }

    [Test]
    public async Task RebuildView_forwards_the_view_name()
    {
        var status = new TreeViewStatus { ViewName = "by-region", SourceTreeId = "orders" };
        var control = Substitute.For<ILatticeTreeAdmin>();
        control.RebuildViewAsync("by-region", Arg.Any<CancellationToken>()).Returns(Task.FromResult(status));
        var service = CreateService(control);

        var result = await service.RebuildView(
            new TreeAdminViewRequest { ViewName = "by-region" }, Context("RebuildView"));

        Assert.That(result, Is.SameAs(status));
    }

    [Test]
    public async Task ReconcileView_forwards_the_view_name()
    {
        var reconcile = new TreeViewReconcileResult
        {
            ViewName = "by-region",
            SourceTreeId = "orders",
            DriftRepaired = true,
        };
        var control = Substitute.For<ILatticeTreeAdmin>();
        control.ReconcileViewAsync("by-region", Arg.Any<CancellationToken>()).Returns(Task.FromResult(reconcile));
        var service = CreateService(control);

        var result = await service.ReconcileView(
            new TreeAdminViewRequest { ViewName = "by-region" }, Context("ReconcileView"));

        Assert.That(result, Is.SameAs(reconcile));
    }

    [Test]
    public async Task GetViewStatus_forwards_the_view_name()
    {
        var status = new TreeViewStatus { ViewName = "by-region", SourceTreeId = "orders" };
        var control = Substitute.For<ILatticeTreeAdmin>();
        control.GetViewStatusAsync("by-region", Arg.Any<CancellationToken>()).Returns(Task.FromResult(status));
        var service = CreateService(control);

        var result = await service.GetViewStatus(
            new TreeAdminViewRequest { ViewName = "by-region" }, Context("GetViewStatus"));

        Assert.That(result, Is.SameAs(status));
    }

    [Test]
    public async Task ListTagIndexes_forwards_to_the_facade_catalog()
    {
        var catalog = new TreeTagIndexCatalog();
        var control = Substitute.For<ILatticeTreeAdmin>();
        control.ListTagIndexesAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(catalog));
        var service = CreateService(control);

        var result = await service.ListTagIndexes(new TreeAdminTagIndexListRequest(), Context("ListTagIndexes"));

        Assert.That(result, Is.SameAs(catalog));
    }
}
