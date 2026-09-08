using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// Unit coverage for the runtime enforcement path of
/// <see cref="LatticeTreeAdminApiGrpcAuthInterceptor"/>, driven directly with no
/// live server. <c>TreeAdminGrpcInterceptorMappingTests</c> already pins the
/// static operation/target decoding; this fixture covers what actually happens to
/// an inbound call: the enforcement-disabled short circuit, the service-prefix and
/// unauthenticated-method bypasses, the deny rejection, and the
/// authorizer-cancellation mapping.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminApiGrpcAuthInterceptorUnitTests
{
    private static string FullMethod(string methodName) =>
        $"/{LatticeTreeAdminGrpcMethods.ServiceName}/{methodName}";

    private static LatticeTreeAdminApiGrpcAuthInterceptor Create(
        ILatticeTreeAdminApiAuthorizer authorizer,
        bool requireAuthorization = true)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeTreeAdminApiGrpcOptions>>();
        monitor.CurrentValue.Returns(new LatticeTreeAdminApiGrpcOptions { RequireAuthorization = requireAuthorization });
        return new LatticeTreeAdminApiGrpcAuthInterceptor(
            authorizer,
            monitor,
            Substitute.For<ILogger<LatticeTreeAdminApiGrpcAuthInterceptor>>());
    }

    [Test]
    public void Constructor_rejects_a_null_authorizer()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeTreeAdminApiGrpcOptions>>();

        Assert.Throws<ArgumentNullException>(() => new LatticeTreeAdminApiGrpcAuthInterceptor(
            null!,
            monitor,
            Substitute.For<ILogger<LatticeTreeAdminApiGrpcAuthInterceptor>>()));
    }

    [Test]
    public void Constructor_rejects_a_null_options_monitor()
    {
        Assert.Throws<ArgumentNullException>(() => new LatticeTreeAdminApiGrpcAuthInterceptor(
            new AllowAllTreeAdminApiAuthorizer(),
            null!,
            Substitute.For<ILogger<LatticeTreeAdminApiGrpcAuthInterceptor>>()));
    }

    [Test]
    public void Constructor_rejects_a_null_logger()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeTreeAdminApiGrpcOptions>>();

        Assert.Throws<ArgumentNullException>(() => new LatticeTreeAdminApiGrpcAuthInterceptor(
            new AllowAllTreeAdminApiAuthorizer(),
            monitor,
            null!));
    }

    [Test]
    public async Task UnaryServerHandler_when_authorization_disabled_skips_the_authorizer()
    {
        var authorizer = Substitute.For<ILatticeTreeAdminApiAuthorizer>();
        var interceptor = Create(authorizer, requireAuthorization: false);
        var response = new TreeExistenceResult { TreeId = "orders", Exists = true };

        var result = await interceptor.UnaryServerHandler(
            new TreeAdminTreeRequest { TreeId = "orders" },
            new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.CheckTreeExistsMethodName)),
            (_, _) => Task.FromResult(response));

        Assert.That(result, Is.SameAs(response));
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeTreeAdminApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task UnaryServerHandler_non_tree_admin_service_method_bypasses_enforcement()
    {
        var authorizer = Substitute.For<ILatticeTreeAdminApiAuthorizer>();
        var interceptor = Create(authorizer);
        var response = new TreeExistenceResult { TreeId = "orders" };

        var result = await interceptor.UnaryServerHandler(
            new TreeAdminTreeRequest { TreeId = "orders" },
            new FakeServerCallContext("/some.other.Service/DoThing"),
            (_, _) => Task.FromResult(response));

        Assert.That(result, Is.SameAs(response));
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeTreeAdminApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task UnaryServerHandler_exempts_the_unauthenticated_auth_scheme_discovery_rpc()
    {
        // A client must be able to learn how to sign in before it holds any
        // credential, so GetAuthScheme runs even under the default-deny authorizer.
        var interceptor = Create(new DenyTreeAdminApiAuthorizer());
        var response = new AuthSchemeAdvertisement();

        var result = await interceptor.UnaryServerHandler(
            new AuthSchemeAdvertisementRequest(),
            new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.GetAuthSchemeMethodName)),
            (_, _) => Task.FromResult(response));

        Assert.That(result, Is.SameAs(response));
    }

    [Test]
    public void UnaryServerHandler_default_deny_authorizer_rejects_with_PermissionDenied()
    {
        var interceptor = Create(new DenyTreeAdminApiAuthorizer());
        var continuationRan = false;

        var ex = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            new TreeAdminTreeRequest { TreeId = "orders" },
            new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.CheckTreeExistsMethodName)),
            (_, _) =>
            {
                continuationRan = true;
                return Task.FromResult(new TreeExistenceResult { TreeId = "orders" });
            }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(ex.Status.Detail, Does.Contain("ILatticeTreeAdminApiAuthorizer"));
            Assert.That(continuationRan, Is.False, "a denied call must never reach the continuation");
        });
    }

    [Test]
    public async Task UnaryServerHandler_permissive_authorizer_invokes_the_continuation()
    {
        var interceptor = Create(new AllowAllTreeAdminApiAuthorizer());
        var response = new TreeExistenceResult { TreeId = "orders", Exists = true };

        var result = await interceptor.UnaryServerHandler(
            new TreeAdminTreeRequest { TreeId = "orders" },
            new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.CheckTreeExistsMethodName)),
            (_, _) => Task.FromResult(response));

        Assert.That(result, Is.SameAs(response));
    }

    [Test]
    public void UnaryServerHandler_maps_authorizer_cancellation_to_Cancelled()
    {
        var authorizer = Substitute.For<ILatticeTreeAdminApiAuthorizer>();
        authorizer.IsAuthorizedAsync(Arg.Any<LatticeTreeAdminApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new OperationCanceledException());
        var interceptor = Create(authorizer);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            new TreeAdminTreeRequest { TreeId = "orders" },
            new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.CheckTreeExistsMethodName)),
            (_, _) => Task.FromResult(new TreeExistenceResult { TreeId = "orders" })));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
    }

    [Test]
    public async Task UnaryServerHandler_hands_the_authorizer_the_decoded_operation_and_target()
    {
        LatticeTreeAdminApiAuthorizationContext? observed = null;
        var authorizer = Substitute.For<ILatticeTreeAdminApiAuthorizer>();
        authorizer
            .IsAuthorizedAsync(Arg.Any<LatticeTreeAdminApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                observed = call.Arg<LatticeTreeAdminApiAuthorizationContext>();
                return Task.FromResult(true);
            });
        var interceptor = Create(authorizer);
        var context = new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.CreateTreeMethodName));

        await interceptor.UnaryServerHandler(
            new TreeAdminCreateRequest { TreeId = "orders" },
            context,
            (_, _) => Task.FromResult(new TreeCreationResult { TreeId = "orders" }));

        Assert.That(observed, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(observed!.Value.Operation, Is.EqualTo(LatticeTreeAdminApiOperation.CreateTree));
            Assert.That(observed!.Value.TargetId, Is.EqualTo("orders"));
            Assert.That(observed!.Value.Call, Is.SameAs(context));
        });
    }

    [Test]
    public void UnaryServerHandler_rejects_a_null_request()
    {
        var interceptor = Create(new AllowAllTreeAdminApiAuthorizer());

        Assert.ThrowsAsync<ArgumentNullException>(async () => await interceptor.UnaryServerHandler(
            (TreeAdminTreeRequest)null!,
            new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.CheckTreeExistsMethodName)),
            (_, _) => Task.FromResult(new TreeExistenceResult { TreeId = "orders" })));
    }

    [Test]
    public void UnaryServerHandler_rejects_a_null_context()
    {
        var interceptor = Create(new AllowAllTreeAdminApiAuthorizer());

        Assert.ThrowsAsync<ArgumentNullException>(async () => await interceptor.UnaryServerHandler(
            new TreeAdminTreeRequest { TreeId = "orders" },
            null!,
            (_, _) => Task.FromResult(new TreeExistenceResult { TreeId = "orders" })));
    }

    [Test]
    public void UnaryServerHandler_rejects_a_null_continuation()
    {
        var interceptor = Create(new AllowAllTreeAdminApiAuthorizer());

        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await interceptor.UnaryServerHandler<TreeAdminTreeRequest, TreeExistenceResult>(
                new TreeAdminTreeRequest { TreeId = "orders" },
                new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.CheckTreeExistsMethodName)),
                null!));
    }

    // ---- Every tree a call reaches is authorized ------------------------
    //
    // DescribeCall decodes ONE target tree, but two requests carry a second,
    // independently caller-chosen tree the operation actually writes to or
    // redirects at: SnapshotTree's DestinationTreeId (the facade calls
    // SnapshotTreeAsync(TreeId, DestinationTreeId, ...)) and SetTreeAlias's
    // PhysicalTreeId (SetTreeAliasAsync(TreeId, PhysicalTreeId, ...)). While only
    // the primary was authorized, a caller holding a grant on one tree it owns
    // could snapshot over an arbitrary victim tree, or re-point its own logical
    // name at one, without the authorizer ever being shown the tree at risk.

    /// <summary>
    /// A per-tree authorizer that admits exactly one tree id and records every
    /// target it was asked about, so a target that never reached it is provable.
    /// </summary>
    private sealed class SingleTreeAuthorizer(string allowedTreeId) : ILatticeTreeAdminApiAuthorizer
    {
        public List<string?> Seen { get; } = [];

        public Task<bool> IsAuthorizedAsync(
            LatticeTreeAdminApiAuthorizationContext context,
            CancellationToken cancellationToken = default)
        {
            Seen.Add(context.TargetId);
            return Task.FromResult(string.Equals(context.TargetId, allowedTreeId, StringComparison.Ordinal));
        }
    }

    [Test]
    public void SnapshotTree_destination_tree_is_authorized_and_a_foreign_one_is_refused()
    {
        var authorizer = new SingleTreeAuthorizer("mine");
        var interceptor = Create(authorizer);
        var continuationRan = false;

        var ex = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            new TreeAdminSnapshotRequest
            {
                TreeId = "mine",
                DestinationTreeId = "victim",
                Mode = TreeSnapshotMode.Online,
            },
            new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.SnapshotTreeMethodName)),
            (_, _) =>
            {
                continuationRan = true;
                return Task.FromResult(new TreeSnapshotStatus { TreeId = "mine" });
            }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(continuationRan, Is.False, "the snapshot must never reach the facade");
            Assert.That(
                authorizer.Seen,
                Does.Contain("victim"),
                "the destination tree must be put to the authorizer, not carried in on the source tree's grant");
        });
    }

    [Test]
    public void SetTreeAlias_physical_tree_is_authorized_and_a_foreign_one_is_refused()
    {
        var authorizer = new SingleTreeAuthorizer("mine");
        var interceptor = Create(authorizer);
        var continuationRan = false;

        var ex = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            new TreeAdminSetAliasRequest { TreeId = "mine", PhysicalTreeId = "victim" },
            new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.SetTreeAliasMethodName)),
            (_, _) =>
            {
                continuationRan = true;
                return Task.FromResult(new TreeAliasResolution { TreeId = "mine", PhysicalTreeId = "mine" });
            }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(continuationRan, Is.False, "the alias must never reach the facade");
            Assert.That(authorizer.Seen, Does.Contain("victim"));
        });
    }

    [Test]
    public async Task Both_trees_authorized_still_admits_the_call()
    {
        // The converse control: the second check narrows nothing a legitimate
        // caller was entitled to do.
        var interceptor = Create(new AllowAllTreeAdminApiAuthorizer());
        var response = new TreeSnapshotStatus { TreeId = "mine" };

        var result = await interceptor.UnaryServerHandler(
            new TreeAdminSnapshotRequest
            {
                TreeId = "mine",
                DestinationTreeId = "mine-snap",
                Mode = TreeSnapshotMode.Online,
            },
            new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.SnapshotTreeMethodName)),
            (_, _) => Task.FromResult(response));

        Assert.That(result, Is.SameAs(response));
    }

    [Test]
    public async Task A_single_tree_call_still_costs_exactly_one_authorizer_round_trip()
    {
        // A request naming the same tree twice, and every request naming only one,
        // must not pay a second authorizer call.
        var authorizer = new SingleTreeAuthorizer("mine");
        var interceptor = Create(authorizer);

        await interceptor.UnaryServerHandler(
            new TreeAdminSetAliasRequest { TreeId = "mine", PhysicalTreeId = "mine" },
            new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.SetTreeAliasMethodName)),
            (_, _) => Task.FromResult(new TreeAliasResolution { TreeId = "mine", PhysicalTreeId = "mine" }));

        await interceptor.UnaryServerHandler(
            new TreeAdminTreeRequest { TreeId = "mine" },
            new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.CheckTreeExistsMethodName)),
            (_, _) => Task.FromResult(new TreeExistenceResult { TreeId = "mine" }));

        Assert.That(authorizer.Seen, Has.Count.EqualTo(2), "one authorizer call per call, not per tree slot");
    }

    [Test]
    public void DescribeSecondaryTarget_decodes_only_a_distinct_second_tree()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeTreeAdminApiGrpcAuthInterceptor.DescribeSecondaryTarget(
                    new TreeAdminSnapshotRequest { TreeId = "a", DestinationTreeId = "b" }, "a"),
                Is.EqualTo("b"));
            Assert.That(
                LatticeTreeAdminApiGrpcAuthInterceptor.DescribeSecondaryTarget(
                    new TreeAdminSetAliasRequest { TreeId = "a", PhysicalTreeId = "b" }, "a"),
                Is.EqualTo("b"));
            Assert.That(
                LatticeTreeAdminApiGrpcAuthInterceptor.DescribeSecondaryTarget(
                    new TreeAdminSetAliasRequest { TreeId = "a", PhysicalTreeId = "a" }, "a"),
                Is.Null,
                "the same tree twice is already adjudicated");
            Assert.That(
                LatticeTreeAdminApiGrpcAuthInterceptor.DescribeSecondaryTarget(
                    new TreeAdminSnapshotRequest { TreeId = "a", DestinationTreeId = "   " }, "a"),
                Is.Null,
                "a blank id names no tree");
            Assert.That(
                LatticeTreeAdminApiGrpcAuthInterceptor.DescribeSecondaryTarget(
                    new TreeAdminTreeRequest { TreeId = "a" }, "a"),
                Is.Null,
                "a request carrying one tree has no second target");
            Assert.That(
                LatticeTreeAdminApiGrpcAuthInterceptor.DescribeSecondaryTarget<TreeAdminSnapshotRequest>(null!, null),
                Is.Null,
                "the streaming shape passes no request message");
        });
    }
}
