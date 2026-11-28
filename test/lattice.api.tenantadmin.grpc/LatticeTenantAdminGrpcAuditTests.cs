using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Auth;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Unit tests for the <c>ListTenantAudit</c> RPC on
/// <see cref="LatticeTenantAdminGrpcService"/> and the
/// <see cref="ILatticeTenantAuditLog"/> half of
/// <see cref="LatticeTenantAdminApiGrpcClient"/>, driven through the in-memory
/// <see cref="LoopbackCallInvoker"/> so the request and the page cross the real
/// Orleans marshallers. The facade is a substitute: the binding must hand the tenant
/// and query through untouched and translate the facade's refusals, never decide
/// authorization itself.
/// </summary>
[TestFixture]
public sealed class LatticeTenantAdminGrpcAuditTests
{
    private ServiceProvider _serializers = null!;
    private LatticeTenantAdminGrpcMethods _methods = null!;
    private ILatticeTenantAuditLog _auditLog = null!;
    private LatticeTenantAdminApiGrpcClient _client = null!;

    [SetUp]
    public void SetUp()
    {
        _serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(_serializers);
        _auditLog = Substitute.For<ILatticeTenantAuditLog>();
        _client = ClientFor(_auditLog);
    }

    [TearDown]
    public void TearDown() => _serializers.Dispose();

    private LatticeTenantAdminGrpcService CreateService(ILatticeTenantAuditLog? auditLog) =>
        new(
            _methods,
            new FakeTenantAdmin(),
            new FakeTenantSelfService(),
            new NullCredentialBridge(),
            new FixedAuthSchemeSource(new AuthSchemeAdvertisement()),
            Options.Create(new LatticeTenantAdminApiGrpcOptions()),
            NullLogger<LatticeTenantAdminGrpcService>.Instance,
            auditLog: auditLog);

    private LatticeTenantAdminApiGrpcClient ClientFor(ILatticeTenantAuditLog? auditLog) =>
        new(new LoopbackCallInvoker(CreateService(auditLog), _serializers), _methods);

    [Test]
    public async Task ListTenantAudit_round_trips_the_tenant_query_and_page()
    {
        string? seenTenant = null;
        AdminAuditQuery? seenQuery = null;
        _auditLog.ListAuditAsync(
                Arg.Do<string>(t => seenTenant = t),
                Arg.Do<AdminAuditQuery>(q => seenQuery = q),
                Arg.Any<CancellationToken>())
            .Returns(new AdminAuditPage
            {
                Records = new[]
                {
                    new LatticeAdminAuditRecord
                    {
                        OperationId = "op-1",
                        ActorSubjectId = "alice",
                        TargetScope = "t/acme",
                        Action = LatticeAdminAuditAction.Read,
                        Outcome = LatticeAdminAuditOutcome.Denied,
                    },
                },
                NextPageToken = "next",
            });

        var page = await _client.ListAuditAsync(
            "acme",
            new AdminAuditQuery { PageSize = 7, PageToken = "p", Actor = "alice", Outcome = LatticeAdminAuditOutcome.Denied });

        Assert.Multiple(() =>
        {
            Assert.That(seenTenant, Is.EqualTo("acme"));
            Assert.That(seenQuery, Is.Not.Null);
            Assert.That(seenQuery!.PageSize, Is.EqualTo(7));
            Assert.That(seenQuery.PageToken, Is.EqualTo("p"));
            Assert.That(seenQuery.Actor, Is.EqualTo("alice"));
            Assert.That(seenQuery.Outcome, Is.EqualTo(LatticeAdminAuditOutcome.Denied));
            Assert.That(page.Records, Has.Count.EqualTo(1));
            Assert.That(page.Records[0].TargetScope, Is.EqualTo("t/acme"));
            Assert.That(page.Records[0].Action, Is.EqualTo(LatticeAdminAuditAction.Read));
            Assert.That(page.NextPageToken, Is.EqualTo("next"));
        });
    }

    [Test]
    public void ListTenantAudit_maps_the_facade_refusal_to_permission_denied()
    {
        _auditLog.ListAuditAsync(Arg.Any<string>(), Arg.Any<AdminAuditQuery>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new LatticeAuthorizationDeniedException("tenant-admin authority required"));

        var ex = Assert.ThrowsAsync<RpcException>(() => _client.ListAuditAsync("acme", new AdminAuditQuery()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public async Task ListTenantAudit_denies_a_cross_tenant_read_and_serves_the_callers_own_tenant()
    {
        _auditLog.ListAuditAsync(Arg.Is<string>(t => t != "acme"), Arg.Any<AdminAuditQuery>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new LatticeTenantAccessDeniedException());
        _auditLog.ListAuditAsync("acme", Arg.Any<AdminAuditQuery>(), Arg.Any<CancellationToken>())
            .Returns(new AdminAuditPage { Records = Array.Empty<LatticeAdminAuditRecord>() });

        var ex = Assert.ThrowsAsync<RpcException>(() => _client.ListAuditAsync("beta", new AdminAuditQuery { Scope = "t/acme" }));
        var own = await _client.ListAuditAsync("acme", new AdminAuditQuery());

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(own.Records, Is.Empty);
        });
    }

    [Test]
    public void ListTenantAudit_maps_a_bad_tenant_id_to_invalid_argument()
    {
        _auditLog.ListAuditAsync(Arg.Any<string>(), Arg.Any<AdminAuditQuery>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new ArgumentException("bad id", "tenantId"));

        var ex = Assert.ThrowsAsync<RpcException>(() => _client.ListAuditAsync("acme", new AdminAuditQuery()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public void ListTenantAudit_without_the_facade_is_unimplemented()
    {
        var client = ClientFor(auditLog: null);

        var ex = Assert.ThrowsAsync<RpcException>(() => client.ListAuditAsync("acme", new AdminAuditQuery()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unimplemented));
    }

    [Test]
    public void ListAuditAsync_rejects_invalid_arguments_at_the_client()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => _client.ListAuditAsync(null!, new AdminAuditQuery()), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => _client.ListAuditAsync(string.Empty, new AdminAuditQuery()), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => _client.ListAuditAsync("acme", null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void ListTenantAudit_rejects_null_arguments_at_the_service()
    {
        var service = CreateService(_auditLog);
        var context = TenantAccessGrpcHarness.Context(LatticeTenantAdminGrpcMethods.ListTenantAuditMethodName);

        Assert.Multiple(() =>
        {
            Assert.That(() => service.ListTenantAudit(null!, context), Throws.ArgumentNullException);
            Assert.That(
                () => service.ListTenantAudit(new TenantAdminAuditListRequest { TenantId = "acme", Query = new AdminAuditQuery() }, null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void DescribeCall_maps_the_audit_method_to_the_requested_tenant()
    {
        var (operation, targetId) = LatticeTenantAdminApiGrpcAuthInterceptor.DescribeCall(
            $"/{LatticeTenantAdminGrpcMethods.ServiceName}/{LatticeTenantAdminGrpcMethods.ListTenantAuditMethodName}",
            new TenantAdminAuditListRequest { TenantId = "acme", Query = new AdminAuditQuery { Scope = "t/other" } });

        Assert.Multiple(() =>
        {
            Assert.That(operation, Is.EqualTo(LatticeTenantAdminApiOperation.ListTenantAudit));
            Assert.That(targetId, Is.EqualTo("acme"), "the target is the routed tenant, never the query's ignored scope");
        });
    }

    [Test]
    public void The_audit_method_is_neither_unauthenticated_nor_self_service()
    {
        var fullName = $"/{LatticeTenantAdminGrpcMethods.ServiceName}/{LatticeTenantAdminGrpcMethods.ListTenantAuditMethodName}";

        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(fullName), Is.False);
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsSelfServiceMethod(fullName), Is.False);
        });
    }
}
