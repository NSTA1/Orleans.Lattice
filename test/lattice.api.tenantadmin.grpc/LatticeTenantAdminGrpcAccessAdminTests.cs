using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Unit tests for the tenant access-administration (admin-subject) RPCs on
/// <see cref="LatticeTenantAdminGrpcService"/> and
/// <see cref="LatticeTenantAdminApiGrpcClient"/>, driven end to end through the
/// in-memory <see cref="LoopbackCallInvoker"/> (no network, no host): all three
/// RPCs round-trip through the real Orleans marshallers, every facade exception
/// the service can catch is translated onto the expected gRPC
/// <see cref="StatusCode"/>, and the client's own argument guards hold. Also
/// proves the surface answers <see cref="StatusCode.Unimplemented"/> - rather than
/// faulting - when the optional facade is not registered.
/// </summary>
[TestFixture]
public sealed class LatticeTenantAdminGrpcAccessAdminTests
{
    private ServiceProvider _serializers = null!;
    private FakeTenantAccessAdmin _accessAdmin = null!;
    private LatticeTenantAdminApiGrpcClient _client = null!;

    [SetUp]
    public void SetUp()
    {
        _serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _accessAdmin = new FakeTenantAccessAdmin();
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(_serializers);
        var service = BuildService(methods, _accessAdmin);
        _client = new LatticeTenantAdminApiGrpcClient(new LoopbackCallInvoker(service, _serializers), methods);
    }

    [TearDown]
    public void TearDown() => _serializers.Dispose();

    private static LatticeTenantAdminGrpcService BuildService(
        LatticeTenantAdminGrpcMethods methods, ILatticeTenantAccessAdmin? accessAdmin) =>
        new(
            methods,
            new FakeTenantAdmin(),
            new FakeTenantSelfService(),
            new NullCredentialBridge(),
            new FixedAuthSchemeSource(new AuthSchemeAdvertisement
            {
                Schemes = new[] { new AuthSchemeDescriptor { SchemeId = "basic", DisplayName = "Basic" } },
            }),
            Options.Create(new LatticeTenantAdminApiGrpcOptions()),
            NullLogger<LatticeTenantAdminGrpcService>.Instance,
            new FakeTenantRegionAdmin(),
            accessAdmin);

    // ---- round trips -----------------------------------------------------

    [Test]
    public async Task ListTenantAdminSubjects_round_trips_through_the_wire()
    {
        var report = await _client.ListTenantAdminSubjectsAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(report.TenantId, Is.EqualTo("acme"));
            Assert.That(report.Subjects, Is.EqualTo(new[] { "alice@example.com", "bob@example.com" }));
            Assert.That(_accessAdmin.LastTenantId, Is.EqualTo("acme"));
        });
    }

    [Test]
    public async Task AddTenantAdminSubject_round_trips_through_the_wire()
    {
        var result = await _client.AddTenantAdminSubjectAsync("acme", "carol@example.com");

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.SubjectId, Is.EqualTo("carol@example.com"));
            Assert.That(result.Changed, Is.True);
            Assert.That(result.Subjects, Does.Contain("carol@example.com"));
            Assert.That(_accessAdmin.LastTenantId, Is.EqualTo("acme"));
            Assert.That(_accessAdmin.LastSubjectId, Is.EqualTo("carol@example.com"),
                "The subject id must reach the facade unaltered.");
        });
    }

    [Test]
    public async Task RemoveTenantAdminSubject_round_trips_through_the_wire()
    {
        var result = await _client.RemoveTenantAdminSubjectAsync("acme", "bob@example.com");

        Assert.Multiple(() =>
        {
            Assert.That(result.SubjectId, Is.EqualTo("bob@example.com"));
            Assert.That(result.Changed, Is.True);
            Assert.That(result.Subjects, Is.EqualTo(new[] { "alice@example.com" }));
            Assert.That(_accessAdmin.LastSubjectId, Is.EqualTo("bob@example.com"));
        });
    }

    // ---- client argument guards ------------------------------------------

    [TestCase(null)]
    [TestCase("")]
    public void ListTenantAdminSubjectsAsync_an_empty_tenant_id_throws(string? tenantId) =>
        Assert.That(
            async () => await _client.ListTenantAdminSubjectsAsync(tenantId!),
            Throws.InstanceOf<ArgumentException>());

    [TestCase(null)]
    [TestCase("")]
    public void AddTenantAdminSubjectAsync_an_empty_tenant_id_throws(string? tenantId) =>
        Assert.That(
            async () => await _client.AddTenantAdminSubjectAsync(tenantId!, "carol@example.com"),
            Throws.InstanceOf<ArgumentException>());

    [TestCase(null)]
    [TestCase("")]
    public void AddTenantAdminSubjectAsync_an_empty_subject_id_throws(string? subjectId) =>
        Assert.That(
            async () => await _client.AddTenantAdminSubjectAsync("acme", subjectId!),
            Throws.InstanceOf<ArgumentException>());

    [TestCase(null)]
    [TestCase("")]
    public void RemoveTenantAdminSubjectAsync_an_empty_tenant_id_throws(string? tenantId) =>
        Assert.That(
            async () => await _client.RemoveTenantAdminSubjectAsync(tenantId!, "carol@example.com"),
            Throws.InstanceOf<ArgumentException>());

    [TestCase(null)]
    [TestCase("")]
    public void RemoveTenantAdminSubjectAsync_an_empty_subject_id_throws(string? subjectId) =>
        Assert.That(
            async () => await _client.RemoveTenantAdminSubjectAsync("acme", subjectId!),
            Throws.InstanceOf<ArgumentException>());

    // ---- status mapping ---------------------------------------------------

    [Test]
    public void An_authorization_denial_maps_to_permission_denied()
    {
        _accessAdmin.Throw = new LatticeAuthorizationDeniedException(
            "acme", LatticeOperation.Admin, "mallory", "denied");

        var fault = Assert.ThrowsAsync<RpcException>(
            async () => await _client.ListTenantAdminSubjectsAsync("acme"));

        Assert.That(fault!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void A_missing_tenant_maps_to_not_found()
    {
        _accessAdmin.Throw = new TenantNotFoundException("ghost");

        var fault = Assert.ThrowsAsync<RpcException>(
            async () => await _client.ListTenantAdminSubjectsAsync("ghost"));

        Assert.That(fault!.StatusCode, Is.EqualTo(StatusCode.NotFound));
    }

    [Test]
    public void The_last_admin_subject_guard_maps_to_failed_precondition()
    {
        // Without its own arm this would reach the caller as an opaque Internal,
        // because the exception derives directly from Exception.
        _accessAdmin.Throw = new TenantLastAdminSubjectException("acme", "alice@example.com");

        var fault = Assert.ThrowsAsync<RpcException>(
            async () => await _client.RemoveTenantAdminSubjectAsync("acme", "alice@example.com"));

        Assert.Multiple(() =>
        {
            Assert.That(fault!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
            Assert.That(fault.Status.Detail, Does.Contain("alice@example.com"));
        });
    }

    [Test]
    public void A_reserved_tenant_refusal_maps_to_failed_precondition()
    {
        _accessAdmin.Throw = new ReservedTenantOperationException(TenantId.DefaultId, "add-admin-subject");

        var fault = Assert.ThrowsAsync<RpcException>(
            async () => await _client.AddTenantAdminSubjectAsync(TenantId.DefaultId, "carol@example.com"));

        Assert.That(fault!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
    }

    [Test]
    public void A_bad_argument_maps_to_invalid_argument()
    {
        _accessAdmin.Throw = new ArgumentException("bad subject id", "subjectId");

        var fault = Assert.ThrowsAsync<RpcException>(
            async () => await _client.AddTenantAdminSubjectAsync("acme", "carol@example.com"));

        Assert.That(fault!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public void A_tenant_access_denial_maps_to_permission_denied()
    {
        _accessAdmin.Throw = new LatticeTenantAccessDeniedException("no valid active tenant for the caller");

        var fault = Assert.ThrowsAsync<RpcException>(
            async () => await _client.ListTenantAdminSubjectsAsync("acme"));

        Assert.That(fault!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void An_unexpected_fault_maps_to_internal_without_leaking_the_message()
    {
        _accessAdmin.Throw = new NotSupportedException("internal detail that must not leak");

        var fault = Assert.ThrowsAsync<RpcException>(
            async () => await _client.ListTenantAdminSubjectsAsync("acme"));

        Assert.Multiple(() =>
        {
            Assert.That(fault!.StatusCode, Is.EqualTo(StatusCode.Internal));
            Assert.That(fault.Status.Detail, Does.Not.Contain("internal detail"));
        });
    }

    // ---- optional facade --------------------------------------------------

    [Test]
    public void An_access_admin_rpc_reports_unimplemented_when_the_facade_is_absent()
    {
        // A host that binds tenant administration without the access-administration
        // facade must keep serving every other RPC and answer these three honestly.
        using var serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(serializers);
        var service = BuildService(methods, accessAdmin: null);
        var context = new FakeServerCallContext(
            LatticeTenantAdminGrpcMethods.AddTenantAdminSubjectMethodName);

        var fault = Assert.ThrowsAsync<RpcException>(async () =>
            await service.AddTenantAdminSubject(
                new TenantAdminSubjectRequest { TenantId = "acme", SubjectId = "carol@example.com" },
                context));

        Assert.That(fault!.StatusCode, Is.EqualTo(StatusCode.Unimplemented));
    }

    [Test]
    public async Task The_lifecycle_rpcs_still_serve_when_the_access_facade_is_absent()
    {
        using var serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(serializers);
        var service = BuildService(methods, accessAdmin: null);
        var client = new LatticeTenantAdminApiGrpcClient(new LoopbackCallInvoker(service, serializers), methods);

        var result = await client.SuspendTenantAsync("acme");

        Assert.That(result.TenantId, Is.EqualTo("acme"));
    }

    // ---- server-side argument guards --------------------------------------

    [Test]
    public void ListTenantAdminSubjects_with_a_null_request_throws()
    {
        using var serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(serializers);
        var service = BuildService(methods, _accessAdmin);

        Assert.That(
            async () => await service.ListTenantAdminSubjects(
                null!, new FakeServerCallContext(LatticeTenantAdminGrpcMethods.ListTenantAdminSubjectsMethodName)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddTenantAdminSubject_with_a_null_context_throws()
    {
        using var serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(serializers);
        var service = BuildService(methods, _accessAdmin);

        Assert.That(
            async () => await service.AddTenantAdminSubject(
                new TenantAdminSubjectRequest { TenantId = "acme", SubjectId = "carol@example.com" }, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void RemoveTenantAdminSubject_with_a_null_request_throws()
    {
        using var serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(serializers);
        var service = BuildService(methods, _accessAdmin);

        Assert.That(
            async () => await service.RemoveTenantAdminSubject(
                null!, new FakeServerCallContext(LatticeTenantAdminGrpcMethods.RemoveTenantAdminSubjectMethodName)),
            Throws.ArgumentNullException);
    }
}
