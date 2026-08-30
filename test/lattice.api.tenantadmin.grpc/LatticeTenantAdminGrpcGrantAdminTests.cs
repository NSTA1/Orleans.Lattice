using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Unit tests for the five cross-tenant grant RPCs on
/// <see cref="LatticeTenantAdminGrpcService"/> and
/// <see cref="LatticeTenantAdminApiGrpcClient"/>, driven end to end through the
/// in-memory <see cref="LoopbackCallInvoker"/> (no network, no host): every RPC
/// round-trips through the real Orleans marshallers, the two tenant ids and the
/// scope reach the facade in the right roles, every facade exception the service
/// can catch is translated onto the expected gRPC <see cref="StatusCode"/>, and
/// the client's own argument guards hold. Also proves the surface answers
/// <see cref="StatusCode.Unimplemented"/> - rather than faulting - when the
/// optional facade is not registered.
/// </summary>
[TestFixture]
public sealed class LatticeTenantAdminGrpcGrantAdminTests
{
    private const string Granter = "acme";
    private const string Grantee = "beta";
    private const string Scope = "orders";

    private ServiceProvider _serializers = null!;
    private FakeTenantGrantAdmin _grantAdmin = null!;
    private LatticeTenantAdminApiGrpcClient _client = null!;

    [SetUp]
    public void SetUp()
    {
        _serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _grantAdmin = new FakeTenantGrantAdmin();
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(_serializers);
        var service = BuildService(methods, _grantAdmin);
        _client = new LatticeTenantAdminApiGrpcClient(new LoopbackCallInvoker(service, _serializers), methods);
    }

    [TearDown]
    public void TearDown() => _serializers.Dispose();

    private static LatticeTenantAdminGrpcService BuildService(
        LatticeTenantAdminGrpcMethods methods, ILatticeTenantGrantAdmin? grantAdmin) =>
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
            new FakeTenantQuotaUsage(),
            new FakeTenantAccessAdmin(),
            grantAdmin);

    // ---- round trips -------------------------------------------------------

    [Test]
    public async Task ListCrossTenantGrants_round_trips_both_directions_through_the_wire()
    {
        var report = await _client.ListCrossTenantGrantsAsync(Granter);

        Assert.Multiple(() =>
        {
            Assert.That(report.TenantId, Is.EqualTo(Granter));
            Assert.That(report.Issued, Has.Count.EqualTo(1));
            Assert.That(report.Issued[0].GranteeTenantId, Is.EqualTo("beta"));
            Assert.That(report.Issued[0].State, Is.EqualTo(TenantGrantLifecycleState.Active));
            Assert.That(report.Received, Has.Count.EqualTo(1));
            Assert.That(report.Received[0].GranterTenantId, Is.EqualTo("gamma"));
            Assert.That(report.Received[0].State, Is.EqualTo(TenantGrantLifecycleState.Pending));
            Assert.That(_grantAdmin.LastTenantId, Is.EqualTo(Granter));
        });
    }

    [Test]
    public async Task OfferCrossTenantGrant_round_trips_through_the_wire()
    {
        _grantAdmin.State = TenantGrantLifecycleState.Pending;

        var result = await _client.OfferCrossTenantGrantAsync(
            Granter, Grantee, Scope, TenantGrantAccess.ReadWrite);

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(result.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Pending));
            Assert.That(result.Grant.Operations, Is.EqualTo(TenantGrantAccess.ReadWrite));
            Assert.That(_grantAdmin.LastGranterTenantId, Is.EqualTo(Granter));
            Assert.That(_grantAdmin.LastGranteeTenantId, Is.EqualTo(Grantee));
            Assert.That(_grantAdmin.LastScope, Is.EqualTo(Scope));
            Assert.That(
                _grantAdmin.LastOperations,
                Is.EqualTo(TenantGrantAccess.ReadWrite),
                "the operation set must reach the facade unaltered");
        });
    }

    [Test]
    public async Task ApproveCrossTenantGrant_round_trips_through_the_wire()
    {
        _grantAdmin.State = TenantGrantLifecycleState.Active;

        var result = await _client.ApproveCrossTenantGrantAsync(Granter, Grantee, Scope);

        Assert.Multiple(() =>
        {
            Assert.That(result.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Active));
            Assert.That(_grantAdmin.LastGranterTenantId, Is.EqualTo(Granter));
            Assert.That(_grantAdmin.LastGranteeTenantId, Is.EqualTo(Grantee));
            Assert.That(_grantAdmin.LastScope, Is.EqualTo(Scope));
        });
    }

    [Test]
    public async Task RejectCrossTenantGrant_round_trips_through_the_wire()
    {
        _grantAdmin.State = TenantGrantLifecycleState.Rejected;

        var result = await _client.RejectCrossTenantGrantAsync(Granter, Grantee, Scope);

        Assert.That(result.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Rejected));
    }

    [Test]
    public async Task RevokeCrossTenantGrant_round_trips_through_the_wire()
    {
        _grantAdmin.State = TenantGrantLifecycleState.Revoked;

        var result = await _client.RevokeCrossTenantGrantAsync(Granter, Grantee, Scope);

        Assert.That(result.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Revoked));
    }

    /// <summary>
    /// The two tenant ids must never be swapped on the wire: the role each plays
    /// decides which tenant's admin authority the facade demands, so a transposed
    /// pair would authorize the wrong side.
    /// </summary>
    [Test]
    public async Task The_two_tenant_ids_keep_their_roles_across_every_transition()
    {
        await _client.ApproveCrossTenantGrantAsync(Granter, Grantee, Scope);
        var afterApprove = (_grantAdmin.LastGranterTenantId, _grantAdmin.LastGranteeTenantId);

        await _client.RejectCrossTenantGrantAsync(Granter, Grantee, Scope);
        var afterReject = (_grantAdmin.LastGranterTenantId, _grantAdmin.LastGranteeTenantId);

        await _client.RevokeCrossTenantGrantAsync(Granter, Grantee, Scope);
        var afterRevoke = (_grantAdmin.LastGranterTenantId, _grantAdmin.LastGranteeTenantId);

        Assert.Multiple(() =>
        {
            Assert.That(afterApprove, Is.EqualTo((Granter, Grantee)));
            Assert.That(afterReject, Is.EqualTo((Granter, Grantee)));
            Assert.That(afterRevoke, Is.EqualTo((Granter, Grantee)));
        });
    }

    // ---- exception to status-code translation ------------------------------

    [Test]
    public void An_authorization_denial_maps_to_permission_denied()
    {
        _grantAdmin.Throw = new LatticeAuthorizationDeniedException(
            "beta", LatticeOperation.Admin, "mallory", "denied by test");

        Assert.That(
            async () => await _client.ApproveCrossTenantGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<RpcException>()
                .With.Property(nameof(RpcException.StatusCode)).EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void An_unoffered_grant_maps_to_not_found()
    {
        _grantAdmin.Throw = new TenantGrantNotFoundException(Granter, Grantee, Scope);

        Assert.That(
            async () => await _client.ApproveCrossTenantGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<RpcException>()
                .With.Property(nameof(RpcException.StatusCode)).EqualTo(StatusCode.NotFound));
    }

    [Test]
    public void An_unregistered_tenant_maps_to_not_found()
    {
        _grantAdmin.Throw = new TenantNotFoundException("ghost");

        Assert.That(
            async () => await _client.ListCrossTenantGrantsAsync("ghost"),
            Throws.TypeOf<RpcException>()
                .With.Property(nameof(RpcException.StatusCode)).EqualTo(StatusCode.NotFound));
    }

    /// <summary>
    /// <see cref="TenantGrantTransitionException"/> derives directly from
    /// <see cref="Exception"/>, so without its own arm it would fall to the
    /// catch-all and reach the caller as an opaque <c>Internal</c>.
    /// </summary>
    [Test]
    public void An_illegal_transition_maps_to_failed_precondition()
    {
        _grantAdmin.Throw = new TenantGrantTransitionException(
            Granter,
            Grantee,
            Scope,
            TenantGrantLifecycleState.Revoked,
            TenantGrantLifecycleState.Active);

        Assert.That(
            async () => await _client.ApproveCrossTenantGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<RpcException>()
                .With.Property(nameof(RpcException.StatusCode)).EqualTo(StatusCode.FailedPrecondition));
    }

    [Test]
    public void A_reserved_tenant_refusal_maps_to_failed_precondition()
    {
        _grantAdmin.Throw = new ReservedTenantOperationException(
            TenantId.DefaultId, "offer-cross-tenant-grant");

        Assert.That(
            async () => await _client.OfferCrossTenantGrantAsync(
                Granter, Grantee, Scope, TenantGrantAccess.Read),
            Throws.TypeOf<RpcException>()
                .With.Property(nameof(RpcException.StatusCode)).EqualTo(StatusCode.FailedPrecondition));
    }

    [Test]
    public void A_bad_argument_maps_to_invalid_argument()
    {
        _grantAdmin.Throw = new ArgumentException("bad scope", "scope");

        Assert.That(
            async () => await _client.OfferCrossTenantGrantAsync(
                Granter, Grantee, Scope, TenantGrantAccess.Read),
            Throws.TypeOf<RpcException>()
                .With.Property(nameof(RpcException.StatusCode)).EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public void An_unexpected_fault_maps_to_internal_without_leaking_its_message()
    {
        _grantAdmin.Throw = new InvalidCastException("internal detail that must not leak");

        Assert.That(
            async () => await _client.RevokeCrossTenantGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<RpcException>()
                .With.Property(nameof(RpcException.StatusCode)).EqualTo(StatusCode.Internal)
                .And.Message.Not.Contains("internal detail"));
    }

    // ---- the facade is optional --------------------------------------------

    [Test]
    public void Every_grant_rpc_answers_unimplemented_when_the_facade_is_not_registered()
    {
        using var serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(serializers);
        var client = new LatticeTenantAdminApiGrpcClient(
            new LoopbackCallInvoker(BuildService(methods, grantAdmin: null), serializers), methods);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await client.ListCrossTenantGrantsAsync(Granter),
                Throws.TypeOf<RpcException>()
                    .With.Property(nameof(RpcException.StatusCode)).EqualTo(StatusCode.Unimplemented));
            Assert.That(
                async () => await client.OfferCrossTenantGrantAsync(
                    Granter, Grantee, Scope, TenantGrantAccess.Read),
                Throws.TypeOf<RpcException>()
                    .With.Property(nameof(RpcException.StatusCode)).EqualTo(StatusCode.Unimplemented));
            Assert.That(
                async () => await client.ApproveCrossTenantGrantAsync(Granter, Grantee, Scope),
                Throws.TypeOf<RpcException>()
                    .With.Property(nameof(RpcException.StatusCode)).EqualTo(StatusCode.Unimplemented));
            Assert.That(
                async () => await client.RejectCrossTenantGrantAsync(Granter, Grantee, Scope),
                Throws.TypeOf<RpcException>()
                    .With.Property(nameof(RpcException.StatusCode)).EqualTo(StatusCode.Unimplemented));
            Assert.That(
                async () => await client.RevokeCrossTenantGrantAsync(Granter, Grantee, Scope),
                Throws.TypeOf<RpcException>()
                    .With.Property(nameof(RpcException.StatusCode)).EqualTo(StatusCode.Unimplemented));
        });
    }

    // ---- client-side argument guards ---------------------------------------

    [Test]
    public void The_client_rejects_an_empty_tenant_id_or_scope()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await _client.ListCrossTenantGrantsAsync(string.Empty), Throws.ArgumentException);
            Assert.That(
                async () => await _client.OfferCrossTenantGrantAsync(
                    string.Empty, Grantee, Scope, TenantGrantAccess.Read),
                Throws.ArgumentException);
            Assert.That(
                async () => await _client.OfferCrossTenantGrantAsync(
                    Granter, string.Empty, Scope, TenantGrantAccess.Read),
                Throws.ArgumentException);
            Assert.That(
                async () => await _client.OfferCrossTenantGrantAsync(
                    Granter, Grantee, string.Empty, TenantGrantAccess.Read),
                Throws.ArgumentException);
            Assert.That(
                async () => await _client.ApproveCrossTenantGrantAsync(string.Empty, Grantee, Scope),
                Throws.ArgumentException);
            Assert.That(
                async () => await _client.RejectCrossTenantGrantAsync(Granter, string.Empty, Scope),
                Throws.ArgumentException);
            Assert.That(
                async () => await _client.RevokeCrossTenantGrantAsync(Granter, Grantee, string.Empty),
                Throws.ArgumentException);
        });
    }

    [Test]
    public void The_client_rejects_a_null_tenant_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await _client.ListCrossTenantGrantsAsync(null!), Throws.ArgumentNullException);
            Assert.That(
                async () => await _client.ApproveCrossTenantGrantAsync(null!, Grantee, Scope),
                Throws.ArgumentNullException);
        });
    }
}
