using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Closes the remaining decision-level gaps on the binding's smaller seams: the
/// internal client constructors' own argument guards (the public
/// <c>Create</c> factory validates first, so these throws are otherwise never
/// reached), the credential bridge's header-collection and scheme-prefix edge
/// cases, and the service's credential-stamping branch for a call that does carry
/// a credential.
/// </summary>
[TestFixture]
public sealed class LatticeTenantAdminApiGrpcBindingEdgeCaseTests
{
    private static HeaderLatticeTenantAdminApiCredentialBridge Bridge(LatticeTenantAdminApiGrpcOptions? options = null) =>
        new(Options.Create(options ?? new LatticeTenantAdminApiGrpcOptions()));

    private static FakeServerCallContext CallWith(params (string Key, string Value)[] headers)
    {
        var metadata = new global::Grpc.Core.Metadata();
        foreach (var (key, value) in headers)
        {
            metadata.Add(key, value);
        }

        return new FakeServerCallContext("/orleans.lattice.api.tenantadmin/CreateTenant", metadata);
    }

    [Test]
    public void Admin_client_constructor_rejects_a_null_invoker()
    {
        using var provider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(provider);

        Assert.That(
            () => new LatticeTenantAdminApiGrpcClient(null!, methods),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Admin_client_constructor_rejects_null_methods()
    {
        Assert.That(
            () => new LatticeTenantAdminApiGrpcClient(new UnusableCallInvoker(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Self_service_client_constructor_rejects_a_null_invoker()
    {
        using var provider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(provider);

        Assert.That(
            () => new LatticeTenantSelfServiceApiGrpcClient(null!, methods),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Self_service_client_constructor_rejects_null_methods()
    {
        Assert.That(
            () => new LatticeTenantSelfServiceApiGrpcClient(new UnusableCallInvoker(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Client_factories_build_a_usable_client_from_a_serializer_provider()
    {
        using var provider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var invoker = new UnusableCallInvoker();

        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantAdminApiGrpcClient.Create(invoker, provider), Is.Not.Null);
            Assert.That(LatticeTenantSelfServiceApiGrpcClient.Create(invoker, provider), Is.Not.Null);
        });
    }

    [Test]
    public void Resolve_returns_null_when_the_call_carries_no_header_collection()
    {
        Assert.That(Bridge().Resolve(new HeaderlessServerCallContext()), Is.Null,
            "a call with no request-header collection must read as anonymous, not throw");
    }

    [Test]
    public void Resolve_keeps_a_value_shorter_than_the_configured_scheme_verbatim()
    {
        // "ab" is shorter than "Bearer", so the prefix test short-circuits on
        // length and the raw value is used as the token.
        var credential = Bridge().Resolve(CallWith(("authorization", "ab")));

        Assert.Multiple(() =>
        {
            Assert.That(credential!.Value.Token, Is.EqualTo("ab"));
            Assert.That(credential!.Value.Scheme, Is.EqualTo("Bearer"),
                "the configured scheme is still stamped even when no prefix was stripped");
        });
    }

    [Test]
    public void Resolve_does_not_strip_a_scheme_that_is_only_a_prefix_of_a_longer_word()
    {
        // "Bearerish" starts with "Bearer" but the next character is not
        // whitespace, so it is not a scheme prefix and must survive intact.
        var credential = Bridge().Resolve(CallWith(("authorization", "Bearerish")));

        Assert.Multiple(() =>
        {
            Assert.That(credential!.Value.Token, Is.EqualTo("Bearerish"),
                "a scheme that is merely a prefix of a longer word must not be stripped");
            Assert.That(credential!.Value.Scheme, Is.EqualTo("Bearer"));
        });
    }

    [Test]
    public void The_service_stamps_a_resolved_credential_for_the_duration_of_the_call()
    {
        using var provider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var facade = new CredentialCapturingTenantAdmin();
        var service = new LatticeTenantAdminGrpcService(
            LatticeTenantAdminGrpcMethods.FromServiceProvider(provider),
            facade,
            new FakeTenantSelfService(),
            Bridge(),
            new FixedAuthSchemeSource(new AuthSchemeAdvertisement()),
            Options.Create(new LatticeTenantAdminApiGrpcOptions()), NullLogger<LatticeTenantAdminGrpcService>.Instance,
            new FakeTenantRegionAdmin());

        Assert.That(
            async () => await service.SuspendTenant(
                new TenantAdminTenantRequest { TenantId = "acme" },
                CallWith(("authorization", "Bearer caller-token"))),
            Throws.Nothing);

        Assert.Multiple(() =>
        {
            Assert.That(facade.SeenToken, Is.EqualTo("caller-token"),
                "the bridged credential must be ambient while the facade runs");
            Assert.That(LatticeCredentialContext.Current, Is.Null,
                "the credential scope must be disposed once the call completes");
        });
    }

    [Test]
    public void The_service_leaves_the_caller_anonymous_when_no_credential_is_present()
    {
        using var provider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var facade = new CredentialCapturingTenantAdmin();
        var service = new LatticeTenantAdminGrpcService(
            LatticeTenantAdminGrpcMethods.FromServiceProvider(provider),
            facade,
            new FakeTenantSelfService(),
            Bridge(),
            new FixedAuthSchemeSource(new AuthSchemeAdvertisement()),
            Options.Create(new LatticeTenantAdminApiGrpcOptions()), NullLogger<LatticeTenantAdminGrpcService>.Instance,
            new FakeTenantRegionAdmin());

        Assert.That(
            async () => await service.SuspendTenant(new TenantAdminTenantRequest { TenantId = "acme" }, CallWith()),
            Throws.Nothing);

        Assert.That(facade.SeenToken, Is.Null);
    }

    /// <summary>A call context whose request-header collection is absent.</summary>
    private sealed class HeaderlessServerCallContext : ServerCallContext
    {
        protected override string MethodCore => "/orleans.lattice.api.tenantadmin/CreateTenant";

        protected override string HostCore => "localhost";

        protected override string PeerCore => "ipv4:127.0.0.1:0";

        protected override DateTime DeadlineCore => DateTime.MaxValue;

        protected override global::Grpc.Core.Metadata RequestHeadersCore => null!;

        protected override CancellationToken CancellationTokenCore => default;

        protected override global::Grpc.Core.Metadata ResponseTrailersCore { get; } = new();

        protected override Status StatusCore { get; set; }

        protected override WriteOptions? WriteOptionsCore { get; set; }

        protected override AuthContext AuthContextCore { get; } =
            new(null, new Dictionary<string, List<AuthProperty>>());

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
            throw new NotSupportedException();

        protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) =>
            Task.CompletedTask;
    }

    /// <summary>An invoker that is only ever used to satisfy a constructor.</summary>
    private sealed class UnusableCallInvoker : CallInvoker
    {
        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options) =>
            throw new NotSupportedException();

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options) =>
            throw new NotSupportedException();

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new NotSupportedException();

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new NotSupportedException();

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new NotSupportedException();
    }

    /// <summary>
    /// Records the ambient credential token observed while the facade call runs,
    /// so the service's credential-stamping scope can be asserted.
    /// </summary>
    private sealed class CredentialCapturingTenantAdmin : ILatticeTenantAdmin
    {
        public string? SeenToken { get; private set; }

        public Task<TenantCreationResult> CreateTenantAsync(
            string tenantId,
            IReadOnlyCollection<string>? adminSubjects = null,
            CancellationToken cancellationToken = default)
        {
            Capture();
            return Task.FromResult(new TenantCreationResult
            {
                TenantId = tenantId,
                Status = TenantLifecycleStatus.Active,
                AdminSubjects = [],
            });
        }

        public Task<TenantStatusChangeResult> SuspendTenantAsync(string tenantId, CancellationToken cancellationToken = default)
        {
            Capture();
            return Task.FromResult(new TenantStatusChangeResult
            {
                TenantId = tenantId,
                PreviousStatus = TenantLifecycleStatus.Active,
                NewStatus = TenantLifecycleStatus.Suspended,
                Changed = true,
            });
        }

        public Task<TenantStatusChangeResult> ResumeTenantAsync(string tenantId, CancellationToken cancellationToken = default)
        {
            Capture();
            return Task.FromResult(new TenantStatusChangeResult
            {
                TenantId = tenantId,
                PreviousStatus = TenantLifecycleStatus.Suspended,
                NewStatus = TenantLifecycleStatus.Active,
                Changed = true,
            });
        }

        public Task<TenantDeletionResult> DeleteTenantAsync(string tenantId, CancellationToken cancellationToken = default)
        {
            Capture();
            return Task.FromResult(new TenantDeletionResult { TenantId = tenantId, CascadedTreeCount = 0 });
        }

        public Task<TenantQuotasUpdateResult> SetTenantQuotasAsync(
            string tenantId, TenantQuotasDescriptor quotas, CancellationToken cancellationToken = default)
        {
            Capture();
            return Task.FromResult(new TenantQuotasUpdateResult { TenantId = tenantId, Quotas = quotas });
        }

        private void Capture() => SeenToken = LatticeCredentialContext.Current?.Token;
    }
}
