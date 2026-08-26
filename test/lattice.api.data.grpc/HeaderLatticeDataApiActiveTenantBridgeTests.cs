using Grpc.Core;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// Unit coverage for the header-based active-tenant bridge that lifts an inbound
/// gRPC request header into a <see cref="TenantId"/> for the write-capable data
/// path. Proves the happy-path parse, casing tolerance on the header name, the
/// custom-header knob, the disable-via-empty-header-name knob, and the
/// fail-closed null results for an absent, blank, or syntactically invalid tenant
/// header - the cases the tenancy resolver then governs by its own membership
/// rules.
/// </summary>
[TestFixture]
public sealed class HeaderLatticeDataApiActiveTenantBridgeTests
{
    private static HeaderLatticeDataApiActiveTenantBridge CreateBridge(
        LatticeDataApiGrpcOptions? options = null) =>
        new(Options.Create(options ?? new LatticeDataApiGrpcOptions()));

    private static ServerCallContext ContextWith(params (string Key, string Value)[] headers)
    {
        var metadata = new global::Grpc.Core.Metadata();
        foreach (var (key, value) in headers)
        {
            metadata.Add(key, value);
        }

        return new FakeServerCallContext(metadata);
    }

    [Test]
    public void Resolve_parses_a_valid_tenant_id()
    {
        var bridge = CreateBridge();

        var tenant = bridge.Resolve(ContextWith(("lattice-active-tenant", "acme")));

        Assert.That(tenant, Is.EqualTo(TenantId.Parse("acme")));
    }

    [Test]
    public void Resolve_trims_surrounding_whitespace()
    {
        var bridge = CreateBridge();

        var tenant = bridge.Resolve(ContextWith(("lattice-active-tenant", "  acme  ")));

        Assert.That(tenant, Is.EqualTo(TenantId.Parse("acme")));
    }

    [Test]
    public void Resolve_returns_null_when_header_absent()
    {
        var bridge = CreateBridge();

        var tenant = bridge.Resolve(ContextWith(("x-other", "ignored")));

        Assert.That(tenant, Is.Null);
    }

    [Test]
    public void Resolve_returns_null_when_header_blank()
    {
        var bridge = CreateBridge();

        var tenant = bridge.Resolve(ContextWith(("lattice-active-tenant", "   ")));

        Assert.That(tenant, Is.Null);
    }

    [Test]
    public void Resolve_returns_null_on_a_syntactically_invalid_tenant()
    {
        var bridge = CreateBridge();

        // Upper-case and a leading hyphen both violate the tenant-id grammar; a
        // non-assertion must fail closed rather than resolve a bogus tenant.
        Assert.Multiple(() =>
        {
            Assert.That(bridge.Resolve(ContextWith(("lattice-active-tenant", "ACME"))), Is.Null);
            Assert.That(bridge.Resolve(ContextWith(("lattice-active-tenant", "-acme"))), Is.Null);
            Assert.That(bridge.Resolve(ContextWith(("lattice-active-tenant", "a/b"))), Is.Null);
        });
    }

    [Test]
    public void Resolve_accepts_the_reserved_default_tenant_id()
    {
        var bridge = CreateBridge();

        var tenant = bridge.Resolve(ContextWith(("lattice-active-tenant", TenantId.DefaultId)));

        Assert.That(tenant, Is.EqualTo(TenantId.Default));
    }

    [Test]
    public void Resolve_honours_a_custom_header_name()
    {
        var bridge = CreateBridge(new LatticeDataApiGrpcOptions
        {
            ActiveTenantHeaderName = "x-tenant",
        });

        var tenant = bridge.Resolve(ContextWith(("x-tenant", "acme")));

        Assert.That(tenant, Is.EqualTo(TenantId.Parse("acme")));
    }

    [Test]
    public void Resolve_returns_null_when_header_name_disabled()
    {
        var bridge = CreateBridge(new LatticeDataApiGrpcOptions
        {
            ActiveTenantHeaderName = string.Empty,
        });

        var tenant = bridge.Resolve(ContextWith(("lattice-active-tenant", "acme")));

        Assert.That(tenant, Is.Null);
    }

    [Test]
    public void Resolve_throws_on_null_context()
    {
        var bridge = CreateBridge();

        Assert.Throws<ArgumentNullException>(() => bridge.Resolve(null!));
    }

    /// <summary>
    /// Minimal <see cref="ServerCallContext"/> test double that carries only the
    /// inbound request headers the bridge reads; every other member is inert.
    /// </summary>
    private sealed class FakeServerCallContext : ServerCallContext
    {
        private readonly global::Grpc.Core.Metadata _requestHeaders;

        public FakeServerCallContext(global::Grpc.Core.Metadata requestHeaders) => _requestHeaders = requestHeaders;

        protected override string MethodCore => "/test/Method";

        protected override string HostCore => "localhost";

        protected override string PeerCore => "ipv4:127.0.0.1:0";

        protected override DateTime DeadlineCore => DateTime.MaxValue;

        protected override global::Grpc.Core.Metadata RequestHeadersCore => _requestHeaders;

        protected override CancellationToken CancellationTokenCore => CancellationToken.None;

        protected override global::Grpc.Core.Metadata ResponseTrailersCore { get; } = new();

        protected override Status StatusCore { get; set; } = Status.DefaultSuccess;

        protected override WriteOptions? WriteOptionsCore { get; set; }

        protected override AuthContext AuthContextCore { get; } =
            new(null, new Dictionary<string, List<global::Grpc.Core.AuthProperty>>());

        protected override IDictionary<object, object> UserStateCore { get; } =
            new Dictionary<object, object>();

        protected override ContextPropagationToken CreatePropagationTokenCore(
            ContextPropagationOptions? options) =>
            throw new NotSupportedException();

        protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) =>
            Task.CompletedTask;
    }
}
