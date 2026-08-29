using Grpc.Core;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Auth.Grpc.Tests;

/// <summary>
/// Unit coverage for the header-based identity bridge that lifts an inbound gRPC
/// request header into a <see cref="LatticeCredential"/> for the auth-API control
/// path. Proves scheme stripping, casing tolerance, the custom-header /
/// custom-scheme knobs, and the fail-closed null results for an absent, blank, or
/// scheme-only header - the anonymous cases the facade's administrator check
/// denies on every operation.
/// </summary>
[TestFixture]
public sealed class HeaderLatticeAuthApiCredentialBridgeTests
{
    private static HeaderLatticeAuthApiCredentialBridge CreateBridge(
        LatticeAuthApiGrpcOptions? options = null) =>
        new(Options.Create(options ?? new LatticeAuthApiGrpcOptions()));

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
    public void Resolve_strips_bearer_scheme_and_keeps_token()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(ContextWith(("authorization", "Bearer alice-token")));

        Assert.Multiple(() =>
        {
            Assert.That(credential, Is.Not.Null);
            Assert.That(credential!.Value.Token, Is.EqualTo("alice-token"));
            Assert.That(credential.Value.Scheme, Is.EqualTo("Bearer"));
        });
    }

    [Test]
    public void Resolve_is_case_insensitive_on_the_scheme_prefix()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(ContextWith(("authorization", "bEaReR alice-token")));

        Assert.That(credential!.Value.Token, Is.EqualTo("alice-token"));
    }

    [Test]
    public void Resolve_keeps_raw_token_when_no_scheme_prefix_present()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(ContextWith(("authorization", "bare-token")));

        Assert.Multiple(() =>
        {
            Assert.That(credential!.Value.Token, Is.EqualTo("bare-token"));
            Assert.That(credential.Value.Scheme, Is.EqualTo("Bearer"));
        });
    }

    [Test]
    public void Resolve_returns_null_when_header_absent()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(ContextWith(("x-other", "ignored")));

        Assert.That(credential, Is.Null);
    }

    [Test]
    public void Resolve_returns_null_when_header_blank()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(ContextWith(("authorization", "   ")));

        Assert.That(credential, Is.Null);
    }

    [Test]
    public void Resolve_returns_null_when_only_the_scheme_is_present()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(ContextWith(("authorization", "Bearer ")));

        Assert.That(credential, Is.Null);
    }

    [Test]
    public void Resolve_honours_a_custom_header_name()
    {
        var bridge = CreateBridge(new LatticeAuthApiGrpcOptions
        {
            CredentialHeaderName = "x-lattice-cred",
        });

        var credential = bridge.Resolve(ContextWith(("x-lattice-cred", "Bearer alice-token")));

        Assert.That(credential!.Value.Token, Is.EqualTo("alice-token"));
    }

    [Test]
    public void Resolve_honours_a_custom_scheme()
    {
        var bridge = CreateBridge(new LatticeAuthApiGrpcOptions
        {
            CredentialScheme = "test-scheme",
        });

        var credential = bridge.Resolve(ContextWith(("authorization", "test-scheme alice")));

        Assert.Multiple(() =>
        {
            Assert.That(credential!.Value.Token, Is.EqualTo("alice"));
            Assert.That(credential.Value.Scheme, Is.EqualTo("test-scheme"));
        });
    }

    [Test]
    public void Resolve_with_empty_scheme_keeps_whole_header_and_null_scheme()
    {
        var bridge = CreateBridge(new LatticeAuthApiGrpcOptions
        {
            CredentialScheme = string.Empty,
        });

        var credential = bridge.Resolve(ContextWith(("authorization", "Bearer alice-token")));

        Assert.Multiple(() =>
        {
            Assert.That(credential!.Value.Token, Is.EqualTo("Bearer alice-token"));
            Assert.That(credential.Value.Scheme, Is.Null);
        });
    }

    [Test]
    public void Resolve_with_an_empty_header_name_disables_the_bridge()
    {
        // An empty CredentialHeaderName is how a host turns the header bridge
        // off. It must read as anonymous rather than falling back to a default
        // header name, which would silently re-enable header-derived identity.
        var bridge = CreateBridge(new LatticeAuthApiGrpcOptions
        {
            CredentialHeaderName = string.Empty,
        });

        var credential = bridge.Resolve(ContextWith(("authorization", "Bearer alice-token")));

        Assert.That(credential, Is.Null);
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
