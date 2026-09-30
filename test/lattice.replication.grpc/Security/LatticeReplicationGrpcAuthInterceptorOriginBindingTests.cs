using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;

namespace Orleans.Lattice.Replication.Grpc.Tests.Security;

/// <summary>
/// Coverage for <c>LatticeReplicationSecurityOptions.BindCredentialToOriginCluster</c>.
/// </summary>
/// <remarks>
/// <para>
/// The accepted-secret set carries no peer attribution:
/// <c>IReplicationSecretProvider.IsAcceptedAsync</c> takes only the presented
/// credential, so a successful match proves the caller holds <i>some</i>
/// accepted secret and never that it is a particular cluster. Every
/// downstream origin gate therefore compares two values the caller chose -
/// the stamped header and the body-declared origin - which a peer holding any
/// accepted secret can set to the same arbitrary cluster id.
/// </para>
/// <para>
/// The binding closes that by re-resolving the secret this cluster
/// would use to call the claimed origin and requiring the presented
/// credential to equal it, which turns the origin into an authenticated fact.
/// It is on by default: a single cluster-wide secret resolves the same value
/// for every peer so the check passes and costs only a resolution, while a
/// symmetric per-peer scheme gets real isolation. Only an asymmetric scheme,
/// where the secret a peer presents is deliberately not the one this cluster
/// would send it, must turn it off.
/// </para>
/// </remarks>
[TestFixture]
public class LatticeReplicationGrpcAuthInterceptorOriginBindingTests
{
    private const string PushMethod = "/orleans.lattice.replication.LatticeReplication/Push";
    private const string SiteASecret = "secret-for-site-a";
    private const string SiteBSecret = "secret-for-site-b";

    private static IOptionsMonitor<LatticeReplicationSecurityOptions> OptionsFor(LatticeReplicationSecurityOptions o)
    {
        var m = Substitute.For<IOptionsMonitor<LatticeReplicationSecurityOptions>>();
        m.CurrentValue.Returns(o);
        return m;
    }

    /// <summary>
    /// A provider modelling a symmetric per-peer scheme: both secrets are in
    /// the accepted set, and each peer has its own outbound secret.
    /// </summary>
    private static IReplicationSecretProvider PerPeerSecrets()
    {
        var secrets = Substitute.For<IReplicationSecretProvider>();
        secrets.IsAcceptedAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(ci => new ValueTask<bool>(
                ci.ArgAt<string?>(0) is SiteASecret or SiteBSecret));
        secrets.GetOutboundSecretAsync("site-a", Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>(SiteASecret));
        secrets.GetOutboundSecretAsync("site-b", Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>(SiteBSecret));
        secrets.GetOutboundSecretAsync(
                Arg.Is<string>(s => s != "site-a" && s != "site-b"),
                Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>((string?)null));
        return secrets;
    }

    private static LatticeReplicationGrpcAuthInterceptor CreateInterceptor(
        IReplicationSecretProvider secrets,
        bool bindToOrigin)
        => new(
            secrets,
            OptionsFor(new LatticeReplicationSecurityOptions
            {
                RequireAuthentication = true,
                BindCredentialToOriginCluster = bindToOrigin,
            }),
            NullLogger<LatticeReplicationGrpcAuthInterceptor>.Instance);

    private static Task<string> InvokeAsync(
        LatticeReplicationGrpcAuthInterceptor interceptor,
        ServerCallContext context)
        => interceptor.UnaryServerHandler<object, string>(
            request: new object(),
            context: context,
            continuation: (_, _) => Task.FromResult("ok"));

    [Test]
    public async Task Binding_disabled_admits_any_accepted_secret_under_any_claimed_origin()
    {
        // Establishes the pre-existing behaviour the option leaves untouched,
        // so the default-off flag is additive rather than breaking. It is also
        // the vulnerability in its plainest form: site-b's credential is
        // admitted while claiming to be site-a.
        var interceptor = CreateInterceptor(PerPeerSecrets(), bindToOrigin: false);
        var ctx = ContextWith(SiteBSecret, origin: "site-a");

        var result = await InvokeAsync(interceptor, ctx);

        Assert.That(result, Is.EqualTo("ok"));
    }

    [Test]
    public async Task Binding_enabled_admits_a_credential_that_matches_the_claimed_origin()
    {
        var interceptor = CreateInterceptor(PerPeerSecrets(), bindToOrigin: true);
        var ctx = ContextWith(SiteASecret, origin: "site-a");

        var result = await InvokeAsync(interceptor, ctx);

        Assert.That(result, Is.EqualTo("ok"));
    }

    [Test]
    public void Binding_enabled_refuses_an_accepted_credential_claiming_another_cluster()
    {
        // The regression: site-b holds a genuinely accepted secret, so the
        // accepted-set walk passes. Only the per-peer binding catches that it
        // is impersonating site-a.
        var interceptor = CreateInterceptor(PerPeerSecrets(), bindToOrigin: true);
        var ctx = ContextWith(SiteBSecret, origin: "site-a");

        var rpc = Assert.ThrowsAsync<RpcException>(async () => await InvokeAsync(interceptor, ctx));

        Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void Binding_enabled_refuses_a_call_that_stamps_no_origin()
    {
        // An unstamped call asserts no origin at all, so there is nothing to
        // bind the credential to; admitting it would reinstate the bypass.
        var interceptor = CreateInterceptor(PerPeerSecrets(), bindToOrigin: true);
        var ctx = ContextWith(SiteASecret, origin: null);

        var rpc = Assert.ThrowsAsync<RpcException>(async () => await InvokeAsync(interceptor, ctx));

        Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void Binding_enabled_refuses_an_origin_this_cluster_has_no_secret_for()
    {
        var interceptor = CreateInterceptor(PerPeerSecrets(), bindToOrigin: true);
        var ctx = ContextWith(SiteASecret, origin: "unconfigured-peer");

        var rpc = Assert.ThrowsAsync<RpcException>(async () => await InvokeAsync(interceptor, ctx));

        Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void Binding_enabled_reports_one_indistinguishable_refusal_for_every_failure_shape()
    {
        // A refusal that distinguished "no such peer configured" from "wrong
        // secret for that peer" would let a caller enumerate which clusters
        // this receiver peers with, one probe at a time.
        var interceptor = CreateInterceptor(PerPeerSecrets(), bindToOrigin: true);

        var unstamped = Assert.ThrowsAsync<RpcException>(
            async () => await InvokeAsync(interceptor, ContextWith(SiteASecret, origin: null)));
        var unconfigured = Assert.ThrowsAsync<RpcException>(
            async () => await InvokeAsync(interceptor, ContextWith(SiteASecret, origin: "unconfigured-peer")));
        var mismatched = Assert.ThrowsAsync<RpcException>(
            async () => await InvokeAsync(interceptor, ContextWith(SiteBSecret, origin: "site-a")));

        Assert.Multiple(() =>
        {
            Assert.That(unconfigured!.Status.Detail, Is.EqualTo(unstamped!.Status.Detail));
            Assert.That(mismatched!.Status.Detail, Is.EqualTo(unstamped.Status.Detail));
            Assert.That(unconfigured.StatusCode, Is.EqualTo(unstamped.StatusCode));
            Assert.That(mismatched.StatusCode, Is.EqualTo(unstamped.StatusCode));
        });
    }

    [Test]
    public void Binding_enabled_still_refuses_an_unaccepted_secret_before_consulting_the_origin()
    {
        var secrets = PerPeerSecrets();
        var interceptor = CreateInterceptor(secrets, bindToOrigin: true);
        var ctx = ContextWith("not-an-accepted-secret", origin: "site-a");

        var rpc = Assert.ThrowsAsync<RpcException>(async () => await InvokeAsync(interceptor, ctx));

        Assert.Multiple(() =>
        {
            Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            // The accepted-set gate rejects first, so the binding never runs.
            _ = secrets.DidNotReceiveWithAnyArgs().GetOutboundSecretAsync(default!, default);
        });
    }

    [Test]
    public void Binding_enabled_still_refuses_a_call_with_no_credential_at_all()
    {
        var interceptor = CreateInterceptor(PerPeerSecrets(), bindToOrigin: true);
        var ctx = ContextWith(secret: null, origin: "site-a");

        var rpc = Assert.ThrowsAsync<RpcException>(async () => await InvokeAsync(interceptor, ctx));

        Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.Unauthenticated));
    }

    [Test]
    public void Binding_defaults_to_on_so_a_claimed_origin_is_an_authenticated_fact()
    {
        Assert.That(new LatticeReplicationSecurityOptions().BindCredentialToOriginCluster, Is.True);
    }

    [Test]
    public void Default_options_refuse_an_accepted_credential_claiming_another_cluster()
    {
        // The regression that matters: the fix is the default, so the impersonation
        // must be refused by an interceptor nobody configured. Building the options
        // with only RequireAuthentication set - as a host that never heard of the
        // binding would - must still close the hole.
        var interceptor = new LatticeReplicationGrpcAuthInterceptor(
            PerPeerSecrets(),
            OptionsFor(new LatticeReplicationSecurityOptions()),
            NullLogger<LatticeReplicationGrpcAuthInterceptor>.Instance);
        var ctx = ContextWith(SiteBSecret, origin: "site-a");

        var rpc = Assert.ThrowsAsync<RpcException>(async () => await InvokeAsync(interceptor, ctx));

        Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public async Task Default_options_admit_a_shared_secret_estate_unchanged()
    {
        // The common estate: one cluster-wide secret, so every origin resolves the
        // same value and the binding passes. Pins that defaulting the flag on does
        // not cost a standard deployment its replication traffic.
        var shared = Substitute.For<IReplicationSecretProvider>();
        shared.IsAcceptedAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(ci => new ValueTask<bool>(ci.ArgAt<string?>(0) == SiteASecret));
        shared.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>(SiteASecret));

        var interceptor = new LatticeReplicationGrpcAuthInterceptor(
            shared,
            OptionsFor(new LatticeReplicationSecurityOptions()),
            NullLogger<LatticeReplicationGrpcAuthInterceptor>.Instance);

        var result = await InvokeAsync(interceptor, ContextWith(SiteASecret, origin: "any-peer"));

        Assert.That(result, Is.EqualTo("ok"));
    }

    private static ServerCallContext ContextWith(string? secret, string? origin)
    {
        var metadata = new global::Grpc.Core.Metadata();
        if (secret is not null)
        {
            metadata.Add(LatticeReplicationGrpcMetadataNames.SecretHeader, secret);
        }
        if (origin is not null)
        {
            metadata.Add(LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader, origin);
        }
        return new StubServerCallContext(PushMethod, metadata);
    }

    private sealed class StubServerCallContext(string method, global::Grpc.Core.Metadata headers) : ServerCallContext
    {
        protected override string MethodCore => method;
        protected override string HostCore => string.Empty;
        protected override string PeerCore => "ipv4:127.0.0.1:0";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override global::Grpc.Core.Metadata RequestHeadersCore => headers;
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override global::Grpc.Core.Metadata ResponseTrailersCore { get; } = new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new(string.Empty, new Dictionary<string, List<AuthProperty>>());
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => null!;
        protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) => Task.CompletedTask;
    }
}
