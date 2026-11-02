using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Unit coverage for the credential-refusal check of
/// <see cref="LatticeStateApiGrpcAuthInterceptor"/>: a call presenting a credential
/// the cluster refuses (a wrong password) is failed as
/// <see cref="StatusCode.Unauthenticated"/> before the authorizer runs, whether or
/// not enforcement is on, so it is never answered like an anonymous or an
/// ungranted caller. Accepted, absent and unrecognised credentials pass through.
/// </summary>
[TestFixture]
public sealed class LatticeStateApiGrpcAuthInterceptorCredentialRefusalTests
{
    private static global::Grpc.Core.Metadata Credential(string token = "dXNlcjp3cm9uZw==") => new() { { "authorization", "Basic " + token } };

    [TestCase(true)]
    [TestCase(false)]
    public void UnaryServerHandler_fails_a_refused_credential_as_unauthenticated(bool requireAuthorization)
    {
        var authorizer = Substitute.For<ILatticeStateApiAuthorizer>();
        var membership = new StubMembership(LatticeCredentialVerdict.Refused);
        var interceptor = Create(authorizer, membership, requireAuthorization);
        var called = false;

        var ex = Assert.ThrowsAsync<RpcException>(() => interceptor.UnaryServerHandler(
            new CatalogRequest(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ListTreesMethodName, Credential()),
            (_, _) =>
            {
                called = true;
                return Task.FromResult(new TreeCatalogPage());
            }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unauthenticated));
            Assert.That(ex.Status.Detail, Is.EqualTo(LatticeStateApiGrpcAuthInterceptor.CredentialRefusedMessage));
            Assert.That(ex.Status.Detail, Does.Not.Contain("dXNlcjp3cm9uZw=="), "the credential is never echoed");
            Assert.That(called, Is.False);
            Assert.That(membership.SeenToken, Is.EqualTo("dXNlcjp3cm9uZw=="), "the credential is judged as the call presents it");
        });
        authorizer.DidNotReceiveWithAnyArgs().IsAuthorizedAsync(default!, default);
    }

    [TestCase(LatticeCredentialVerdict.Accepted)]
    [TestCase(LatticeCredentialVerdict.Unverified)]
    public async Task UnaryServerHandler_serves_a_credential_that_is_not_refused(LatticeCredentialVerdict verdict)
    {
        var interceptor = Create(new AllowAllStateApiAuthorizer(), new StubMembership(verdict));
        var response = new TreeCatalogPage();

        var result = await interceptor.UnaryServerHandler(
            new CatalogRequest(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ListTreesMethodName, Credential()),
            (_, _) => Task.FromResult(response));

        Assert.That(result, Is.SameAs(response));
    }

    [Test]
    public async Task UnaryServerHandler_without_a_credential_does_not_consult_membership()
    {
        var membership = new StubMembership(LatticeCredentialVerdict.Refused);
        var interceptor = Create(new AllowAllStateApiAuthorizer(), membership);
        var response = new TreeCatalogPage();

        var result = await interceptor.UnaryServerHandler(
            new CatalogRequest(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ListTreesMethodName),
            (_, _) => Task.FromResult(response));

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(response));
            Assert.That(membership.Calls, Is.Zero);
        });
    }

    [Test]
    public async Task UnaryServerHandler_with_no_membership_context_serves_the_call()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeStateApiGrpcOptions>>();
        monitor.CurrentValue.Returns(new LatticeStateApiGrpcOptions { RequireAuthorization = false });
        var interceptor = new LatticeStateApiGrpcAuthInterceptor(
            new AllowAllStateApiAuthorizer(),
            monitor,
            Substitute.For<ILogger<LatticeStateApiGrpcAuthInterceptor>>(),
            Bridge(),
            membership: null);
        var response = new TreeCatalogPage();

        var result = await interceptor.UnaryServerHandler(
            new CatalogRequest(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ListTreesMethodName, Credential()),
            (_, _) => Task.FromResult(response));

        Assert.That(result, Is.SameAs(response));
    }

    [Test]
    public async Task UnaryServerHandler_keeps_the_auth_scheme_discovery_rpc_reachable_with_a_refused_credential()
    {
        var interceptor = Create(new AllowAllStateApiAuthorizer(), new StubMembership(LatticeCredentialVerdict.Refused));
        var response = new AuthSchemeAdvertisement();

        var result = await interceptor.UnaryServerHandler(
            new AuthSchemeAdvertisementRequest(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetAuthSchemeMethodName, Credential()),
            (_, _) => Task.FromResult(response));

        Assert.That(result, Is.SameAs(response));
    }

    [Test]
    public void ServerStreamingServerHandler_fails_a_refused_credential_as_unauthenticated()
    {
        var interceptor = Create(new AllowAllStateApiAuthorizer(), new StubMembership(LatticeCredentialVerdict.Refused));

        var ex = Assert.ThrowsAsync<RpcException>(() => interceptor.ServerStreamingServerHandler(
            new StateObserveRequest { TreeId = "orders" },
            Substitute.For<IServerStreamWriter<StateChangeNotification>>(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveChangesMethodName, Credential()),
            (_, _, _) => Task.CompletedTask));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unauthenticated));
    }

    [Test]
    public void ClientStreamingServerHandler_fails_a_refused_credential_as_unauthenticated()
    {
        var interceptor = Create(new AllowAllStateApiAuthorizer(), new StubMembership(LatticeCredentialVerdict.Refused));

        var ex = Assert.ThrowsAsync<RpcException>(() => interceptor.ClientStreamingServerHandler(
            Substitute.For<IAsyncStreamReader<CatalogRequest>>(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ListTreesMethodName, Credential()),
            (_, _) => Task.FromResult(new TreeCatalogPage())));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unauthenticated));
    }

    [Test]
    public void DuplexStreamingServerHandler_fails_a_refused_credential_as_unauthenticated()
    {
        var interceptor = Create(new AllowAllStateApiAuthorizer(), new StubMembership(LatticeCredentialVerdict.Refused));

        var ex = Assert.ThrowsAsync<RpcException>(() => interceptor.DuplexStreamingServerHandler(
            Substitute.For<IAsyncStreamReader<CatalogRequest>>(),
            Substitute.For<IServerStreamWriter<TreeCatalogPage>>(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ListTreesMethodName, Credential()),
            (_, _, _) => Task.CompletedTask));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unauthenticated));
    }

    private static LatticeStateApiGrpcAuthInterceptor Create(
        ILatticeStateApiAuthorizer authorizer,
        ILatticeMembershipContext membership,
        bool requireAuthorization = true)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeStateApiGrpcOptions>>();
        monitor.CurrentValue.Returns(new LatticeStateApiGrpcOptions { RequireAuthorization = requireAuthorization });
        return new LatticeStateApiGrpcAuthInterceptor(
            authorizer,
            monitor,
            Substitute.For<ILogger<LatticeStateApiGrpcAuthInterceptor>>(),
            Bridge(),
            membership);
    }

    private static HeaderLatticeStateApiCredentialBridge Bridge() =>
        new(Options.Create(new LatticeStateApiGrpcOptions { CredentialScheme = "Basic" }));

    /// <summary>A warm membership context that reports a fixed verdict and records the credential it judged.</summary>
    private sealed class StubMembership(LatticeCredentialVerdict verdict) : ILatticeMembershipContext
    {
        public int Calls { get; private set; }

        public string? SeenToken { get; private set; }

        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(LatticeSubject.Anonymous);

        public bool TryResolveCurrent(out LatticeSubject subject)
        {
            subject = LatticeSubject.Anonymous;
            return true;
        }

        public ValueTask<LatticeCredentialVerdict> VerifyCurrentCredentialAsync(CancellationToken cancellationToken = default)
        {
            Calls++;
            SeenToken = LatticeCredentialContext.Current?.Token;
            return new(verdict);
        }
    }
}
