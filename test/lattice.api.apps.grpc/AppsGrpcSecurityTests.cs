using Grpc.Core;
using GrpcMetadata = Grpc.Core.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.Apps.Grpc.Tests;

[TestFixture]
public sealed class AppsGrpcSecurityTests
{
    [TestCase(null, null)]
    [TestCase("", null)]
    [TestCase("   ", null)]
    [TestCase("Bearer", null)]
    [TestCase("Bearer ", null)]
    [TestCase(" bEaReR  token ", "token")]
    [TestCase("Bearerish", "Bearerish")]
    [TestCase("token", "token")]
    public void Credential_bridge_strips_only_a_delimited_scheme(string? header, string? expected)
    {
        var headers = new GrpcMetadata();
        if (header is not null) headers.Add("authorization", header);
        var bridge = new HeaderLatticeAppsApiCredentialBridge(Options.Create(new LatticeAppsApiGrpcOptions()));
        var result = bridge.Resolve(new TestCallContext("test", headers));
        Assert.That(result?.Token, Is.EqualTo(expected));
        Assert.That(result?.Scheme, Is.EqualTo(expected is null ? null : "Bearer"));
    }

    [TestCase("X-Credential", "Custom", "Custom token", "token", "Custom")]
    [TestCase("x-credential", "", "token", "token", null)]
    [TestCase("", "Bearer", "token", null, null)]
    public void Credential_bridge_honors_configured_header_and_scheme(
        string name, string scheme, string raw, string? token, string? expectedScheme)
    {
        var options = Options.Create(new LatticeAppsApiGrpcOptions
            { CredentialHeaderName = name, CredentialScheme = scheme });
        var bridge = new HeaderLatticeAppsApiCredentialBridge(options);
        var result = bridge.Resolve(new TestCallContext("test", new GrpcMetadata { { "x-credential", raw } }));
        Assert.That(result?.Token, Is.EqualTo(token));
        Assert.That(result?.Scheme, Is.EqualTo(expectedScheme));
    }

    [Test]
    public async Task Registration_is_default_deny_and_advertises_nothing()
    {
        using var services = new ServiceCollection().AddLogging().AddLatticeAppsApiGrpc().BuildServiceProvider();
        var authorizer = services.GetRequiredService<ILatticeAppsApiAuthorizer>();
        Assert.That(authorizer, Is.TypeOf<DenyAppsApiAuthorizer>());
        Assert.That(await authorizer.IsAuthorizedAsync(default, default), Is.False);
        Assert.That(services.GetRequiredService<ILatticeAppsApiAuthSchemeSource>().GetAdvertisement().Schemes, Is.Empty);
        var interceptor = services.GetRequiredService<LatticeAppsApiGrpcAuthInterceptor>();
        var error = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            new AppsEmptyRequest(), new TestCallContext(LatticeAppsGrpcMethods.ServicePrefix + "List"),
            (_, _) => Task.FromResult(new AppCatalog())));
        Assert.That(error!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [TestCase("Unknown")]
    [TestCase("List")]
    [TestCase("GetAuthScheme")]
    public void Unknown_or_mismatched_requests_fail_closed_even_when_enforcement_disabled(string method)
    {
        var interceptor = Create(Substitute.For<ILatticeAppsApiAuthorizer>(), false);
        var error = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            new AppsSlugRequest { Slug = "demo" }, new TestCallContext(LatticeAppsGrpcMethods.ServicePrefix + method),
            (_, _) => Task.FromResult(new AppCatalog())));
        Assert.That(error!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public async Task Authorization_off_skips_authorizer_but_preserves_the_operation()
    {
        var authorizer = Substitute.For<ILatticeAppsApiAuthorizer>();
        var response = new AppCatalog();
        Assert.That(await Create(authorizer, false).UnaryServerHandler(new AppsEmptyRequest(),
            new TestCallContext(LatticeAppsGrpcMethods.ServicePrefix + "List"), (_, _) => Task.FromResult(response)),
            Is.SameAs(response));
        Assert.That(authorizer.ReceivedCalls(), Is.Empty);
    }

    [TestCase(true)]
    [TestCase(false)]
    public void Streaming_shapes_are_rejected_even_for_discovery_and_opt_out(bool required)
    {
        var authorizer = Substitute.For<ILatticeAppsApiAuthorizer>();
        authorizer.IsAuthorizedAsync(Arg.Any<LatticeAppsApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(true);
        var interceptor = Create(authorizer, required);
        var context = new TestCallContext(LatticeAppsGrpcMethods.ServicePrefix + "GetAuthScheme");
        var stream = Substitute.For<IAsyncStreamReader<AppsEmptyRequest>>();
        var writer = Substitute.For<IServerStreamWriter<AppCatalog>>();
        Assert.Throws<RpcException>(() => interceptor.ServerStreamingServerHandler(
            new AppsEmptyRequest(), writer, context, (_, _, _) => Task.CompletedTask));
        Assert.Throws<RpcException>(() => interceptor.ClientStreamingServerHandler(
            stream, context, (_, _) => Task.FromResult(new AppCatalog())));
        Assert.Throws<RpcException>(() => interceptor.DuplexStreamingServerHandler(
            stream, writer, context, (_, _, _) => Task.CompletedTask));
    }

    [Test]
    public async Task Other_services_are_unaffected_in_every_call_shape()
    {
        var interceptor = Create(new DenyAppsApiAuthorizer());
        var context = new TestCallContext("/other/List");
        var stream = Substitute.For<IAsyncStreamReader<AppsEmptyRequest>>();
        var writer = Substitute.For<IServerStreamWriter<AppCatalog>>();
        var calls = 0;
        await interceptor.UnaryServerHandler(new AppsEmptyRequest(), context,
            (_, _) => { calls++; return Task.FromResult(new AppCatalog()); });
        await interceptor.ServerStreamingServerHandler(new AppsEmptyRequest(), writer, context,
            (_, _, _) => { calls++; return Task.CompletedTask; });
        await interceptor.ClientStreamingServerHandler(stream, context,
            (_, _) => { calls++; return Task.FromResult(new AppCatalog()); });
        await interceptor.DuplexStreamingServerHandler(stream, writer, context,
            (_, _, _) => { calls++; return Task.CompletedTask; });
        Assert.That(calls, Is.EqualTo(4));
    }

    [Test]
    public void Authorizer_cancellation_is_explicit_and_never_continues()
    {
        var authorizer = Substitute.For<ILatticeAppsApiAuthorizer>();
        authorizer.IsAuthorizedAsync(Arg.Any<LatticeAppsApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<bool>(new OperationCanceledException()));
        var error = Assert.ThrowsAsync<RpcException>(async () => await Create(authorizer).UnaryServerHandler<AppsEmptyRequest, AppCatalog>(
            new AppsEmptyRequest(), new TestCallContext(LatticeAppsGrpcMethods.ServicePrefix + "List"),
            (_, _) => throw new AssertionException("Must not continue")));
        Assert.That(error!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
    }

    private static LatticeAppsApiGrpcAuthInterceptor Create(ILatticeAppsApiAuthorizer authorizer, bool required = true)
    {
        var options = Substitute.For<IOptionsMonitor<LatticeAppsApiGrpcOptions>>();
        options.CurrentValue.Returns(new LatticeAppsApiGrpcOptions { RequireAuthorization = required });
        return new(authorizer, options, NullLogger<LatticeAppsApiGrpcAuthInterceptor>.Instance);
    }
}
