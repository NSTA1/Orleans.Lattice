using Grpc.Core;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.Apps.Grpc.Tests;

[TestFixture]
public sealed class AppsGrpcRegistrationTests
{
    [Test]
    public void Registration_preserves_request_scoped_collaborators()
    {
        var services = new ServiceCollection().AddLogging();
        services.AddScoped(_ => Substitute.For<ILatticeAppsControl>());
        services.AddScoped(_ => Substitute.For<ILatticeAppsApiAuthorizer>());
        services.AddScoped(_ => Substitute.For<ILatticeAppsApiCredentialBridge>());
        services.AddScoped(_ => Substitute.For<ILatticeAppsApiAuthSchemeSource>());
        Assert.That(services.AddLatticeAppsApiGrpc(), Is.SameAs(services));
        services.AddLatticeAppsApiGrpc();
        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        using var first = provider.CreateScope();
        using var second = provider.CreateScope();
        Assert.That(first.ServiceProvider.GetRequiredService<LatticeAppsGrpcService>(),
            Is.SameAs(first.ServiceProvider.GetRequiredService<LatticeAppsGrpcService>()));
        Assert.That(second.ServiceProvider.GetRequiredService<LatticeAppsGrpcService>(),
            Is.Not.SameAs(first.ServiceProvider.GetRequiredService<LatticeAppsGrpcService>()));
        Assert.That(first.ServiceProvider.GetRequiredService<LatticeAppsApiGrpcAuthInterceptor>(), Is.Not.Null);
        Assert.That(first.ServiceProvider.GetRequiredService<ILatticeAppsApiAuthorizer>(),
            Is.Not.TypeOf<DenyAppsApiAuthorizer>());
        Assert.That(first.ServiceProvider.GetRequiredService<ILatticeAppsApiCredentialBridge>(),
            Is.Not.TypeOf<HeaderLatticeAppsApiCredentialBridge>());
    }

    [Test]
    public void Options_default_to_enforcement_and_standard_headers()
    {
        var options = new LatticeAppsApiGrpcOptions();
        Assert.Multiple(() =>
        {
            Assert.That(options.RequireAuthorization, Is.True);
            Assert.That(options.CredentialHeaderName, Is.EqualTo("authorization"));
            Assert.That(options.CredentialScheme, Is.EqualTo("Bearer"));
            Assert.That(options.ActiveTenantHeaderName, Is.EqualTo(LatticeActiveTenantAssertion.DefaultHeaderName));
            Assert.That(options.AdvertisedAuthSchemes, Is.Empty);
        });
    }

    [Test]
    public void Advertisement_uses_current_options_and_returns_an_immutable_snapshot()
    {
        var options = new LatticeAppsApiGrpcOptions();
        options.AdvertisedAuthSchemes.Add(new() { SchemeId = "first", DisplayName = "First" });
        var monitor = Substitute.For<IOptionsMonitor<LatticeAppsApiGrpcOptions>>();
        monitor.CurrentValue.Returns(options);
        var source = new OptionsLatticeAppsApiAuthSchemeSource(monitor);
        var first = source.GetAdvertisement();
        options.AdvertisedAuthSchemes.Clear();
        Assert.That(first.Schemes.Single().SchemeId, Is.EqualTo("first"));
        Assert.That(source.GetAdvertisement().Schemes, Is.Empty);
        var replacement = new LatticeAppsApiGrpcOptions();
        replacement.AdvertisedAuthSchemes.Add(new() { SchemeId = "second", DisplayName = "Second" });
        monitor.CurrentValue.Returns(replacement);
        Assert.That(source.GetAdvertisement().Schemes.Single().SchemeId, Is.EqualTo("second"));
    }

    [Test]
    public async Task Custom_bridge_and_tenant_options_apply_without_inheriting_credentials()
    {
        var facade = Substitute.For<ILatticeAppsControl>();
        var bridge = Substitute.For<ILatticeAppsApiCredentialBridge>();
        var context = new TestCallContext("test", new global::Grpc.Core.Metadata { { "custom-tenant", "asserted" } });
        bridge.Resolve(context).Returns(new LatticeCredential("custom", "Test"));
        var observed = new List<(string? Token, string? Tenant)>();
        facade.ListAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            observed.Add((LatticeCredentialContext.Current?.Token, LatticeActiveTenantContext.Current?.Value));
            return new AppCatalog();
        });
        var options = new LatticeAppsApiGrpcOptions { ActiveTenantHeaderName = "custom-tenant" };
        var service = new LatticeAppsGrpcService(facade, bridge, Substitute.For<ILatticeAppsApiAuthSchemeSource>(),
            Options.Create(options), NullLogger<LatticeAppsGrpcService>.Instance);
        using (LatticeCredentialContext.With(new LatticeCredential("outer", "Test")))
        {
            await service.List(new(), context);
            bridge.Resolve(context).Returns((LatticeCredential?)null);
            options.ActiveTenantHeaderName = "";
            await service.List(new(), context);
            Assert.That(LatticeCredentialContext.Current?.Token, Is.EqualTo("outer"));
        }
        Assert.That(observed, Is.EqualTo(new (string?, string?)[] { ("custom", "asserted"), (null, null) }));
        Assert.That(LatticeCredentialContext.Current, Is.Null);
        Assert.That(LatticeActiveTenantContext.Current, Is.Null);
    }

    [Test]
    public void Custom_discovery_failures_are_sanitized()
    {
        var source = Substitute.For<ILatticeAppsApiAuthSchemeSource>();
        source.GetAdvertisement().Returns(_ => throw new RpcException(new Status(StatusCode.Internal, "private")));
        var service = new LatticeAppsGrpcService(Substitute.For<ILatticeAppsControl>(),
            Substitute.For<ILatticeAppsApiCredentialBridge>(), source, Options.Create(new LatticeAppsApiGrpcOptions()),
            NullLogger<LatticeAppsGrpcService>.Instance);
        var error = Assert.Throws<RpcException>(() => service.GetAuthScheme(new(), new TestCallContext("test")));
        Assert.That(error!.Status.Detail, Does.Not.Contain("private"));
    }

    [Test]
    public void Registration_rejects_null_builders()
    {
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddLatticeAppsApiGrpc());
        Assert.Throws<ArgumentNullException>(() => ((IEndpointRouteBuilder)null!).MapLatticeAppsApiGrpc());
    }
}
