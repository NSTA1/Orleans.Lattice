using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// Registration-front-door tests for
/// <see cref="LatticeDataApiGrpcServiceCollectionExtensions.AddLatticeDataApiGrpc"/>.
/// Proves the binding wires its method factory, the fail-closed default-deny
/// authorizer, the header identity bridge, the service, and the auth interceptor,
/// that it is idempotent, that a host-supplied permissive authorizer or bridge
/// registered first is preserved (TryAdd semantics), and that options bind.
/// </summary>
[TestFixture]
public sealed class LatticeDataApiGrpcRegistrationTests
{
    private static ServiceCollection SeedServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSerializer();
        services.AddSingleton(Substitute.For<ILatticeDataApi>());
        return services;
    }

    [Test]
    public void AddLatticeDataApiGrpc_registers_the_default_deny_authorizer()
    {
        var services = SeedServices();
        services.AddLatticeDataApiGrpc();

        using var provider = services.BuildServiceProvider();
        var authorizer = provider.GetRequiredService<ILatticeDataApiAuthorizer>();
        Assert.That(authorizer, Is.TypeOf<DenyAllDataApiAuthorizer>());
    }

    [Test]
    public void AddLatticeDataApiGrpc_preserves_a_host_supplied_authorizer()
    {
        var services = SeedServices();
        services.AddSingleton<ILatticeDataApiAuthorizer, AllowAllDataApiAuthorizer>();

        services.AddLatticeDataApiGrpc();

        using var provider = services.BuildServiceProvider();
        var authorizer = provider.GetRequiredService<ILatticeDataApiAuthorizer>();
        Assert.That(authorizer, Is.TypeOf<AllowAllDataApiAuthorizer>(),
            "TryAdd must not overwrite a permissive authorizer the host opted into first.");
    }

    [Test]
    public void AddLatticeDataApiGrpc_registers_the_header_bridge_by_default()
    {
        var services = SeedServices();
        services.AddLatticeDataApiGrpc();

        using var provider = services.BuildServiceProvider();
        var bridge = provider.GetRequiredService<ILatticeDataApiCredentialBridge>();
        Assert.That(bridge, Is.TypeOf<HeaderLatticeDataApiCredentialBridge>());
    }

    [Test]
    public void AddLatticeDataApiGrpc_resolves_the_method_definitions()
    {
        var services = SeedServices();
        services.AddLatticeDataApiGrpc();

        using var provider = services.BuildServiceProvider();
        var methods = provider.GetRequiredService<LatticeDataApiGrpcMethods>();
        Assert.That(methods.Set.ServiceName, Is.EqualTo(LatticeDataApiGrpcMethods.ServiceName));
    }

    [Test]
    public void AddLatticeDataApiGrpc_resolves_the_service()
    {
        var services = SeedServices();
        services.AddLatticeDataApiGrpc();

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetService<LatticeDataApiGrpcService>(), Is.Not.Null);
    }

    [Test]
    public void AddLatticeDataApiGrpc_binds_options()
    {
        var services = SeedServices();
        services.AddLatticeDataApiGrpc(o =>
        {
            o.RequireAuthorization = false;
            o.CredentialHeaderName = "x-cred";
            o.CredentialScheme = "custom";
        });

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeDataApiGrpcOptions>>().Value;
        Assert.Multiple(() =>
        {
            Assert.That(options.RequireAuthorization, Is.False);
            Assert.That(options.CredentialHeaderName, Is.EqualTo("x-cred"));
            Assert.That(options.CredentialScheme, Is.EqualTo("custom"));
        });
    }

    [Test]
    public void AddLatticeDataApiGrpc_defaults_require_authorization_to_true()
    {
        var services = SeedServices();
        services.AddLatticeDataApiGrpc();

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeDataApiGrpcOptions>>().Value;
        Assert.That(options.RequireAuthorization, Is.True,
            "A write-capable surface must default to fail-closed enforcement.");
    }

    [Test]
    public void AddLatticeDataApiGrpc_is_idempotent_for_the_authorizer()
    {
        var services = SeedServices();
        services.AddLatticeDataApiGrpc();
        services.AddLatticeDataApiGrpc();

        var registrations = services.Count(d => d.ServiceType == typeof(ILatticeDataApiAuthorizer));
        Assert.That(registrations, Is.EqualTo(1));
    }

    [Test]
    public void AddLatticeDataApiGrpc_throws_on_null_services()
    {
        Assert.Throws<ArgumentNullException>(
            () => LatticeDataApiGrpcServiceCollectionExtensions.AddLatticeDataApiGrpc(null!));
    }
}
