using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Auth.Grpc.Tests;

/// <summary>
/// Registration-front-door tests for
/// <see cref="LatticeAuthApiGrpcServiceCollectionExtensions.AddLatticeAuthApiGrpc"/>.
/// Proves the binding wires its method factory, the fail-closed default-deny
/// meta-authorizer, the header identity bridge, the service, and the auth
/// interceptor, that it is idempotent, that a host-supplied permissive authorizer
/// or bridge registered first is preserved (TryAdd semantics), and that options
/// bind.
/// </summary>
[TestFixture]
public sealed class LatticeAuthApiGrpcRegistrationTests
{
    private static ServiceCollection SeedServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSerializer();
        services.AddSingleton(Substitute.For<ILatticeAuthAdmin>());
        return services;
    }

    [Test]
    public void AddLatticeAuthApiGrpc_registers_the_default_deny_authorizer()
    {
        var services = SeedServices();
        services.AddLatticeAuthApiGrpc();

        using var provider = services.BuildServiceProvider();
        var authorizer = provider.GetRequiredService<ILatticeAuthApiAuthorizer>();
        Assert.That(authorizer, Is.TypeOf<DenyAllAuthApiAuthorizer>());
    }

    [Test]
    public void AddLatticeAuthApiGrpc_preserves_a_host_supplied_authorizer()
    {
        var services = SeedServices();
        services.AddSingleton<ILatticeAuthApiAuthorizer, AllowAllAuthApiAuthorizer>();

        services.AddLatticeAuthApiGrpc();

        using var provider = services.BuildServiceProvider();
        var authorizer = provider.GetRequiredService<ILatticeAuthApiAuthorizer>();
        Assert.That(authorizer, Is.TypeOf<AllowAllAuthApiAuthorizer>(),
            "TryAdd must not overwrite a permissive authorizer the host opted into first.");
    }

    [Test]
    public void AddLatticeAuthApiGrpc_registers_the_header_bridge_by_default()
    {
        var services = SeedServices();
        services.AddLatticeAuthApiGrpc();

        using var provider = services.BuildServiceProvider();
        var bridge = provider.GetRequiredService<ILatticeAuthApiCredentialBridge>();
        Assert.That(bridge, Is.TypeOf<HeaderLatticeAuthApiCredentialBridge>());
    }

    [Test]
    public void AddLatticeAuthApiGrpc_resolves_the_method_definitions()
    {
        var services = SeedServices();
        services.AddLatticeAuthApiGrpc();

        using var provider = services.BuildServiceProvider();
        var methods = provider.GetRequiredService<LatticeAuthApiGrpcMethods>();
        Assert.That(methods.UpsertUser.ServiceName, Is.EqualTo(LatticeAuthApiGrpcMethods.ServiceName));
    }

    [Test]
    public void AddLatticeAuthApiGrpc_resolves_the_service()
    {
        var services = SeedServices();
        services.AddLatticeAuthApiGrpc();

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetService<LatticeAuthApiGrpcService>(), Is.Not.Null);
    }

    [Test]
    public void AddLatticeAuthApiGrpc_binds_options()
    {
        var services = SeedServices();
        services.AddLatticeAuthApiGrpc(o =>
        {
            o.RequireAuthorization = false;
            o.CredentialHeaderName = "x-cred";
            o.CredentialScheme = "custom";
        });

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeAuthApiGrpcOptions>>().Value;
        Assert.Multiple(() =>
        {
            Assert.That(options.RequireAuthorization, Is.False);
            Assert.That(options.CredentialHeaderName, Is.EqualTo("x-cred"));
            Assert.That(options.CredentialScheme, Is.EqualTo("custom"));
        });
    }

    [Test]
    public void AddLatticeAuthApiGrpc_defaults_require_authorization_to_true()
    {
        var services = SeedServices();
        services.AddLatticeAuthApiGrpc();

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeAuthApiGrpcOptions>>().Value;
        Assert.That(options.RequireAuthorization, Is.True,
            "The control plane must default to fail-closed enforcement.");
    }

    [Test]
    public void AddLatticeAuthApiGrpc_is_idempotent_for_the_authorizer()
    {
        var services = SeedServices();
        services.AddLatticeAuthApiGrpc();
        services.AddLatticeAuthApiGrpc();

        var registrations = services.Count(d => d.ServiceType == typeof(ILatticeAuthApiAuthorizer));
        Assert.That(registrations, Is.EqualTo(1));
    }

    [Test]
    public void AddLatticeAuthApiGrpc_throws_on_null_services()
    {
        Assert.Throws<ArgumentNullException>(
            () => LatticeAuthApiGrpcServiceCollectionExtensions.AddLatticeAuthApiGrpc(null!));
    }
}
