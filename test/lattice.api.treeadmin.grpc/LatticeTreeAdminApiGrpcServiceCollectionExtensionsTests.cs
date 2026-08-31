using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// Registration coverage for
/// <see cref="LatticeTreeAdminApiGrpcServiceCollectionExtensions.AddLatticeTreeAdminApiGrpc"/>.
/// The binding drives destructive whole-tree administration, so the shape that
/// matters most is the default-closed one: with no host opt-in the container must
/// resolve the deny-everything authorizer and leave
/// <see cref="LatticeTreeAdminApiGrpcOptions.RequireAuthorization"/> on.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminApiGrpcServiceCollectionExtensionsTests
{
    [Test]
    public void AddLatticeTreeAdminApiGrpc_rejects_a_null_service_collection()
    {
        Assert.Throws<ArgumentNullException>(() =>
            LatticeTreeAdminApiGrpcServiceCollectionExtensions.AddLatticeTreeAdminApiGrpc(null!));
    }

    [Test]
    public void AddLatticeTreeAdminApiGrpc_returns_the_same_collection_for_chaining()
    {
        var services = new ServiceCollection();

        Assert.That(services.AddLatticeTreeAdminApiGrpc(), Is.SameAs(services));
    }

    [Test]
    public void AddLatticeTreeAdminApiGrpc_without_a_configure_delegate_registers_the_default_options()
    {
        // The no-delegate overload takes the AddOptions branch, so the binding must
        // still resolve fully-defaulted (and therefore fail-closed) options.
        var services = new ServiceCollection();
        services.AddLatticeTreeAdminApiGrpc();
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<LatticeTreeAdminApiGrpcOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.RequireAuthorization, Is.True);
            Assert.That(options.CredentialHeaderName, Is.EqualTo("authorization"));
            Assert.That(options.CredentialScheme, Is.EqualTo("Bearer"));
            Assert.That(options.AdvertisedAuthSchemes, Is.Empty);
        });
    }

    [Test]
    public void AddLatticeTreeAdminApiGrpc_applies_a_supplied_configure_delegate()
    {
        var services = new ServiceCollection();
        services.AddLatticeTreeAdminApiGrpc(o =>
        {
            o.RequireAuthorization = false;
            o.CredentialHeaderName = "x-lattice-token";
        });
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<LatticeTreeAdminApiGrpcOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.RequireAuthorization, Is.False);
            Assert.That(options.CredentialHeaderName, Is.EqualTo("x-lattice-token"));
        });
    }

    [Test]
    public void AddLatticeTreeAdminApiGrpc_defaults_to_the_deny_everything_authorizer()
    {
        var services = new ServiceCollection();
        services.AddLatticeTreeAdminApiGrpc();
        using var provider = services.BuildServiceProvider();

        Assert.That(
            provider.GetRequiredService<ILatticeTreeAdminApiAuthorizer>(),
            Is.InstanceOf<DenyTreeAdminApiAuthorizer>());
    }

    [Test]
    public void AddLatticeTreeAdminApiGrpc_preserves_a_host_registered_authorizer()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ILatticeTreeAdminApiAuthorizer, AllowAllTreeAdminApiAuthorizer>();
        services.AddLatticeTreeAdminApiGrpc();
        using var provider = services.BuildServiceProvider();

        Assert.That(
            provider.GetRequiredService<ILatticeTreeAdminApiAuthorizer>(),
            Is.InstanceOf<AllowAllTreeAdminApiAuthorizer>());
    }

    [Test]
    public void AddLatticeTreeAdminApiGrpc_registers_the_default_credential_bridge_and_scheme_source()
    {
        var services = new ServiceCollection();
        services.AddLatticeTreeAdminApiGrpc();
        using var provider = services.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetRequiredService<ILatticeTreeAdminApiCredentialBridge>(), Is.Not.Null);
            Assert.That(
                provider.GetRequiredService<ILatticeTreeAdminApiAuthSchemeSource>(),
                Is.InstanceOf<OptionsLatticeTreeAdminApiAuthSchemeSource>());
        });
    }

    [Test]
    public void AddLatticeTreeAdminApiGrpc_is_idempotent()
    {
        var services = new ServiceCollection();
        services.AddLatticeTreeAdminApiGrpc();
        services.AddLatticeTreeAdminApiGrpc();
        using var provider = services.BuildServiceProvider();

        Assert.That(
            provider.GetServices<ILatticeTreeAdminApiAuthorizer>().Count(),
            Is.EqualTo(1));
    }

    [Test]
    public void AddLatticeTreeAdminApiGrpc_registers_the_method_definitions_and_populates_the_static_holder()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        services.AddLatticeTreeAdminApiGrpc();
        using var provider = services.BuildServiceProvider();

        var methods = provider.GetRequiredService<LatticeTreeAdminGrpcMethods>();

        Assert.Multiple(() =>
        {
            Assert.That(methods, Is.Not.Null);
            Assert.That(LatticeTreeAdminGrpcMethodsHolder.Current, Is.SameAs(methods));
        });
    }

    [Test]
    public void MapLatticeTreeAdminApiGrpc_rejects_a_null_endpoint_builder()
    {
        Assert.Throws<ArgumentNullException>(() =>
            LatticeTreeAdminApiGrpcServiceCollectionExtensions.MapLatticeTreeAdminApiGrpc(null!));
    }
}
