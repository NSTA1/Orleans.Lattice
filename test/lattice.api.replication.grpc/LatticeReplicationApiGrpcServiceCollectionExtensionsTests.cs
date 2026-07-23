using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests;

/// <summary>
/// Verifies <see cref="LatticeReplicationApiGrpcServiceCollectionExtensions.AddLatticeReplicationApiGrpc"/>
/// wires the default-deny posture: the auto-registered authorizer is
/// <see cref="DenyAllReplicationApiAuthorizer"/>, the options default to
/// <c>RequireAuthorization = true</c>, the credential bridge and auth-scheme
/// source resolve, and a host-supplied authorizer registered first is preserved
/// (TryAdd does not overwrite it).
/// </summary>
public sealed class LatticeReplicationApiGrpcServiceCollectionExtensionsTests
{
    [Test]
    public void AddLatticeReplicationApiGrpc_registers_default_deny_authorizer()
    {
        using var provider = new ServiceCollection()
            .AddSerializer()
            .AddLatticeReplicationApiGrpc()
            .BuildServiceProvider();

        var authorizer = provider.GetRequiredService<ILatticeReplicationApiAuthorizer>();
        Assert.That(authorizer, Is.TypeOf<DenyAllReplicationApiAuthorizer>());
    }

    [Test]
    public void AddLatticeReplicationApiGrpc_defaults_require_authorization_to_true()
    {
        using var provider = new ServiceCollection()
            .AddSerializer()
            .AddLatticeReplicationApiGrpc()
            .BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<LatticeReplicationApiGrpcOptions>>().Value;
        Assert.That(options.RequireAuthorization, Is.True);
    }

    [Test]
    public void AddLatticeReplicationApiGrpc_honours_configure_delegate()
    {
        using var provider = new ServiceCollection()
            .AddSerializer()
            .AddLatticeReplicationApiGrpc(o => o.RequireAuthorization = false)
            .BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<LatticeReplicationApiGrpcOptions>>().Value;
        Assert.That(options.RequireAuthorization, Is.False);
    }

    [Test]
    public void AddLatticeReplicationApiGrpc_preserves_a_host_supplied_authorizer()
    {
        using var provider = new ServiceCollection()
            .AddSerializer()
            .AddSingleton<ILatticeReplicationApiAuthorizer, AllowAllReplicationApiAuthorizer>()
            .AddLatticeReplicationApiGrpc()
            .BuildServiceProvider();

        var authorizer = provider.GetRequiredService<ILatticeReplicationApiAuthorizer>();
        Assert.That(authorizer, Is.TypeOf<AllowAllReplicationApiAuthorizer>());
    }

    [Test]
    public void AddLatticeReplicationApiGrpc_registers_credential_bridge_and_auth_scheme_source()
    {
        using var provider = new ServiceCollection()
            .AddSerializer()
            .AddLatticeReplicationApiGrpc()
            .BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetService<ILatticeReplicationApiCredentialBridge>(), Is.Not.Null);
            Assert.That(provider.GetService<ILatticeReplicationApiAuthSchemeSource>(), Is.Not.Null);
        });
    }
}
