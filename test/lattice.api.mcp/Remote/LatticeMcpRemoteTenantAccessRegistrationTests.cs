using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// The remote head's delegated tenant access facades: with a tenant endpoint
/// configured, one tenant-administration gRPC client serves both
/// <see cref="ILatticeTenantDirectoryAdmin"/> and <see cref="ILatticeTenantPolicyAdmin"/>,
/// so the tenant access tools can bind to them; without one, neither is registered.
/// </summary>
[TestFixture]
public sealed class LatticeMcpRemoteTenantAccessRegistrationTests
{
    private static readonly FakeCallInvoker Idle = new(_ => throw new InvalidOperationException());

    private static LatticeApiMcpRemoteEndpoint Endpoint(string address)
        => new() { Endpoint = address, CallInvoker = Idle };

    [Test]
    public void A_tenant_endpoint_registers_one_client_serving_both_tenant_access_facades()
    {
        using var provider = new ServiceCollection()
            .AddLatticeMcpRemote(o => o.TenantAdmin = Endpoint("https://tenant:5007"))
            .BuildServiceProvider();

        var directory = provider.GetService<ILatticeTenantDirectoryAdmin>();
        var policy = provider.GetService<ILatticeTenantPolicyAdmin>();

        Assert.Multiple(() =>
        {
            Assert.That(directory, Is.TypeOf<LatticeTenantAdminApiGrpcClient>());
            Assert.That(policy, Is.SameAs(directory), "one client serves both facades");
            Assert.That(provider.GetService<ILatticeTenantDirectoryAdmin>(), Is.SameAs(directory), "the client is a singleton");
        });
    }

    [Test]
    public void A_tenant_endpoint_with_tenant_control_lights_up_every_tenant_access_tool()
    {
        using var provider = new ServiceCollection()
            .AddLatticeMcpRemote(o =>
            {
                o.TenantAdmin = Endpoint("https://tenant:5007");
                o.EnableTenantControl = true;
            })
            .BuildServiceProvider();

        var group = provider.GetServices<ILatticeApiMcpToolGroup>().OfType<TenantAccessToolGroup>().Single();

        Assert.That(
            group.Tools.Select(tool => tool.ProtocolTool.Name),
            Is.EquivalentTo(TenantAccessToolGroup.DirectoryToolNames.Concat(TenantAccessToolGroup.PolicyToolNames)));
    }

    [Test]
    public void No_tenant_endpoint_registers_neither_tenant_access_facade()
    {
        using var provider = new ServiceCollection()
            .AddLatticeMcpRemote(o => o.Auth = Endpoint("https://auth:5003"))
            .BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetService<ILatticeTenantDirectoryAdmin>(), Is.Null);
            Assert.That(provider.GetService<ILatticeTenantPolicyAdmin>(), Is.Null);
        });
    }

    [Test]
    public void A_host_registered_facade_is_kept()
    {
        var own = NSubstitute.Substitute.For<ILatticeTenantPolicyAdmin>();
        var services = new ServiceCollection();
        services.AddSingleton(own);

        using var provider = services
            .AddLatticeMcpRemote(o => o.TenantAdmin = Endpoint("https://tenant:5007"))
            .BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetService<ILatticeTenantPolicyAdmin>(), Is.SameAs(own));
            Assert.That(provider.GetService<ILatticeTenantDirectoryAdmin>(), Is.TypeOf<LatticeTenantAdminApiGrpcClient>());
        });
    }
}
