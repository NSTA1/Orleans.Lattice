using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Access;

[TestFixture]
public class ExplorerAccessServiceCollectionExtensionsTests
{
    [Test]
    public void AddExplorerAccess_null_services_throws()
    {
        Assert.That(() => ((IServiceCollection)null!).AddExplorerAccess(), Throws.ArgumentNullException);
    }

    [Test]
    public void AddExplorerAccess_registers_navigation_store_and_access_services()
    {
        var services = new ServiceCollection();

        services.AddExplorerAccess();

        Assert.Multiple(() =>
        {
            Assert.That(services.Any(d => d.ServiceType == typeof(IExplorerCapabilityStore)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(IAuthAdminClient)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(IMembershipAdminService)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(IPolicyAdminService)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(IAuthAdminCapabilityService)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(PrincipalLabelResolver)), Is.True);
        });
    }

    [Test]
    public async Task AddExplorerAccess_principal_label_resolver_resolves_over_a_fake_client()
    {
        var services = new ServiceCollection();
        services.AddExplorerAccess();
        services.AddSingleton<IAuthAdminClient, FakeAuthAdminClient>();
        await using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<PrincipalLabelResolver>(), Is.InstanceOf<PrincipalLabelResolver>());
    }

    [Test]
    public async Task AddExplorerAccess_services_resolve_over_a_fake_client()
    {
        var services = new ServiceCollection();
        services.AddExplorerAccess();
        services.AddSingleton<IAuthAdminClient, FakeAuthAdminClient>();
        await using var provider = services.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetRequiredService<IMembershipAdminService>(), Is.InstanceOf<MembershipAdminService>());
            Assert.That(provider.GetRequiredService<IPolicyAdminService>(), Is.InstanceOf<PolicyAdminService>());
            Assert.That(provider.GetRequiredService<IAuthAdminCapabilityService>(), Is.InstanceOf<AuthAdminCapabilityService>());
        });
    }

    [Test]
    public async Task AddExplorerAccess_capability_store_resolves()
    {
        var services = new ServiceCollection();
        services.AddExplorerAccess();
        await using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<IExplorerCapabilityStore>(), Is.InstanceOf<ExplorerCapabilityStore>());
    }

    [Test]
    public async Task AddExplorerAccess_auth_admin_client_owns_orleans_serializer()
    {
        // Regression: the auth-admin client must build its own Orleans serializer
        // provider. If it captured the application root provider (which has no
        // AddSerializer), resolving its per-message serializers throws
        // InvalidOperationException before any network call, and the Access area
        // silently greys out. With a real serializer the call instead proceeds to
        // the transport and fails to reach the dead endpoint with an RpcException.
        var session = Substitute.For<IExplorerSession>();
        session.Current.Returns(new ExplorerConfiguration
        {
            Endpoint = "http://127.0.0.1:1",
            AllowUnencryptedHttp2 = true,
        });
        var auth = Substitute.For<IExplorerAuthSession>();

        var services = new ServiceCollection();
        services.AddSingleton(session);
        services.AddSingleton(auth);
        services.AddExplorerAccess();
        await using var provider = services.BuildServiceProvider();

        var client = provider.GetRequiredService<IAuthAdminClient>();

        Assert.That(
            async () => await client.ListGroupsAsync(new AuthPageRequest { PageSize = 1 }),
            Throws.InstanceOf<Grpc.Core.RpcException>());
    }
}
