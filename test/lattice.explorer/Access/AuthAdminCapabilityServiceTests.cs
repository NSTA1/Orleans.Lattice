using Grpc.Core;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Access;

[TestFixture]
public class AuthAdminCapabilityServiceTests
{
    private static (AuthAdminCapabilityService Service, ExplorerCapabilityStore Store) Create(FakeAuthAdminClient client)
    {
        var store = new ExplorerCapabilityStore();
        return (new AuthAdminCapabilityService(client, store), store);
    }

    [Test]
    public void Constructor_null_client_throws()
    {
        Assert.That(() => new AuthAdminCapabilityService(null!, new ExplorerCapabilityStore()), Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_store_throws()
    {
        Assert.That(() => new AuthAdminCapabilityService(new FakeAuthAdminClient(), null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task RefreshAsync_probe_reachable_sets_coarse_allowed()
    {
        var client = new FakeAuthAdminClient { UsersResult = new AuthUserPage() };
        var (service, store) = Create(client);

        await service.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.Current.AuthAdminAllowed, Is.True);
            Assert.That(client.LastUsersRequest!.PageSize, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RefreshAsync_denied_leaves_coarse_disabled()
    {
        var client = new FakeAuthAdminClient
        {
            ListUsersThrows = new LatticeAuthorizationDeniedException("denied"),
        };
        var (service, store) = Create(client);

        await service.RefreshAsync();

        Assert.That(store.Current.AuthAdminAllowed, Is.False);
    }

    [Test]
    public async Task RefreshAsync_transport_failure_leaves_coarse_disabled()
    {
        var client = new FakeAuthAdminClient
        {
            ListUsersThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")),
        };
        var (service, store) = Create(client);

        await service.RefreshAsync();

        Assert.That(store.Current.AuthAdminAllowed, Is.False);
    }

    [Test]
    public async Task RefreshAsync_unconfigured_session_leaves_coarse_disabled()
    {
        var client = new FakeAuthAdminClient
        {
            ListUsersThrows = new InvalidOperationException("explorer is not configured with an endpoint"),
        };
        var (service, store) = Create(client);

        await service.RefreshAsync();

        Assert.That(store.Current.AuthAdminAllowed, Is.False);
    }

    [Test]
    public async Task RefreshAsync_preserves_other_capability_fields()
    {
        var client = new FakeAuthAdminClient();
        var store = new ExplorerCapabilityStore();
        store.Set(ExplorerCapabilities.Empty with { BackupListAllowed = true });
        var service = new AuthAdminCapabilityService(client, store);

        await service.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.Current.AuthAdminAllowed, Is.True);
            Assert.That(store.Current.BackupListAllowed, Is.True);
        });
    }
}
