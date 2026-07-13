using Grpc.Core;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Backup;

[TestFixture]
public class BackupCapabilityServiceTests
{
    private static (BackupCapabilityService Service, ExplorerCapabilityStore Store) Create(FakeBackupControlClient client)
    {
        var store = new ExplorerCapabilityStore();
        return (new BackupCapabilityService(client, store), store);
    }

    [Test]
    public void Constructor_null_client_throws()
    {
        Assert.That(() => new BackupCapabilityService(null!, new ExplorerCapabilityStore()), Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_store_throws()
    {
        Assert.That(() => new BackupCapabilityService(new FakeBackupControlClient(), null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task RefreshAsync_list_reachable_sets_coarse_allowed()
    {
        var client = new FakeBackupControlClient { ListResult = new BackupCatalogPage() };
        var (service, store) = Create(client);

        await service.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.Current.BackupListAllowed, Is.True);
            Assert.That(client.LastListRequest!.PageSize, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RefreshAsync_denied_leaves_coarse_disabled()
    {
        var client = new FakeBackupControlClient
        {
            ListThrows = new LatticeAuthorizationDeniedException("denied"),
        };
        var (service, store) = Create(client);

        await service.RefreshAsync();

        Assert.That(store.Current.BackupListAllowed, Is.False);
    }

    [Test]
    public async Task RefreshAsync_transport_failure_leaves_coarse_disabled()
    {
        var client = new FakeBackupControlClient
        {
            ListThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")),
        };
        var (service, store) = Create(client);

        await service.RefreshAsync();

        Assert.That(store.Current.BackupListAllowed, Is.False);
    }

    [Test]
    public async Task RefreshAsync_unconfigured_session_leaves_coarse_disabled()
    {
        var client = new FakeBackupControlClient
        {
            ListThrows = new InvalidOperationException("explorer is not configured with an endpoint"),
        };
        var (service, store) = Create(client);

        await service.RefreshAsync();

        Assert.That(store.Current.BackupListAllowed, Is.False);
    }

    [Test]
    public async Task ProbeScopeAsync_maps_flags_into_store()
    {
        var client = new FakeBackupControlClient
        {
            CapabilitiesResult = new BackupScopeCapabilities
            {
                Scope = BackupScopeSelector.WholeTree("tree-a"),
                CanList = true,
                CanCapture = true,
                CanCaptureIncremental = true,
                CanRestore = false,
                CanDelete = true,
            },
        };
        var (service, store) = Create(client);

        var snapshot = await service.ProbeScopeAsync("tree-a");

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.CanList, Is.True);
            Assert.That(snapshot.CanCapture, Is.True);
            Assert.That(snapshot.CanCaptureIncremental, Is.True);
            Assert.That(snapshot.CanRestore, Is.False);
            Assert.That(snapshot.CanDelete, Is.True);
            Assert.That(store.Current.ForScope("tree-a").CanDelete, Is.True);
            // A scope that grants list access implies the coarse gate.
            Assert.That(store.Current.BackupListAllowed, Is.True);
            Assert.That(client.LastProbedScope!.TreeId, Is.EqualTo("tree-a"));
        });
    }

    [Test]
    public async Task ProbeScopeAsync_denied_stores_none_snapshot()
    {
        var client = new FakeBackupControlClient
        {
            CapabilitiesThrows = new LatticeAuthorizationDeniedException("nope"),
        };
        var (service, store) = Create(client);

        var snapshot = await service.ProbeScopeAsync("tree-a");

        Assert.Multiple(() =>
        {
            Assert.That(snapshot, Is.SameAs(BackupScopeCapabilitySnapshot.None));
            Assert.That(store.Current.ForScope("tree-a").CanList, Is.False);
        });
    }

    [Test]
    public void ProbeScopeAsync_empty_tree_throws()
    {
        var (service, _) = Create(new FakeBackupControlClient());

        Assert.That(() => service.ProbeScopeAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }
}
