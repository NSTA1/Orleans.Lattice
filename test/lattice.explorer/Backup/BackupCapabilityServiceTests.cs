using Grpc.Core;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tests.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Backup;

/// <summary>
/// The Backups plugin's own access gate. It reproduces the coarse catalog gate
/// the area used to read from the shared capability record, now as the
/// plugin-level decision keyed under <see cref="BackupsPluginKeys.PluginId"/>,
/// plus the per-tree decisions the scope probe files.
/// </summary>
[TestFixture]
public class BackupCapabilityServiceTests
{
    private static readonly IExplorerPluginHostContext Context =
        PluginTestHost.Context(BackupsPluginKeys.PluginId);

    private static (BackupCapabilityService Service, ExplorerPluginAccessStore Store) Create(
        FakeBackupControlClient client)
    {
        var store = new ExplorerPluginAccessStore();
        return (new BackupCapabilityService(client, store), store);
    }

    [Test]
    public void Constructor_null_client_throws()
    {
        Assert.That(
            () => new BackupCapabilityService(null!, new ExplorerPluginAccessStore()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_store_throws()
    {
        Assert.That(
            () => new BackupCapabilityService(new FakeBackupControlClient(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ProbeAsync_null_context_throws()
    {
        var (service, _) = Create(new FakeBackupControlClient());

        Assert.That(async () => await service.ProbeAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task ProbeAsync_list_reachable_allows_the_plugin()
    {
        var client = new FakeBackupControlClient { ListResult = new BackupCatalogPage() };
        var (service, _) = Create(client);

        var access = await service.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(client.LastListRequest!.PageSize, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task ProbeAsync_denied_denies_the_plugin()
    {
        var client = new FakeBackupControlClient
        {
            ListThrows = new LatticeAuthorizationDeniedException("denied"),
        };
        var (service, _) = Create(client);

        var access = await service.ProbeAsync(Context);

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
    }

    [Test]
    public async Task ProbeAsync_transport_failure_denies_the_plugin()
    {
        var client = new FakeBackupControlClient
        {
            ListThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")),
        };
        var (service, _) = Create(client);

        var access = await service.ProbeAsync(Context);

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
    }

    [Test]
    public async Task ProbeAsync_unconfigured_session_denies_the_plugin()
    {
        var client = new FakeBackupControlClient
        {
            ListThrows = new InvalidOperationException("explorer is not configured with an endpoint"),
        };
        var (service, _) = Create(client);

        var access = await service.ProbeAsync(Context);

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
    }

    [Test]
    public async Task ProbeAsync_never_reports_unavailable_so_a_denied_area_greys_out_rather_than_hiding()
    {
        var client = new FakeBackupControlClient
        {
            ListThrows = new LatticeAuthorizationDeniedException("denied"),
        };
        var (service, _) = Create(client);

        var access = await service.ProbeAsync(Context);

        Assert.That(access.IsVisible, Is.True);
    }

    [Test]
    public async Task ProbeScopeAsync_maps_every_flag_onto_its_own_scoped_key()
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

            Assert.That(Scope(store, BackupsPluginKeys.ListScope("tree-a")), Is.True);
            Assert.That(Scope(store, BackupsPluginKeys.CaptureScope("tree-a")), Is.True);
            Assert.That(Scope(store, BackupsPluginKeys.CaptureIncrementalScope("tree-a")), Is.True);
            Assert.That(Scope(store, BackupsPluginKeys.RestoreScope("tree-a")), Is.False);
            Assert.That(Scope(store, BackupsPluginKeys.DeleteScope("tree-a")), Is.True);

            // A scope that grants list access implies the plugin-level gate.
            Assert.That(store.Get(BackupsPluginKeys.PluginId).IsAllowed, Is.True);
            Assert.That(client.LastProbedScope!.TreeId, Is.EqualTo("tree-a"));
        });
    }

    [Test]
    public async Task ProbeScopeAsync_denied_files_a_denied_scope_and_leaves_the_plugin_closed()
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
            Assert.That(Scope(store, BackupsPluginKeys.ListScope("tree-a")), Is.False);
            Assert.That(store.Get(BackupsPluginKeys.PluginId).IsAllowed, Is.False);
        });
    }

    [Test]
    public async Task A_scope_grant_keeps_the_plugin_open_across_a_later_coarse_denial()
    {
        // The retired capability record kept its per-scope map across a coarse
        // re-probe, and the area's enable rule was "coarse OR any scope grants
        // list". The keyed model must reproduce that exactly.
        var client = new FakeBackupControlClient
        {
            CapabilitiesResult = new BackupScopeCapabilities
            {
                Scope = BackupScopeSelector.WholeTree("tree-a"),
                CanList = true,
            },
        };
        var (service, _) = Create(client);
        await service.ProbeScopeAsync("tree-a");

        client.ListThrows = new LatticeAuthorizationDeniedException("denied");
        var access = await service.ProbeAsync(Context);

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
    }

    [Test]
    public void ProbeScopeAsync_empty_tree_throws()
    {
        var (service, _) = Create(new FakeBackupControlClient());

        Assert.That(() => service.ProbeScopeAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    private static bool Scope(ExplorerPluginAccessStore store, string scope) =>
        store.Get(BackupsPluginKeys.PluginId, scope).IsAllowed;
}
