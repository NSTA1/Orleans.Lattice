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
    public async Task Revoking_the_scope_that_opened_the_plugin_closes_it_on_the_next_probe()
    {
        // The other direction of the rule above, and the one the retired record
        // also had: the sticky half is *derived* from the scope entries, so a
        // re-probe that finds the grant gone closes the area again. Latching the
        // answer in the service instead would leave the gate reporting Allowed
        // for the rest of the circuit's life.
        var client = new FakeBackupControlClient
        {
            CapabilitiesResult = new BackupScopeCapabilities
            {
                Scope = BackupScopeSelector.WholeTree("tree-a"),
                CanList = true,
            },
        };
        var (service, store) = Create(client);
        await service.ProbeScopeAsync("tree-a");
        client.ListThrows = new LatticeAuthorizationDeniedException("denied");
        var whileGranted = await service.ProbeAsync(Context);

        // The grant behind the scope goes away, and the scope is re-probed.
        client.CapabilitiesResult = new BackupScopeCapabilities
        {
            Scope = BackupScopeSelector.WholeTree("tree-a"),
            CanList = false,
        };
        await service.ProbeScopeAsync("tree-a");
        var afterRevocation = await service.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(whileGranted.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(
                afterRevocation.State,
                Is.EqualTo(ExplorerPluginAccessState.Denied),
                "a revoked scope must close the area rather than keep it latched open");
            Assert.That(Scope(store, BackupsPluginKeys.ListScope("tree-a")), Is.False);
        });
    }

    [Test]
    public async Task A_sign_out_that_resets_the_store_closes_a_plugin_a_scope_grant_had_opened()
    {
        // Reset is what the shell drives on sign-out: every decision is dropped
        // so a stale admission cannot survive an identity change. A gate whose
        // sticky half lived in the service would survive it regardless.
        var client = new FakeBackupControlClient
        {
            CapabilitiesResult = new BackupScopeCapabilities
            {
                Scope = BackupScopeSelector.WholeTree("tree-a"),
                CanList = true,
            },
        };
        var (service, store) = Create(client);
        await service.ProbeScopeAsync("tree-a");
        client.ListThrows = new LatticeAuthorizationDeniedException("denied");

        store.Reset();
        var access = await service.ProbeAsync(Context);

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
    }

    [Test]
    public async Task Only_a_list_scope_keeps_the_plugin_open_across_a_coarse_denial()
    {
        // A tree that grants capture / restore / delete but not list must not
        // hold the area open: the enable rule is "any scope grants *list*", and
        // reading the operation scopes as list grants would re-open an area the
        // caller cannot read a single backup in.
        var client = new FakeBackupControlClient
        {
            CapabilitiesResult = new BackupScopeCapabilities
            {
                Scope = BackupScopeSelector.WholeTree("tree-a"),
                CanList = false,
                CanCapture = true,
                CanCaptureIncremental = true,
                CanRestore = true,
                CanDelete = true,
            },
        };
        var (service, store) = Create(client);
        await service.ProbeScopeAsync("tree-a");

        client.ListThrows = new LatticeAuthorizationDeniedException("denied");
        var access = await service.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
            Assert.That(Scope(store, BackupsPluginKeys.CaptureScope("tree-a")), Is.True);
        });
    }

    [Test]
    public async Task A_denied_plugin_re_opens_when_the_coarse_probe_becomes_reachable_again()
    {
        // The allowed -> denied -> allowed transition. It is the shape that has
        // twice turned an idempotent initializer into a second-call no-op, so
        // the gate is asserted to make the round trip rather than only the first
        // leg of it.
        var client = new FakeBackupControlClient { ListResult = new BackupCatalogPage() };
        var (service, store) = Create(client);
        var first = await service.ProbeAsync(Context);

        client.ListThrows = new LatticeAuthorizationDeniedException("denied");
        var denied = await service.ProbeAsync(Context);

        client.ListThrows = null;
        var reopened = await service.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(first.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(denied.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
            Assert.That(reopened.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(store.Get(BackupsPluginKeys.PluginId).IsAllowed, Is.False,
                "the gate reports its decision; the refresher is what files it");
            Assert.That(client.ListCallCount, Is.EqualTo(3), "every probe is a fresh read");
        });
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
