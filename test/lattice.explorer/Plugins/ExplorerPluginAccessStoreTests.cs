using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

[TestFixture]
public sealed class ExplorerPluginAccessStoreTests
{
    private const string PluginId = "orleans.lattice.backups";

    [Test]
    public void Get_returns_denied_for_an_unwritten_key()
    {
        var store = new ExplorerPluginAccessStore();

        Assert.Multiple(() =>
        {
            Assert.That(store.Get(PluginId), Is.EqualTo(ExplorerPluginAccess.Denied));
            Assert.That(store.Get(PluginId, "tree-a"), Is.EqualTo(ExplorerPluginAccess.Denied));
            Assert.That(
                store.Get(new ExplorerPluginAccessKey(PluginId, "tree-a")),
                Is.EqualTo(ExplorerPluginAccess.Denied));
        });
    }

    [Test]
    public void Set_files_a_plugin_level_decision_and_raises_changed()
    {
        var store = new ExplorerPluginAccessStore();
        var changes = new List<ExplorerPluginAccessChange>();
        store.Changed += changes.Add;

        store.Set(PluginId, ExplorerPluginAccess.Allowed);

        Assert.Multiple(() =>
        {
            Assert.That(store.Get(PluginId), Is.EqualTo(ExplorerPluginAccess.Allowed));
            Assert.That(changes, Has.Count.EqualTo(1));
            Assert.That(changes[0].Key, Is.EqualTo(new ExplorerPluginAccessKey(PluginId)));
            Assert.That(changes[0].Access, Is.EqualTo(ExplorerPluginAccess.Allowed));
        });
    }

    [Test]
    public void Set_with_an_unchanged_value_does_not_raise_changed()
    {
        var store = new ExplorerPluginAccessStore();
        store.Set(PluginId, ExplorerPluginAccess.Allowed);

        var raised = 0;
        store.Changed += _ => raised++;
        store.Set(PluginId, ExplorerPluginAccess.Allowed);

        Assert.That(raised, Is.Zero);
    }

    [Test]
    public void Set_by_key_files_the_same_entry_as_the_string_overload()
    {
        var store = new ExplorerPluginAccessStore();

        store.Set(new ExplorerPluginAccessKey(PluginId), ExplorerPluginAccess.Allowed);

        Assert.That(store.Get(PluginId), Is.EqualTo(ExplorerPluginAccess.Allowed));
    }

    [Test]
    public void Scoped_entries_are_independent_of_the_plugin_level_entry()
    {
        var store = new ExplorerPluginAccessStore();

        store.Set(PluginId, ExplorerPluginAccess.Allowed);

        // An unprobed scope stays denied: a coarse plugin-level admission never
        // implies a per-scope one.
        Assert.That(store.Get(PluginId, "tree-a"), Is.EqualTo(ExplorerPluginAccess.Denied));

        store.Set(PluginId, "tree-a", ExplorerPluginAccess.Allowed);

        Assert.Multiple(() =>
        {
            Assert.That(store.Get(PluginId, "tree-a"), Is.EqualTo(ExplorerPluginAccess.Allowed));
            Assert.That(store.Get(PluginId, "tree-b"), Is.EqualTo(ExplorerPluginAccess.Denied));
            Assert.That(store.Get(PluginId), Is.EqualTo(ExplorerPluginAccess.Allowed));
        });
    }

    [Test]
    public void One_plugins_entry_never_disturbs_another_plugins_entry()
    {
        var store = new ExplorerPluginAccessStore();

        store.Set("a", ExplorerPluginAccess.Allowed);
        store.Set("b", ExplorerPluginAccess.Unavailable);

        Assert.Multiple(() =>
        {
            Assert.That(store.Get("a"), Is.EqualTo(ExplorerPluginAccess.Allowed));
            Assert.That(store.Get("b"), Is.EqualTo(ExplorerPluginAccess.Unavailable));
        });
    }

    [Test]
    public void Store_expresses_every_state_the_former_capability_record_collapsed()
    {
        var store = new ExplorerPluginAccessStore();

        // The former fat record's flags, one keyed entry each.
        store.Set("backups", ExplorerPluginAccess.Allowed);                       // BackupListAllowed
        store.Set("backups", "tree-a", ExplorerPluginAccess.Allowed);             // per-scope snapshot
        store.Set("access", ExplorerPluginAccess.AuthenticationRequired);         // AuthAdminAuthenticationRequired
        store.Set("access", "directory", ExplorerPluginAccess.Unavailable);       // AuthDirectoryAvailable == false
        store.Set("schema", ExplorerPluginAccess.Denied);                         // SchemaAllowed == false

        Assert.Multiple(() =>
        {
            Assert.That(store.Get("backups").IsAllowed, Is.True);
            Assert.That(store.Get("backups", "tree-a").IsAllowed, Is.True);
            Assert.That(store.Get("backups", "tree-b").IsAllowed, Is.False);
            Assert.That(
                store.Get("access").State,
                Is.EqualTo(ExplorerPluginAccessState.AuthenticationRequired));
            Assert.That(store.Get("access", "directory").State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(store.Get("schema").State, Is.EqualTo(ExplorerPluginAccessState.Denied));
        });
    }

    [Test]
    public void Clear_drops_only_the_named_plugins_entries()
    {
        var store = new ExplorerPluginAccessStore();
        store.Set("a", ExplorerPluginAccess.Allowed);
        store.Set("a", "tree-1", ExplorerPluginAccess.Allowed);
        store.Set("b", ExplorerPluginAccess.Allowed);

        store.Clear("a");

        Assert.Multiple(() =>
        {
            Assert.That(store.Get("a"), Is.EqualTo(ExplorerPluginAccess.Denied));
            Assert.That(store.Get("a", "tree-1"), Is.EqualTo(ExplorerPluginAccess.Denied));
            Assert.That(store.Get("b"), Is.EqualTo(ExplorerPluginAccess.Allowed));
        });
    }

    [Test]
    public void Clear_raises_changed_once_per_dropped_key()
    {
        var store = new ExplorerPluginAccessStore();
        store.Set("a", ExplorerPluginAccess.Allowed);
        store.Set("a", "tree-1", ExplorerPluginAccess.Allowed);

        var changes = new List<ExplorerPluginAccessChange>();
        store.Changed += changes.Add;
        store.Clear("a");

        Assert.Multiple(() =>
        {
            Assert.That(changes, Has.Count.EqualTo(2));
            Assert.That(changes.Select(c => c.Access), Is.All.EqualTo(ExplorerPluginAccess.Denied));
        });
    }

    [Test]
    public void Clear_of_an_unknown_plugin_is_a_no_op()
    {
        var store = new ExplorerPluginAccessStore();
        store.Set("a", ExplorerPluginAccess.Allowed);

        var raised = 0;
        store.Changed += _ => raised++;
        store.Clear("missing");

        Assert.Multiple(() =>
        {
            Assert.That(raised, Is.Zero);
            Assert.That(store.Get("a"), Is.EqualTo(ExplorerPluginAccess.Allowed));
        });
    }

    [Test]
    public void Reset_drops_every_entry_and_reads_fail_closed_again()
    {
        var store = new ExplorerPluginAccessStore();
        store.Set("a", ExplorerPluginAccess.Allowed);
        store.Set("b", "scope", ExplorerPluginAccess.Allowed);

        var changes = new List<ExplorerPluginAccessChange>();
        store.Changed += changes.Add;
        store.Reset();

        Assert.Multiple(() =>
        {
            Assert.That(store.Get("a"), Is.EqualTo(ExplorerPluginAccess.Denied));
            Assert.That(store.Get("b", "scope"), Is.EqualTo(ExplorerPluginAccess.Denied));
            Assert.That(store.Snapshot(), Is.Empty);
            Assert.That(changes, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void Reset_does_not_raise_for_an_entry_that_already_held_the_default_denial()
    {
        var store = new ExplorerPluginAccessStore();
        store.Set("a", ExplorerPluginAccess.Denied);

        var raised = 0;
        store.Changed += _ => raised++;
        store.Reset();

        Assert.Multiple(() =>
        {
            Assert.That(raised, Is.Zero);
            Assert.That(store.Snapshot(), Is.Empty);
        });
    }

    [Test]
    public void Snapshot_is_a_copy_and_does_not_track_later_writes()
    {
        var store = new ExplorerPluginAccessStore();
        store.Set("a", ExplorerPluginAccess.Allowed);

        var snapshot = store.Snapshot();
        store.Set("b", ExplorerPluginAccess.Allowed);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot, Has.Count.EqualTo(1));
            Assert.That(snapshot[new ExplorerPluginAccessKey("a")], Is.EqualTo(ExplorerPluginAccess.Allowed));
            Assert.That(store.Snapshot(), Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void Snapshot_of_an_untouched_store_is_empty()
    {
        Assert.That(new ExplorerPluginAccessStore().Snapshot(), Is.Empty);
    }

    [Test]
    public void Null_arguments_throw()
    {
        var store = new ExplorerPluginAccessStore();

        Assert.Multiple(() =>
        {
            Assert.That(() => store.Get(null!), Throws.ArgumentNullException);
            Assert.That(() => store.Get(null!, "scope"), Throws.ArgumentNullException);
            Assert.That(() => store.Get("a", null!), Throws.ArgumentNullException);
            Assert.That(() => store.Set(null!, ExplorerPluginAccess.Allowed), Throws.ArgumentNullException);
            Assert.That(() => store.Set(null!, "scope", ExplorerPluginAccess.Allowed), Throws.ArgumentNullException);
            Assert.That(() => store.Set("a", null!, ExplorerPluginAccess.Allowed), Throws.ArgumentNullException);
            Assert.That(
                () => store.Set(default(ExplorerPluginAccessKey), ExplorerPluginAccess.Allowed),
                Throws.ArgumentNullException);
            Assert.That(() => store.Clear(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Get_of_a_default_key_reads_denied_rather_than_throwing()
    {
        Assert.That(
            new ExplorerPluginAccessStore().Get(default(ExplorerPluginAccessKey)),
            Is.EqualTo(ExplorerPluginAccess.Denied));
    }
}
