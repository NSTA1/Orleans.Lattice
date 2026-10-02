using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>The area's pure helpers: addresses, tree naming, text forms and fault sentences.</summary>
[TestFixture]
public sealed class BackupsHelperTests
{
    [Test]
    public void The_area_addresses_follow_the_route_grammar()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BackupsAddresses.Root.Format(), Is.EqualTo("/backups"));
            Assert.That(BackupsAddresses.Capture.Format(), Is.EqualTo("/backups/new"));
            Assert.That(BackupsAddresses.Schedules.Format(), Is.EqualTo("/backups/schedules"));
            Assert.That(BackupsAddresses.Health.Format(), Is.EqualTo("/backups/health"));
            Assert.That(BackupsAddresses.Maintenance.Format(), Is.EqualTo("/backups/maintenance"));
            Assert.That(BackupsAddresses.Backup("ab12").Format(), Is.EqualTo("/backups/ab12"));
            Assert.That(BackupsAddresses.Operation("3").Format(), Is.EqualTo("/backups/operations/3"));
            Assert.That(BackupsAddresses.HealthOf("ab12").Format(), Is.EqualTo("/backups/health?backup=ab12"));
            Assert.That(BackupsAddresses.SchedulesOf("orders").Format(), Is.EqualTo("/backups/schedules?tree=orders"));
            Assert.That(() => BackupsAddresses.Backup(""), Throws.ArgumentException);
            Assert.That(() => BackupsAddresses.Operation(""), Throws.ArgumentException);
            Assert.That(() => BackupsAddresses.HealthOf(""), Throws.ArgumentException);
            Assert.That(() => BackupsAddresses.SchedulesOf(""), Throws.ArgumentException);
        });
    }

    [Test]
    [TestCase("orders", null, "orders")]
    [TestCase("a/crm/orders", "crm", "orders")]
    [TestCase("t/acme/a/crm/orders", "crm", "orders")]
    [TestCase("t/acme/orders", null, "orders")]
    [TestCase("a/crm", null, "a/crm")]
    [TestCase("a//orders", null, "a//orders")]
    [TestCase("t/acme", null, "t/acme")]
    public void A_tree_id_names_its_app_local_tree_and_owning_app(string treeId, string? app, string name)
    {
        var tree = BackupTreeName.Parse(treeId);

        Assert.Multiple(() =>
        {
            Assert.That(tree.TreeId, Is.EqualTo(treeId));
            Assert.That(tree.AppSlug, Is.EqualTo(app));
            Assert.That(tree.Name, Is.EqualTo(name));
            Assert.That(tree.IsAppTree, Is.EqualTo(app is not null));
        });
    }

    [Test]
    public void An_empty_tree_id_is_refused() =>
        Assert.That(() => BackupTreeName.Parse(""), Throws.ArgumentException);

    [Test]
    public void The_app_label_falls_back_to_the_slug_and_knows_its_rebuildable_trees()
    {
        var named = new BackupAppInfo("crm", "CRM", ["cache"]);
        var bare = new BackupAppInfo("crm", null, []);

        Assert.Multiple(() =>
        {
            Assert.That(named.Label, Is.EqualTo("CRM"));
            Assert.That(bare.Label, Is.EqualTo("crm"));
            Assert.That(named.IsRebuildable("cache"), Is.True);
            Assert.That(named.IsRebuildable("orders"), Is.False);
        });
    }

    [Test]
    public void Text_forms_are_culture_invariant()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BackupsFormat.Time(new DateTimeOffset(2026, 9, 28, 15, 2, 11, TimeSpan.FromHours(1))), Is.EqualTo("2026-09-28 14:02:11 UTC"));
            Assert.That(BackupsFormat.Time(null, "Never"), Is.EqualTo("Never"));
            Assert.That(BackupsFormat.Bytes(512), Is.EqualTo("512 B"));
            Assert.That(BackupsFormat.Bytes(1536), Is.EqualTo("1.5 KiB"));
            Assert.That(BackupsFormat.Bytes(3L * 1024 * 1024 * 1024), Is.EqualTo("3 GiB"));
            Assert.That(BackupsFormat.Count(1204), Is.EqualTo("1,204"));
            Assert.That(BackupsFormat.Interval(TimeSpan.FromMinutes(90)), Is.EqualTo("1 h 30 min"));
            Assert.That(BackupsFormat.Interval(TimeSpan.FromDays(1)), Is.EqualTo("1 d"));
            Assert.That(BackupsFormat.Interval(TimeSpan.FromSeconds(20)), Is.EqualTo("20 s"));
            Assert.That(BackupsFormat.Interval(TimeSpan.Zero), Is.EqualTo("none"));
            Assert.That(BackupsFormat.Kind(BackupKind.Full), Is.EqualTo("Full"));
            Assert.That(BackupsFormat.Kind(BackupKind.Incremental), Is.EqualTo("Incremental"));
            Assert.That(BackupsFormat.Scope(BackupScopeSelector.WholeTree("t")), Is.EqualTo("Whole tree"));
            Assert.That(BackupsFormat.Scope(BackupScopeSelector.Prefix("t", "p/")), Is.EqualTo("Keys under prefix p/"));
            Assert.That(BackupsFormat.Scope(BackupScopeSelector.Key("t", "k")), Is.EqualTo("One key: k"));
            Assert.That(BackupsFormat.Health(BackupHealthStatus.Missing), Is.EqualTo("Missing"));
            Assert.That(BackupsFormat.Health(BackupHealthStatus.Unknown), Is.EqualTo("Unknown"));
            Assert.That(BackupsFormat.Outcome(BackupScopeRunOutcome.Denied), Is.EqualTo("Denied"));
            Assert.That(BackupsFormat.Outcome(BackupScopeRunOutcome.None), Is.EqualTo("No run yet"));
            Assert.That(BackupsFormat.OperationStatus(BackupOperationStatus.Cancelled), Is.EqualTo("Stopped"));
            Assert.That(BackupsFormat.ShortId("0123456789abcdef"), Is.EqualTo("0123456789ab"));
            Assert.That(BackupsFormat.Name(FakeBackupControl.Manifest("0123456789abcdef", name: " ")), Is.EqualTo("0123456789ab"));
            Assert.That(BackupsFormat.Name(FakeBackupControl.Manifest("b1", name: "nightly")), Is.EqualTo("nightly"));
        });
    }

    [Test]
    public void The_shortest_interval_is_the_one_minute_the_engine_raises_a_shorter_one_to() =>
        Assert.That(BackupsFormat.ShortestInterval, Is.EqualTo(TimeSpan.FromMinutes(1)));

    [Test]
    public void Every_fault_becomes_one_plain_sentence()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BackupsFaults.Describe(new LatticeAuthorizationDeniedException("x")), Is.EqualTo(BackupsFaults.NotPermitted));
            Assert.That(BackupsFaults.Describe(new UnauthorizedAccessException()), Is.EqualTo(BackupsFaults.NotPermitted));
            Assert.That(BackupsFaults.Describe(new NotSupportedException()), Is.EqualTo(BackupsFaults.NotServed));
            Assert.That(BackupsFaults.Describe(new KeyNotFoundException()), Is.EqualTo(BackupsFaults.NotFound));
            Assert.That(BackupsFaults.Describe(new ShellTransportException("down", isTransient: true, new IOException())), Is.EqualTo(BackupsFaults.Unreachable));
            Assert.That(BackupsFaults.Describe(new ShellTransportException("bad", isTransient: false, new IOException())), Is.EqualTo("The operation failed: bad"));
            Assert.That(BackupsFaults.Describe(new InvalidOperationException("the manifest is absent from the sink")), Does.StartWith(BackupsFaults.UnsharedStore));
            Assert.That(BackupsFaults.Describe(new InvalidOperationException("busy")), Is.EqualTo("The operation could not be completed: busy"));
            Assert.That(BackupsFaults.Describe(new InvalidOperationException(" ")), Is.EqualTo("The operation could not be completed: no reason was given."));
            Assert.That(BackupsFaults.Describe(new LatticeRestoreValidationException("chain broken")), Is.EqualTo("The backup failed validation: chain broken"));
            Assert.That(BackupsFaults.Describe(new ArgumentException("bad name")), Is.EqualTo("The request was not accepted: bad name"));
            Assert.That(BackupsFaults.Describe(new IOException("disk")), Is.EqualTo("The operation failed unexpectedly."));
            Assert.That(BackupsFaults.IsDenied(new LatticeAuthorizationDeniedException()), Is.True);
            Assert.That(BackupsFaults.IsDenied(new InvalidOperationException()), Is.False);
            Assert.That(() => BackupsFaults.Describe(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Only_the_callers_own_cancellation_counts_as_cancellation()
    {
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        Assert.Multiple(() =>
        {
            Assert.That(BackupsFaults.IsCancellation(new OperationCanceledException(), cancelled.Token), Is.True);
            Assert.That(BackupsFaults.IsCancellation(new OperationCanceledException(), CancellationToken.None), Is.False);
            Assert.That(BackupsFaults.IsCancellation(new InvalidOperationException(), cancelled.Token), Is.False);
        });
    }

    [Test]
    public void The_area_assets_live_under_the_shell_content_root()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BackupsAssets.Stylesheet, Is.EqualTo("_content/Orleans.Lattice.Explorer.UI/backups/lattice-backups.css"));
            Assert.That(BackupsAssets.Module, Is.EqualTo("_content/Orleans.Lattice.Explorer.UI/backups/lattice-backups.js"));
            Assert.That(BackupsAssets.ModuleSpecifier, Is.EqualTo("./" + BackupsAssets.Module));
        });
    }

    [Test]
    public void An_artifact_file_name_is_safe_on_every_file_system()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BackupsInterop.FileNameFor("0123456789abcdef", "shard/0:1"), Is.EqualTo("0123456789ab-shard_0_1.bin"));
            Assert.That(BackupsInterop.FileNameFor("b1", "a.1"), Is.EqualTo("b1-a.1.bin"));
            Assert.That(() => BackupsInterop.FileNameFor("", "a"), Throws.ArgumentException);
            Assert.That(() => BackupsInterop.FileNameFor("b", ""), Throws.ArgumentException);
        });
    }
}
