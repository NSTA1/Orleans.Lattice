using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Shell.Areas.Backups;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Backups;

/// <summary>
/// The area's services: the probes, the completions, the owning-app lookup, and
/// the artifact stream.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupsServiceTests : BackupsTestContext
{
    [Test]
    public async Task A_scope_probe_that_fails_allows_nothing()
    {
        Backups.Probe = _ => Task.FromException<BackupScopeCapabilities>(new InvalidOperationException("down"));

        var capabilities = await Access.ProbeAsync(BackupScopeSelector.WholeTree("orders"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(capabilities.Scope.TreeId, Is.EqualTo("orders"));
            Assert.That(capabilities.CanList || capabilities.CanCapture || capabilities.CanCaptureIncremental || capabilities.CanRestore || capabilities.CanDelete, Is.False);
        });
    }

    [Test]
    public async Task Health_availability_is_remembered_and_a_fault_reads_as_unavailable()
    {
        var calls = 0;
        Backups.HealthAvailable = () => ++calls == 1 ? Task.FromException<bool>(new InvalidOperationException()) : Task.FromResult(true);

        Assert.Multiple(async () =>
        {
            Assert.That(await Access.IsHealthMonitoringAvailableAsync(CancellationToken.None), Is.False);
            Assert.That(await Access.IsHealthMonitoringAvailableAsync(CancellationToken.None), Is.True);
            Assert.That(await Access.IsHealthMonitoringAvailableAsync(CancellationToken.None), Is.True);
            Assert.That(calls, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task An_inventory_that_is_not_served_withdraws_the_catalogue_extensions()
    {
        Assert.That(Access.ExtensionsServed, Is.Null);

        var inventory = await Access.GetInventoryAsync(CancellationToken.None);
        var again = await Access.GetInventoryAsync(CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(inventory, Is.Null);
            Assert.That(again, Is.Null);
            Assert.That(Access.ExtensionsServed, Is.False);
            Assert.That(await Access.AreExtensionsServedAsync(CancellationToken.None), Is.False);
            Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.GetInventoryAsync)), Is.EqualTo(1), "a connection that does not serve it is not asked again");
        });
    }

    [Test]
    public async Task A_served_or_denied_inventory_keeps_the_extensions_offered()
    {
        Backups.Inventory = () => Task.FromException<BackupInventoryReport>(new LatticeAuthorizationDeniedException());
        Assert.That(await Access.GetInventoryAsync(CancellationToken.None), Is.Null);
        Assert.That(Access.ExtensionsServed, Is.True);

        var report = new BackupInventoryReport(1, 1, 1, 0, null, null, 0, 0, 0);
        Backups.Inventory = () => Task.FromResult(report);
        Assert.That(await Access.GetInventoryAsync(CancellationToken.None), Is.SameAs(report));
    }

    [Test]
    public async Task An_inconclusive_inventory_leaves_the_extensions_offered()
    {
        Backups.Inventory = () => Task.FromException<BackupInventoryReport>(new InvalidOperationException("down"));

        Assert.Multiple(async () =>
        {
            Assert.That(await Access.AreExtensionsServedAsync(CancellationToken.None), Is.True);
            Assert.That(Access.ExtensionsServed, Is.Null);
        });
        Access.MarkExtensionsNotServed();
        Assert.That(Access.ExtensionsServed, Is.False);
    }

    [Test]
    public async Task Backup_id_completions_come_from_the_id_ordered_stream_and_stop_early()
    {
        Seed(
            FakeBackupControl.Manifest("aa01", name: "one", tree: "a/crm/orders"),
            FakeBackupControl.Manifest("ab02", name: "two"),
            FakeBackupControl.Manifest("ab03", name: "three"),
            FakeBackupControl.Manifest("zz99", name: "four"));

        var results = await Completions.CompleteAsync(Query("backup:AB"), CancellationToken.None);
        var byAddress = await Completions.CompleteAsync(new AddressQuery("/backups/aa", AddressQueryMode.Address, ExplorerAddress.Home), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(results.Select(result => result.Label), Is.EqualTo(new[] { "backup:ab02", "backup:ab03" }));
            Assert.That(results[0].Target, Is.EqualTo(BackupsAddresses.Backup("ab02")));
            Assert.That(results[0].Detail, Is.EqualTo("two - orders"));
            Assert.That(byAddress.Single().Detail, Is.EqualTo("one - orders"), "an app tree is named by its app-local name");
        });
    }

    [Test]
    public async Task Free_text_completes_backup_names_newest_first()
    {
        Seed(FakeBackupControl.Manifest("b1", name: "nightly"), FakeBackupControl.Manifest("b2", name: "weekly"));

        var results = await Completions.CompleteAsync(Query("night"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(results.Single().Label, Is.EqualTo("backup:b1"));
            var request = Backups.LastOf<BackupCatalogRequest>(nameof(ILatticeBackupControl.ListBackupsAsync));
            Assert.That(request.NamePrefix, Is.EqualTo("night"));
            Assert.That(request.OrderByCreatedDescending, Is.True);
            Assert.That(request.PageSize, Is.EqualTo(AddressQuery.MaximumResults));
        });
    }

    [Test]
    public async Task Other_modes_and_empty_text_complete_nothing()
    {
        Seed(FakeBackupControl.Manifest("b1"));

        Assert.Multiple(async () =>
        {
            Assert.That(await Completions.CompleteAsync(new AddressQuery("crm", AddressQueryMode.App, ExplorerAddress.Home), CancellationToken.None), Is.Empty);
            Assert.That(await Completions.CompleteAsync(Query("  "), CancellationToken.None), Is.Empty);
            Assert.That(await Completions.CompleteAsync(new AddressQuery("/data/x", AddressQueryMode.Address, ExplorerAddress.Home), CancellationToken.None), Is.Empty);
            Assert.That(() => Completions.CompleteAsync(null!, CancellationToken.None).AsTask(), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task Id_completions_read_at_most_the_bound()
    {
        for (var i = 0; i < BackupsCompletionSource.MaximumScanned + 50; i++)
        {
            Backups.Catalogue.Add(FakeBackupControl.Manifest("a" + i.ToString("D5", System.Globalization.CultureInfo.InvariantCulture)));
        }

        var results = await Completions.CompleteAsync(Query("backup:"), CancellationToken.None);

        Assert.That(results, Has.Count.EqualTo(AddressQuery.MaximumResults));
    }

    [Test]
    public async Task The_owning_app_is_read_from_the_apps_control_first()
    {
        AppsControl.DescribeAsync("crm", Arg.Any<string?>(), Arg.Any<CancellationToken>()).Returns(new AppDescriptor
        {
            Slug = "crm",
            Version = "2.1.0",
            Provenance = new AppProvenanceDescriptor { Source = "in-image", Publisher = "Contoso" },
            Presentation = new AppPresentationDescriptor { DisplayName = "CRM" },
            Trees = [new AppTreeDescriptor { Name = "orders" }, new AppTreeDescriptor { Name = "search", Rebuildable = true }],
        });

        var info = await Apps.FindAsync(BackupTreeName.Parse("a/crm/search"), CancellationToken.None);
        var again = await Apps.FindAsync(BackupTreeName.Parse("a/crm/orders"), CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(info!.Label, Is.EqualTo("CRM"));
            Assert.That(info.IsRebuildable("search"), Is.True);
            Assert.That(again, Is.SameAs(info), "a found app is remembered");
            await AppsControl.Received(1).DescribeAsync("crm", Arg.Any<string?>(), Arg.Any<CancellationToken>());
            await Workspace.DidNotReceiveWithAnyArgs().DescribeMyAppAsync(default!, default);
        });
    }

    [Test]
    public async Task The_owning_app_falls_back_to_the_callers_workspace()
    {
        AppsControl.DescribeAsync("crm", Arg.Any<string?>(), Arg.Any<CancellationToken>()).ThrowsAsync(new LatticeAuthorizationDeniedException());
        Workspace.DescribeMyAppAsync("crm", Arg.Any<CancellationToken>()).Returns(new WorkspaceAppDescriptor
        {
            Slug = "crm",
            Version = "2.1.0",
            Trees = [new WorkspaceTreeDescriptor { Name = "search", Rebuildable = true }],
        });

        var info = await Apps.FindAsync(BackupTreeName.Parse("a/crm/search"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(info!.Label, Is.EqualTo("crm"));
            Assert.That(info.RebuildableTrees, Is.EqualTo(new[] { "search" }));
        });
    }

    [Test]
    public async Task A_caller_who_may_read_neither_apps_facade_learns_only_the_slug()
    {
        AppsControl.DescribeAsync("crm", Arg.Any<string?>(), Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException());
        Workspace.DescribeMyAppAsync("crm", Arg.Any<CancellationToken>()).ThrowsAsync(new LatticeAuthorizationDeniedException());

        Assert.Multiple(async () =>
        {
            Assert.That(await Apps.FindAsync(BackupTreeName.Parse("a/crm/search"), CancellationToken.None), Is.Null);
            Assert.That(await Apps.FindAsync(BackupTreeName.Parse("orders"), CancellationToken.None), Is.Null, "a tree no app owns has no app");
            Assert.That(() => Apps.FindAsync(null!, CancellationToken.None), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task Without_the_apps_facades_no_app_is_found()
    {
        var lookup = new BackupAppTrees(new ServiceCollection().BuildServiceProvider());

        Assert.That(await lookup.FindAsync(BackupTreeName.Parse("a/crm/search"), CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task The_artifact_stream_reads_every_chunk_in_order()
    {
        var bytes = Enumerable.Range(0, 11).Select(value => (byte)value).ToArray();
        Backups.Artifacts["a1"] = bytes;

        await using var stream = await BackupArtifactStream.OpenAsync(Backups.ExportArtifactAsync("b1", "a1"));
        using var copy = new MemoryStream();
        await stream.CopyToAsync(copy);

        Assert.Multiple(() =>
        {
            Assert.That(copy.ToArray(), Is.EqualTo(bytes));
            Assert.That(stream.Position, Is.EqualTo(11));
            Assert.That(stream.CanRead, Is.True);
            Assert.That(stream.CanSeek, Is.False);
            Assert.That(stream.CanWrite, Is.False);
            Assert.That(() => stream.Length, Throws.InstanceOf<NotSupportedException>());
            Assert.That(() => stream.Position = 0, Throws.InstanceOf<NotSupportedException>());
            Assert.That(() => stream.Seek(0, SeekOrigin.Begin), Throws.InstanceOf<NotSupportedException>());
            Assert.That(() => stream.SetLength(0), Throws.InstanceOf<NotSupportedException>());
            Assert.That(() => stream.Write([1], 0, 1), Throws.InstanceOf<NotSupportedException>());
            Assert.That(stream.Read(new byte[4], 0, 4), Is.Zero, "a drained stream reads nothing");
        });
        stream.Flush();
    }

    [Test]
    public void Opening_the_artifact_stream_surfaces_a_denial_before_any_byte_is_sent()
    {
        Backups.ExportFault = new LatticeAuthorizationDeniedException();

        Assert.That(async () => await BackupArtifactStream.OpenAsync(Backups.ExportArtifactAsync("b1", "a1")), Throws.InstanceOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task An_empty_artifact_reads_as_empty_and_disposes_once()
    {
        Backups.Artifacts["empty"] = [];

        var stream = await BackupArtifactStream.OpenAsync(Backups.ExportArtifactAsync("b1", "empty"));

        Assert.That(await stream.ReadAsync(new byte[8]), Is.Zero);
        Assert.That(await stream.ReadAsync(Memory<byte>.Empty), Is.Zero);
        await stream.DisposeAsync();
        stream.Dispose();
        Assert.That(() => new BackupArtifactStream(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task Saving_an_artifact_hands_the_stream_to_the_area_module()
    {
        var module = JSInterop.SetupModule(BackupsAssets.ModuleSpecifier);
        module.SetupVoid("saveArtifact", _ => true).SetVoidResult();
        var interop = Services.GetRequiredService<BackupsInterop>();

        await interop.SaveAsync("b1-a1.bin", new MemoryStream([1, 2, 3]));
        await interop.DisposeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(module.Invocations["saveArtifact"].Single().Arguments[0], Is.EqualTo("b1-a1.bin"));
            Assert.That(() => interop.SaveAsync("", new MemoryStream()), Throws.ArgumentException);
            Assert.That(() => interop.SaveAsync("x", null!), Throws.ArgumentNullException);
            Assert.That(() => new BackupsInterop(null!), Throws.ArgumentNullException);
        });
    }

    private BackupsAccess Access => Services.GetRequiredService<BackupsAccess>();

    private BackupsCompletionSource Completions => Services.GetRequiredService<BackupsCompletionSource>();

    private BackupAppTrees Apps => Services.GetRequiredService<BackupAppTrees>();

    private static AddressQuery Query(string text) => new(text, AddressQueryMode.Search, ExplorerAddress.Home);
}
