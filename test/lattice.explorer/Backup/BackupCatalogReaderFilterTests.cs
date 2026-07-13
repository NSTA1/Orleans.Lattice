using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Backup;

namespace Orleans.Lattice.Explorer.Tests.Backup;

/// <summary>
/// Coverage for the filter / sort push-down the redesigned Existing Backups list
/// relies on: <see cref="BackupCatalogReader.LoadPageAsync"/> maps a
/// <see cref="BackupCatalogFilter"/> onto the request and always asks for the
/// newest-first ordering, and <see cref="BackupCatalogReader.LoadSummaryAsync"/>
/// gathers the distinct facet values and full backups for the filter row.
/// </summary>
[TestFixture]
public sealed class BackupCatalogReaderFilterTests
{
    private static BackupCatalogReader CreateReader(FakeBackupControlClient client) => new(client);

    [Test]
    public async Task LoadPageAsync_requests_newest_first_and_maps_the_filter()
    {
        var client = new FakeBackupControlClient
        {
            ListResult = new BackupCatalogPage { Entries = Array.Empty<BackupManifest>() },
        };

        var filter = new BackupCatalogFilter
        {
            Kind = BackupKind.Incremental,
            Scope = "orders",
            NamePrefix = "night",
            CreatedPrefix = "2024-02",
        };

        await CreateReader(client).LoadPageAsync(10, "cursor", filter);

        var request = client.LastListRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(request.OrderByCreatedDescending, Is.True);
            Assert.That(request.PageSize, Is.EqualTo(10));
            Assert.That(request.PageToken, Is.EqualTo("cursor"));
            Assert.That(request.Kind, Is.EqualTo(BackupKind.Incremental));
            Assert.That(request.TreeId, Is.EqualTo("orders"));
            Assert.That(request.NamePrefix, Is.EqualTo("night"));
            Assert.That(request.CreatedPrefix, Is.EqualTo("2024-02"));
        });
    }

    [Test]
    public async Task LoadPageAsync_with_no_filter_still_requests_newest_first()
    {
        var client = new FakeBackupControlClient
        {
            ListResult = new BackupCatalogPage { Entries = Array.Empty<BackupManifest>() },
        };

        await CreateReader(client).LoadPageAsync();

        Assert.Multiple(() =>
        {
            Assert.That(client.LastListRequest!.OrderByCreatedDescending, Is.True);
            Assert.That(client.LastListRequest!.Kind, Is.Null);
            Assert.That(client.LastListRequest!.NamePrefix, Is.Null);
        });
    }

    [Test]
    public async Task LoadSummaryAsync_gathers_distinct_sorted_facets()
    {
        var client = new FakeBackupControlClient
        {
            ListResult = new BackupCatalogPage
            {
                Entries = new[]
                {
                    SampleBackup.Manifest("f1", BackupKind.Full, treeId: "orders"),
                    SampleBackup.Manifest("i1", BackupKind.Incremental, treeId: "orders"),
                    SampleBackup.Manifest("f2", BackupKind.Full, treeId: "customers"),
                    SampleBackup.Manifest("m1", BackupKind.Full, treeId: "orders", setId: "set-1", setName: "nightly"),
                },
                NextPageToken = null,
            },
        };

        var summary = await CreateReader(client).LoadSummaryAsync();

        Assert.Multiple(() =>
        {
            Assert.That(summary.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(summary.Kinds, Is.EqualTo(new[] { BackupKind.Full, BackupKind.Incremental }));
            Assert.That(summary.Scopes, Is.EqualTo(new[] { "customers", "orders" }));
        });
    }

    [Test]
    public async Task LoadFullBackupsAsync_pushes_kind_and_tree_and_keeps_set_members()
    {
        var client = new FakeBackupControlClient
        {
            ListResult = new BackupCatalogPage
            {
                Entries = new[]
                {
                    SampleBackup.Manifest("f1", BackupKind.Full, treeId: "orders"),
                    SampleBackup.Manifest("m1", BackupKind.Full, treeId: "orders", setId: "set-1", setName: "nightly"),
                },
                NextPageToken = null,
            },
        };

        var fulls = await CreateReader(client).LoadFullBackupsAsync("orders");

        Assert.Multiple(() =>
        {
            // The kind / scope predicates are pushed into the index-backed query.
            Assert.That(client.LastListRequest!.Kind, Is.EqualTo(BackupKind.Full));
            Assert.That(client.LastListRequest!.TreeId, Is.EqualTo("orders"));
            Assert.That(client.LastListRequest!.OrderByCreatedDescending, Is.True);

            // Every full the tree owns is a candidate base, set members included.
            Assert.That(fulls.Select(m => m.Id), Is.EquivalentTo(new[] { "f1", "m1" }));
        });
    }

    [Test]
    public async Task LoadFullBackupsAsync_denied_folds_into_empty_list()
    {
        var client = new FakeBackupControlClient
        {
            ListThrows = new LatticeAuthorizationDeniedException("no"),
        };

        var fulls = await CreateReader(client).LoadFullBackupsAsync("orders");

        Assert.That(fulls, Is.Empty);
    }

    [Test]
    public async Task LoadSummaryAsync_denied_folds_into_status()
    {
        var client = new FakeBackupControlClient
        {
            ListThrows = new LatticeAuthorizationDeniedException("no"),
        };

        var summary = await CreateReader(client).LoadSummaryAsync();

        Assert.That(summary.Status, Is.EqualTo(BackupOperationStatus.Denied));
    }
}
