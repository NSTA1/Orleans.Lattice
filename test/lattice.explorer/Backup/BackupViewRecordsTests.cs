using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Backup;

namespace Orleans.Lattice.Explorer.Tests.Backup;

/// <summary>
/// Unit tests for the Backup list view records (<see cref="BackupListView"/>,
/// <see cref="BackupCatalogSummary"/>, and <see cref="BackupRow"/>): their empty
/// singletons, success projections, and the row's set / incremental-chain and
/// distinct-tree derivations. They build on the existing <see cref="SampleBackup"/>
/// helper to construct well-formed member manifests.
/// </summary>
[TestFixture]
public class BackupViewRecordsTests
{
    private static BackupRow StandaloneRow(BackupKind kind) => new()
    {
        DisplayId = "b1",
        Name = "nightly",
        Kind = kind,
        CreatedAtUtc = DateTimeOffset.UnixEpoch,
        Members = new[] { SampleBackup.Manifest("b1", kind, "orders") },
    };

    [Test]
    public void BackupListView_Empty_is_successful_and_has_no_entries()
    {
        var view = BackupListView.Empty;

        Assert.Multiple(() =>
        {
            Assert.That(view.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Entries, Is.Empty);
            Assert.That(view.NextPageToken, Is.Null);
        });
    }

    [Test]
    public void BackupListView_failed_view_is_not_success_and_carries_message()
    {
        var view = new BackupListView { Status = BackupOperationStatus.Failed, Message = "gone" };

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.False);
            Assert.That(view.Message, Is.EqualTo("gone"));
        });
    }

    [Test]
    public void BackupCatalogSummary_Empty_is_successful_with_no_facets()
    {
        var summary = BackupCatalogSummary.Empty;

        Assert.Multiple(() =>
        {
            Assert.That(summary.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(summary.Kinds, Is.Empty);
            Assert.That(summary.Scopes, Is.Empty);
        });
    }

    [Test]
    public void BackupCatalogSummary_carries_supplied_facets()
    {
        var summary = new BackupCatalogSummary
        {
            Status = BackupOperationStatus.Succeeded,
            Kinds = new[] { BackupKind.Full, BackupKind.Incremental },
            Scopes = new[] { "orders", "shipments" },
        };

        Assert.Multiple(() =>
        {
            Assert.That(summary.Kinds, Has.Count.EqualTo(2));
            Assert.That(summary.Scopes, Does.Contain("shipments"));
        });
    }

    [Test]
    public void BackupRow_standalone_full_is_not_set_and_not_incremental_chain()
    {
        var row = StandaloneRow(BackupKind.Full);

        Assert.Multiple(() =>
        {
            Assert.That(row.IsSet, Is.False);
            Assert.That(row.IsIncrementalChain, Is.False);
            Assert.That(row.TreeIds, Is.EqualTo(new[] { "orders" }));
        });
    }

    [Test]
    public void BackupRow_standalone_incremental_is_incremental_chain()
    {
        var row = StandaloneRow(BackupKind.Incremental);

        Assert.Multiple(() =>
        {
            Assert.That(row.IsSet, Is.False);
            Assert.That(row.IsIncrementalChain, Is.True);
        });
    }

    [Test]
    public void BackupRow_set_row_is_set_and_not_incremental_chain()
    {
        var row = new BackupRow
        {
            SetId = "set-1",
            DisplayId = "set-1",
            Name = "nightly-set",
            Kind = BackupKind.Incremental,
            CreatedAtUtc = DateTimeOffset.UnixEpoch,
            Members = new[]
            {
                SampleBackup.Manifest("b1", BackupKind.Full, "orders"),
                SampleBackup.Manifest("b2", BackupKind.Full, "orders"),
                SampleBackup.Manifest("b3", BackupKind.Full, "shipments"),
            },
        };

        Assert.Multiple(() =>
        {
            Assert.That(row.IsSet, Is.True);
            Assert.That(row.IsIncrementalChain, Is.False);
            Assert.That(row.TreeIds, Is.EqualTo(new[] { "orders", "shipments" }));
        });
    }
}
