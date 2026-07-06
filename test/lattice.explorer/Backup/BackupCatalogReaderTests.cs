using Grpc.Core;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Backup;

namespace Orleans.Lattice.Explorer.Tests.Backup;

[TestFixture]
public class BackupCatalogReaderTests
{
    private static BackupCatalogReader CreateReader(FakeBackupControlClient client) => new(client);

    [Test]
    public void Constructor_null_client_throws()
    {
        Assert.That(() => new BackupCatalogReader(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task LoadPageAsync_success_returns_entries()
    {
        var client = new FakeBackupControlClient
        {
            ListResult = new BackupCatalogPage
            {
                Entries = new[] { SampleBackup.Manifest("b1") },
                NextPageToken = "next",
            },
        };

        var view = await CreateReader(client).LoadPageAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(view.Entries, Has.Count.EqualTo(1));
            Assert.That(view.NextPageToken, Is.EqualTo("next"));
        });
    }

    [Test]
    public async Task LoadPageAsync_denied_returns_denied_view()
    {
        var client = new FakeBackupControlClient
        {
            ListThrows = new LatticeAuthorizationDeniedException("no list for you"),
        };

        var view = await CreateReader(client).LoadPageAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.False);
            Assert.That(view.Status, Is.EqualTo(BackupOperationStatus.Denied));
            Assert.That(view.Message, Is.EqualTo("no list for you"));
            Assert.That(view.Entries, Is.Empty);
        });
    }

    [Test]
    public async Task LoadPageAsync_transport_failure_returns_failed_view()
    {
        var client = new FakeBackupControlClient
        {
            ListThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")),
        };

        var view = await CreateReader(client).LoadPageAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.Status, Is.EqualTo(BackupOperationStatus.Failed));
            Assert.That(view.Message, Does.Contain("Unavailable"));
        });
    }

    [Test]
    public async Task TriggerFullAsync_success_returns_success_result()
    {
        var client = new FakeBackupControlClient();

        var result = await CreateReader(client).TriggerFullAsync("nightly", BackupScopeSelector.WholeTree("tree-a"));

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(result.Message, Does.Contain("full-1"));
        });
    }

    [Test]
    public async Task TriggerFullAsync_denied_degrades_gracefully()
    {
        var client = new FakeBackupControlClient
        {
            MutationThrows = new LatticeAuthorizationDeniedException("capture denied"),
        };

        var result = await CreateReader(client).TriggerFullAsync("nightly", BackupScopeSelector.WholeTree("tree-a"));

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.False);
            Assert.That(result.Status, Is.EqualTo(BackupOperationStatus.Denied));
            Assert.That(result.Message, Is.EqualTo("capture denied"));
        });
    }

    [Test]
    public async Task TriggerIncrementalAsync_success_returns_success_result()
    {
        var client = new FakeBackupControlClient();

        var result = await CreateReader(client).TriggerIncrementalAsync("delta", BackupScopeSelector.WholeTree("tree-a"), "base-1");

        Assert.That(result.Message, Does.Contain("inc-1"));
    }

    [Test]
    public async Task RestoreAsync_success_reports_target_and_entries()
    {
        var client = new FakeBackupControlClient();

        var result = await CreateReader(client).RestoreAsync("b1", "tree-b");

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Message, Does.Contain("tree-b"));
            Assert.That(result.Message, Does.Contain("7"));
        });
    }

    [Test]
    public async Task RestoreAsync_transport_failure_returns_failed_result()
    {
        var client = new FakeBackupControlClient
        {
            MutationThrows = new RpcException(new Status(StatusCode.Internal, "boom")),
        };

        var result = await CreateReader(client).RestoreAsync("b1", "tree-b");

        Assert.That(result.Status, Is.EqualTo(BackupOperationStatus.Failed));
    }

    [Test]
    public async Task DeleteAsync_absent_reports_already_absent()
    {
        var client = new FakeBackupControlClient { DeleteResult = false };

        var result = await CreateReader(client).DeleteAsync("b1");

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Message, Does.Contain("already absent"));
        });
    }

    [Test]
    public void TriggerFullAsync_empty_name_throws()
    {
        var client = new FakeBackupControlClient();

        Assert.That(
            () => CreateReader(client).TriggerFullAsync(string.Empty, BackupScopeSelector.WholeTree("t")),
            Throws.InstanceOf<ArgumentException>());
    }
}
