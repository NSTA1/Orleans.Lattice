using Grpc.Core;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.Schema;

[TestFixture]
public class SchemaVersioningServiceTests
{
    private static SchemaVersioningService Create(FakeSchemaAdminClient client) => new(client);

    [Test]
    public void Constructor_null_client_throws()
    {
        Assert.That(() => new SchemaVersioningService(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task GetVersionConfigAsync_success_returns_config()
    {
        var config = new LatticeSchemaVersionConfig(7, 3);
        var client = new FakeSchemaAdminClient { VersionConfigResult = config };
        var service = Create(client);

        var view = await service.GetVersionConfigAsync("t");

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Value.SchemaId, Is.EqualTo(7u));
            Assert.That(view.Value.TargetVersion, Is.EqualTo(3u));
        });
    }

    [Test]
    public async Task GetVersionConfigAsync_unversioned_is_success_with_default_value()
    {
        var client = new FakeSchemaAdminClient { VersionConfigResult = null };
        var service = Create(client);

        var view = await service.GetVersionConfigAsync("t");

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Value.TargetVersion, Is.EqualTo(0u));
        });
    }

    [Test]
    public async Task GetVersionConfigAsync_denied_returns_denied_view()
    {
        var client = new FakeSchemaAdminClient { ReadThrows = new LatticeAuthorizationDeniedException("denied") };
        var service = Create(client);

        var view = await service.GetVersionConfigAsync("t");

        Assert.That(view.Status, Is.EqualTo(SchemaOperationStatus.Denied));
    }

    [Test]
    public async Task GetVersionConfigAsync_transport_failure_returns_failed_view()
    {
        var client = new FakeSchemaAdminClient { ReadThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")) };
        var service = Create(client);

        var view = await service.GetVersionConfigAsync("t");

        Assert.That(view.Status, Is.EqualTo(SchemaOperationStatus.Failed));
    }

    [Test]
    public void GetVersionConfigAsync_empty_tree_throws()
    {
        var service = Create(new FakeSchemaAdminClient());

        Assert.That(() => service.GetVersionConfigAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task SetVersionConfigAsync_forwards_and_succeeds()
    {
        var client = new FakeSchemaAdminClient();
        var service = Create(client);
        var config = new LatticeSchemaVersionConfig(5, 2);

        var result = await service.SetVersionConfigAsync("t", config);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(client.LastSetVersionConfig, Is.EqualTo(config));
        });
    }

    [Test]
    public void SetVersionConfigAsync_empty_tree_throws()
    {
        var service = Create(new FakeSchemaAdminClient());

        Assert.That(() => service.SetVersionConfigAsync(string.Empty, new LatticeSchemaVersionConfig(1, 1)),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task SetVersionConfigAsync_denied_folds_into_denied_result()
    {
        var client = new FakeSchemaAdminClient { MutationThrows = new LatticeAuthorizationDeniedException("denied") };
        var service = Create(client);

        var result = await service.SetVersionConfigAsync("t", new LatticeSchemaVersionConfig(1, 1));

        Assert.That(result.Status, Is.EqualTo(SchemaOperationStatus.Denied));
    }

    [Test]
    public async Task AdvanceTargetVersionAsync_forwards_and_succeeds()
    {
        var client = new FakeSchemaAdminClient { AdvanceResult = new LatticeSchemaVersionConfig(1, 4) };
        var service = Create(client);

        var result = await service.AdvanceTargetVersionAsync("t", 4);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(client.LastAdvanceTargetVersion, Is.EqualTo(4u));
        });
    }

    [Test]
    public void AdvanceTargetVersionAsync_empty_tree_throws()
    {
        var service = Create(new FakeSchemaAdminClient());

        Assert.That(() => service.AdvanceTargetVersionAsync(string.Empty, 2), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task AdvanceTargetVersionAsync_transport_failure_folds_into_failed_result()
    {
        var client = new FakeSchemaAdminClient { MutationThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")) };
        var service = Create(client);

        var result = await service.AdvanceTargetVersionAsync("t", 2);

        Assert.That(result.Status, Is.EqualTo(SchemaOperationStatus.Failed));
    }

    [Test]
    public async Task AdvanceAndMigrateAsync_completed_report_reports_success()
    {
        var client = new FakeSchemaAdminClient
        {
            RemediationResult = LatticeSchemaRemediationReport.Completed(12, "dest", "op-1"),
        };
        var service = Create(client);

        var result = await service.AdvanceAndMigrateAsync("t", 3);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(client.LastAdvanceTargetVersion, Is.EqualTo(3u));
        });
    }

    [Test]
    public void AdvanceAndMigrateAsync_empty_tree_throws()
    {
        var service = Create(new FakeSchemaAdminClient());

        Assert.That(() => service.AdvanceAndMigrateAsync(string.Empty, 2), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task MigrateToTargetVersionAsync_aborted_report_still_reports_success_envelope()
    {
        var client = new FakeSchemaAdminClient
        {
            RemediationResult = LatticeSchemaRemediationReport.Aborted(4, "k", "bad", new byte[] { 1 }, "op-2"),
        };
        var service = Create(client);

        var result = await service.MigrateToTargetVersionAsync("t");

        Assert.That(result.IsSuccess, Is.True);
    }

    [Test]
    public void MigrateToTargetVersionAsync_empty_tree_throws()
    {
        var service = Create(new FakeSchemaAdminClient());

        Assert.That(() => service.MigrateToTargetVersionAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task MigrateToTargetVersionAsync_denied_folds_into_denied_result()
    {
        var client = new FakeSchemaAdminClient { MutationThrows = new LatticeAuthorizationDeniedException("denied") };
        var service = Create(client);

        var result = await service.MigrateToTargetVersionAsync("t");

        Assert.That(result.Status, Is.EqualTo(SchemaOperationStatus.Denied));
    }

    [Test]
    public async Task ClearVersionConfigAsync_removed_reports_success()
    {
        var client = new FakeSchemaAdminClient { ClearVersionConfigResult = true };
        var service = Create(client);

        var result = await service.ClearVersionConfigAsync("t");

        Assert.That(result.IsSuccess, Is.True);
    }

    [Test]
    public async Task ClearVersionConfigAsync_absent_still_reports_success()
    {
        var client = new FakeSchemaAdminClient { ClearVersionConfigResult = false };
        var service = Create(client);

        var result = await service.ClearVersionConfigAsync("t");

        Assert.That(result.IsSuccess, Is.True);
    }

    [Test]
    public void ClearVersionConfigAsync_empty_tree_throws()
    {
        var service = Create(new FakeSchemaAdminClient());

        Assert.That(() => service.ClearVersionConfigAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task GetRemediationStatusAsync_success_returns_report()
    {
        var client = new FakeSchemaAdminClient
        {
            RemediationResult = LatticeSchemaRemediationReport.Completed(9, "dest", "op-3"),
        };
        var service = Create(client);

        var view = await service.GetRemediationStatusAsync("t");

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Value.ScannedCount, Is.EqualTo(9));
        });
    }

    [Test]
    public async Task GetRemediationStatusAsync_denied_returns_denied_view()
    {
        var client = new FakeSchemaAdminClient { ReadThrows = new LatticeAuthorizationDeniedException("denied") };
        var service = Create(client);

        var view = await service.GetRemediationStatusAsync("t");

        Assert.That(view.Status, Is.EqualTo(SchemaOperationStatus.Denied));
    }

    [Test]
    public void GetRemediationStatusAsync_empty_tree_throws()
    {
        var service = Create(new FakeSchemaAdminClient());

        Assert.That(() => service.GetRemediationStatusAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }
}
