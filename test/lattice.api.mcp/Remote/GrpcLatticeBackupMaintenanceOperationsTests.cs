using Orleans.Lattice.Api.Backup.Grpc;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for the backup maintenance starts (#4125) of
/// <see cref="GrpcLatticeBackupOperations"/>, the remote-host adapter that fronts
/// the accept-then-poll backup verbs over the backup-API gRPC client: each start
/// sends its arguments on the matching wire request and unwraps the handle.
/// Deterministic over a <see cref="FakeCallInvoker"/>.
/// </summary>
[TestFixture]
public sealed class GrpcLatticeBackupMaintenanceOperationsTests
{
    private static GrpcLatticeBackupOperations Adapter(FakeCallInvoker invoker)
        => new(RemoteTestSupport.BackupClient(invoker));

    private static LatticeOperationHandle Handle(string kind) => new()
    {
        OperationId = "op-1",
        Kind = kind,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = ["t"] },
        Created = true,
    };

    [Test]
    public async Task StartBackupHealthCheckAsync_sends_the_backup_and_tracking_id()
    {
        var invoker = new FakeCallInvoker(_ => Handle(BackupOperationKinds.HealthCheck));

        var handle = await Adapter(invoker).StartBackupHealthCheckAsync("bk-1", "track-1");

        var sent = (BackupHealthCheckRequestMessage)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(handle.Kind, Is.EqualTo(BackupOperationKinds.HealthCheck));
            Assert.That(sent.BackupId, Is.EqualTo("bk-1"));
            Assert.That(sent.TrackingOperationId, Is.EqualTo("track-1"));
        });
    }

    [Test]
    public async Task StartCatalogRebuildAsync_sends_the_tracking_id()
    {
        var invoker = new FakeCallInvoker(_ => Handle(BackupOperationKinds.CatalogRebuild));

        var handle = await Adapter(invoker).StartCatalogRebuildAsync("track-2");

        Assert.Multiple(() =>
        {
            Assert.That(handle.Kind, Is.EqualTo(BackupOperationKinds.CatalogRebuild));
            Assert.That(((BackupCatalogRebuildRequestMessage)invoker.LastRequest!).TrackingOperationId, Is.EqualTo("track-2"));
        });
    }

    [Test]
    public async Task StartCatalogScrubAsync_sends_the_prune_flag_and_tracking_id()
    {
        var invoker = new FakeCallInvoker(_ => Handle(BackupOperationKinds.CatalogScrub));

        await Adapter(invoker).StartCatalogScrubAsync(pruneOrphans: true, operationId: "track-3");

        var sent = (BackupCatalogScrubRequestMessage)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.PruneOrphans, Is.True);
            Assert.That(sent.TrackingOperationId, Is.EqualTo("track-3"));
        });
    }
}
