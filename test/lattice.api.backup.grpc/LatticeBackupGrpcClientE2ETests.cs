using System.Text;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// End-to-end coverage for the backup control-API gRPC binding driven purely
/// through the public <see cref="LatticeBackupApiGrpcClient"/> over a real gRPC
/// channel into an in-process <c>TestServer</c> hosting the service on a live
/// single-silo cluster. Proves the client re-exposes the whole
/// <see cref="ILatticeBackupControl"/> facade over the wire: capture, catalog
/// listing and streaming, describe, restore, delete, and chunk-wise artifact
/// export - and that a restored tree reproduces the captured values. The host
/// runs the permissive <see cref="AllowAllBackupApiAuthorizer"/> so these tests
/// exercise the transport, not the auth gate (covered separately).
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeBackupGrpcClientE2ETests
{
    private const string Source = "orders";

    private GrpcBackupClusterFixture _fixture = null!;
    private GrpcBackupHost _host = null!;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new GrpcBackupClusterFixture();
        await _fixture.InitializeAsync();
        _host = await _fixture.CreateGrpcHostAsync(new AllowAllBackupApiAuthorizer());
    }

    [TearDown]
    public async Task TearDown()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }

        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    [Test]
    public async Task Client_drives_create_list_describe_and_restore_end_to_end()
    {
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        await source.SetAsync("k2", Bytes("v2"));

        var full = await _host.Client.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Source)));

        await source.SetAsync("k3", Bytes("v3"));
        var incremental = await _host.Client.CreateIncrementalBackupAsync(
            new LatticeBackupIncrementalCaptureRequest(
                "incr", BackupScopeSelector.WholeTree(Source), full.BackupId));

        var page = await _host.Client.ListBackupsAsync(new BackupCatalogRequest());
        var listedIds = page.Entries.Select(e => e.Id).ToList();

        var description = await _host.Client.DescribeBackupAsync(incremental.BackupId);

        const string target = "orders-restored";
        var restore = await _host.Client.RestoreBackupAsync(
            new LatticeRestoreRequest(incremental.BackupId, target));

        var restored = _fixture.GrainFactory.GetGrain<ILattice>(target);

        Assert.Multiple(() =>
        {
            Assert.That(listedIds, Does.Contain(full.BackupId));
            Assert.That(listedIds, Does.Contain(incremental.BackupId));
            Assert.That(description, Is.Not.Null);
            Assert.That(description!.Manifest.Id, Is.EqualTo(incremental.BackupId));
            Assert.That(description.ChainBackupIds, Is.EqualTo(new[] { full.BackupId, incremental.BackupId }));
            Assert.That(restore.TargetTreeId, Is.EqualTo(target));
        });

        Assert.Multiple(() =>
        {
            Assert.That(Str(restored.GetAsync("k1").Result!), Is.EqualTo("v1"));
            Assert.That(Str(restored.GetAsync("k2").Result!), Is.EqualTo("v2"));
            Assert.That(Str(restored.GetAsync("k3").Result!), Is.EqualTo("v3"));
        });
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_round_trips_the_capability_set_over_the_wire()
    {
        var caps = await _host.Client.ProbeCapabilitiesAsync(BackupScopeSelector.WholeTree(Source));

        Assert.Multiple(() =>
        {
            Assert.That(caps.Scope, Is.EqualTo(BackupScopeSelector.WholeTree(Source)));
            Assert.That(caps.CanList, Is.True);
            Assert.That(caps.CanCapture, Is.True);
            Assert.That(caps.CanCaptureIncremental, Is.True);
            Assert.That(caps.CanRestore, Is.True);
            Assert.That(caps.CanDelete, Is.True);
        });
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_null_scope_throws()
    {
        Assert.That(
            async () => await _host.Client.ProbeCapabilitiesAsync(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task CreateBackupSetAsync_captures_a_multi_tree_set_over_the_wire()
    {
        var treeA = _fixture.GrainFactory.GetGrain<ILattice>("set-a");
        await treeA.SetAsync("k", Bytes("a"));
        var treeB = _fixture.GrainFactory.GetGrain<ILattice>("set-b");
        await treeB.SetAsync("k", Bytes("b"));

        var set = await _host.Client.CreateBackupSetAsync(
            new LatticeBackupSetCaptureRequest(
                "nightly-set",
                new[]
                {
                    BackupScopeSelector.WholeTree("set-a"),
                    BackupScopeSelector.WholeTree("set-b"),
                },
                crossTreeConsistent: true));

        var page = await _host.Client.ListBackupsAsync(new BackupCatalogRequest());
        var listedIds = page.Entries.Select(e => e.Id).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(set.Members, Has.Count.EqualTo(2));
            Assert.That(set.SetManifest.CrossTreeConsistent, Is.True);
            Assert.That(set.SetManifest.MemberBackupIds, Has.Count.EqualTo(2));
            foreach (var member in set.Members)
            {
                Assert.That(listedIds, Does.Contain(member.BackupId));
            }
        });
    }

    [Test]
    public async Task DescribeBackupAsync_returns_null_for_an_unknown_backup_over_the_wire()
    {
        var description = await _host.Client.DescribeBackupAsync(
            "0000000000000000000000000000000000000000000000000000000000000000");

        Assert.That(description, Is.Null);
    }

    [Test]
    public async Task DeleteBackupAsync_removes_a_backup_and_is_idempotent_over_the_wire()
    {
        var tree = _fixture.GrainFactory.GetGrain<ILattice>("tree-a");
        await tree.SetAsync("k", Bytes("a"));
        var backup = await _host.Client.CreateBackupAsync(
            new LatticeBackupCaptureRequest("a", BackupScopeSelector.WholeTree("tree-a")));

        var deleted = await _host.Client.DeleteBackupAsync(backup.BackupId);
        var deletedAgain = await _host.Client.DeleteBackupAsync(backup.BackupId);

        var page = await _host.Client.ListBackupsAsync(new BackupCatalogRequest());

        Assert.Multiple(() =>
        {
            Assert.That(deleted, Is.True);
            Assert.That(deletedAgain, Is.False);
            Assert.That(page.Entries.Select(e => e.Id), Does.Not.Contain(backup.BackupId));
        });
    }

    [Test]
    public async Task StreamBackupsAsync_streams_every_backup_as_an_async_enumerable()
    {
        var expected = new List<string>();
        for (var i = 0; i < 3; i++)
        {
            var treeId = $"stream-{i}";
            var tree = _fixture.GrainFactory.GetGrain<ILattice>(treeId);
            await tree.SetAsync("k", Bytes($"v{i}"));
            var backup = await _host.Client.CreateBackupAsync(
                new LatticeBackupCaptureRequest($"s{i}", BackupScopeSelector.WholeTree(treeId)));
            expected.Add(backup.BackupId);
        }

        var streamed = new List<string>();
        await foreach (var manifest in _host.Client.StreamBackupsAsync())
        {
            streamed.Add(manifest.Id);
        }

        Assert.Multiple(() =>
        {
            Assert.That(streamed, Is.EquivalentTo(expected));
            Assert.That(streamed, Is.Ordered.Using((IComparer<string>)StringComparer.Ordinal));
        });
    }

    [Test]
    public async Task ExportArtifactAsync_streams_an_owned_artifact_chunk_wise_as_an_async_enumerable()
    {
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        var backup = await _host.Client.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Source)));

        var artifactId = backup.Manifest.ContentDescriptors.Single().ArtifactId;

        var chunkCount = 0;
        long exportedBytes = 0;
        await foreach (var chunk in _host.Client.ExportArtifactAsync(backup.BackupId, artifactId))
        {
            chunkCount++;
            exportedBytes += chunk.Length;
        }

        Assert.Multiple(() =>
        {
            Assert.That(chunkCount, Is.GreaterThan(0));
            Assert.That(exportedBytes, Is.GreaterThan(0));
        });
    }

    [Test]
    public void ExportArtifactAsync_maps_an_unowned_artifact_to_not_found_over_the_wire()
    {
        Assert.That(
            async () =>
            {
                var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
                await source.SetAsync("k1", Bytes("v1"));
                var backup = await _host.Client.CreateBackupAsync(
                    new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Source)));

                await foreach (var _ in _host.Client.ExportArtifactAsync(backup.BackupId, "not-an-artifact"))
                {
                }
            },
            Throws.InstanceOf<global::Grpc.Core.RpcException>()
                .With.Property(nameof(global::Grpc.Core.RpcException.StatusCode))
                .EqualTo(global::Grpc.Core.StatusCode.NotFound));
    }

    [Test]
    public async Task RestoreBackupAsync_then_RevertRestore_round_trips_over_the_wire()
    {
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        var backup = await _host.Client.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Source)));

        const string target = "orders-shadow";
        var restore = await _host.Client.RestoreBackupAsync(
            new LatticeRestoreRequest(backup.BackupId, target, mode: LatticeRestoreMode.ShadowCutover));

        // Revert is idempotent and must not throw when replayed with the same result.
        Assert.That(async () => await _host.Client.RevertRestoreAsync(restore), Throws.Nothing);
    }

    [Test]
    public void RestoreBackupAsync_maps_a_restore_validation_failure_to_failed_precondition_over_the_wire()
    {
        // Restoring an unknown backup id fails the pre-apply validation
        // (LatticeRestoreValidationException, an InvalidOperationException). The
        // service must surface it as FailedPrecondition with its actionable
        // message, not the opaque Internal "request failed", so the operator UI
        // can explain what went wrong (for example a backup store that is not
        // shared across every cluster).
        Assert.That(
            async () => await _host.Client.RestoreBackupAsync(
                new LatticeRestoreRequest(
                    "0000000000000000000000000000000000000000000000000000000000000000",
                    "orders-missing")),
            Throws.InstanceOf<global::Grpc.Core.RpcException>()
                .With.Property(nameof(global::Grpc.Core.RpcException.StatusCode))
                .EqualTo(global::Grpc.Core.StatusCode.FailedPrecondition));
    }

    [Test]
    public async Task ScheduleBackupAsync_registers_a_recurring_schedule_over_the_wire()
    {
        var scope = BackupScopeSelector.WholeTree(Source);

        var effective = await _host.Client.ScheduleBackupAsync(scope, incremental: false, TimeSpan.FromMinutes(30));

        var grain = _fixture.GrainFactory.GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(scope));
        var hasFull = await grain.HasScheduleAsync(incremental: false);
        Assert.Multiple(() =>
        {
            Assert.That(effective, Is.EqualTo(TimeSpan.FromMinutes(30)));
            Assert.That(hasFull, Is.True);
        });
    }

    [Test]
    public async Task ScheduleBackupAsync_clamps_a_sub_minimum_interval_and_reports_the_effective_cadence()
    {
        var scope = BackupScopeSelector.WholeTree("orders-clamp");

        var effective = await _host.Client.ScheduleBackupAsync(scope, incremental: true, TimeSpan.FromSeconds(5));

        var grain = _fixture.GrainFactory.GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(scope));
        var hasIncremental = await grain.HasScheduleAsync(incremental: true);
        Assert.Multiple(() =>
        {
            Assert.That(effective, Is.EqualTo(LatticeBackupScheduleOptions.MinimumInterval));
            Assert.That(hasIncremental, Is.True);
        });
    }

    [Test]
    public async Task CancelScheduleAsync_removes_a_recurring_schedule_over_the_wire()
    {
        var scope = BackupScopeSelector.WholeTree("orders-cancel");
        await _host.Client.ScheduleBackupAsync(scope, incremental: false, TimeSpan.FromMinutes(30));

        await _host.Client.CancelScheduleAsync(scope, incremental: false);

        var grain = _fixture.GrainFactory.GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(scope));
        var hasFull = await grain.HasScheduleAsync(incremental: false);
        var status = await _host.Client.GetScopeStatusAsync(scope);
        Assert.Multiple(() =>
        {
            Assert.That(hasFull, Is.False);
            Assert.That(status, Is.Null);
        });
    }

    [Test]
    public async Task GetScopeStatusAsync_round_trips_runtime_intervals_over_the_wire()
    {
        var scope = BackupScopeSelector.WholeTree("orders-status");

        await _host.Client.ScheduleBackupAsync(scope, incremental: false, TimeSpan.FromMinutes(20));
        await _host.Client.ScheduleBackupAsync(scope, incremental: true, TimeSpan.FromMinutes(45));

        var status = await _host.Client.GetScopeStatusAsync(scope);
        Assert.That(status, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(status!.FullScheduleRegistered, Is.True);
            Assert.That(status.IncrementalScheduleRegistered, Is.True);
            Assert.That(status.RuntimeFullBackupInterval, Is.EqualTo(TimeSpan.FromMinutes(20)));
            Assert.That(status.RuntimeIncrementalBackupInterval, Is.EqualTo(TimeSpan.FromMinutes(45)));
        });
    }

    [Test]
    public void ScheduleBackupAsync_null_scope_throws()
    {
        Assert.That(
            async () => await _host.Client.ScheduleBackupAsync(null!, incremental: false, TimeSpan.FromMinutes(10)),
            Throws.ArgumentNullException);
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string Str(byte[] b) => Encoding.UTF8.GetString(b);
}
