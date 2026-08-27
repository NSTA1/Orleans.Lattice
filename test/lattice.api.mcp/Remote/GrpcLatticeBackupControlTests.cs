using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Backup.Grpc;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="GrpcLatticeBackupControl"/>, the remote-host adapter
/// that fronts <see cref="ILatticeBackupControl"/> over the backup-API gRPC
/// client. Every wire-backed member is proven to forward and unwrap; the two
/// streaming members preserve their sequences; the four members with no gRPC
/// binding fail loud with <see cref="NotSupportedException"/>; found / not-found
/// projections and the argument guards are covered. Deterministic over a
/// <see cref="FakeCallInvoker"/>.
/// </summary>
[TestFixture]
public sealed class GrpcLatticeBackupControlTests
{
    private static readonly BackupScopeSelector Scope = BackupScopeSelector.Prefix("orders", "eu/");

    private static GrpcLatticeBackupControl Adapter(FakeCallInvoker invoker)
        => new(RemoteTestSupport.BackupClient(invoker));

    private static BackupManifest Manifest(string id = "bk-1")
        => BackupToolTestData.Manifest(id, "nightly", Scope);

    [Test]
    public void Constructor_null_client_throws()
        => Assert.That(() => new GrpcLatticeBackupControl(null!), Throws.ArgumentNullException);

    [Test]
    public async Task CreateBackupAsync_unwraps_id_and_manifest()
    {
        var manifest = Manifest();
        var invoker = new FakeCallInvoker(_ => new BackupCaptureResponse { BackupId = "bk-1", Manifest = manifest });

        var result = await Adapter(invoker).CreateBackupAsync(new LatticeBackupCaptureRequest("nightly", Scope));

        Assert.Multiple(() =>
        {
            Assert.That(result.BackupId, Is.EqualTo("bk-1"));
            Assert.That(result.Manifest, Is.SameAs(manifest));
        });
    }

    [Test]
    public async Task CreateIncrementalBackupAsync_unwraps_id_and_manifest()
    {
        var manifest = Manifest("bk-2");
        var invoker = new FakeCallInvoker(_ => new BackupCaptureResponse { BackupId = "bk-2", Manifest = manifest });

        var result = await Adapter(invoker).CreateIncrementalBackupAsync(
            new LatticeBackupIncrementalCaptureRequest("nightly", Scope, "bk-1"));

        Assert.That(result.BackupId, Is.EqualTo("bk-2"));
    }

    [Test]
    public async Task CreateBackupSetAsync_unwraps_set_manifest_and_members()
    {
        var manifest = Manifest("bk-1");
        var setManifest = new BackupSetManifest("set-1", "nightly", DateTimeOffset.UnixEpoch, false, null, new[] { "bk-1" });
        var invoker = new FakeCallInvoker(_ => new BackupSetCaptureResponse
        {
            SetManifest = setManifest,
            Members = new[] { new BackupCaptureResponse { BackupId = "bk-1", Manifest = manifest } },
        });

        var result = await Adapter(invoker).CreateBackupSetAsync(
            new LatticeBackupSetCaptureRequest("nightly", new[] { Scope }));

        Assert.Multiple(() =>
        {
            Assert.That(result.SetManifest.SetId, Is.EqualTo("set-1"));
            Assert.That(result.Members, Has.Count.EqualTo(1));
            Assert.That(result.Members[0].BackupId, Is.EqualTo("bk-1"));
        });
    }

    [Test]
    public async Task CreateBackupSetAsync_unwraps_an_absent_set_id_as_null()
    {
        // The MCP remote adapter must not invent an id for a single-scope set: the
        // only thing a remote consumer can do with a set id is group catalog rows,
        // and a one-member set stamps none.
        var manifest = Manifest("bk-1");
        var setManifest = new BackupSetManifest(null, "solo", DateTimeOffset.UnixEpoch, false, null, new[] { "bk-1" });
        var invoker = new FakeCallInvoker(_ => new BackupSetCaptureResponse
        {
            SetManifest = setManifest,
            Members = new[] { new BackupCaptureResponse { BackupId = "bk-1", Manifest = manifest } },
        });

        var result = await Adapter(invoker).CreateBackupSetAsync(
            new LatticeBackupSetCaptureRequest("solo", new[] { Scope }));

        Assert.Multiple(() =>
        {
            Assert.That(result.SetManifest.SetId, Is.Null);
            Assert.That(result.SetManifest.Name, Is.EqualTo("solo"));
            Assert.That(result.Members[0].BackupId, Is.EqualTo("bk-1"));
        });
    }

    [Test]
    public async Task ScheduleBackupAsync_forwards_scope_flag_and_interval()
    {
        var invoker = new FakeCallInvoker(_ => new BackupScheduleResponse { Scheduled = true, EffectiveIntervalTicks = 10 });

        await Adapter(invoker).ScheduleBackupAsync(
            new LatticeBackupScheduleRequest(Scope, incremental: true, TimeSpan.FromHours(1)));

        var sent = (BackupScheduleRequestMessage)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.Incremental, Is.True);
            Assert.That(sent.IntervalTicks, Is.EqualTo(TimeSpan.FromHours(1).Ticks));
        });
    }

    [Test]
    public void ScheduleBackupAsync_null_request_throws()
        => Assert.That(
            async () => await Adapter(new FakeCallInvoker(_ => new BackupScheduleResponse())).ScheduleBackupAsync(null!),
            Throws.ArgumentNullException);

    [Test]
    public async Task CancelScheduleAsync_forwards_scope_and_flag()
    {
        var invoker = new FakeCallInvoker(_ => new BackupCancelScheduleResponse());

        await Adapter(invoker).CancelScheduleAsync(Scope, incremental: false);

        Assert.That(((BackupCancelScheduleRequestMessage)invoker.LastRequest!).Incremental, Is.False);
    }

    [Test]
    public async Task ListBackupsAsync_returns_page()
    {
        var page = new BackupCatalogPage { NextPageToken = "n" };
        var result = await Adapter(new FakeCallInvoker(_ => page)).ListBackupsAsync(new BackupCatalogRequest());
        Assert.That(result, Is.SameAs(page));
    }

    [Test]
    public async Task StreamBackupsAsync_yields_every_manifest()
    {
        var manifests = new[] { Manifest("bk-1"), Manifest("bk-2") };
        var invoker = new FakeCallInvoker(_ => throw new InvalidOperationException(), _ => manifests);

        var seen = new List<string>();
        await foreach (var m in Adapter(invoker).StreamBackupsAsync())
        {
            seen.Add(m.Id);
        }

        Assert.That(seen, Is.EqualTo(new[] { "bk-1", "bk-2" }));
    }

    [Test]
    public async Task DescribeBackupAsync_found_returns_chain()
    {
        var manifest = Manifest();
        var invoker = new FakeCallInvoker(_ => new BackupChainResponse
        {
            Found = true,
            Manifest = manifest,
            ChainBackupIds = new[] { "bk-1" },
        });

        var result = await Adapter(invoker).DescribeBackupAsync("bk-1");

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.Manifest, Is.SameAs(manifest));
    }

    [Test]
    public async Task DescribeBackupAsync_not_found_returns_null()
    {
        var result = await Adapter(new FakeCallInvoker(_ => new BackupChainResponse { Found = false }))
            .DescribeBackupAsync("missing");
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task DeleteBackupAsync_unwraps_deleted()
    {
        var result = await Adapter(new FakeCallInvoker(_ => new BackupDeleteResponse { Deleted = true }))
            .DeleteBackupAsync("bk-1");
        Assert.That(result, Is.True);
    }

    [Test]
    public async Task RestoreBackupAsync_maps_response()
    {
        var invoker = new FakeCallInvoker(_ => new RestoreResponse
        {
            BackupId = "bk-1",
            TargetTreeId = "orders",
            Mode = LatticeRestoreMode.InPlace,
            OperationId = "op-1",
            EntriesApplied = 5,
        });

        var result = await Adapter(invoker).RestoreBackupAsync(new LatticeRestoreRequest("bk-1"));

        Assert.Multiple(() =>
        {
            Assert.That(result.BackupId, Is.EqualTo("bk-1"));
            Assert.That(result.TargetTreeId, Is.EqualTo("orders"));
            Assert.That(result.EntriesApplied, Is.EqualTo(5));
        });
    }

    [Test]
    public async Task RevertRestoreAsync_forwards_restore()
    {
        var invoker = new FakeCallInvoker(_ => new RevertRestoreResponse());
        var restore = new LatticeRestoreResult("bk-1", "orders", LatticeRestoreMode.InPlace, "op-1", Array.Empty<string>(), 0);

        await Adapter(invoker).RevertRestoreAsync(restore);

        Assert.That(((RestoreResponse)invoker.LastRequest!).BackupId, Is.EqualTo("bk-1"));
    }

    [Test]
    public async Task ExportArtifactAsync_yields_chunk_bytes()
    {
        var chunks = new[]
        {
            new ArtifactChunk { Data = new byte[] { 1, 2 } },
            new ArtifactChunk { Data = new byte[] { 3 } },
        };
        var invoker = new FakeCallInvoker(_ => throw new InvalidOperationException(), _ => chunks);

        var bytes = new List<byte>();
        await foreach (var chunk in Adapter(invoker).ExportArtifactAsync("bk-1", "artifact-0"))
        {
            bytes.AddRange(chunk.ToArray());
        }

        Assert.That(bytes, Is.EqualTo(new byte[] { 1, 2, 3 }));
    }

    [Test]
    public async Task GetScopeStatusAsync_found_projects_status()
    {
        var invoker = new FakeCallInvoker(_ => new BackupScopeStatusResponse
        {
            Found = true,
            Scope = Scope,
            FullScheduleRegistered = true,
            ChainDepth = 2,
        });

        var result = await Adapter(invoker).GetScopeStatusAsync(Scope);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.FullScheduleRegistered, Is.True);
    }

    [Test]
    public async Task GetScopeStatusAsync_not_found_returns_null()
    {
        var result = await Adapter(new FakeCallInvoker(_ => new BackupScopeStatusResponse { Found = false }))
            .GetScopeStatusAsync(Scope);
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_returns_capabilities()
    {
        var caps = new BackupScopeCapabilities { Scope = Scope, CanCapture = true };
        var result = await Adapter(new FakeCallInvoker(_ => caps)).ProbeCapabilitiesAsync(Scope);
        Assert.That(result, Is.SameAs(caps));
    }

    [Test]
    public async Task IsHealthMonitoringAvailableAsync_unwraps_flag()
    {
        var result = await Adapter(new FakeCallInvoker(_ => new BackupHealthAvailabilityResponse { Available = true }))
            .IsHealthMonitoringAvailableAsync();
        Assert.That(result, Is.True);
    }

    [Test]
    public async Task CheckBackupHealthAsync_returns_report()
    {
        var report = new BackupHealthReport(
            "bk-1", BackupHealthStatus.Healthy, true,
            Array.Empty<string>(), Array.Empty<string>(), DateTimeOffset.UnixEpoch, "ok");
        var result = await Adapter(new FakeCallInvoker(_ => new BackupHealthReportResponse { Found = true, Report = report }))
            .CheckBackupHealthAsync("bk-1");
        Assert.That(result, Is.SameAs(report));
    }

    [Test]
    public async Task GetBackupHealthAsync_found_returns_report()
    {
        var report = new BackupHealthReport(
            "bk-1", BackupHealthStatus.Healthy, true,
            Array.Empty<string>(), Array.Empty<string>(), DateTimeOffset.UnixEpoch, "ok");
        var result = await Adapter(new FakeCallInvoker(_ => new BackupHealthReportResponse { Found = true, Report = report }))
            .GetBackupHealthAsync("bk-1");
        Assert.That(result, Is.SameAs(report));
    }

    [Test]
    public async Task GetBackupHealthAsync_not_found_returns_null()
    {
        var result = await Adapter(new FakeCallInvoker(_ => new BackupHealthReportResponse { Found = false }))
            .GetBackupHealthAsync("bk-1");
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task ConfigureBackupHealthAsync_forwards_config()
    {
        var invoker = new FakeCallInvoker(_ => new BackupHealthConfigureResponse());

        await Adapter(invoker).ConfigureBackupHealthAsync("bk-1", new BackupHealthConfig(true, TimeSpan.FromMinutes(5)));

        var sent = (BackupHealthConfigureRequestMessage)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.BackupId, Is.EqualTo("bk-1"));
            Assert.That(sent.MonitoringEnabled, Is.True);
            Assert.That(sent.IntervalTicks, Is.EqualTo(TimeSpan.FromMinutes(5).Ticks));
        });
    }

    [Test]
    public void GetInventoryAsync_has_no_binding_and_throws()
        => Assert.That(
            () => Adapter(new FakeCallInvoker(_ => throw new InvalidOperationException())).GetInventoryAsync(),
            Throws.TypeOf<NotSupportedException>());

    [Test]
    public void RebuildCatalogFromSinkAsync_has_no_binding_and_throws()
        => Assert.That(
            () => Adapter(new FakeCallInvoker(_ => throw new InvalidOperationException())).RebuildCatalogFromSinkAsync(),
            Throws.TypeOf<NotSupportedException>());

    [Test]
    public void ScrubCatalogAgainstSinkAsync_has_no_binding_and_throws()
        => Assert.That(
            () => Adapter(new FakeCallInvoker(_ => throw new InvalidOperationException())).ScrubCatalogAgainstSinkAsync(),
            Throws.TypeOf<NotSupportedException>());

    [Test]
    public void ColdRestoreAsync_has_no_binding_and_throws()
        => Assert.That(
            () => Adapter(new FakeCallInvoker(_ => throw new InvalidOperationException())).ColdRestoreAsync(new LatticeRestoreRequest("bk-1")),
            Throws.TypeOf<NotSupportedException>());
}
