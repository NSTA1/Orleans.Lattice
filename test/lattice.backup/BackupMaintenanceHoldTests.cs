using System.Text;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// The catalog rebuild and the catalog scrub register and remove rows under the
/// same per-id hold as captures and retention passes (issue #5418), on a real
/// single-silo cluster: a rebuild never lists a manifest removed from the sink
/// while it walks, and a scrub never removes a row that a capture republished
/// after the scrub found it orphaned.
/// </summary>
[Category("Integration")]
public sealed class BackupMaintenanceHoldTests
{
    private const string Tree = "orders";

    private SchedulerClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(intercepted: true, recordGate: true);
    }

    [TearDown]
    public async Task TearDown()
    {
        InterceptingBackupSink.Reset();
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task A_rebuild_lists_each_manifest_under_its_hold_and_never_one_removed_while_it_walks()
    {
        RedProof.Routing = true; // RED-PROOF
        var scope = BackupScopeSelector.WholeTree(Tree);
        await SeedAsync("k1", "v1");
        var kept = await _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("kept", scope));
        await SeedAsync("k2", "v2");
        var removed = await _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("removed", scope));
        await _fixture.Catalog.RemoveAsync(kept.BackupId);
        await _fixture.Catalog.RemoveAsync(removed.BackupId);

        // The second backup's sink manifest is deleted as the rebuild's walk reads
        // it, before the rebuild can register it.
        var deleted = false;
        InterceptingBackupSink.OnManifestListed = async manifest =>
        {
            if (deleted || manifest.Id != removed.BackupId)
            {
                return;
            }

            deleted = true;
            await _fixture.Sink.DeleteManifestAsync(removed.BackupId);
        };

        await _fixture.SiloServices.GetRequiredService<ILatticeBackupCatalogRebuildService>().RebuildFromSinkAsync();

        Assert.That(deleted, Is.True, "the manifest must have been removed during the walk");
        Assert.Multiple(async () =>
        {
            Assert.That(await _fixture.Catalog.GetAsync(kept.BackupId), Is.Not.Null, "a manifest still in the sink is listed");
            Assert.That(await _fixture.Catalog.GetAsync(removed.BackupId), Is.Null, "a manifest removed during the walk is not listed");
            Assert.That(RecordingPublicationGate.Saw(nameof(IBackupPublicationGate.RegisterRebuiltAsync), kept.BackupId, "listed"), Is.True);
            Assert.That(RecordingPublicationGate.Saw(nameof(IBackupPublicationGate.RegisterRebuiltAsync), removed.BackupId, "skipped"), Is.True);
        });
        await AssertEveryListedBackupResolvesFromTheSinkAsync();
    }

    [Test]
    public async Task A_scrub_keeps_an_orphan_row_a_capture_republished_after_the_scrub_found_it()
    {
        RedProof.Routing = true; // RED-PROOF
        var scope = BackupScopeSelector.WholeTree(Tree);
        await SeedAsync("k1", "v1");
        var orphan = await _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("orphan", scope));
        await _fixture.Sink.DeleteManifestAsync(orphan.BackupId);

        // Once the scrub's read pass has found the row orphaned, a capture of the
        // unchanged tree republishes the id, which makes the row resolvable again.
        var republished = false;
        InterceptingBackupSink.AfterProbe = async (backupId, resolution) =>
        {
            if (republished || backupId != orphan.BackupId || resolution.IsResolvable)
            {
                return;
            }

            republished = true;
            await _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("again", scope));
        };

        var report = await _fixture.SiloServices.GetRequiredService<ILatticeBackupCatalogScrubService>()
            .ScrubAsync(pruneOrphans: true);

        Assert.That(republished, Is.True, "the capture must have republished the id during the scrub");
        Assert.Multiple(async () =>
        {
            Assert.That(report.OrphanBackupIds, Does.Contain(orphan.BackupId), "the read pass found the row orphaned");
            Assert.That(report.RemovedCount, Is.Zero, "a row resolvable again under its hold is not removed");
            Assert.That(await _fixture.Catalog.GetAsync(orphan.BackupId), Is.Not.Null, "the republished backup stays listed");
            Assert.That(
                RecordingPublicationGate.Saw(nameof(IBackupPublicationGate.RetireAsync), orphan.BackupId, nameof(BackupRetireKind.OrphanRow)),
                Is.True,
                "the scrub removes an orphan row under its id's hold");
        });
        await AssertEveryListedBackupResolvesFromTheSinkAsync();
    }

    private async Task AssertEveryListedBackupResolvesFromTheSinkAsync()
    {
        await foreach (var manifest in _fixture.Catalog.ListAsync())
        {
            var probe = await _fixture.Sink.ProbeAsync(manifest.Id);
            Assert.That(probe.IsResolvable, Is.True, $"listed backup '{manifest.Id}' is not resolvable from the sink");
        }
    }

    private async Task SeedAsync(string key, string value)
    {
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync(key, Encoding.UTF8.GetBytes(value));
    }
}
