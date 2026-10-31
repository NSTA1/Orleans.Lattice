using System.Text;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Regression for issue #5450 against a live single-silo cluster: after a burst of
/// captures, a full backup with an incremental on it, and a cold restore (which
/// rebuilds the catalog from the sink), the newest-first listing - the one every
/// Explorer catalogue surface reads, served from the backup-catalog index - lists
/// exactly the backups the catalog holds. The full backup an increment builds on
/// is listed too, and is offered when the tree's full backups are asked for.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeBackupCatalogNewestFirstIntegrationTests
{
    private const string BurstTree = "newest-first-telemetry";
    private const string LinesTree = "newest-first-lines";
    private const string ColdTree = "newest-first-lines-cold";
    private const int BurstCaptures = 12;

    private ApiBackupClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ApiBackupClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private ILatticeBackupOperations Operations => (ILatticeBackupOperations)_fixture.Control;

    [Test]
    public async Task Newest_first_listing_holds_every_catalogued_backup_after_a_burst_an_increment_and_a_cold_restore()
    {
        var burstTree = _fixture.GrainFactory.GetGrain<ILattice>(BurstTree);
        for (var i = 0; i < BurstCaptures; i++)
        {
            // A backup id is a content address, so each capture writes first to
            // catalogue a distinct backup.
            await burstTree.SetAsync($"reading-{i}", Encoding.UTF8.GetBytes($"value-{i}"));
            await CaptureAsync(Operations.StartBackupAsync(
                new LatticeBackupCaptureRequest($"telemetry-{i}", BackupScopeSelector.WholeTree(BurstTree))));
        }

        var linesTree = _fixture.GrainFactory.GetGrain<ILattice>(LinesTree);
        await linesTree.SetAsync("line-1", Encoding.UTF8.GetBytes("rate-1"));
        await linesTree.SetAsync("line-2", Encoding.UTF8.GetBytes("rate-2"));
        var linesFull = await CaptureAsync(Operations.StartBackupAsync(
            new LatticeBackupCaptureRequest("lines-full", BackupScopeSelector.WholeTree(LinesTree))));

        await linesTree.SetAsync("line-1", Encoding.UTF8.GetBytes("rate-1-edited"));
        var linesIncrement = await CaptureAsync(Operations.StartIncrementalBackupAsync(
            new LatticeBackupIncrementalCaptureRequest("lines-inc-1", BackupScopeSelector.WholeTree(LinesTree), linesFull)));

        await UntilSucceededAsync(await Operations.StartColdRestoreAsync(new LatticeRestoreRequest(linesFull, ColdTree)));

        await burstTree.SetAsync("reading-after-cold-restore", Encoding.UTF8.GetBytes("value"));
        await CaptureAsync(Operations.StartBackupAsync(
            new LatticeBackupCaptureRequest("telemetry-after-cold-restore", BackupScopeSelector.WholeTree(BurstTree))));

        var catalogued = new List<string>();
        await foreach (var manifest in _fixture.Control.StreamBackupsAsync())
        {
            catalogued.Add(manifest.Id);
        }

        var listed = await NewestFirstAsync(new BackupCatalogRequest { PageSize = 5, OrderByCreatedDescending = true });
        var bases = await NewestFirstAsync(new BackupCatalogRequest
        {
            OrderByCreatedDescending = true,
            Kind = BackupKind.Full,
            TreeId = LinesTree,
        });

        Assert.Multiple(() =>
        {
            Assert.That(catalogued, Has.Count.EqualTo(BurstCaptures + 3), "the burst, lines-full, lines-inc-1 and the last capture");
            Assert.That(listed.Select(m => m.Id), Is.EquivalentTo(catalogued),
                "the newest-first listing lists exactly the catalogued backups");
            Assert.That(listed.Select(m => m.Id), Is.Unique, "no backup is listed twice across pages");
            Assert.That(listed.Select(m => m.Id), Does.Contain(linesFull).And.Contain(linesIncrement),
                "the full backup an increment builds on is listed with its increment");
            Assert.That(listed.Select(m => m.CreatedAtUtc), Is.Ordered.Descending, "the listing is newest first");
            Assert.That(bases.Select(m => m.Id), Does.Contain(linesFull),
                "the full backup an increment builds on is still offered as a base");
        });
    }

    private async Task<List<BackupManifest>> NewestFirstAsync(BackupCatalogRequest request)
    {
        var all = new List<BackupManifest>();
        string? token = null;
        do
        {
            var page = await _fixture.Control.ListBackupsAsync(request with { PageToken = token });
            all.AddRange(page.Entries);
            token = page.NextPageToken;
        }
        while (token is not null);

        return all;
    }

    private async Task<string> CaptureAsync(Task<LatticeOperationHandle> start)
    {
        var status = await UntilSucceededAsync(await start);
        return status.ResultReference!;
    }

    private async Task<LatticeOperationStatus> UntilSucceededAsync(LatticeOperationHandle handle)
    {
        LatticeOperationStatus? status = null;
        await TestPoll.UntilAsync(
            async () => (status = await Operations.GetOperationStatusAsync(handle.OperationId)) is { IsTerminal: true },
            $"operation {handle.OperationId} to finish");
        Assert.That(status!.State, Is.EqualTo(LatticeOperationState.Succeeded), status.FailureReason);
        return status;
    }
}
