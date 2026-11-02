using System.Text;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// A scope that owns a bare content-addressed id keeps its claim on it while one
/// of its captures waits for the scope's own retention pass to finish retiring
/// the id (issue #5627). Without that claim the id is briefly ownerless, a
/// capture of identical content in another scope takes it, and the waiting
/// capture then overwrites the other scope's backup with its own.
/// </summary>
[Category("Integration")]
public sealed class BackupIdClaimRaceTests
{
    private const string TreeA = "tree-a";

    private const string TreeB = "tree-b";

    private SchedulerClusterFixture _fixture = null!;

    [TearDown]
    public async Task TearDown()
    {
        InterceptingBackupSink.Reset();
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task Another_scope_cannot_take_an_id_its_owner_is_republishing_through_a_retire()
    {
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(
            o =>
            {
                o.RetentionEnabled = true;
                o.RetentionMaxAge = TimeSpan.FromSeconds(3);
            },
            intercepted: true,
            configureBackup: o => o.PublishHoldWait = TimeSpan.FromSeconds(25));
        await _fixture.GrainFactory.GetGrain<ILattice>(TreeA).SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await _fixture.GrainFactory.GetGrain<ILattice>(TreeB).SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        // Two empty scopes capture the same, empty, payload: one bare id, X.
        var scopeA = BackupScopeSelector.Prefix(TreeA, "empty/");
        var scopeB = BackupScopeSelector.Prefix(TreeB, "empty/");
        var expired = await _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("a-first", scopeA));
        await Task.Delay(TimeSpan.FromSeconds(3.5));

        // A's pass retires X. With X's row removed but its sink manifest still in
        // the sink, a capture in A resolves X (A still owns it) and waits on the
        // pass's hold. Once the pass has deleted X's manifest, and before it lets
        // go, B captures identical content.
        Task<LatticeBackupCaptureResult>? republishA = null;
        LatticeBackupCaptureResult? captureB = null;
        InterceptingBackupSink.BeforeManifestList = async () =>
        {
            if (republishA is not null)
            {
                return;
            }

            republishA = Task.Run(() => _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("a-again", scopeA)));
            await Task.Delay(TimeSpan.FromSeconds(3));
        };
        InterceptingBackupSink.BeforeArtifactDelete = async artifactId =>
        {
            if (captureB is not null || artifactId != expired.Manifest.ContentDescriptors.Single().ArtifactId)
            {
                return;
            }

            captureB = await _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("b", scopeB));
        };

        var report = await _fixture.Scheduler(scopeA).PruneAsync(scopeA);
        Assert.That(republishA, Is.Not.Null, "A's capture must have started inside the pass");
        var republishedA = await republishA!;

        Assert.That(captureB, Is.Not.Null, "B's capture must have run inside the pass");
        var rowA = await _fixture.Catalog.GetAsync(republishedA.BackupId);
        var rowB = await _fixture.Catalog.GetAsync(captureB!.BackupId);
        Assert.Multiple(() =>
        {
            Assert.That(report.PrunedBackupIds, Is.EqualTo(new[] { expired.BackupId }), "the pass retires X");
            Assert.That(republishedA.BackupId, Is.EqualTo(expired.BackupId), "A republishes X");
            Assert.That(captureB.BackupId, Is.Not.EqualTo(expired.BackupId), "B must not take an id A is still publishing");
            Assert.That(rowA?.Scope, Is.EqualTo(scopeA), "A's backup lists as A's");
            Assert.That(rowB?.Scope, Is.EqualTo(scopeB), "B's backup lists as B's: nothing overwrote it");
        });
    }
}
