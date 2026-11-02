using System.Text;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Overlapping operator deletes of one backup id - a retry after a timeout, a
/// double submission - each hold a lease of their own, and each deletes the sink
/// manifest in the scheduler grain, under the id's hold, only while no row lists
/// the id and the manifest is still its scope's (issue #5631). Driven through the
/// silo's publication gate, which the backup control's delete uses.
/// </summary>
[Category("Integration")]
public sealed class BackupDeleteOverlapTests
{
    private const string Tree = "orders";

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
    public async Task A_delete_overlapping_another_keeps_the_id_reserved_until_its_own_end()
    {
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(intercepted: true, configureBackup: o => o.PublishHoldWait = TimeSpan.FromSeconds(2));
        var captured = await SeedAndCaptureTreeAsync();

        // The first delete's artifact delete starts a second delete of the id and
        // lets it reach its own artifact delete, where it is held.
        var (second, release) = OverlapDeletes(captured.Manifest);
        var first = await Gate.RetireAsync(captured.Manifest, BackupRetireKind.Delete, CancellationToken.None);

        // The first delete has ended. A capture of identical content must still
        // wait: the second delete is in progress, and its lease is its own.
        var republish = Task.Run(() => CaptureTreeAsync("again"));
        var republishedEarly = await Task.WhenAny(republish, Task.Delay(TimeSpan.FromSeconds(3))) == republish;
        release.TrySetResult();
        var secondDeleted = await second();
        var republished = await republish;

        Assert.Multiple(async () =>
        {
            Assert.That(first, Is.True, "the first delete completes");
            Assert.That(secondDeleted, Is.True, "the second delete completes");
            Assert.That(republishedEarly, Is.False, "no capture of the id publishes while a delete of it is in progress");
            Assert.That(republished.BackupId, Is.EqualTo(captured.BackupId), "the capture republishes the id once both deletes end");
            Assert.That(await _fixture.Catalog.GetAsync(captured.BackupId), Is.Not.Null);
            Assert.That(await _fixture.Sink.ManifestExistsAsync(captured.BackupId), Is.True, "the republished backup is in the sink");
        });
    }

    [Test]
    public async Task A_delete_whose_lease_ran_out_keeps_a_backup_a_capture_republished_meanwhile()
    {
        RedProof.EndIgnoresRow = true; // RED-PROOF
        // A delete's caller cannot reach the grain for longer than its lease, and
        // a capture of identical content republishes the id. The delete then ends:
        // it must not delete the manifest of the backup that is listed again.
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(intercepted: true, configureBackup: o => o.DeleteLease = TimeSpan.FromSeconds(1));
        var captured = await SeedAndCaptureTreeAsync();
        var scheduler = _fixture.Scheduler(BackupScopeSelector.WholeTree(Tree));
        var token = Guid.NewGuid();
        await scheduler.BeginDeleteAsync(captured.BackupId, token);
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        var republished = await CaptureTreeAsync("again");

        var ended = await scheduler.EndDeleteAsync(captured.BackupId, token, finish: true);

        Assert.Multiple(async () =>
        {
            Assert.That(republished.BackupId, Is.EqualTo(captured.BackupId), "the capture republishes the id");
            Assert.That(ended, Is.EqualTo(BackupRetireOutcome.Unchanged), "the end deletes nothing");
            Assert.That(await _fixture.Catalog.GetAsync(captured.BackupId), Is.Not.Null, "the republished backup is listed");
            Assert.That(await _fixture.Sink.ManifestExistsAsync(captured.BackupId), Is.True, "the listed backup's manifest is in the sink");
            Assert.That((await _fixture.Sink.ProbeAsync(captured.BackupId)).IsResolvable, Is.True);
        });
    }

    [Test]
    public async Task An_overlapping_delete_never_deletes_the_manifest_another_scope_published_under_the_id()
    {
        // Two empty scopes capture the same, empty, payload: one bare id, X.
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(intercepted: true);
        var (scopeA, scopeB) = await TwoEmptyScopesAsync();
        var captured = await _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("a", scopeA));

        // Two deletes of A's backup overlap; the first ends, removing A's manifest,
        // so X is ownerless. B captures identical content and takes X; between B's
        // manifest and B's row, the second delete of A's backup ends.
        var (second, release) = OverlapDeletes(captured.Manifest);
        await Gate.RetireAsync(captured.Manifest, BackupRetireKind.Delete, CancellationToken.None);
        InterceptingBackupSink.AfterManifestWrite = async manifest =>
        {
            if (manifest.Id == captured.BackupId && manifest.Scope.Equals(scopeB))
            {
                release.TrySetResult();
                await second();
            }
        };
        var captureB = await _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("b", scopeB));

        Assert.That(captureB.BackupId, Is.EqualTo(captured.BackupId), "precondition: B took the ownerless bare id");
        Assert.Multiple(async () =>
        {
            Assert.That((await _fixture.Catalog.GetAsync(captureB.BackupId))?.Scope, Is.EqualTo(scopeB), "B's backup is listed");
            Assert.That(await _fixture.Sink.ManifestExistsAsync(captureB.BackupId), Is.True, "B's manifest is in the sink");
            Assert.That((await _fixture.Sink.ProbeAsync(captureB.BackupId)).IsResolvable, Is.True);
        });
    }

    [Test]
    public async Task A_delete_of_an_id_another_scope_has_since_taken_leaves_that_scopes_backup()
    {
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(intercepted: true);
        var (scopeA, scopeB) = await TwoEmptyScopesAsync();
        var captured = await _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("a", scopeA));
        Assert.That(await Gate.RetireAsync(captured.Manifest, BackupRetireKind.Delete, CancellationToken.None), Is.True);
        var captureB = await _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("b", scopeB));
        Assert.That(captureB.BackupId, Is.EqualTo(captured.BackupId), "precondition: B took the ownerless bare id");

        // A stale delete of A's backup - a retry, or a second submission that read
        // A's row - reaches A's scheduler grain after B has taken the id.
        var deleted = await Gate.RetireAsync(captured.Manifest, BackupRetireKind.Delete, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(deleted, Is.False, "nothing of A's is left to delete");
            Assert.That((await _fixture.Catalog.GetAsync(captureB.BackupId))?.Scope, Is.EqualTo(scopeB), "B's backup is still listed");
            Assert.That((await _fixture.Sink.ProbeAsync(captureB.BackupId)).IsResolvable, Is.True, "B's backup still restores");
        });
    }

    private IBackupPublicationGate Gate => _fixture.SiloServices.GetRequiredService<IBackupPublicationGate>();

    // Hooks the first artifact delete of a delete of manifest to start a second
    // delete of it and wait until that one is held at its own artifact delete,
    // until released. Returns the second delete's result and its release.
    private (Func<Task<bool>> Second, TaskCompletionSource Release) OverlapDeletes(BackupManifest manifest)
    {
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task<bool>? second = null;
        var calls = 0;
        InterceptingBackupSink.BeforeArtifactDelete = async _ =>
        {
            if (Interlocked.Increment(ref calls) == 1)
            {
                second = Task.Run(() => Gate.RetireAsync(manifest, BackupRetireKind.Delete, CancellationToken.None));
                await reached.Task.WaitAsync(TimeSpan.FromSeconds(30));
                return;
            }

            reached.TrySetResult();
            await release.Task.WaitAsync(TimeSpan.FromSeconds(60));
        };

        return (() => second ?? Task.FromResult(false), release);
    }

    // Written once, so every capture of the tree has identical content: one id.
    private async Task<LatticeBackupCaptureResult> SeedAndCaptureTreeAsync()
    {
        await _fixture.GrainFactory.GetGrain<ILattice>(Tree).SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        return await CaptureTreeAsync("first");
    }

    private Task<LatticeBackupCaptureResult> CaptureTreeAsync(string label) =>
        _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest(label, BackupScopeSelector.WholeTree(Tree)));

    private async Task<(BackupScopeSelector A, BackupScopeSelector B)> TwoEmptyScopesAsync()
    {
        // A prefix nothing is written under, on each of two trees.
        await _fixture.GrainFactory.GetGrain<ILattice>(TreeA).SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await _fixture.GrainFactory.GetGrain<ILattice>(TreeB).SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        return (BackupScopeSelector.Prefix(TreeA, "empty/"), BackupScopeSelector.Prefix(TreeB, "empty/"));
    }
}
