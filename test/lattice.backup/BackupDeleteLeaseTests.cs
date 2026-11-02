using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Core.Internal;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// An operator delete's lease on its backup id (issues #5628, #5631 and #5637):
/// the caller renews it while it deletes artifacts outside the scheduler grain and
/// stops once a renewal is refused; a catalog rebuild waits on it and re-lists
/// only a backup whose every artifact is still in the sink; a lost caller's lease
/// runs out by itself, and only its own; and a retried start renews one lease.
/// </summary>
[Category("Integration")]
public sealed class BackupDeleteLeaseTests
{
    private const string Tree = "orders";

    private SchedulerClusterFixture _fixture = null!;

    [TearDown]
    public async Task TearDown()
    {
        InterceptingBackupSink.Reset();
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task A_rebuild_after_an_interrupted_delete_leaves_the_half_deleted_backup_unlisted()
    {
        RedProof.RebuildUnprobed = true; // RED-PROOF
        await StartAsync(o => o.DeleteLease = TimeSpan.FromSeconds(1));
        var captured = await SeedAndCaptureAsync();

        // A delete removes the row and deletes the artifact, then its caller is
        // lost before it deletes the sink manifest; its lease runs out.
        await Scheduler.BeginDeleteAsync(captured.BackupId, Guid.NewGuid());
        await _fixture.Sink.DeleteArtifactAsync(captured.Manifest.ContentDescriptors.Single().ArtifactId);
        await Task.Delay(TimeSpan.FromSeconds(1.5));

        var rebuilt = await Gate.RegisterRebuiltAsync(captured.Manifest, CancellationToken.None);
        var listedAfterRebuild = await _fixture.Catalog.GetAsync(captured.BackupId);
        var retried = await Gate.RetireAsync(captured.Manifest, BackupRetireKind.Delete, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(rebuilt, Is.False, "a rebuild must not list a backup whose artifact is gone");
            Assert.That(listedAfterRebuild, Is.Null, "the half-deleted backup stays unlisted");
            Assert.That(retried, Is.True, "a retry of the delete still finishes it (#5334)");
            Assert.That(await _fixture.Sink.ManifestExistsAsync(captured.BackupId), Is.False);
        });
    }

    [Test]
    public async Task A_delete_whose_artifact_deletes_outlast_its_lease_keeps_a_rebuild_waiting()
    {
        RedProof.NoRenewal = true; // RED-PROOF
        // The artifact delete takes longer than the lease; the caller renews it.
        await StartAsync(o => o.DeleteLease = TimeSpan.FromSeconds(1));
        var captured = await SeedAndCaptureAsync();
        Task<bool>? rebuild = null;
        InterceptingBackupSink.BeforeArtifactDelete = async _ =>
        {
            await Task.Delay(TimeSpan.FromSeconds(1.5));
            rebuild ??= Task.Run(() => Gate.RegisterRebuiltAsync(captured.Manifest, CancellationToken.None));
            await Task.Delay(TimeSpan.FromSeconds(1));
        };

        var deleted = await Gate.RetireAsync(captured.Manifest, BackupRetireKind.Delete, CancellationToken.None);
        Assert.That(rebuild, Is.Not.Null, "precondition: the rebuild ran inside the delete");
        var rebuilt = await rebuild!;

        Assert.Multiple(async () =>
        {
            Assert.That(deleted, Is.True);
            Assert.That(rebuilt, Is.False, "the rebuild waits for the delete, and then finds nothing to list");
            Assert.That(await _fixture.Catalog.GetAsync(captured.BackupId), Is.Null, "the deleted backup is not listed again");
            Assert.That(await _fixture.Sink.ManifestExistsAsync(captured.BackupId), Is.False);
        });
    }

    [Test]
    public async Task A_delete_that_loses_its_lease_deletes_no_further_artifact()
    {
        RedProof.NoStopOnLoss = true; // RED-PROOF
        await StartAsync(o => o.DeleteLease = TimeSpan.FromSeconds(1.5));
        var (manifest, second) = await CaptureWithTwoArtifactsAsync();

        // The scope's scheduler activation is lost during the first artifact
        // delete, and its leases with it: the caller's next renewal is refused.
        var calls = 0;
        InterceptingBackupSink.BeforeArtifactDelete = async _ =>
        {
            if (Interlocked.Increment(ref calls) == 1)
            {
                await Scheduler.AsReference<IGrainManagementExtension>().DeactivateOnIdle();
                await Task.Delay(TimeSpan.FromSeconds(2));
            }
        };

        await Assert.ThatAsync(
            () => Gate.RetireAsync(manifest, BackupRetireKind.Delete, CancellationToken.None),
            Throws.TypeOf<TimeoutException>(),
            "a delete that lost its reservation stops and reports it");

        var artifacts = new List<string>();
        await foreach (var artifactId in _fixture.Sink.ListArtifactIdsAsync())
        {
            artifacts.Add(artifactId);
        }

        Assert.Multiple(async () =>
        {
            Assert.That(artifacts, Does.Contain(second), "no artifact is deleted once a renewal is refused");
            Assert.That(await _fixture.Sink.ManifestExistsAsync(manifest.Id), Is.True, "the sink manifest is kept for a retry");
        });
    }

    [Test]
    public async Task A_lost_deletes_lease_runs_out_and_a_capture_then_republishes()
    {
        RedProof.LeaseNeverLapses = true; // RED-PROOF
        await StartAsync(o =>
        {
            o.DeleteLease = TimeSpan.FromSeconds(1);
            o.PublishHoldTimeout = TimeSpan.FromSeconds(8);
        });
        var captured = await SeedAndCaptureAsync();

        // A delete begins and its caller is lost: it never renews or ends.
        await Scheduler.BeginDeleteAsync(captured.BackupId, Guid.NewGuid());

        var republished = await CaptureAsync("again");

        Assert.Multiple(async () =>
        {
            Assert.That(republished.BackupId, Is.EqualTo(captured.BackupId), "the capture republishes the id once the lease runs out");
            Assert.That(await _fixture.Catalog.GetAsync(captured.BackupId), Is.Not.Null);
            Assert.That((await _fixture.Sink.ProbeAsync(captured.BackupId)).IsResolvable, Is.True);
        });
    }

    [Test]
    public async Task One_deletes_lease_running_out_leaves_another_deletes_lease_in_force()
    {
        RedProof.ExpiryDropsEveryLease = true; // RED-PROOF
        await StartAsync(o => o.DeleteLease = TimeSpan.FromSeconds(1), publishHoldWait: TimeSpan.FromSeconds(2));
        var captured = await SeedAndCaptureAsync();

        // A first delete's caller is lost; a second delete of the id runs through
        // the gate, renewing its own lease, and is slow to delete its artifact.
        await Scheduler.BeginDeleteAsync(captured.BackupId, Guid.NewGuid());
        Task<LatticeBackupCaptureResult>? republish = null;
        var republishedEarly = false;
        InterceptingBackupSink.BeforeArtifactDelete = async _ =>
        {
            await Task.Delay(TimeSpan.FromSeconds(1.5));
            republish ??= Task.Run(() => CaptureAsync("again"));
            republishedEarly = await Task.WhenAny(republish, Task.Delay(TimeSpan.FromSeconds(2))) == republish;
        };

        var deleted = await Gate.RetireAsync(captured.Manifest, BackupRetireKind.Delete, CancellationToken.None);
        Assert.That(republish, Is.Not.Null, "precondition: the capture ran inside the delete");
        var republished = await republish!;

        Assert.Multiple(() =>
        {
            Assert.That(deleted, Is.True);
            Assert.That(republishedEarly, Is.False, "the first lease running out does not end the second");
            Assert.That(republished.BackupId, Is.EqualTo(captured.BackupId), "the capture republishes once the second delete ends");
        });
    }

    [Test]
    public async Task A_delete_start_retried_with_its_token_holds_one_lease()
    {
        RedProof.BeginMintsToken = true; // RED-PROOF
        await StartAsync(o => o.PublishHoldTimeout = TimeSpan.FromSeconds(5));
        var captured = await SeedAndCaptureAsync();

        // The first call's response was lost; the caller retries with its token.
        var token = Guid.NewGuid();
        await Scheduler.BeginDeleteAsync(captured.BackupId, token);
        await Scheduler.BeginDeleteAsync(captured.BackupId, token);
        await Scheduler.EndDeleteAsync(captured.BackupId, token, finish: false);

        var republished = await CaptureAsync("again");

        Assert.That(republished.BackupId, Is.EqualTo(captured.BackupId), "no orphaned lease keeps the capture waiting");
    }

    private IBackupPublicationGate Gate => _fixture.SiloServices.GetRequiredService<IBackupPublicationGate>();

    private ILatticeBackupSchedulerGrain Scheduler => _fixture.Scheduler(BackupScopeSelector.WholeTree(Tree));

    private async Task StartAsync(Action<LatticeBackupOptions> configureBackup, TimeSpan? publishHoldWait = null)
    {
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(
            intercepted: true,
            configureBackup: o =>
            {
                configureBackup(o);
                if (publishHoldWait is { } wait)
                {
                    o.PublishHoldWait = wait;
                }
            });
    }

    // Written once, so every capture of the tree has identical content: one id.
    private async Task<LatticeBackupCaptureResult> SeedAndCaptureAsync()
    {
        await _fixture.GrainFactory.GetGrain<ILattice>(Tree).SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        return await CaptureAsync("first");
    }

    private Task<LatticeBackupCaptureResult> CaptureAsync(string label) =>
        _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest(label, BackupScopeSelector.WholeTree(Tree)));

    // A listed backup whose manifest references two artifacts: the capture's, and
    // a second written alongside it.
    private async Task<(BackupManifest Manifest, string Second)> CaptureWithTwoArtifactsAsync()
    {
        var captured = await SeedAndCaptureAsync();
        var first = captured.Manifest.ContentDescriptors.Single();
        var second = "second-" + Guid.NewGuid().ToString("N");
        await _fixture.Sink.WriteArtifactAsync(second, OneChunk(Encoding.UTF8.GetBytes("payload")));
        var manifest = captured.Manifest with
        {
            ContentDescriptors = [first, first with { ArtifactId = second, ChunkCount = 1 }],
        };
        await _fixture.Sink.WriteManifestAsync(manifest);
        await _fixture.Catalog.RegisterAsync(manifest);
        return (manifest, second);
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> OneChunk(byte[] payload)
    {
        await Task.CompletedTask;
        yield return payload;
    }
}
