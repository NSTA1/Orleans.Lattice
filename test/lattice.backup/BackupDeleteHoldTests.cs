using System.Text;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// An operator delete runs its catalog-row removal under the id's hold in the
/// scope's scheduler grain, and deletes the artifacts and then the sink manifest
/// outside it (issue #5628), so a large delete cannot outlast the grain response
/// timeout, and a delete interrupted part-way is finished by a retry (#5334).
/// Driven through the silo's publication gate, which the backup control's
/// delete uses.
/// </summary>
[Category("Integration")]
public sealed class BackupDeleteHoldTests
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
    public async Task A_delete_whose_artifact_deletes_outlast_the_grain_response_timeout_completes()
    {
        RedProof.DeleteInGrain = true; // RED-PROOF
        // The artifact delete takes longer than a grain call may; run inside the
        // scheduler grain, the delete would time out and report failure.
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(intercepted: true, responseTimeout: TimeSpan.FromSeconds(5));
        var captured = await CaptureAsync();
        InterceptingBackupSink.BeforeArtifactDelete = async _ => await Task.Delay(TimeSpan.FromSeconds(8));

        var deleted = await Gate.RetireAsync(captured.Manifest, BackupRetireKind.Delete, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(deleted, Is.True, "the delete must complete and report it");
            Assert.That(await _fixture.Catalog.GetAsync(captured.BackupId), Is.Null);
            Assert.That(await _fixture.Sink.ManifestExistsAsync(captured.BackupId), Is.False);
        });
    }

    [Test]
    public async Task A_delete_interrupted_after_its_row_was_removed_is_finished_by_a_retry()
    {
        RedProof.NoResume = true; // RED-PROOF
        // The first artifact delete fails, after the row is gone. The sink manifest
        // is deleted last, so the retry finds it and finishes the delete.
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(intercepted: true);
        var captured = await CaptureAsync();
        var failed = false;
        InterceptingBackupSink.BeforeArtifactDelete = _ =>
        {
            if (failed)
            {
                return Task.CompletedTask;
            }

            failed = true;
            throw new InvalidOperationException("simulated sink fault mid-delete");
        };

        await Assert.ThatAsync(
            () => Gate.RetireAsync(captured.Manifest, BackupRetireKind.Delete, CancellationToken.None),
            Throws.InvalidOperationException);
        Assert.That(await _fixture.Catalog.GetAsync(captured.BackupId), Is.Null, "precondition: the row is gone");

        var retried = await Gate.RetireAsync(captured.Manifest, BackupRetireKind.Delete, CancellationToken.None);

        var artifacts = new List<string>();
        await foreach (var artifactId in _fixture.Sink.ListArtifactIdsAsync())
        {
            artifacts.Add(artifactId);
        }

        Assert.Multiple(async () =>
        {
            Assert.That(retried, Is.True, "the retry must finish the interrupted delete");
            Assert.That(await _fixture.Sink.ManifestExistsAsync(captured.BackupId), Is.False, "the sink manifest is deleted");
            Assert.That(artifacts, Does.Not.Contain(captured.Manifest.ContentDescriptors.Single().ArtifactId), "the artifact is deleted");
        });
    }

    private IBackupPublicationGate Gate => _fixture.SiloServices.GetRequiredService<IBackupPublicationGate>();

    private async Task<LatticeBackupCaptureResult> CaptureAsync()
    {
        await _fixture.GrainFactory.GetGrain<ILattice>(Tree).SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        return await _fixture.CaptureService.CaptureAsync(
            new LatticeBackupCaptureRequest("delete-me", BackupScopeSelector.WholeTree(Tree)));
    }
}
