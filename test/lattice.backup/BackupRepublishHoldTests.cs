using System.Text;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// A backup id is a content address, so a capture of identical content
/// republishes an existing backup (issue #5418). These tests drive a republish
/// against a retention pass, a set's membership stamp and the publication gate's
/// retry on a real single-silo cluster, and pin that the scope's per-id hold
/// keeps the catalog and the sink agreeing: no pass deletes a backup a capture
/// is republishing or has just republished, and a republish that meets a pass
/// waits for it rather than failing.
/// </summary>
[Category("Integration")]
public sealed class BackupRepublishHoldTests
{
    private const string Tree = "orders";

    private const string ItemsTree = "items";

    private SchedulerClusterFixture _fixture = null!;

    [TearDown]
    public async Task TearDown()
    {
        InterceptingBackupSink.Reset();
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task A_pass_never_dooms_a_backup_a_capture_is_republishing()
    {
        // The republish has written its sink manifest and not yet registered its
        // row when a pass lists the old, expired row. Were the id not held, the
        // pass would remove the row, delete the manifest the republish had just
        // written, and the republish would then register a row the sink no longer
        // backs.
        var (scope, expired) = await DeployWithExpiredBackupAsync();
        BackupRetentionReport? report = null;
        InterceptingBackupSink.AfterManifestWrite = async manifest =>
        {
            if (manifest.Id != expired.BackupId || report is not null)
            {
                return;
            }

            report = await _fixture.Scheduler(scope).PruneAsync(scope);
        };

        var republished = await _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("unchanged", scope));

        Assert.That(report, Is.Not.Null, "the pass must have run inside the republish");
        Assert.Multiple(async () =>
        {
            Assert.That(republished.BackupId, Is.EqualTo(expired.BackupId), "identical content re-derives the id");
            Assert.That(report!.PrunedBackupIds, Does.Not.Contain(expired.BackupId), "a pass never dooms an id being published");
            Assert.That(await _fixture.Catalog.GetAsync(expired.BackupId), Is.Not.Null, "the republished backup must stay listed");
        });
        await AssertEveryListedBackupResolvesFromTheSinkAsync();
    }

    [Test]
    public async Task A_republish_that_meets_a_pass_retiring_its_id_waits_beyond_one_call_and_then_publishes()
    {
        // The pass is held deleting the doomed backup's artifact for longer than
        // one publish call may wait, as on a slow sink. The republish of that id
        // must be refused busy, retried by the publication gate, and published
        // fresh once the pass is done with the id - never failed.
        var (scope, expired) = await DeployWithExpiredBackupAsync(
            o =>
            {
                o.PublishHoldWait = TimeSpan.FromMilliseconds(200);
                o.PublishHoldRetryDelay = TimeSpan.FromMilliseconds(50);
            });
        Task<LatticeBackupCaptureResult>? republish = null;
        InterceptingBackupSink.BeforeArtifactDelete = async artifactId =>
        {
            if (republish is not null || artifactId != expired.Manifest.ContentDescriptors.Single().ArtifactId)
            {
                return;
            }

            republish = Task.Run(() => _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("unchanged", scope)));
            await Task.Delay(TimeSpan.FromSeconds(1.5));
        };

        var report = await _fixture.Scheduler(scope).PruneAsync(scope);
        Assert.That(republish, Is.Not.Null, "the republish must have started inside the pass");
        var republished = await republish!;

        Assert.Multiple(async () =>
        {
            Assert.That(report.PrunedBackupIds, Is.EqualTo(new[] { expired.BackupId }), "the pass deletes the backup it doomed");
            Assert.That(republished.BackupId, Is.EqualTo(expired.BackupId), "identical content re-derives the id");
            Assert.That(await _fixture.Catalog.GetAsync(republished.BackupId), Is.Not.Null, "the republish lists the backup afresh");
        });
        await AssertEveryListedBackupResolvesFromTheSinkAsync();
    }

    [Test]
    public async Task A_pass_never_dooms_a_set_member_awaiting_its_stamp()
    {
        // The set's orders member republishes an expired plain backup: its sink
        // manifest is rewritten and its row waits for the set's membership stamp.
        // A pass in between lists the old, expired row; the member is held, so the
        // pass must not doom it, and the stamp lists it in the set.
        var (orders, expired) = await DeployWithExpiredBackupAsync();
        var items = BackupScopeSelector.WholeTree(ItemsTree);
        await _fixture.GrainFactory.GetGrain<ILattice>(ItemsTree).SetAsync("i1", Encoding.UTF8.GetBytes("v1"));
        BackupRetentionReport? report = null;
        InterceptingBackupSink.BeforeManifestWrite = async manifest =>
        {
            if (manifest.Scope.TreeId != ItemsTree || report is not null)
            {
                return;
            }

            report = await _fixture.Scheduler(orders).PruneAsync(orders);
        };

        var set = await _fixture.CaptureService.CaptureSetAsync(new LatticeBackupSetCaptureRequest("set", [orders, items]));

        var member = set.Members.Single(m => m.Manifest.Scope.TreeId == Tree);
        Assert.That(report, Is.Not.Null, "the pass must have run inside the set capture");
        Assert.Multiple(async () =>
        {
            Assert.That(member.BackupId, Is.EqualTo(expired.BackupId), "the member republishes the unchanged tree's backup");
            Assert.That(report!.PrunedBackupIds, Does.Not.Contain(member.BackupId), "a pass never dooms a member awaiting its stamp");
            Assert.That((await _fixture.Catalog.GetAsync(member.BackupId))?.SetId, Is.EqualTo(set.SetManifest.SetId), "the stamp lists the member in the set");
        });
        await AssertEveryListedBackupResolvesFromTheSinkAsync();
    }

    [Test]
    public async Task A_republish_refreshes_the_ranking_so_retention_keeps_the_backup_a_capture_just_returned()
    {
        // As 10.2.0 shipped it, a republish kept the backup's first capture time,
        // which is all retention ranked by: a max-age pass right after a capture
        // of an unchanged tree pruned the very backup the capture had returned.
        var (scope, expired) = await DeployWithExpiredBackupAsync();

        var republished = await _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("unchanged", scope));
        var report = await _fixture.Scheduler(scope).PruneAsync(scope);

        var row = await _fixture.Catalog.GetAsync(republished.BackupId);
        Assert.Multiple(() =>
        {
            Assert.That(republished.BackupId, Is.EqualTo(expired.BackupId), "identical content re-derives the id");
            Assert.That(report.PrunedBackupIds, Is.Empty, "a backup captured just now is not pruned as old");
            Assert.That(row, Is.Not.Null);
            Assert.That(row!.CreatedAtUtc, Is.EqualTo(expired.Manifest.CreatedAtUtc), "the first capture time is immutable");
            Assert.That(row.LastCapturedAtUtc, Is.GreaterThan(row.CreatedAtUtc), "the latest capture is recorded");
        });
        await AssertEveryListedBackupResolvesFromTheSinkAsync();
    }

    [Test]
    public async Task An_identical_increment_whose_base_row_is_gone_falls_back_without_withdrawing_the_published_one()
    {
        // An increment is published; then its base's row is removed, as a pass
        // that crashed after removing it would leave it. A second, identical
        // increment re-derives the published one's id and must abandon - its base's
        // row is gone - but without withdrawing the manifest the published
        // increment's listed row needs.
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(intercepted: true, recordGate: true);
        var scope = BackupScopeSelector.WholeTree(Tree);
        await SeedAsync("k1", "v1");
        var baseBackup = await _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("base", scope));
        await SeedAsync("k2", "v2");
        var published = await _fixture.IncrementalService.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("incr", scope, baseBackup.BackupId));
        Assert.That(published.Manifest.Kind, Is.EqualTo(BackupKind.Incremental), "precondition: the first increment is published");
        await _fixture.Catalog.RemoveAsync(baseBackup.BackupId);

        var second = await _fixture.IncrementalService.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("incr-again", scope, baseBackup.BackupId));

        Assert.Multiple(async () =>
        {
            Assert.That(
                RecordingPublicationGate.Saw(nameof(IBackupPublicationGate.PublishIncrementAsync), published.BackupId, "abandoned"),
                Is.True,
                "the identical increment must reach the publish of the same id, and abandon there");
            Assert.That(second.Manifest.Kind, Is.EqualTo(BackupKind.Full), "the abandoned increment falls back to a full backup");
            Assert.That(await _fixture.Catalog.GetAsync(published.BackupId), Is.Not.Null, "the published increment stays listed");
            Assert.That(await _fixture.Sink.ManifestExistsAsync(published.BackupId), Is.True, "its sink manifest is not withdrawn");
        });
    }

    [Test]
    public async Task A_set_member_whose_stamp_never_comes_is_held_from_retention_only_until_its_lease_runs_out()
    {
        // A set's member republishes an expired backup and its capture dies before
        // the set's stamp (issue #5629): the member's hold must lapse, or the
        // backup it republished is exempt from retention for good.
        var (scope, expired) = await DeployWithExpiredBackupAsync(o => o.PendingStampLease = TimeSpan.FromSeconds(1));
        var published = await _fixture.Scheduler(scope).PublishAsync(
            new BackupPublishRequest(expired.Manifest, BackupPublishMode.Full, Register: false));
        Assert.That(published.Outcome, Is.EqualTo(BackupPublishOutcome.Published), "precondition: the member is written");

        var held = await _fixture.Scheduler(scope).PruneAsync(scope);
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        var lapsed = await _fixture.Scheduler(scope).PruneAsync(scope);

        Assert.Multiple(() =>
        {
            Assert.That(held.PrunedBackupIds, Is.Empty, "a member awaiting its stamp is held from the pass");
            Assert.That(lapsed.PrunedBackupIds, Is.EqualTo(new[] { expired.BackupId }), "the hold lapses with its lease");
        });
    }

    private async Task<(BackupScopeSelector Scope, LatticeBackupCaptureResult Expired)> DeployWithExpiredBackupAsync(
        Action<LatticeBackupOptions>? configureBackup = null)
    {
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(
            o =>
            {
                o.RetentionEnabled = true;
                o.RetentionMaxAge = TimeSpan.FromSeconds(3);
            },
            intercepted: true,
            configureBackup: configureBackup);
        var scope = BackupScopeSelector.WholeTree(Tree);
        await SeedAsync("k1", "v1");
        var expired = await _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("expired", scope));
        await Task.Delay(TimeSpan.FromSeconds(3.5));
        return (scope, expired);
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
