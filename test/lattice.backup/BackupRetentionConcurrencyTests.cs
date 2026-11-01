using System.Text;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Retention racing an in-flight incremental capture (issue #5357), on a real
/// single-silo cluster. A retention pass is run from inside the increment's own
/// manifest write - just before it, or just after it - so the pass meets the
/// increment at each side of the fence: the increment's base, which the pass's
/// listing dooms because the increment is not yet registered, must either be
/// kept by the pass or refused by the increment, and the catalog must never list
/// an increment whose base is gone.
/// </summary>
[Category("Integration")]
public sealed class BackupRetentionConcurrencyTests
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
    public async Task A_pass_after_the_increment_wrote_its_manifest_keeps_the_base_the_increment_builds_on()
    {
        var (scope, baseId) = await DeployWithBaseAsync();
        string? newerFull = null;
        InterceptingBackupSink.AfterManifestWrite = async manifest =>
        {
            if (manifest.BaseBackupId != baseId || newerFull is not null)
            {
                return;
            }

            newerFull = await CaptureNewerFullAndPruneAsync(scope);
        };

        var increment = await _fixture.IncrementalService.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("incr", scope, baseId));

        Assert.Multiple(() =>
        {
            Assert.That(newerFull, Is.Not.Null, "the pass must have run inside the capture");
            Assert.That(increment.Manifest.Kind, Is.EqualTo(BackupKind.Incremental));
            Assert.That(increment.Manifest.BaseBackupId, Is.EqualTo(baseId));
        });
        await AssertEveryListedChainResolvesAsync();
        Assert.That(await _fixture.Catalog.GetAsync(baseId), Is.Not.Null, "the depended-on base must stay listed");
        Assert.That(await _fixture.Catalog.GetAsync(increment.BackupId), Is.Not.Null, "the published increment must be listed");
    }

    [Test]
    public async Task A_pass_before_the_increment_wrote_its_manifest_makes_the_increment_fall_back_to_a_full_backup()
    {
        RedProof.Routing = true; // RED-PROOF
        var (scope, baseId) = await DeployWithBaseAsync(recordGate: true);
        string? newerFull = null;
        string? incrementId = null;
        InterceptingBackupSink.BeforeManifestWrite = async manifest =>
        {
            if (manifest.BaseBackupId != baseId || newerFull is not null)
            {
                return;
            }

            incrementId = manifest.Id;
            newerFull = await CaptureNewerFullAndPruneAsync(scope);
        };

        var result = await _fixture.IncrementalService.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("incr", scope, baseId));

        Assert.Multiple(() =>
        {
            Assert.That(newerFull, Is.Not.Null, "the pass must have run inside the capture");
            Assert.That(result.Manifest.Kind, Is.EqualTo(BackupKind.Full), "an increment on a pruned base must fall back");
            Assert.That(
                RecordingPublicationGate.Saw(nameof(IBackupPublicationGate.PublishIncrementAsync), incrementId!, "abandoned"),
                Is.True,
                "the abandoned increment's manifest is withdrawn by its publish, under the id's hold");
        });
        await AssertEveryListedChainResolvesAsync();
        await foreach (var manifest in _fixture.Sink.ListManifestsAsync())
        {
            Assert.That(manifest.BaseBackupId, Is.Not.EqualTo(baseId), "the withdrawn increment must leave no sink manifest");
        }
    }

    [Test]
    public async Task A_full_capture_of_an_unchanged_tree_during_a_pass_waits_for_the_pass_and_republishes_the_backup()
    {
        RedProof.BypassHolds = true; // RED-PROOF
        // Issue #5418. A pass lists an expired full backup and removes its row;
        // at the pass's sink scan, a full capture of the unchanged tree starts. A
        // backup id is a content address, so the capture re-derives the expired
        // backup's id. Were that id not held while the pass retires it, the capture
        // would write its sink manifest and register its row, and the pass would
        // then delete the sink manifest it had just written, leaving the catalog
        // listing a backup the sink no longer holds. Held, the capture waits until
        // the pass has deleted the backup and then publishes it afresh.
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(
            o =>
            {
                o.RetentionEnabled = true;
                o.RetentionMaxAge = TimeSpan.FromMilliseconds(200);
            },
            intercepted: true);
        var scope = BackupScopeSelector.WholeTree(Tree);
        await SeedAsync("k1", "v1");
        var expired = await _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("expired", scope));
        await Task.Delay(400);

        Task<LatticeBackupCaptureResult>? recapture = null;
        InterceptingBackupSink.BeforeManifestList = async () =>
        {
            if (recapture is not null)
            {
                return;
            }

            // The capture runs alongside the pass, not inside it: held, its publish
            // waits for this pass. Unheld, it registers the id again here, which ends
            // the wait at once.
            recapture = Task.Run(() => _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("unchanged", scope)));
            var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(3);
            while (DateTimeOffset.UtcNow < deadline && await _fixture.Catalog.GetAsync(expired.BackupId) is null)
            {
                await Task.Delay(50);
            }
        };

        var report = await _fixture.Scheduler(scope).PruneAsync(scope);
        Assert.That(recapture, Is.Not.Null, "the capture must have started inside the pass");
        var recaptured = await recapture!;

        Assert.Multiple(async () =>
        {
            Assert.That(report.PrunedBackupIds, Is.EqualTo(new[] { expired.BackupId }));
            Assert.That(recaptured.BackupId, Is.EqualTo(expired.BackupId), "identical content re-derives the id");
            Assert.That(await _fixture.Catalog.GetAsync(recaptured.BackupId), Is.Not.Null, "the republished backup must stay listed");
            Assert.That((await _fixture.Sink.ProbeAsync(recaptured.BackupId)).IsResolvable, Is.True, "the republished backup must stay resolvable from the sink");
        });
        await AssertEveryListedBackupResolvesFromTheSinkAsync();
    }

    [Test]
    public async Task A_pass_during_a_set_capture_does_not_leave_a_member_listed_over_a_deleted_artifact()
    {
        RedProof.Routing = true; // RED-PROOF
        // Issue #5449. A multi-tree set captures its orders member, and before the
        // items member is written a newer orders backup is captured and a pass runs,
        // which under keep-last-one dooms every older orders backup. Were the member
        // registered at capture and re-registered by the set's membership stamp, the
        // pass would prune it and the stamp would list it again over the artifact
        // the pass had deleted.
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(
            o =>
            {
                o.RetentionEnabled = true;
                o.RetentionKeepLast = 1;
            },
            intercepted: true,
            recordGate: true);
        var orders = BackupScopeSelector.WholeTree(Tree);
        var items = BackupScopeSelector.WholeTree(ItemsTree);
        await SeedAsync("k1", "v1");
        await _fixture.GrainFactory.GetGrain<ILattice>(ItemsTree).SetAsync("i1", Encoding.UTF8.GetBytes("v1"));

        BackupRetentionReport? report = null;
        InterceptingBackupSink.BeforeManifestWrite = async manifest =>
        {
            if (manifest.Scope.TreeId != ItemsTree || report is not null)
            {
                return;
            }

            await Task.Delay(10);
            await SeedAsync("k2", "v2");
            await _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("newer", orders));
            report = await _fixture.Scheduler(orders).PruneAsync(orders);
        };

        var set = await _fixture.CaptureService.CaptureSetAsync(
            new LatticeBackupSetCaptureRequest("set", [orders, items]));

        var member = set.Members.Single(m => m.Manifest.Scope.TreeId == Tree);
        Assert.That(report, Is.Not.Null, "the pass must have run inside the set capture");
        Assert.Multiple(async () =>
        {
            Assert.That(report!.PrunedBackupIds, Does.Not.Contain(member.BackupId), "the pass must not prune an unpublished member");
            Assert.That((await _fixture.Catalog.GetAsync(member.BackupId))?.SetId, Is.EqualTo(set.SetManifest.SetId), "the member must be listed, stamped");
            Assert.That(
                RecordingPublicationGate.Saw(nameof(IBackupPublicationGate.RegisterAsync), member.BackupId, "stamped"),
                Is.True,
                "the stamp registers the member under its id's hold");
        });
        await AssertEveryListedBackupResolvesFromTheSinkAsync();
    }

    [Test]
    public async Task A_set_that_fails_after_a_member_was_captured_lists_that_member_unstamped()
    {
        RedProof.Routing = true; // RED-PROOF
        // Issue #5449. A multi-tree set member's catalog row waits for the set's
        // membership stamp. When the set fails before the stamp - here the items
        // member's manifest write throws, after the orders member was captured -
        // nothing will stamp the orders member, so it must be listed unstamped, as
        // an ordinary backup, rather than left in the sink with no catalog row.
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(intercepted: true, recordGate: true);
        var orders = BackupScopeSelector.WholeTree(Tree);
        var items = BackupScopeSelector.WholeTree(ItemsTree);
        await SeedAsync("k1", "v1");
        await _fixture.GrainFactory.GetGrain<ILattice>(ItemsTree).SetAsync("i1", Encoding.UTF8.GetBytes("v1"));
        InterceptingBackupSink.BeforeManifestWrite = manifest =>
            manifest.Scope.TreeId == ItemsTree
                ? throw new InvalidOperationException("simulated sink fault on the second member")
                : Task.CompletedTask;

        await Assert.ThatAsync(
            async () => await _fixture.CaptureService.CaptureSetAsync(new LatticeBackupSetCaptureRequest("set", [orders, items])),
            Throws.InvalidOperationException);

        var listed = await _fixture.ListScopeAsync(orders);
        Assert.Multiple(() =>
        {
            Assert.That(listed, Has.Count.EqualTo(1), "the captured orders member must be listed");
            Assert.That(listed.Select(m => m.SetId), Has.All.Null, "a member nothing stamped is listed unstamped");
            Assert.That(
                RecordingPublicationGate.Saw(nameof(IBackupPublicationGate.RegisterAsync), listed.Single().Id, "unstamped"),
                Is.True,
                "the unstamped member is registered under its id's hold");
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

    private async Task<(BackupScopeSelector Scope, string BaseId)> DeployWithBaseAsync(bool recordGate = false)
    {
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(
            o =>
            {
                o.RetentionEnabled = true;
                o.RetentionKeepLast = 1;
            },
            intercepted: true,
            recordGate: recordGate);
        var scope = BackupScopeSelector.WholeTree(Tree);
        await SeedAsync("k1", "v1");
        var baseId = await _fixture.Scheduler(scope).TriggerFullAsync(scope);
        Assert.That(baseId, Is.Not.Null);
        await SeedAsync("k2", "v2");
        return (scope, baseId!);
    }

    // A newer full backup is published, so keep-last-one dooms the base the
    // in-flight increment chose, and a retention pass runs.
    private async Task<string> CaptureNewerFullAndPruneAsync(BackupScopeSelector scope)
    {
        await Task.Delay(10);
        await SeedAsync("k3", "v3");
        var newer = await _fixture.CaptureService.CaptureAsync(new LatticeBackupCaptureRequest("newer", scope));
        await _fixture.Scheduler(scope).PruneAsync(scope);
        return newer.BackupId;
    }

    private async Task AssertEveryListedChainResolvesAsync()
    {
        await foreach (var manifest in _fixture.Catalog.ListAsync())
        {
            var current = manifest;
            while (current.BaseBackupId is { } baseId)
            {
                var baseManifest = await _fixture.Catalog.GetAsync(baseId) ?? await _fixture.Sink.ReadManifestAsync(baseId);
                Assert.That(baseManifest, Is.Not.Null, $"listed backup '{manifest.Id}' names missing base '{baseId}'");
                var probe = await _fixture.Sink.ProbeAsync(baseId);
                Assert.That(probe.IsResolvable, Is.True, $"base '{baseId}' of listed backup '{manifest.Id}' is not restorable");
                current = baseManifest!;
            }
        }
    }

    private async Task SeedAsync(string key, string value)
    {
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync(key, Encoding.UTF8.GetBytes(value));
    }
}
