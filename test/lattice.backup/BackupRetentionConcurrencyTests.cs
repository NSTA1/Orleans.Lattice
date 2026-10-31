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
    }

    [Test]
    public async Task A_pass_before_the_increment_wrote_its_manifest_makes_the_increment_fall_back_to_a_full_backup()
    {
        var (scope, baseId) = await DeployWithBaseAsync();
        string? newerFull = null;
        InterceptingBackupSink.BeforeManifestWrite = async manifest =>
        {
            if (manifest.BaseBackupId != baseId || newerFull is not null)
            {
                return;
            }

            newerFull = await CaptureNewerFullAndPruneAsync(scope);
        };

        var result = await _fixture.IncrementalService.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("incr", scope, baseId));

        Assert.Multiple(() =>
        {
            Assert.That(newerFull, Is.Not.Null, "the pass must have run inside the capture");
            Assert.That(result.Manifest.Kind, Is.EqualTo(BackupKind.Full), "an increment on a pruned base must fall back");
        });
        await AssertEveryListedChainResolvesAsync();
        await foreach (var manifest in _fixture.Sink.ListManifestsAsync())
        {
            Assert.That(manifest.BaseBackupId, Is.Not.EqualTo(baseId), "the withdrawn increment must leave no sink manifest");
        }
    }

    private async Task<(BackupScopeSelector Scope, string BaseId)> DeployWithBaseAsync()
    {
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(
            o =>
            {
                o.RetentionEnabled = true;
                o.RetentionKeepLast = 1;
            },
            intercepted: true);
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
