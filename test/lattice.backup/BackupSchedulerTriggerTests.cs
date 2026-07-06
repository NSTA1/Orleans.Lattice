using System.Text;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// On-demand triggering coverage for the backup scheduler grain: a full trigger
/// captures a full backup; an incremental trigger with an existing base captures
/// an increment that records the base id and chains to it; and an incremental
/// trigger with no existing backup falls back to a full baseline capture.
/// </summary>
[Category("Integration")]
public sealed class BackupSchedulerTriggerTests
{
    private const string Tree = "orders";

    private SchedulerClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new SchedulerClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task TriggerFullAsync_captures_a_full_backup_for_the_scope()
    {
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(Tree);
        await SeedAsync("k1", "v1");

        var backupId = await _fixture.Scheduler(scope).TriggerFullAsync(scope);

        Assert.That(backupId, Is.Not.Null);
        var manifest = await _fixture.Catalog.GetAsync(backupId!);
        Assert.Multiple(() =>
        {
            Assert.That(manifest, Is.Not.Null);
            Assert.That(manifest!.Kind, Is.EqualTo(BackupKind.Full));
            Assert.That(manifest.BaseBackupId, Is.Null);
        });
    }

    [Test]
    public async Task TriggerIncrementalAsync_layers_the_increment_on_the_latest_base()
    {
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(Tree);
        await SeedAsync("k1", "v1");
        var baseId = await _fixture.Scheduler(scope).TriggerFullAsync(scope);

        // Mutate so the increment's snapshot differs from the base snapshot.
        await SeedAsync("k2", "v2");
        var incrementalId = await _fixture.Scheduler(scope).TriggerIncrementalAsync(scope);

        var increment = await _fixture.Catalog.GetAsync(incrementalId!);
        var stillHasBase = await _fixture.Catalog.GetAsync(baseId!);
        Assert.Multiple(() =>
        {
            Assert.That(incrementalId, Is.Not.Null.And.Not.EqualTo(baseId));
            Assert.That(increment, Is.Not.Null);
            Assert.That(increment!.Kind, Is.EqualTo(BackupKind.Incremental));
            Assert.That(increment.BaseBackupId, Is.EqualTo(baseId));
            // The base the increment chains to must survive the incremental capture.
            Assert.That(stillHasBase, Is.Not.Null);
        });
    }

    [Test]
    public async Task TriggerIncrementalAsync_with_no_base_falls_back_to_a_full_baseline()
    {
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(Tree);
        await SeedAsync("k1", "v1");

        var backupId = await _fixture.Scheduler(scope).TriggerIncrementalAsync(scope);

        var manifest = await _fixture.Catalog.GetAsync(backupId!);
        Assert.Multiple(() =>
        {
            Assert.That(backupId, Is.Not.Null);
            Assert.That(manifest, Is.Not.Null);
            // No base existed, so the first increment is captured as a full baseline.
            Assert.That(manifest!.Kind, Is.EqualTo(BackupKind.Full));
            Assert.That(manifest.BaseBackupId, Is.Null);
        });
    }

    private async Task SeedAsync(string key, string value)
    {
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync(key, Encoding.UTF8.GetBytes(value));
    }
}
