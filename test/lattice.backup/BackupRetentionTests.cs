using System.Text;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Retention coverage for the backup scheduler grain: pruning is a no-op when
/// retention is disabled; a keep-last-N policy prunes the oldest backups beyond
/// the bound; and the base chain of a retained increment is always preserved even
/// when the count bound alone would prune it.
/// </summary>
[Category("Integration")]
public sealed class BackupRetentionTests
{
    private const string Tree = "orders";

    private SchedulerClusterFixture _fixture = null!;

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task PruneAsync_retains_everything_when_retention_is_disabled()
    {
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(Tree);
        await CaptureChainAsync(scope);

        var report = await _fixture.Scheduler(scope).PruneAsync(scope);

        var remaining = await _fixture.ListScopeAsync(scope);
        Assert.Multiple(() =>
        {
            Assert.That(report.PrunedCount, Is.Zero);
            Assert.That(report.RetainedCount, Is.EqualTo(4));
            Assert.That(remaining, Has.Count.EqualTo(4));
        });
    }

    [Test]
    public async Task PruneAsync_keep_last_two_prunes_the_oldest_chain()
    {
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(o =>
        {
            o.RetentionEnabled = true;
            o.RetentionKeepLast = 2;
        });
        var scope = BackupScopeSelector.WholeTree(Tree);
        var (b0, i1, b2, i3) = await CaptureChainAsync(scope);

        var report = await _fixture.Scheduler(scope).PruneAsync(scope);

        var remainingIds = (await _fixture.ListScopeAsync(scope)).Select(m => m.Id).ToHashSet();
        Assert.Multiple(() =>
        {
            Assert.That(report.RetainedCount, Is.EqualTo(2));
            Assert.That(report.PrunedBackupIds, Is.EquivalentTo(new[] { b0, i1 }));
            Assert.That(remainingIds, Is.EquivalentTo(new[] { b2, i3 }));
        });
    }

    [Test]
    public async Task PruneAsync_keep_last_one_preserves_the_base_of_a_retained_increment()
    {
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(o =>
        {
            o.RetentionEnabled = true;
            o.RetentionKeepLast = 1;
        });
        var scope = BackupScopeSelector.WholeTree(Tree);
        var (b0, i1, b2, i3) = await CaptureChainAsync(scope);

        var report = await _fixture.Scheduler(scope).PruneAsync(scope);

        var remainingIds = (await _fixture.ListScopeAsync(scope)).Select(m => m.Id).ToHashSet();
        Assert.Multiple(() =>
        {
            // keep-last-1 selects only i3, but its base b2 is pulled in by the
            // base-closure protection, so two backups survive, not one.
            Assert.That(report.RetainedCount, Is.EqualTo(2));
            Assert.That(report.PrunedBackupIds, Is.EquivalentTo(new[] { b0, i1 }));
            Assert.That(remainingIds, Is.EquivalentTo(new[] { b2, i3 }));
            Assert.That(remainingIds, Does.Contain(b2), "the base of the retained increment must survive");
        });
    }

    // Captures full -> incremental -> full -> incremental, mutating between each so
    // every capture is a distinct content-addressed backup. Returns the four ids in
    // capture order: b0 (full), i1 (increment of b0), b2 (full), i3 (increment of b2).
    private async Task<(string B0, string I1, string B2, string I3)> CaptureChainAsync(
        BackupScopeSelector scope)
    {
        var scheduler = _fixture.Scheduler(scope);

        await SeedAsync("k1", "v1");
        var b0 = await scheduler.TriggerFullAsync(scope);
        await Task.Delay(10);

        await SeedAsync("k2", "v2");
        var i1 = await scheduler.TriggerIncrementalAsync(scope);
        await Task.Delay(10);

        await SeedAsync("k3", "v3");
        var b2 = await scheduler.TriggerFullAsync(scope);
        await Task.Delay(10);

        await SeedAsync("k4", "v4");
        var i3 = await scheduler.TriggerIncrementalAsync(scope);

        Assert.Multiple(() =>
        {
            Assert.That(b0, Is.Not.Null);
            Assert.That(i1, Is.Not.Null);
            Assert.That(b2, Is.Not.Null);
            Assert.That(i3, Is.Not.Null);
        });
        return (b0!, i1!, b2!, i3!);
    }

    private async Task SeedAsync(string key, string value)
    {
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync(key, Encoding.UTF8.GetBytes(value));
    }
}
