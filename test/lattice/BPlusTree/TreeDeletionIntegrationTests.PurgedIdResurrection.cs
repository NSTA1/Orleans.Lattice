using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression coverage for issue #4219: a purged tree id was registered again by
/// any later options resolve, so a deletion did not delete and recover-after-purge
/// silently succeeded. The defect has two independent halves, each pinned by its
/// own test below, plus the reuse that issue #3940 defines and that must keep
/// working.
/// </summary>
public partial class TreeDeletionIntegrationTests
{
    private const string HotShardMonitorReminder = "hot-shard-monitor";
    private const string ShardHealingReminder = "shard-healing";

    /// <summary>
    /// Half 1: a read can create a registry row. A read-side options resolve -
    /// every silo's resolver, as a late shard, leaf or background resolve reaches
    /// it, and a client read through the public API - must not recreate a purged
    /// tree's row.
    /// </summary>
    [Test]
    public async Task A_read_side_options_resolve_does_not_reregister_a_purged_tree()
    {
        var treeName = $"purged-resolve-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        var registry = _cluster.GrainFactory.GetLatticeRegistry();

        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await router.DeleteTreeAsync();
        await router.PurgeTreeAsync();
        Assert.That(await registry.ExistsAsync(treeName), Is.False, "precondition: the purge removed the row");

        foreach (var silo in _cluster.GetActiveSilos().Cast<InProcessSiloHandle>())
        {
            var resolver = silo.SiloHost.Services.GetRequiredService<LatticeOptionsResolver>();
            try { await resolver.ResolveAsync(treeName); }
            catch (InvalidOperationException) { }
        }

        try { await router.GetAsync("a"); }
        catch (InvalidOperationException) { }

        Assert.That(await registry.ExistsAsync(treeName), Is.False, "a read recreated the purged tree's registry row");
        Assert.ThrowsAsync<InvalidOperationException>(() => router.RecoverTreeAsync());
    }

    /// <summary>
    /// Half 2: lifecycle leak. Delete and purge stop the per-tree autonomic loops
    /// the first write armed, and a pass that still runs afterwards - the issue's
    /// deterministic repro - stops instead of sweeping, or recreating, the purged id.
    /// </summary>
    [Test]
    public async Task Delete_and_purge_stop_the_autonomic_loops_and_a_late_pass_skips_the_purged_tree()
    {
        var treeName = $"purged-loops-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        var registry = _cluster.GrainFactory.GetLatticeRegistry();
        var monitor = _cluster.GrainFactory.GetGrain<IHotShardMonitorGrain>(treeName);
        var healing = _cluster.GrainFactory.GetGrain<IShardHealingOrchestratorGrain>(treeName);

        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await monitor.EnsureRunningAsync();
        await healing.EnsureRunningAsync();
        Assert.That(await WaitForLoopRemindersAsync(monitor, healing, present: true), Is.True,
            "precondition: the first write armed both loops");

        await router.DeleteTreeAsync();
        Assert.That(await HasReminderAsync(monitor, HotShardMonitorReminder), Is.False, "delete left the hot-shard monitor armed");
        Assert.That(await HasReminderAsync(healing, ShardHealingReminder), Is.False, "delete left shard healing armed");

        await router.PurgeTreeAsync();
        Assert.That(await registry.ExistsAsync(treeName), Is.False, "precondition: the purge removed the row");

        Assert.DoesNotThrowAsync(() => healing.RunHealingPassAsync());
        Assert.DoesNotThrowAsync(() => monitor.RunSamplingPassAsync());

        Assert.Multiple(async () =>
        {
            Assert.That(await registry.ExistsAsync(treeName), Is.False, "a pass recreated the purged tree's registry row");
            Assert.That(await HasReminderAsync(monitor, HotShardMonitorReminder), Is.False, "the hot-shard monitor is still armed after the purge");
            Assert.That(await HasReminderAsync(healing, ShardHealingReminder), Is.False, "shard healing is still armed after the purge");
        });
        Assert.ThrowsAsync<InvalidOperationException>(() => router.RecoverTreeAsync());
    }

    /// <summary>
    /// Issue #3940's reuse, after a refused read: the read fails closed, and the
    /// next write still registers a new, empty tree under the purged id.
    /// </summary>
    [Test]
    public async Task A_write_after_a_refused_read_reuses_a_purged_id()
    {
        var treeName = $"purged-rewrite-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await router.DeleteTreeAsync();
        await router.PurgeTreeAsync();

        Assert.ThrowsAsync<InvalidOperationException>(() => router.GetAsync("a"), "a read of a purged tree must fail closed");

        await router.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        var status = await _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(treeName).GetDeletionStatusAsync();
        Assert.Multiple(async () =>
        {
            Assert.That(await router.TreeExistsAsync(), Is.True);
            Assert.That(Encoding.UTF8.GetString((await router.GetAsync("b"))!), Is.EqualTo("2"));
            Assert.That(await router.GetAsync("a"), Is.Null, "a purged tree's data must not come back");
            Assert.That(status.IsDeleted, Is.False);
        });
    }

    /// <summary>
    /// Issue #3940's reuse through an explicit create: registering the id again
    /// makes it a live, empty tree that reads answer rather than refuse.
    /// </summary>
    [Test]
    public async Task An_explicit_create_after_a_refused_read_reuses_a_purged_id()
    {
        var treeName = $"purged-recreate-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await router.DeleteTreeAsync();
        await router.PurgeTreeAsync();

        Assert.ThrowsAsync<InvalidOperationException>(() => router.GetAsync("a"), "a read of a purged tree must fail closed");

        await _cluster.GrainFactory.GetLatticeRegistry().RegisterAsync(treeName);

        Assert.Multiple(async () =>
        {
            Assert.That(await router.TreeExistsAsync(), Is.True);
            Assert.That(await router.GetAsync("a"), Is.Null, "a purged tree's data must not come back");
            Assert.That(await _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(treeName).IsDeletedAsync(), Is.False);
        });
    }

    private async Task<bool> WaitForLoopRemindersAsync(
        IHotShardMonitorGrain monitor, IShardHealingOrchestratorGrain healing, bool present)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (true)
        {
            if (await HasReminderAsync(monitor, HotShardMonitorReminder) == present
                && await HasReminderAsync(healing, ShardHealingReminder) == present)
                return true;
            if (DateTime.UtcNow > deadline) return false;
            await Task.Delay(100);
        }
    }

    private async Task<bool> HasReminderAsync(IAddressable grain, string reminderName)
    {
        var silo = (InProcessSiloHandle)_cluster.Primary;
        var table = silo.SiloHost.Services.GetRequiredService<IReminderTable>();
        return await table.ReadRow(grain.GetGrainId(), reminderName) is not null;
    }
}
