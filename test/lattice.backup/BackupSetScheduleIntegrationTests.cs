using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Backup-set schedules end to end (issue #6067): a scheduled set records its
/// exact membership so it restores whole after a later set re-stamps an unchanged
/// member, retention prunes whole sets without deleting a member a retained set
/// still names, an all-trees schedule picks up new trees only when asked to, runs
/// never stack, and pausing, re-registering and removing behave as documented.
/// </summary>
[Category("Integration")]
public sealed class BackupSetScheduleIntegrationTests
{
    private CaptureClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        BackupInventoryRegistry.Instance.Reset();
        _fixture = new CaptureClusterFixture();
        await _fixture.InitializeAsync();
    }

    [TearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    [Test]
    public async Task An_earlier_set_restores_every_member_after_a_later_set_restamps_an_unchanged_tree()
    {
        var (a, b) = await SeedAsync("restamp");
        var grain = await ScheduleAsync("restamp", [a, b], keepLast: 5);

        var first = await grain.RunCycleAsync();
        await Tree(b).SetAsync("k", Bytes("changed"));
        var second = await grain.RunCycleAsync();

        var status = (await grain.GetStatusAsync())!;
        var older = status.RetainedSets.Single(s => s.SetId == first!.SetId);
        var newer = status.RetainedSets.Single(s => s.SetId == second!.SetId);
        var stamped = new List<string>();
        await foreach (var row in _fixture.Catalog.ListAsync())
        {
            if (row.SetId == older.SetId)
            {
                stamped.Add(row.Scope.TreeId);
            }
        }

        var resolved = await _fixture.SiloServices.GetRequiredService<ILatticeBackupSetResolver>()
            .ResolveMembersAsync(older.SetId);

        Assert.Multiple(() =>
        {
            Assert.That(first!.Outcome, Is.EqualTo(BackupScopeRunOutcome.Success));
            Assert.That(second!.Outcome, Is.EqualTo(BackupScopeRunOutcome.Success));
            Assert.That(older.Members.Single(m => m.TreeId == a).BackupId,
                Is.EqualTo(newer.Members.Single(m => m.TreeId == a).BackupId), "tree a did not change");
            Assert.That(stamped, Is.EqualTo(new[] { b }), "the later set moved tree a's stamp");
            Assert.That(resolved.Select(m => m.TreeId), Is.EqualTo(new[] { a, b }), "the recorded membership resolves both trees");
            Assert.That(status.RecentRuns, Has.Count.EqualTo(2));
            Assert.That(status.LastOutcome, Is.EqualTo(BackupScopeRunOutcome.Success));
        });
    }

    [Test]
    public async Task Retention_prunes_whole_sets_and_keeps_a_member_a_retained_set_still_names()
    {
        var (a, b) = await SeedAsync("prune");
        var grain = await ScheduleAsync("prune", [a, b], keepLast: 1);

        var first = await grain.RunCycleAsync();
        await Tree(b).SetAsync("k", Bytes("changed"));
        await grain.RunCycleAsync();

        var status = (await grain.GetStatusAsync())!;
        var firstSet = (await ListRowsAsync()).Where(r => r.Scope.TreeId == b).ToList();
        var sharedA = status.RetainedSets.Single().Members.Single(m => m.TreeId == a).BackupId;
        var heldA = await _fixture.GrainFactory
            .GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(BackupScopeSelector.WholeTree(a)))
            .IsHeldBySetScheduleAsync(sharedA, "another-schedule");

        Assert.Multiple(() =>
        {
            Assert.That(status.RetainedSets, Has.Count.EqualTo(1));
            Assert.That(status.RetainedSets[0].SetId, Is.Not.EqualTo(first!.SetId));
            Assert.That(firstSet, Has.Count.EqualTo(1), "the pruned set's own member of tree b was deleted");
            Assert.That(heldA, Is.True, "the retained set's member is held from per-tree retention");
        });
    }

    [Test]
    public async Task An_all_trees_schedule_captures_new_trees_only_when_it_includes_them()
    {
        var (a, b) = await SeedAsync("all");
        var dynamic = await ScheduleAllTreesAsync("dynamic", includeNewTrees: true);
        var pinned = await ScheduleAllTreesAsync("pinned", includeNewTrees: false, pinnedScopes: [a, b]);

        var c = "all-c-" + Guid.NewGuid().ToString("N");
        await Tree(c).SetAsync("k", Bytes("v"));
        await dynamic.RunCycleAsync();
        await pinned.RunCycleAsync();

        var dynamicMembers = (await dynamic.GetStatusAsync())!.RetainedSets.Single().Members.Select(m => m.TreeId).ToList();
        var pinnedMembers = (await pinned.GetStatusAsync())!.RetainedSets.Single().Members.Select(m => m.TreeId).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(dynamicMembers, Does.Contain(c).And.Contain(a).And.Contain(b));
            Assert.That(dynamicMembers.Where(t => t.StartsWith("sys-", StringComparison.Ordinal)), Is.Empty);
            Assert.That(pinnedMembers, Is.EquivalentTo(new[] { a, b }));
        });
    }

    [Test]
    public async Task RunNowAsync_joins_a_run_already_in_flight()
    {
        var (a, b) = await SeedAsync("coalesce");
        var grain = await ScheduleAsync("coalesce", [a, b], keepLast: 2);

        var started = await grain.RunNowAsync();
        var joined = await grain.RunNowAsync();
        await TestPoll.UntilAsync(
            async () => (await grain.GetStatusAsync()) is { IsRunning: false, RecentRuns.Count: 1 },
            "the run started by RunNowAsync to finish",
            timeout: TimeSpan.FromSeconds(60));
        var status = (await grain.GetStatusAsync())!;

        Assert.Multiple(() =>
        {
            Assert.That(started, Is.True);
            Assert.That(joined, Is.False);
            Assert.That(status.RecentRuns, Has.Count.EqualTo(1), "the second request did not stack a run");
            Assert.That(status.RecentRuns[0].Trigger, Is.EqualTo(BackupSetScheduleTrigger.Manual));
        });
    }

    [Test]
    public async Task Rescheduling_with_the_same_interval_keeps_the_clock_and_pausing_stops_it()
    {
        var (a, b) = await SeedAsync("clock");
        var grain = await ScheduleAsync("clock", [a, b], keepLast: 2);
        var registered = (await grain.GetStatusAsync())!;

        await Task.Delay(50);
        var rescheduled = await ScheduleAsync("clock", [a, b], keepLast: 4);
        var again = (await rescheduled.GetStatusAsync())!;
        await grain.PauseAsync();
        var paused = (await grain.GetStatusAsync())!;

        Assert.Multiple(() =>
        {
            Assert.That(registered.IsScheduled, Is.True);
            Assert.That(registered.NextRunUtc, Is.Not.Null);
            Assert.That(again.RegisteredAtUtc, Is.EqualTo(registered.RegisteredAtUtc), "the clock did not restart");
            Assert.That(again.Request.RetentionKeepLast, Is.EqualTo(4), "the configuration did update");
            Assert.That(paused.IsScheduled, Is.False);
            Assert.That(paused.NextRunUtc, Is.Null);
        });
    }

    [Test]
    public async Task RemoveAsync_deletes_the_retained_sets_and_lifts_the_holds()
    {
        var (a, b) = await SeedAsync("remove");
        var grain = await ScheduleAsync("remove", [a, b], keepLast: 2);
        var run = await grain.RunCycleAsync();
        var members = (await grain.GetSetAsync(run!.SetId!))!.Members;

        var removed = await grain.RemoveAsync(deleteRetainedSets: true);
        var rows = await ListRowsAsync();
        var registry = await _fixture.GrainFactory
            .GetGrain<IBackupSetScheduleRegistryGrain>(IBackupSetScheduleRegistryGrain.SingletonKey).ListAsync();
        var held = await _fixture.GrainFactory
            .GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(members[0].Scope))
            .IsHeldBySetScheduleAsync(members[0].BackupId, "another-schedule");

        var statusAfter = await grain.GetStatusAsync();
        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.True);
            Assert.That(statusAfter, Is.Null);
            Assert.That(rows.Select(r => r.Id).Intersect(members.Select(m => m.BackupId)), Is.Empty);
            Assert.That(registry, Is.Empty);
            Assert.That(held, Is.False);
        });
    }

    private async Task<(string A, string B)> SeedAsync(string prefix)
    {
        var suffix = Guid.NewGuid().ToString("N");
        var a = $"{prefix}-a-{suffix}";
        var b = $"{prefix}-b-{suffix}";
        await Tree(a).SetAsync("k", Bytes("old"));
        await Tree(b).SetAsync("k", Bytes("old"));
        return (a, b);
    }

    private async Task<ILatticeBackupSetSchedulerGrain> ScheduleAsync(string name, string[] trees, int keepLast)
    {
        var scopes = trees.Select(BackupScopeSelector.WholeTree).ToList();
        var grain = _fixture.GrainFactory.GetGrain<ILatticeBackupSetSchedulerGrain>(BackupSetScheduleKey.For(name));
        await grain.ScheduleAsync(
            LatticeBackupSetScheduleRequest.ForTrees(name, scopes, TimeSpan.FromHours(6), keepLast, null),
            new BackupSetScheduleTarget(null, scopes, []));
        return grain;
    }

    private async Task<ILatticeBackupSetSchedulerGrain> ScheduleAllTreesAsync(
        string name, bool includeNewTrees, string[]? pinnedScopes = null)
    {
        var grain = _fixture.GrainFactory.GetGrain<ILatticeBackupSetSchedulerGrain>(BackupSetScheduleKey.For(name));
        await grain.ScheduleAsync(
            LatticeBackupSetScheduleRequest.ForAllTrees(name, TimeSpan.FromHours(6), includeNewTrees, 2, null),
            new BackupSetScheduleTarget(
                null, (pinnedScopes ?? []).Select(BackupScopeSelector.WholeTree).ToList(), []));
        return grain;
    }

    private async Task<List<BackupManifest>> ListRowsAsync()
    {
        var rows = new List<BackupManifest>();
        await foreach (var row in _fixture.Catalog.ListAsync())
        {
            rows.Add(row);
        }

        return rows;
    }

    private ILattice Tree(string treeId) => _fixture.GrainFactory.GetGrain<ILattice>(treeId);

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);
}
