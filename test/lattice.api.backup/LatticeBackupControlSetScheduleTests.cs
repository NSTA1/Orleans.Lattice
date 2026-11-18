using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Coverage for the backup-set schedule verbs of <see cref="ILatticeBackupControl"/>
/// (issue #6067): a permitted call registers, reads, lists, runs, pauses and
/// removes a schedule; a denying gate fails every verb closed before the schedule
/// is touched; and an all-trees schedule that captures trees created later needs
/// the wide grant.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupControlSetScheduleTests
{
    private ApiBackupClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp()
    {
        BackupInventoryRegistry.Instance.Reset();
        _fixture = new ApiBackupClusterFixture();
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task ScheduleBackupSetAsync_registers_a_schedule_that_reads_back_and_lists()
    {
        await _fixture.InitializeAsync();
        var (a, b) = await SeedAsync();

        var status = await _fixture.Control.ScheduleBackupSetAsync(Request("nightly", a, b));
        var read = await _fixture.Control.GetBackupSetScheduleStatusAsync("nightly");
        var listed = await _fixture.Control.ListBackupSetSchedulesAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.Name, Is.EqualTo("nightly"));
            Assert.That(status.IsScheduled, Is.True);
            Assert.That(status.NextRunUtc, Is.Not.Null);
            Assert.That(status.MemberScopes.Select(s => s.TreeId), Is.EqualTo(new[] { a, b }));
            Assert.That(read!.Name, Is.EqualTo("nightly"));
            Assert.That(listed.Select(s => s.Name), Is.EqualTo(new[] { "nightly" }));
        });
    }

    [Test]
    public async Task RunBackupSetScheduleNowAsync_captures_a_set_that_restores_through_the_status()
    {
        await _fixture.InitializeAsync();
        var (a, b) = await SeedAsync();
        await _fixture.Control.ScheduleBackupSetAsync(Request("now", a, b));

        var started = await _fixture.Control.RunBackupSetScheduleNowAsync("now");
        BackupSetScheduleStatus? status = null;
        for (var i = 0; i < 300 && status is not { IsRunning: false, RecentRuns.Count: > 0 }; i++)
        {
            await Task.Delay(100);
            status = await _fixture.Control.GetBackupSetScheduleStatusAsync("now");
        }

        Assert.Multiple(() =>
        {
            Assert.That(started, Is.True);
            Assert.That(status!.LastOutcome, Is.EqualTo(BackupScopeRunOutcome.Success), status.LastFailureReason);
            Assert.That(status.RetainedSets.Single().Members.Select(m => m.TreeId), Is.EqualTo(new[] { a, b }));
        });
    }

    [Test]
    public async Task CancelBackupSetScheduleAsync_pauses_and_RemoveBackupSetScheduleAsync_forgets()
    {
        await _fixture.InitializeAsync();
        var (a, b) = await SeedAsync();
        await _fixture.Control.ScheduleBackupSetAsync(Request("pause", a, b));

        var paused = await _fixture.Control.CancelBackupSetScheduleAsync("pause");
        var afterPause = await _fixture.Control.GetBackupSetScheduleStatusAsync("pause");
        var removed = await _fixture.Control.RemoveBackupSetScheduleAsync("pause", deleteRetainedSets: false);
        var afterRemove = await _fixture.Control.GetBackupSetScheduleStatusAsync("pause");
        var missing = await _fixture.Control.CancelBackupSetScheduleAsync("pause");

        Assert.Multiple(() =>
        {
            Assert.That(paused, Is.True);
            Assert.That(afterPause!.IsScheduled, Is.False);
            Assert.That(removed, Is.True);
            Assert.That(afterRemove, Is.Null);
            Assert.That(missing, Is.False);
        });
    }

    [Test]
    public async Task RunBackupSetScheduleNowAsync_throws_for_an_unknown_schedule()
    {
        await _fixture.InitializeAsync();

        Assert.That(
            async () => await _fixture.Control.RunBackupSetScheduleNowAsync("absent"),
            Throws.InstanceOf<KeyNotFoundException>());
    }

    [Test]
    public async Task Every_set_schedule_verb_fails_closed_under_a_denying_gate()
    {
        await _fixture.InitializeAsync();
        var (a, b) = await SeedAsync();
        await _fixture.Control.ScheduleBackupSetAsync(Request("guarded", a, b));
        var denying = _fixture.CreateControlWith(
            new BackupAccessAuthorizer(new DenyingAccessGate("no backup grant"), membership: null));

        Assert.Multiple(() =>
        {
            Assert.That(async () => await denying.ScheduleBackupSetAsync(Request("other", a, b)),
                Throws.InstanceOf<LatticeAuthorizationDeniedException>());
            Assert.That(async () => await denying.GetBackupSetScheduleStatusAsync("guarded"),
                Throws.InstanceOf<LatticeAuthorizationDeniedException>());
            Assert.That(async () => await denying.CancelBackupSetScheduleAsync("guarded"),
                Throws.InstanceOf<LatticeAuthorizationDeniedException>());
            Assert.That(async () => await denying.RemoveBackupSetScheduleAsync("guarded", true),
                Throws.InstanceOf<LatticeAuthorizationDeniedException>());
            Assert.That(async () => await denying.RunBackupSetScheduleNowAsync("guarded"),
                Throws.InstanceOf<LatticeAuthorizationDeniedException>());
            Assert.That(async () => await denying.ScheduleBackupSetAsync(
                    LatticeBackupSetScheduleRequest.ForAllTrees("all", TimeSpan.FromHours(6), includeNewTrees: true, 1, null)),
                Throws.InstanceOf<LatticeAuthorizationDeniedException>());
        });

        var listed = await denying.ListBackupSetSchedulesAsync();
        var stillScheduled = await _fixture.Control.GetBackupSetScheduleStatusAsync("guarded");
        Assert.Multiple(() =>
        {
            Assert.That(listed, Is.Empty, "a schedule the caller may not read is omitted, not a fault");
            Assert.That(stillScheduled!.IsScheduled, Is.True);
        });
    }

    [Test]
    public async Task ScheduleBackupSetAsync_revalidates_a_request_that_skipped_its_constructor()
    {
        // Regression (#6090 review): the all-trees branch forwarded the request as
        // received, so one decoded from the wire (or copied with `with`) could name
        // another tenant's schedule or carry an invalid retention bound.
        await _fixture.InitializeAsync();
        var valid = LatticeBackupSetScheduleRequest.ForAllTrees("all", TimeSpan.FromHours(6), includeNewTrees: false, 1, null);

        Assert.Multiple(() =>
        {
            Assert.That(async () => await _fixture.Control.ScheduleBackupSetAsync(valid with { Name = "t/other/all" }),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await _fixture.Control.ScheduleBackupSetAsync(valid with { RetentionKeepLast = 0 }),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await _fixture.Control.ScheduleBackupSetAsync(valid with { Membership = (BackupSetMembershipMode)42 }),
                Throws.InstanceOf<ArgumentException>());
        });

        var other = await _fixture.GrainFactory
            .GetGrain<ILatticeBackupSetSchedulerGrain>(BackupSetScheduleKey.For("t/other/all"))
            .GetStatusAsync();
        Assert.That(other, Is.Null, "nothing was written under another tenant's schedule key");
    }

    [Test]
    public async Task Replacing_a_schedule_requires_the_grant_over_the_schedule_it_replaces()
    {
        // Regression (#6090 review): re-registering a name authorized only the new
        // request, so a caller with no grant over the existing schedule's trees could
        // replace it and have its next run prune their backups.
        await _fixture.InitializeAsync();
        var (a, b) = await SeedAsync();
        var c = $"set-c-{Guid.NewGuid():N}";
        await _fixture.GrainFactory.GetGrain<ILattice>(c).SetAsync("k", Encoding.UTF8.GetBytes("v"));
        await _fixture.Control.ScheduleBackupSetAsync(Request("takeover", a, b));
        var onlyC = _fixture.CreateControlWith(
            new BackupAccessAuthorizer(new TreeDenyingAccessGate(a, b), membership: null));

        Assert.That(
            async () => await onlyC.ScheduleBackupSetAsync(LatticeBackupSetScheduleRequest.ForTrees(
                "takeover", [BackupScopeSelector.WholeTree(c)], TimeSpan.FromHours(6), 1, null)),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());

        var unchanged = await _fixture.Control.GetBackupSetScheduleStatusAsync("takeover");
        Assert.That(unchanged!.MemberScopes.Select(s => s.TreeId), Is.EqualTo(new[] { a, b }));
    }

    [Test]
    public async Task Acting_on_a_schedule_requires_the_grant_over_every_tree_its_retained_sets_name()
    {
        // Regression (#6090 review): authorization covered only the schedule's
        // current trees, so after it was narrowed a caller with no grant over a tree
        // only its retained sets still named could delete that tree's backups.
        await _fixture.InitializeAsync();
        var (a, b) = await SeedAsync();
        await _fixture.Control.ScheduleBackupSetAsync(Request("narrowed", a, b));
        await _fixture.Control.RunBackupSetScheduleNowAsync("narrowed");
        await WaitForRunAsync("narrowed");
        await _fixture.Control.ScheduleBackupSetAsync(LatticeBackupSetScheduleRequest.ForTrees(
            "narrowed", [BackupScopeSelector.WholeTree(b)], TimeSpan.FromHours(6), 3, null));
        var onlyB = _fixture.CreateControlWith(
            new BackupAccessAuthorizer(new TreeDenyingAccessGate(a), membership: null));

        Assert.Multiple(() =>
        {
            Assert.That(async () => await onlyB.RemoveBackupSetScheduleAsync("narrowed", deleteRetainedSets: true),
                Throws.InstanceOf<LatticeAuthorizationDeniedException>());
            Assert.That(async () => await onlyB.GetBackupSetScheduleStatusAsync("narrowed"),
                Throws.InstanceOf<LatticeAuthorizationDeniedException>());
        });

        var kept = await _fixture.Control.GetBackupSetScheduleStatusAsync("narrowed");
        Assert.That(kept!.RetainedSets.Single().Members.Select(m => m.TreeId), Is.EqualTo(new[] { a, b }));
    }

    private async Task WaitForRunAsync(string name)
    {
        BackupSetScheduleStatus? status = null;
        for (var i = 0; i < 300 && status is not { IsRunning: false, RecentRuns.Count: > 0 }; i++)
        {
            await Task.Delay(100);
            status = await _fixture.Control.GetBackupSetScheduleStatusAsync(name);
        }

        Assert.That(status!.LastOutcome, Is.EqualTo(BackupScopeRunOutcome.Success), status.LastFailureReason);
    }

    private async Task<(string A, string B)> SeedAsync()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var a = $"set-a-{suffix}";
        var b = $"set-b-{suffix}";
        await _fixture.GrainFactory.GetGrain<ILattice>(a).SetAsync("k", Encoding.UTF8.GetBytes("v"));
        await _fixture.GrainFactory.GetGrain<ILattice>(b).SetAsync("k", Encoding.UTF8.GetBytes("v"));
        return (a, b);
    }

    private static LatticeBackupSetScheduleRequest Request(string name, string a, string b) =>
        LatticeBackupSetScheduleRequest.ForTrees(
            name,
            [BackupScopeSelector.WholeTree(a), BackupScopeSelector.WholeTree(b)],
            TimeSpan.FromHours(6),
            retentionKeepLast: 3,
            retentionMaxAge: null);

    private sealed class DenyingAccessGate(string reason) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Deny(reason));
    }

    private sealed class TreeDenyingAccessGate(params string[] deniedTrees) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(deniedTrees.Contains(request.TreeId, StringComparer.Ordinal)
                ? LatticeAccessDecision.Deny($"no grant over '{request.TreeId}'")
                : LatticeAccessDecision.Allow());
    }
}
