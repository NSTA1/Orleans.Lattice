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
}
