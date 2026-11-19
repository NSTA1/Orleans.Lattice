using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Retirement of the physical copies shadow-cutover restores leave behind
/// (issue #6103): a copy no revert can reach is retired one pass at a time,
/// a copy a still-valid revert needs is kept, and a revert that could only reach
/// a retired copy is refused.
/// </summary>
public sealed partial class LatticeBackupRestoreIntegrationTests
{
    [Test]
    public async Task Restoring_again_retires_the_first_restores_previous_copy_and_only_the_second_reverts()
    {
        await _fixture.InitializeAsync();
        const string target = "retire-twice";
        var first = await CaptureAsync("retire-twice-a", "first");
        var second = await CaptureAsync("retire-twice-b", "second");
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(target);
        await tree.SetAsync("key", Bytes("original"));

        var restoreA = await RestoreShadowAsync(first, target);
        var restoreB = await RestoreShadowAsync(second, target);

        var audit = await _fixture.Restore.AuditRestoreCopiesAsync(target);
        Assert.Multiple(() =>
        {
            Assert.That(audit.Hold, Is.EqualTo(LatticeRestoreCopyHold.None));
            Assert.That(audit.AliasTargetPhysicalTreeId, Is.EqualTo(restoreB.ShadowPhysicalTreeId));
            Assert.That(audit.RevertTargetPhysicalTreeId, Is.EqualTo(restoreA.ShadowPhysicalTreeId));
            Assert.That(audit.RevertCommittedAtUtc, Is.Not.Null);
            Assert.That(audit.RetirablePhysicalTreeIds, Is.EqualTo(new[] { target }), "only the original copy is unreachable");
        });

        Assert.That(await RunRetirementAsync(target), Is.EqualTo(RestoreCopyPassOutcome.Retired));
        Assert.That(await RunRetirementAsync(target), Is.EqualTo(RestoreCopyPassOutcome.Completed));
        Assert.That(await IsRetiredAsync(target), Is.True, "the first restore's previous copy is retired");
        Assert.That(await IsRetiredAsync(restoreA.ShadowPhysicalTreeId!), Is.False, "the second restore still needs it");

        await _fixture.Restore.RevertRestoreAsync(restoreB);
        Assert.That(Str((await tree.GetAsync("key"))!), Is.EqualTo("first"));

        var refusal = Assert.ThrowsAsync<InvalidOperationException>(() => _fixture.Restore.RevertRestoreAsync(restoreA));
        Assert.That(refusal!.Message, Does.Contain("final"));
        Assert.That(Str((await tree.GetAsync("key"))!), Is.EqualTo("first"), "the refused revert changed nothing");
    }

    [Test]
    public async Task Reverting_retires_the_shadow_with_its_loops_and_the_tree_keeps_working()
    {
        await _fixture.InitializeAsync();
        const string target = "retire-revert";
        var backup = await CaptureAsync("retire-revert", "restored");
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(target);
        await tree.SetAsync("key", Bytes("original"));
        var restore = await RestoreShadowAsync(backup, target);
        var shadow = restore.ShadowPhysicalTreeId!;
        await ArmLoopsAsync(shadow);
        Assert.That(await HasLoopRemindersAsync(shadow), Is.True, "precondition: the shadow's loops are armed");

        await _fixture.Restore.RevertRestoreAsync(restore);
        Assert.That(await RunRetirementAsync(target), Is.EqualTo(RestoreCopyPassOutcome.Retired));

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await IsRetiredAsync(shadow), Is.True);
            Assert.That(await HasLoopRemindersAsync(shadow), Is.False, "no reminder outlives the retired copy");
            Assert.That(await IsRetiredAsync(target), Is.False);
        });
        Assert.That(Str((await tree.GetAsync("key"))!), Is.EqualTo("original"));
        await tree.SetAsync("after", Bytes("written"));
        Assert.That(Str((await tree.GetAsync("after"))!), Is.EqualTo("written"));

        // A retried revert of the already-reverted restore stays idempotent.
        await _fixture.Restore.RevertRestoreAsync(restore);
        Assert.That(Str((await tree.GetAsync("key"))!), Is.EqualTo("original"));
    }

    [Test]
    public async Task Restoring_the_same_backup_after_its_shadow_was_retired_revives_the_shadow()
    {
        await _fixture.InitializeAsync();
        const string target = "retire-revive";
        var backup = await CaptureAsync("retire-revive", "restored");
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(target);
        await tree.SetAsync("key", Bytes("original"));
        var restore = await RestoreShadowAsync(backup, target);
        await _fixture.Restore.RevertRestoreAsync(restore);
        await RunRetirementAsync(target);
        Assert.That(await IsRetiredAsync(restore.ShadowPhysicalTreeId!), Is.True, "precondition: the shadow is retired");

        var again = await RestoreShadowAsync(backup, target);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(again.ShadowPhysicalTreeId, Is.EqualTo(restore.ShadowPhysicalTreeId));
            Assert.That(await IsRetiredAsync(again.ShadowPhysicalTreeId!), Is.False);
            Assert.That(Str((await tree.GetAsync("key"))!), Is.EqualTo("restored"));
        });
    }

    [Test]
    public async Task A_released_restore_is_refused_a_revert_and_its_previous_copy_is_retired()
    {
        await _fixture.InitializeAsync();
        const string target = "retire-release";
        var backup = await CaptureAsync("retire-release", "restored");
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(target);
        await tree.SetAsync("key", Bytes("original"));
        var restore = await RestoreShadowAsync(backup, target);
        Assert.That(
            (await _fixture.Restore.AuditRestoreCopiesAsync(target)).RetirablePhysicalTreeIds,
            Is.Empty,
            "precondition: the open revert keeps its previous copy");

        await _fixture.Restore.ReleaseRestoreSetAsync([restore]);

        Assert.ThrowsAsync<InvalidOperationException>(() => _fixture.Restore.RevertRestoreAsync(restore));
        Assert.That(await RunRetirementAsync(target), Is.EqualTo(RestoreCopyPassOutcome.Retired));
        Assert.That(await IsRetiredAsync(target), Is.True);
        Assert.That(Str((await tree.GetAsync("key"))!), Is.EqualTo("restored"));

        // Idempotent: releasing again changes nothing.
        await _fixture.Restore.ReleaseRestoreAsync(restore);
    }

    [Test]
    public async Task Releasing_a_result_naming_a_foreign_physical_tree_is_refused()
    {
        await _fixture.InitializeAsync();
        const string target = "retire-forged";
        var backup = await CaptureAsync("retire-forged", "restored");
        await _fixture.GrainFactory.GetGrain<ILattice>(target).SetAsync("key", Bytes("original"));
        await _fixture.GrainFactory.GetGrain<ILattice>("retire-forged-other").SetAsync("key", Bytes("other"));
        var restore = await RestoreShadowAsync(backup, target);

        Assert.ThrowsAsync<LatticeRestoreValidationException>(() =>
            _fixture.Restore.ReleaseRestoreAsync(restore with { ShadowPhysicalTreeId = "retire-forged-other" }));
        Assert.That((await _fixture.Restore.AuditRestoreCopiesAsync(target)).RevertTargetPhysicalTreeId, Is.EqualTo(target));
    }

    [Test]
    public async Task An_elapsed_revert_window_makes_the_restore_final_and_retires_its_previous_copy()
    {
        await _fixture.InitializeAsync();
        const string target = "retire-window";
        var backup = await CaptureAsync("retire-window", "restored");
        await _fixture.GrainFactory.GetGrain<ILattice>(target).SetAsync("key", Bytes("original"));
        var restore = await RestoreShadowAsync(backup, target);
        var options = _fixture.SiloServices.GetRequiredService<IOptionsMonitor<LatticeBackupOptions>>().CurrentValue;
        options.RestoreRevertRetention = TimeSpan.FromHours(1);
        Assert.That(await RunRetirementAsync(target), Is.EqualTo(RestoreCopyPassOutcome.Waiting), "inside the window");

        options.RestoreRevertRetention = TimeSpan.FromTicks(1);
        Assert.That(await RunRetirementAsync(target), Is.EqualTo(RestoreCopyPassOutcome.Retired));

        Assert.That(await IsRetiredAsync(target), Is.True);
        Assert.ThrowsAsync<InvalidOperationException>(() => _fixture.Restore.RevertRestoreAsync(restore));
    }

    [Test]
    public async Task A_revertible_restore_with_no_recorded_revert_target_holds_every_copy()
    {
        await _fixture.InitializeAsync();
        const string target = "retire-legacy";
        var first = await CaptureAsync("retire-legacy-a", "first");
        var second = await CaptureAsync("retire-legacy-b", "second");
        await _fixture.GrainFactory.GetGrain<ILattice>(target).SetAsync("key", Bytes("original"));
        var restoreA = await RestoreShadowAsync(first, target);
        await _fixture.Restore.RevertRestoreAsync(restoreA);
        var restoreB = await RestoreShadowAsync(second, target);

        // A shadow committed by a build that predates the revert-target record.
        var registry = _fixture.GrainFactory.GetLatticeRegistry();
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var entry = (await registry.GetEntryAsync(restoreB.ShadowPhysicalTreeId!))!;
            await registry.UpdateAsync(
                restoreB.ShadowPhysicalTreeId!,
                entry with { RestoreRevertPhysicalTreeId = null, RestoreCommittedAtUtc = null });
        }

        var audit = await _fixture.Restore.RepairRestoreCopiesAsync(target);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(audit.Hold, Is.EqualTo(LatticeRestoreCopyHold.UnknownRevertTarget));
            Assert.That(audit.RetirablePhysicalTreeIds, Is.Empty);
            Assert.That(
                audit.RestoreShadowPhysicalTreeIds,
                Is.EquivalentTo(new[] { restoreA.ShadowPhysicalTreeId, restoreB.ShadowPhysicalTreeId }));
            Assert.That(await RunRetirementAsync(target), Is.EqualTo(RestoreCopyPassOutcome.Completed));
            Assert.That(await IsRetiredAsync(restoreA.ShadowPhysicalTreeId!), Is.False);
        });

        await _fixture.Restore.RevertRestoreAsync(restoreB);
        Assert.That(Str((await _fixture.GrainFactory.GetGrain<ILattice>(target).GetAsync("key"))!), Is.EqualTo("original"));
    }

    [Test]
    public async Task The_sweep_schedules_every_tree_holding_a_restore_shadow()
    {
        await _fixture.InitializeAsync();
        const string target = "retire-sweep";
        var backup = await CaptureAsync("retire-sweep", "restored");
        await _fixture.GrainFactory.GetGrain<ILattice>(target).SetAsync("key", Bytes("original"));
        var restore = await RestoreShadowAsync(backup, target);
        await _fixture.Restore.RevertRestoreAsync(restore);
        Assert.That(await RunRetirementAsync(target), Is.EqualTo(RestoreCopyPassOutcome.Retired));
        Assert.That(await RunRetirementAsync(target), Is.EqualTo(RestoreCopyPassOutcome.Completed));
        Assert.That(await RunRetirementAsync(target), Is.EqualTo(RestoreCopyPassOutcome.Idle), "precondition: nothing scheduled");

        var scheduled = await _fixture.GrainFactory.GetGrain<IRestoreCopySweepGrain>(IRestoreCopySweepGrain.SingletonKey)
            .SweepAsync();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(scheduled, Is.EqualTo(1));
            Assert.That(await RunRetirementAsync(target), Is.EqualTo(RestoreCopyPassOutcome.Completed));
        });
    }

    private async Task<LatticeBackupCaptureResult> CaptureAsync(string name, string value)
    {
        var source = $"{name}-source";
        await _fixture.GrainFactory.GetGrain<ILattice>(source).SetAsync("key", Bytes(value));
        return await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest(name, BackupScopeSelector.WholeTree(source)));
    }

    private Task<LatticeRestoreResult> RestoreShadowAsync(LatticeBackupCaptureResult backup, string target) =>
        _fixture.Restore.RestoreAsync(new LatticeRestoreRequest(backup.BackupId, target, mode: LatticeRestoreMode.ShadowCutover));

    private Task<RestoreCopyPassOutcome> RunRetirementAsync(string target) =>
        _fixture.GrainFactory.GetGrain<IRestoreCopyRetirementGrain>(target).RunPassAsync();

    private Task<bool> IsRetiredAsync(string physical) =>
        _fixture.GrainFactory.GetGrain<ITreeDeletionGrain>(physical).IsPhysicalDeletedAsync();

    private async Task ArmLoopsAsync(string physical)
    {
        await _fixture.GrainFactory.GetGrain<IHotShardMonitorGrain>(physical).EnsureRunningAsync();
        await _fixture.GrainFactory.GetGrain<IShardHealingOrchestratorGrain>(physical).EnsureRunningAsync();
        await _fixture.GrainFactory.GetGrain<ITombstoneCompactionGrain>(physical).EnsureReminderAsync();
    }

    private async Task<bool> HasLoopRemindersAsync(string physical)
    {
        var table = _fixture.SiloServices.GetRequiredService<IReminderTable>();
        return await table.ReadRow(_fixture.GrainFactory.GetGrain<IHotShardMonitorGrain>(physical).GetGrainId(), "hot-shard-monitor") is not null
            || await table.ReadRow(_fixture.GrainFactory.GetGrain<IShardHealingOrchestratorGrain>(physical).GetGrainId(), "shard-healing") is not null
            || await table.ReadRow(_fixture.GrainFactory.GetGrain<ITombstoneCompactionGrain>(physical).GetGrainId(), "tombstone-compaction") is not null;
    }
}
