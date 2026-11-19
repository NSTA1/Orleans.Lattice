using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Retirement of the physical copies shadow-cutover restores leave behind
/// (issue #6103): the revert bookkeeping a commit records, the refusals that keep
/// a revert from reaching a retired copy, and the release, audit and repair verbs.
/// </summary>
internal sealed partial class LatticeBackupRestoreService
{
    /// <summary>
    /// How long a restore or revert waits, in total, for a retirement pass that
    /// holds the tree's alias reservation. A pass holds it only while it marks one
    /// copy's shards deleted.
    /// </summary>
    private static readonly TimeSpan RetirementYieldBudget = TimeSpan.FromSeconds(5);

    private static readonly TimeSpan RetirementYieldDelay = TimeSpan.FromMilliseconds(100);

    /// <summary>The first retirement pass after a restore, revert or release.</summary>
    private static readonly TimeSpan RetirementScheduleDelay = TimeSpan.FromMinutes(1);

    /// <inheritdoc />
    public async Task ReleaseRestoreAsync(
        LatticeRestoreResult restore,
        CancellationToken cancellationToken = default)
    {
        await AuthorizeReleaseAsync(restore, cancellationToken).ConfigureAwait(false);
        await ReleaseAuthorizedAsync(restore, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task ReleaseRestoreSetAsync(
        IReadOnlyList<LatticeRestoreResult> restores,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(restores);
        foreach (var restore in restores)
        {
            await AuthorizeReleaseAsync(restore, cancellationToken).ConfigureAwait(false);
        }

        foreach (var restore in restores)
        {
            await ReleaseAuthorizedAsync(restore, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<LatticeRestoreCopyAudit> AuditRestoreCopiesAsync(
        string treeId,
        CancellationToken cancellationToken = default)
    {
        await AuthorizeCopyAuditAsync(treeId, cancellationToken).ConfigureAwait(false);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await RestoreCopyPlanner.PlanAsync(grainFactory, treeId).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<LatticeRestoreCopyAudit> RepairRestoreCopiesAsync(
        string treeId,
        CancellationToken cancellationToken = default)
    {
        await AuthorizeCopyAuditAsync(treeId, cancellationToken).ConfigureAwait(false);
        LatticeRestoreCopyAudit audit;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            audit = await RestoreCopyPlanner.PlanAsync(grainFactory, treeId).ConfigureAwait(false);
        }

        if (audit.RetirablePhysicalTreeIds.Count > 0)
        {
            await ScheduleCopyRetirementAsync(treeId, cancellationToken, TimeSpan.Zero).ConfigureAwait(false);
        }

        return audit;
    }

    private async Task AuthorizeCopyAuditAsync(string treeId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        BackupConstants.ThrowIfReservedTree(treeId, nameof(treeId));
        await authorizer.AuthorizeRestoreAsync(BackupScopeSelector.WholeTree(treeId), cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Validates a result handed to a release, authorizes its target, and checks
    /// its caller-supplied physical tree ids against registry provenance, exactly
    /// as a revert does, before anything is written.
    /// </summary>
    private async Task AuthorizeReleaseAsync(LatticeRestoreResult restore, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(restore);
        if (restore.Mode != LatticeRestoreMode.ShadowCutover
            || restore.PreviousPhysicalTreeId is null
            || restore.ShadowPhysicalTreeId is null)
        {
            throw new ArgumentException("Only a shadow-cutover restore result can be released.", nameof(restore));
        }

        await authorizer.AuthorizeRestoreAsync(BackupScopeSelector.WholeTree(restore.TargetTreeId), cancellationToken)
            .ConfigureAwait(false);
        var registry = grainFactory.GetLatticeRegistry();
        await AssertPhysicalBelongsToTargetAsync(
            registry, restore.ShadowPhysicalTreeId, restore.TargetTreeId,
            nameof(restore.ShadowPhysicalTreeId), cancellationToken).ConfigureAwait(false);
        await AssertPhysicalBelongsToTargetAsync(
            registry, restore.PreviousPhysicalTreeId, restore.TargetTreeId,
            nameof(restore.PreviousPhysicalTreeId), cancellationToken).ConfigureAwait(false);
    }

    private async Task ReleaseAuthorizedAsync(LatticeRestoreResult restore, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var reservationId = $"{RestoreCopyRetirement.ReservationPrefix}release:{restore.OperationId}";
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(restore.TargetTreeId);
            await BeginAliasChangeYieldingToRetirementAsync(deletion, reservationId, cancellationToken)
                .ConfigureAwait(false);
            try
            {
                var registry = grainFactory.GetLatticeRegistry();
                if (string.Equals(
                        await registry.ResolveAsync(restore.TargetTreeId).ConfigureAwait(false),
                        restore.ShadowPhysicalTreeId,
                        StringComparison.Ordinal))
                {
                    await RestoreCopyRetirement.FinalizeShadowAsync(
                        registry, restore.TargetTreeId, restore.ShadowPhysicalTreeId!).ConfigureAwait(false);
                }
            }
            finally
            {
                await deletion.EndAliasChangeAsync(reservationId).ConfigureAwait(false);
            }
        }

        logger.LogInformation(
            "Released the shadow-cutover restore of tree {TreeId} onto {ShadowTreeId}; it can no longer be reverted.",
            restore.TargetTreeId, restore.ShadowPhysicalTreeId);
        await ScheduleCopyRetirementAsync(restore.TargetTreeId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Refuses a revert that could only reach a retired copy: the restore is final,
    /// or the copy it returns to has been retired. A revert already applied - the
    /// tree resolves to its previous copy - passes, so a retry stays idempotent.
    /// The caller holds a system-origin scope.
    /// </summary>
    private async Task ThrowIfRevertUnreachableAsync(
        ILatticeRegistry registry, LatticeRestoreResult restore, string current)
    {
        var previous = restore.PreviousPhysicalTreeId!;
        if (string.Equals(current, previous, StringComparison.Ordinal))
        {
            return;
        }

        if (restore.ShadowPhysicalTreeId is { } shadow
            && await registry.GetEntryAsync(shadow).ConfigureAwait(false) is { RestoreFinal: true })
        {
            throw new InvalidOperationException(
                $"Cannot revert the shadow-cutover restore of tree '{restore.TargetTreeId}': the restore is final - "
                + "a later cutover of the tree superseded it, it was released, or its revert window "
                + $"elapsed - so its previous physical copy '{previous}' is retired or being retired.");
        }

        if (await IsPhysicalCopyRetiredAsync(previous).ConfigureAwait(false))
        {
            throw new InvalidOperationException(
                $"Cannot revert the shadow-cutover restore of tree '{restore.TargetTreeId}': its previous physical "
                + $"copy '{previous}' has been retired, so there is nothing left to revert to.");
        }
    }

    private Task<bool> IsPhysicalCopyRetiredAsync(string physicalTreeId) =>
        RestoreCopyPlanner.IsRetiredAsync(grainFactory, physicalTreeId);

    /// <summary>
    /// Takes the tree's alias reservation, waiting a bounded while for a
    /// restore-copy retirement pass that holds it instead of failing the caller.
    /// Any other holder is refused at once, as before.
    /// </summary>
    private static async Task BeginAliasChangeYieldingToRetirementAsync(
        ITreeDeletionGrain deletion, string operationId, CancellationToken cancellationToken)
    {
        var waited = TimeSpan.Zero;
        while (true)
        {
            try
            {
                await deletion.BeginAliasChangeAsync(operationId).ConfigureAwait(false);
                return;
            }
            catch (LatticeAliasOperationInProgressException ex)
                when (ex.HolderOperationId.StartsWith(RestoreCopyRetirement.ReservationPrefix, StringComparison.Ordinal)
                    && !string.Equals(ex.HolderOperationId, operationId, StringComparison.Ordinal)
                    && waited < RetirementYieldBudget)
            {
                await Task.Delay(RetirementYieldDelay, cancellationToken).ConfigureAwait(false);
                waited += RetirementYieldDelay;
            }
        }
    }

    /// <summary>
    /// Brings back a restore shadow a retirement pass retired, before a restore of
    /// the same request builds into it again. A shadow whose purge completed is
    /// already registered afresh by the build, so its stale deletion record is
    /// cleared; one whose purge is still running is refused until it finishes. The
    /// caller holds the tree's alias reservation and a system-origin scope.
    /// </summary>
    private async Task ReviveRetiredShadowAsync(string shadowTreeId)
    {
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(shadowTreeId);
        if (!await deletion.IsPhysicalDeletedAsync().ConfigureAwait(false))
        {
            return;
        }

        if ((await deletion.GetDeletionStatusAsync().ConfigureAwait(false)).PurgeInProgress)
        {
            throw new InvalidOperationException(
                $"Cannot restore into shadow tree '{shadowTreeId}': it was retired after an earlier revert and its "
                + "purge is still running. Retry once the purge completes.");
        }

        await deletion.RecoverAsync().ConfigureAwait(false);
        logger.LogInformation(
            "Recovered retired restore shadow {ShadowTreeId} to restore into it again.", shadowTreeId);
    }

    /// <summary>
    /// Records on the shadow the copy its restore reverts to and when it committed,
    /// and returns that copy - or <see langword="null"/> when there is none. A
    /// commit resumed after its swap keeps what the first attempt recorded. The
    /// caller holds the tree's alias reservation and a system-origin scope.
    /// </summary>
    private static async Task<string?> StampRestoreRevertTargetAsync(
        ILatticeRegistry registry,
        string shadowPhysicalTreeId,
        string replacedPhysicalTreeId,
        string? previousPhysicalTreeId)
    {
        var resumed = string.Equals(replacedPhysicalTreeId, shadowPhysicalTreeId, StringComparison.Ordinal);
        var target = resumed ? previousPhysicalTreeId : replacedPhysicalTreeId;
        if (await registry.GetEntryAsync(shadowPhysicalTreeId).ConfigureAwait(false) is not { } shadow)
        {
            return string.Equals(target, shadowPhysicalTreeId, StringComparison.Ordinal) ? null : target;
        }

        if (resumed)
        {
            return shadow.RestoreRevertPhysicalTreeId
                ?? (string.Equals(target, shadowPhysicalTreeId, StringComparison.Ordinal) ? null : target);
        }

        if (!string.Equals(shadow.RestoreRevertPhysicalTreeId, target, StringComparison.Ordinal)
            || shadow.RestoreFinal
            || shadow.RestoreCommittedAtUtc is null)
        {
            await registry.UpdateAsync(shadowPhysicalTreeId, shadow with
            {
                RestoreRevertPhysicalTreeId = target,
                RestoreCommittedAtUtc = DateTimeOffset.UtcNow,
                RestoreFinal = false,
            }).ConfigureAwait(false);
        }

        return target;
    }

    /// <summary>
    /// Schedules the tree's restore-copy retirement. Best-effort: retirement is
    /// cleanup, so a failure to schedule it is logged and never fails the restore,
    /// revert or release that asked; the estate-wide sweep schedules it again.
    /// </summary>
    private async Task ScheduleCopyRetirementAsync(
        string treeId, CancellationToken cancellationToken, TimeSpan? delay = null)
    {
        try
        {
            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                await grainFactory.GetGrain<IRestoreCopyRetirementGrain>(treeId)
                    .ScheduleAsync(delay ?? RetirementScheduleDelay).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning(
                ex, "Could not schedule the restore-copy retirement of tree {TreeId}; the sweep schedules it later.",
                treeId);
        }
    }
}
