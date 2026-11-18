using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Live progress of a backup-set schedule's run (issue #6067).
/// </summary>
internal sealed partial class BackupSetSchedulerGrain
{
    /// <summary>
    /// Receives the run's own phase reports and, as the ambient
    /// <see cref="ILatticeOperationProgress"/> around the set capture, the
    /// capture's member-phase reports, and publishes each as an immutable
    /// <see cref="BackupSetScheduleRunProgress"/> snapshot. The capture reports
    /// from off the grain's scheduler, so the snapshot is swapped whole and read
    /// without a lock; the tree being worked on is the member at the completed
    /// count, because members are pinned and written in scope order.
    /// </summary>
    internal sealed class RunProgress(DateTimeOffset startedAtUtc, BackupSetScheduleTrigger trigger) : ILatticeOperationProgress
    {
        private IReadOnlyList<BackupScopeSelector> _scopes = Array.Empty<BackupScopeSelector>();
        private int _attempt;
        private BackupSetScheduleRunProgress? _snapshot;

        /// <summary>The latest snapshot, or <see langword="null"/> before the first report.</summary>
        public BackupSetScheduleRunProgress? Snapshot => Volatile.Read(ref _snapshot);

        /// <summary>Starts a capture attempt over <paramref name="scopes"/>.</summary>
        public void BeginAttempt(int attempt, IReadOnlyList<BackupScopeSelector> scopes)
        {
            _attempt = attempt;
            _scopes = scopes;
        }

        /// <inheritdoc />
        public ValueTask ReportAsync(string phase, long completedUnits = 0, long? totalUnits = null, string? unitName = null)
        {
            var scopes = _scopes;
            var isMemberPhase = phase is BackupOperationPhases.CapturingMembers or BackupOperationPhases.WritingMembers;
            var current = isMemberPhase && completedUnits >= 0 && completedUnits < scopes.Count
                ? scopes[(int)completedUnits].TreeId
                : null;
            Volatile.Write(ref _snapshot, new BackupSetScheduleRunProgress(
                startedAtUtc,
                trigger,
                Math.Max(1, _attempt),
                phase,
                completedUnits,
                totalUnits,
                unitName,
                current,
                DateTimeOffset.UtcNow));
            return ValueTask.CompletedTask;
        }
    }
}
