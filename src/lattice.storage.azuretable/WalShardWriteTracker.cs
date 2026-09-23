namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// Counts the WAL writes a provider instance has in motion against one
/// shard and signals when that count drains to zero.
/// <para>
/// <see cref="AzureTableWalStorageProvider.ReconcileAsync"/> waits on
/// this before it scans, because its orphan scan cannot tell an
/// abandoned batch from one whose phase 1 or phase 2 is merely still
/// running: rolling the latter back deletes rows a phase-2 commit is
/// about to reference, and rolling it forward races that commit for the
/// same manifest row (#3348). The tracker only covers writes issued
/// through this provider instance; a commit issued by another process
/// is covered by reconciliation's conditional commit instead.
/// </para>
/// </summary>
internal sealed class WalShardWriteTracker
{
    private readonly object _gate = new();
    private int _active;
    private TaskCompletionSource? _idle;

    /// <summary>Number of writes currently in motion.</summary>
    internal int Active
    {
        get
        {
            lock (_gate)
            {
                return _active;
            }
        }
    }

    /// <summary>Records the start of one write.</summary>
    internal void Enter()
    {
        lock (_gate)
        {
            _active++;
        }
    }

    /// <summary>
    /// Records the end of one write, releasing any
    /// <see cref="WhenIdleAsync"/> waiter once nothing remains in motion.
    /// </summary>
    internal void Exit()
    {
        TaskCompletionSource? release = null;
        lock (_gate)
        {
            // Clamped so an unmatched Exit can never park the count below
            // zero, where no later Exit could release a waiter.
            if (_active > 0 && --_active == 0)
            {
                release = _idle;
                _idle = null;
            }
        }

        release?.TrySetResult();
    }

    /// <summary>
    /// Completes when no write is in motion. Returns a completed task when
    /// the tracker is already idle.
    /// </summary>
    internal Task WhenIdleAsync()
    {
        lock (_gate)
        {
            if (_active == 0)
            {
                return Task.CompletedTask;
            }

            _idle ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            return _idle.Task;
        }
    }
}
