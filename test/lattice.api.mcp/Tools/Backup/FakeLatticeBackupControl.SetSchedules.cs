using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// The backup-set schedule half of the fake (#6067): an in-memory registry keyed by
/// schedule name. Setting a schedule records the request and answers a status built
/// from it; cancel pauses, remove forgets, and run-now answers
/// <see cref="RunNowResult"/> or throws <see cref="KeyNotFoundException"/> for an
/// unknown name, as the real facade does. Every verb passes the fail-closed gate.
/// </summary>
internal sealed partial class FakeLatticeBackupControl
{
    private readonly SortedDictionary<string, BackupSetScheduleStatus> _setSchedules = new(StringComparer.Ordinal);

    /// <summary>The most recent request passed to <see cref="ScheduleBackupSetAsync"/>.</summary>
    public LatticeBackupSetScheduleRequest? LastSetScheduleRequest { get; private set; }

    /// <summary>The <c>deleteRetainedSets</c> flag of the most recent remove.</summary>
    public bool? LastRemoveDeleteRetainedSets { get; private set; }

    /// <summary>What <see cref="RunBackupSetScheduleNowAsync"/> answers for a known schedule.</summary>
    public bool RunNowResult { get; set; } = true;

    /// <summary>Seeds a schedule status the fake reports as-is.</summary>
    public void SeedSetSchedule(BackupSetScheduleStatus status) => _setSchedules[status.Name] = status;

    public Task<BackupSetScheduleStatus> ScheduleBackupSetAsync(
        LatticeBackupSetScheduleRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        Gate();
        LastSetScheduleRequest = request;
        var status = new BackupSetScheduleStatus(
            request.Name,
            request,
            request.Scopes,
            isScheduled: true,
            nextRunUtc: DateTimeOffset.UnixEpoch + request.Interval,
            isRunning: false,
            registeredAtUtc: DateTimeOffset.UnixEpoch,
            lastRunUtc: null,
            lastSuccessUtc: null,
            BackupScopeRunOutcome.None,
            lastFailureReason: null,
            consecutiveFailures: 0,
            Array.Empty<BackupSetScheduleRun>(),
            Array.Empty<ScheduledBackupSet>());
        _setSchedules[request.Name] = status;
        return Task.FromResult(status);
    }

    public Task<bool> CancelBackupSetScheduleAsync(string name, CancellationToken cancellationToken = default)
    {
        Gate();
        if (!_setSchedules.TryGetValue(name, out var status))
        {
            return Task.FromResult(false);
        }

        _setSchedules[name] = status with { IsScheduled = false, NextRunUtc = null };
        return Task.FromResult(true);
    }

    public Task<bool> RemoveBackupSetScheduleAsync(
        string name,
        bool deleteRetainedSets,
        CancellationToken cancellationToken = default)
    {
        Gate();
        LastRemoveDeleteRetainedSets = deleteRetainedSets;
        return Task.FromResult(_setSchedules.Remove(name));
    }

    public Task<BackupSetScheduleStatus?> GetBackupSetScheduleStatusAsync(
        string name,
        CancellationToken cancellationToken = default)
    {
        Gate();
        return Task.FromResult(_setSchedules.TryGetValue(name, out var status) ? status : null);
    }

    public Task<IReadOnlyList<BackupSetScheduleStatus>> ListBackupSetSchedulesAsync(
        CancellationToken cancellationToken = default)
    {
        Gate();
        return Task.FromResult<IReadOnlyList<BackupSetScheduleStatus>>(_setSchedules.Values.ToArray());
    }

    public Task<bool> RunBackupSetScheduleNowAsync(string name, CancellationToken cancellationToken = default)
    {
        Gate();
        if (!_setSchedules.ContainsKey(name))
        {
            throw new KeyNotFoundException($"No backup-set schedule is named '{name}'.");
        }

        return Task.FromResult(RunNowResult);
    }
}
