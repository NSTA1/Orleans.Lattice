using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The backup-set schedule verbs of the Shell's backup control adapter (#6067),
/// each one RPC on the backup binding. A cluster whose backup control does not
/// support backup-set schedules answers them with <see cref="NotSupportedException"/>,
/// which the Schedules page reads as "not served" and hides its backup-set section.
/// </summary>
internal sealed partial class ShellBackupControlTransport
{
    /// <inheritdoc />
    public Task<BackupSetScheduleStatus> ScheduleBackupSetAsync(
        LatticeBackupSetScheduleRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(request, static (client, state, ct) => client.ScheduleBackupSetAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<bool> CancelBackupSetScheduleAsync(string name, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        return CallAsync(name, static (client, state, ct) => client.CancelBackupSetScheduleAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<bool> RemoveBackupSetScheduleAsync(
        string name,
        bool deleteRetainedSets,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        return CallAsync(
            (Name: name, DeleteRetainedSets: deleteRetainedSets),
            static (client, state, ct) => client.RemoveBackupSetScheduleAsync(state.Name, state.DeleteRetainedSets, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<BackupSetScheduleStatus?> GetBackupSetScheduleStatusAsync(string name, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        return CallAsync(name, static (client, state, ct) => client.GetBackupSetScheduleStatusAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<BackupSetScheduleStatus>> ListBackupSetSchedulesAsync(CancellationToken cancellationToken = default) =>
        CallAsync((object?)null, static (client, _, ct) => client.ListBackupSetSchedulesAsync(ct), null, cancellationToken);

    /// <inheritdoc />
    public Task<bool> RunBackupSetScheduleNowAsync(string name, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        return CallAsync(name, static (client, state, ct) => client.RunBackupSetScheduleNowAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<BackupSetScheduleRunProgress?> GetBackupSetScheduleProgressAsync(string name, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        return CallAsync(name, static (client, state, ct) => client.GetBackupSetScheduleProgressAsync(state, ct), null, cancellationToken);
    }
}
