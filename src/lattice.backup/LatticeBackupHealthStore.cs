using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="ILatticeBackupHealthStore"/>. Dogfoods the reserved
/// <c>sys-backup-health</c> <c>ILattice</c> tree: each health report is stored under
/// <c>r\u001f{backupId}</c> and each per-backup configuration under
/// <c>c\u001f{backupId}</c>, so a report is a single point read, a config is a
/// single point read, and listing every report is one bounded prefix scan. Every
/// mutation runs on the system-origin path, because the reserved tree is created
/// lazily by its first write and rejects a non-system self-registration of a
/// <c>sys-</c> tree.
/// </summary>
internal sealed class LatticeBackupHealthStore(IGrainFactory grainFactory) : ILatticeBackupHealthStore
{
    private ILattice Health => grainFactory.GetGrain<ILattice>(BackupConstants.HealthTree);

    /// <inheritdoc />
    public async Task SetReportAsync(BackupHealthReport report, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(report);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Health.SetAsync(ReportKey(report.BackupId), report, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<BackupHealthReport?> GetReportAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await Health.GetAsync<BackupHealthReport>(ReportKey(backupId), cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<BackupHealthReport> ListReportsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var prefix = ReportPrefix();
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await foreach (var entry in Health
                .ScanEntriesAsync<BackupHealthReport>(prefix, BackupConstants.PrefixUpperBound(prefix), cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                if (entry.Value is { } report)
                {
                    yield return report;
                }
            }
        }
    }

    /// <inheritdoc />
    public async Task<bool> RemoveAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var removedReport = await Health.DeleteAsync(ReportKey(backupId), cancellationToken).ConfigureAwait(false);
            var removedConfig = await Health.DeleteAsync(ConfigKey(backupId), cancellationToken).ConfigureAwait(false);
            return removedReport || removedConfig;
        }
    }

    /// <inheritdoc />
    public async Task SetConfigAsync(string backupId, BackupHealthConfig config, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        ArgumentNullException.ThrowIfNull(config);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Health.SetAsync(ConfigKey(backupId), config, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<BackupHealthConfig?> GetConfigAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await Health.GetAsync<BackupHealthConfig>(ConfigKey(backupId), cancellationToken).ConfigureAwait(false);
        }
    }

    private static string ReportKey(string backupId) =>
        string.Concat(BackupConstants.HealthReportKeyPrefix.ToString(), BackupConstants.KeySeparator.ToString(), backupId);

    private static string ConfigKey(string backupId) =>
        string.Concat(BackupConstants.HealthConfigKeyPrefix.ToString(), BackupConstants.KeySeparator.ToString(), backupId);

    private static string ReportPrefix() =>
        string.Concat(BackupConstants.HealthReportKeyPrefix.ToString(), BackupConstants.KeySeparator.ToString());
}
