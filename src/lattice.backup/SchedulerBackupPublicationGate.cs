using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="IBackupPublicationGate"/>: each publish and delete runs
/// on the backup's scope's <see cref="ILatticeBackupSchedulerGrain"/>, under its
/// per-id hold (issue #5418). One call waits at most
/// <see cref="LatticeBackupOptions.PublishHoldWait"/> for a held id, well inside
/// the grain response timeout; when the id is still held - a retention pass may
/// be deleting a large artifact from a slow sink - the call returns busy having
/// written nothing, and this gate retries it across calls, for up to
/// <see cref="LatticeBackupOptions.PublishHoldTimeout"/>.
/// </summary>
/// <param name="grainFactory">Reaches the scheduler grains.</param>
/// <param name="options">The backup options.</param>
internal sealed class SchedulerBackupPublicationGate(
    IGrainFactory grainFactory,
    IOptions<LatticeBackupOptions> options) : IBackupPublicationGate
{
    /// <inheritdoc />
    public async Task<BackupManifest> PublishFullAsync(BackupManifest manifest, bool register, CancellationToken cancellationToken) =>
        (await PublishAsync(new BackupPublishRequest(manifest, BackupPublishMode.Full, register), cancellationToken)
            .ConfigureAwait(false)).Manifest!;

    /// <inheritdoc />
    public async Task<BackupManifest?> PublishIncrementAsync(BackupManifest manifest, CancellationToken cancellationToken) =>
        (await PublishAsync(new BackupPublishRequest(manifest, BackupPublishMode.Increment), cancellationToken)
            .ConfigureAwait(false)).Manifest;

    /// <inheritdoc />
    public async Task<BackupManifest> RegisterAsync(BackupManifest manifest, CancellationToken cancellationToken) =>
        (await PublishAsync(new BackupPublishRequest(manifest, BackupPublishMode.Register), cancellationToken)
            .ConfigureAwait(false)).Manifest!;

    /// <inheritdoc />
    public async Task<bool> RegisterRebuiltAsync(BackupManifest manifest, CancellationToken cancellationToken) =>
        (await PublishAsync(new BackupPublishRequest(manifest, BackupPublishMode.Rebuild), cancellationToken)
            .ConfigureAwait(false)).Outcome == BackupPublishOutcome.Published;

    /// <inheritdoc />
    public async Task<bool> RetireAsync(BackupManifest row, BackupRetireKind kind, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(row);
        var scheduler = grainFactory.GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(row.Scope));
        var outcome = await RetryWhileBusyAsync(
                () => scheduler.RetireAsync(row.Id, kind),
                o => o == BackupRetireOutcome.Busy,
                row.Id,
                cancellationToken)
            .ConfigureAwait(false);
        return outcome == BackupRetireOutcome.Removed;
    }

    private async Task<BackupPublishResult> PublishAsync(BackupPublishRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request.Manifest);
        var scheduler = grainFactory.GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(request.Manifest.Scope));
        return await RetryWhileBusyAsync(
                () => scheduler.PublishAsync(request),
                r => r.Outcome == BackupPublishOutcome.Busy,
                request.Manifest.Id,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task<T> RetryWhileBusyAsync<T>(
        Func<Task<T>> call,
        Func<T, bool> busy,
        string backupId,
        CancellationToken cancellationToken)
    {
        var settings = options.Value;
        var deadline = DateTimeOffset.UtcNow + settings.PublishHoldTimeout;
        while (true)
        {
            var result = await call().ConfigureAwait(false);
            if (!busy(result))
            {
                return result;
            }

            if (DateTimeOffset.UtcNow >= deadline)
            {
                throw new TimeoutException(
                    $"Backup '{backupId}' stayed held by a retention pass, a delete or another publish for longer than "
                    + $"{settings.PublishHoldTimeout}; nothing was written. Retry the operation.");
            }

            await Task.Delay(settings.PublishHoldRetryDelay, cancellationToken).ConfigureAwait(false);
        }
    }
}
