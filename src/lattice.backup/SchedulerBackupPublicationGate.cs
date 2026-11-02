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
/// <remarks>
/// <para>
/// A call that times out at the caller may still have completed in the grain - a
/// slow sink write, say. Every call is idempotent, so the gate retries a grain
/// response timeout exactly as it retries a busy call: a publish republishes the
/// same content-addressed id in place, a registration re-registers it, and a
/// delete finds what is left and finishes it. A capture whose publish completed
/// after its caller's first attempt timed out therefore returns that backup.
/// </para>
/// <para>
/// An operator delete runs its short first step - removing the catalog row and
/// reserving the id by a lease of its own - in the grain, deletes the artifacts
/// here, outside any grain call, so a large backup cannot outlast the grain
/// response timeout (issue #5628), renewing that lease throughout and stopping
/// once a renewal is refused (issue #5637), and ends in the grain, which deletes
/// the sink manifest under the id's hold (issue #5631).
/// </para>
/// </remarks>
/// <param name="grainFactory">Reaches the scheduler grains.</param>
/// <param name="options">The backup options.</param>
/// <param name="publisher">Runs the steps of a delete that happen outside the grain.</param>
internal sealed class SchedulerBackupPublicationGate(
    IGrainFactory grainFactory,
    IOptions<LatticeBackupOptions> options,
    BackupPublisher publisher) : IBackupPublicationGate
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
        if (kind == BackupRetireKind.OrphanRow)
        {
            var outcome = await RetryAsync(
                    () => scheduler.RetireAsync(row.Id, kind),
                    o => o == BackupRetireOutcome.Busy,
                    row.Id,
                    options.Value,
                    cancellationToken)
                .ConfigureAwait(false);
            return outcome == BackupRetireOutcome.Removed;
        }

        // One token for the whole delete, so a Begin retried after a timeout whose
        // first attempt completed renews the same lease rather than orphaning it.
        var token = Guid.NewGuid();
        var start = await RetryAsync(
                () => scheduler.BeginDeleteAsync(row.Id, token),
                s => s.Outcome == BackupRetireOutcome.Busy,
                row.Id,
                options.Value,
                cancellationToken)
            .ConfigureAwait(false);
        if (start.Outcome != BackupRetireOutcome.Removed || start.Manifest is null)
        {
            return false;
        }

        try
        {
            await DeleteArtifactsUnderLeaseAsync(scheduler, start.Manifest, token).ConfigureAwait(false);
        }
        catch
        {
            // The sink manifest stays, so a retry finds it and finishes (#5334).
            // Ending the lease early is a courtesy - it runs out by itself - so a
            // failure here must not hide why the delete failed.
            try
            {
                await scheduler.EndDeleteAsync(row.Id, token, finish: false).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // The original exception is rethrown below.
            }

            throw;
        }

        // The sink manifest is deleted in the grain, under the id's hold (#5631).
        var ended = await RetryAsync(
                () => scheduler.EndDeleteAsync(row.Id, token, finish: true),
                o => o == BackupRetireOutcome.Busy,
                row.Id,
                options.Value,
                cancellationToken)
            .ConfigureAwait(false);

        // The end keeps the manifest of a backup a row lists again - a capture
        // republished it, or a rebuild re-listed it, once this delete's lease had
        // run out - and that backup was not deleted (issue #5639). Decided by the
        // end, under the hold: a capture the end's own release lets in lists the
        // id only after this delete has finished, and does not undo it.
        return ended != BackupRetireOutcome.Kept;
    }

    // Deletes the artifacts under the delete's lease (issues #5637 and #5639).
    // Before each artifact delete the lease is renewed synchronously, and the
    // delete stops - keeping the sink manifest, so a retry resumes it - unless
    // the grain confirms it: a refused renewal (the lease ran out, or the
    // scheduler's activation was lost with its leases) and an unreachable grain
    // both stop it, because a catalog rebuild may since have re-listed the
    // backup. A background renewal keeps the lease alive while one artifact
    // delete runs longer than the lease.
    private async Task DeleteArtifactsUnderLeaseAsync(
        ILatticeBackupSchedulerGrain scheduler,
        BackupManifest manifest,
        Guid token)
    {
        var lease = options.Value.DeleteLease;
        var interval = TimeSpan.FromTicks(Math.Max(TimeSpan.TicksPerMillisecond, lease.Ticks / 3));
        using var stop = new CancellationTokenSource();
        var heartbeat = RenewWhileDeletingAsync(scheduler, manifest.Id, token, interval, stop.Token);
        try
        {
            await publisher.DeleteArtifactsAsync(manifest, () => ConfirmLeaseAsync(scheduler, manifest.Id, token))
                .ConfigureAwait(false);
        }
        finally
        {
            await stop.CancelAsync().ConfigureAwait(false);
            await heartbeat.ConfigureAwait(false);
        }
    }

    private static async Task ConfirmLeaseAsync(ILatticeBackupSchedulerGrain scheduler, string backupId, Guid token)
    {
        bool renewed;
        try
        {
            renewed = await scheduler.RenewDeleteAsync(backupId, token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw LostLease(backupId, ex);
        }

        if (!renewed)
        {
            throw LostLease(backupId, null);
        }
    }

    private static TimeoutException LostLease(string backupId, Exception? inner) =>
        new(
            $"The delete of backup '{backupId}' could not confirm its reservation of the id before deleting an "
                + "artifact: its lease ran out, or the scope's scheduler could not be reached. The backup's sink "
                + "manifest is kept; retry the delete.",
            inner);

    // Best effort: keeps the lease alive while a single artifact delete outlasts
    // it. Whether the delete may go on is decided before each artifact, never
    // here.
    private static async Task RenewWhileDeletingAsync(
        ILatticeBackupSchedulerGrain scheduler,
        string backupId,
        Guid token,
        TimeSpan interval,
        CancellationToken stop)
    {
        try
        {
            while (true)
            {
                await Task.Delay(interval, stop).ConfigureAwait(false);
                try
                {
                    await scheduler.RenewDeleteAsync(backupId, token).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    // The next artifact's own renewal decides.
                }
            }
        }
        catch (OperationCanceledException)
        {
            // The artifact deletes are over.
        }
    }

    /// <summary>
    /// Runs <paramref name="call"/> until it returns a result that is not busy,
    /// retrying a busy result and a grain response timeout - both of which leave
    /// an idempotent call safe to repeat - every
    /// <see cref="LatticeBackupOptions.PublishHoldRetryDelay"/>, for up to
    /// <see cref="LatticeBackupOptions.PublishHoldTimeout"/>.
    /// </summary>
    /// <typeparam name="T">The call's result.</typeparam>
    /// <param name="call">The idempotent grain call.</param>
    /// <param name="busy">Whether a result says the id was held and nothing was done.</param>
    /// <param name="backupId">The backup id, for the failure message.</param>
    /// <param name="settings">The backup options.</param>
    /// <param name="cancellationToken">Cancels the pause between retries.</param>
    /// <returns>The first result that is not busy.</returns>
    /// <exception cref="TimeoutException">The call stayed busy, or timing out, past <see cref="LatticeBackupOptions.PublishHoldTimeout"/>.</exception>
    internal static async Task<T> RetryAsync<T>(
        Func<Task<T>> call,
        Func<T, bool> busy,
        string backupId,
        LatticeBackupOptions settings,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow + settings.PublishHoldTimeout;
        while (true)
        {
            TimeoutException? timedOut = null;
            try
            {
                var result = await call().ConfigureAwait(false);
                if (!busy(result))
                {
                    return result;
                }
            }
            catch (TimeoutException ex)
            {
                timedOut = ex;
            }

            if (DateTimeOffset.UtcNow >= deadline)
            {
                throw new TimeoutException(
                    $"Backup '{backupId}' stayed held by a retention pass, a delete or another publish, or its call kept "
                    + $"timing out, for longer than {settings.PublishHoldTimeout}. Retry the operation.",
                    timedOut);
            }

            await Task.Delay(settings.PublishHoldRetryDelay, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<BackupPublishResult> PublishAsync(BackupPublishRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request.Manifest);
        var scheduler = grainFactory.GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(request.Manifest.Scope));
        return await RetryAsync(
                () => scheduler.PublishAsync(request),
                r => r.Outcome == BackupPublishOutcome.Busy,
                request.Manifest.Id,
                options.Value,
                cancellationToken)
            .ConfigureAwait(false);
    }
}
