using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Drives the durable-memory archive: one restore attempt at startup, a periodic
/// export while the host runs, and one bounded export on graceful shutdown.
/// <para>
/// <b>Why a stop-time export exists.</b> A container stop sends SIGTERM and waits a
/// grace period before killing, so the destructive gesture this feature exists to
/// survive announces itself. Exporting then closes most of the window the periodic
/// cadence leaves open: without it, memory authored since the last interval is lost
/// even though the process was told in advance it was going away.
/// </para>
/// <para>
/// <b>Why it is bounded rather than run to completion.</b> That same grace period is
/// not this service's to spend. The store's own drain is budgeted from it, and an
/// export that overruns is killed mid-write - which is safe for the archive (the
/// write is never in place) but is paid for by the drain it delayed. So the export
/// gets an explicit slice, and an export that does not finish inside it is reported
/// at warning level naming what was not captured, rather than failing silently and
/// leaving an operator believing shutdown was clean.
/// </para>
/// <para>
/// The service is registered only when an archive directory is configured, so a host
/// that does not opt in gains no background work.
/// </para>
/// </summary>
internal sealed class RepoContextMemoryArchiveService(
    IGrainFactory grainFactory,
    Serializer serializer,
    RepoContextMemoryArchiveOptions options,
    RepoContextMemoryArchive archive,
    RepoContextMemoryRestoreReporter restoreReporter,
    IRepoIndexRunAuthority runAuthority,
    TimeProvider timeProvider,
    ILogger<RepoContextMemoryArchiveService> logger) : BackgroundService
{
    /// <summary>
    /// The delay before the first archive operation, so the silo is active before the
    /// first grain call. The restore runs after it, which is also why a restore cannot
    /// race a tool call that arrives earlier: an early write simply means the store is
    /// no longer empty, and <see cref="RepoContextMemoryArchiveRestoreMode.Auto"/>
    /// then correctly declines to restore.
    /// </summary>
    private static readonly TimeSpan InitialDelay = TimeSpan.FromSeconds(5);

    private ILattice MemoryTree => grainFactory.GetGrain<ILattice>(RepoContextTrees.Memory);

    /// <summary>
    /// Stamps the run authority's fixed identity onto an archive turn, so both the
    /// export's range read and the restore's writes carry a subject the access gate
    /// can authorize.
    /// <para>
    /// <b>This is load-bearing, and its absence would be silent.</b> A
    /// <see cref="BackgroundService"/> loop is not a request and carries no ambient
    /// credential. Under a default-deny gate a denied <b>range read</b> does not
    /// throw: it resolves to a reject-all key filter and returns a clean, empty
    /// result. An uncredentialed export would therefore read zero memory records,
    /// conclude the store is empty, and report a healthy export of nothing - the
    /// exact silent-empty shape behind issues #2277, #2406, #2426 and #2480, except
    /// that here it would overwrite the one copy of state that does not rebuild.
    /// </para>
    /// <para>
    /// The empty-over-non-empty refusal in
    /// <see cref="RepoContextMemoryArchive.ExportAsync"/> is a second line of defence
    /// against exactly that outcome and would stop the archive being clobbered - but
    /// it would leave the archive frozen and never updated again, which is a slower
    /// version of the same loss. The credential is what stops it at the source.
    /// </para>
    /// <para>
    /// A host that registers no authority resolves <see langword="null"/> and the
    /// ambient credential is left untouched, so an in-process host with no access
    /// gate is unaffected.
    /// </para>
    /// </summary>
    private IDisposable? BeginCredentialScope()
    {
        try
        {
            var credential = runAuthority.Resolve();
            return credential is null ? null : LatticeCredentialContext.With(credential);
        }
        catch (Exception ex)
        {
            // Reported rather than swallowed: without a credential the turn reads as
            // anonymous, and an anonymous read is the failure this scope exists to
            // prevent. The caller still proceeds, because the refusal guard makes a
            // degraded pass safe, and because a host with no gate resolves null here
            // anyway.
            logger.LogWarning(
                ex,
                "The durable-memory archive could not resolve a run credential and will proceed "
                    + "without one. Under a fail-closed access gate its reads return empty rather "
                    + "than failing, so an export made now may see no memory at all.");
            return null;
        }
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await Task.Delay(InitialDelay, timeProvider, stoppingToken).ConfigureAwait(false);
            await TryRestoreAsync(stoppingToken).ConfigureAwait(false);

            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(options.EffectiveInterval, timeProvider, stoppingToken)
                    .ConfigureAwait(false);
                await TryExportAsync("periodic", stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // The host is stopping. StopAsync takes the final export from here.
        }
    }

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        using var budget = new CancellationTokenSource(options.EffectiveStopTimeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            budget.Token, cancellationToken);

        await TryExportAsync("shutdown", linked.Token).ConfigureAwait(false);
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task TryRestoreAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var credentialScope = BeginCredentialScope();
            var result = await archive.RestoreAsync(MemoryTree, serializer, cancellationToken)
                .ConfigureAwait(false);
            restoreReporter.Record(result.Outcome);

            switch (result.Outcome)
            {
                case RepoContextMemoryRestoreOutcome.Restored:
                    logger.LogWarning(
                        "Durable memory was restored from the archive at {Path}: {Records} record(s) merged "
                            + "back into the {Tree} tree, which now holds {Held}. The store came up without "
                            + "them, which means its volume was replaced or wiped.",
                        result.SourcePath,
                        result.RecordsRead,
                        RepoContextTrees.Memory,
                        result.RecordsInStore);
                    break;

                case RepoContextMemoryRestoreOutcome.Partial:
                    // The state this whole discriminator exists for. A tree left
                    // holding a partial import presents as a populated store, so
                    // nothing else will report it and no later boot will heal it
                    // unless it is told the store is wreckage rather than state.
                    logger.LogError(
                        "Durable memory was only PARTIALLY restored into the {Tree} tree: it now holds "
                            + "{Held} record(s) from an import that did not finish ({Reason}). The tree is "
                            + "short of the archive and will look like a normally populated store to "
                            + "everything except the restore-state marker, which is now set so the next "
                            + "restore heals it instead of declining. Do not export over the archive until "
                            + "this is resolved.",
                        RepoContextTrees.Memory,
                        result.RecordsInStore,
                        result.Reason);
                    break;

                case RepoContextMemoryRestoreOutcome.Failed:
                    logger.LogWarning(
                        "Durable memory could not be restored from the archive and nothing was written: "
                            + "{Reason}. The {Tree} tree is exactly as it was.",
                        result.Reason,
                        RepoContextTrees.Memory);
                    break;

                default:
                    logger.LogInformation(
                        "Durable memory was not restored from the archive: {Reason}.", result.Reason);
                    break;
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "Restoring durable memory from the archive at {Directory} failed. The store is running "
                    + "with whatever memory it already held; the archive was not modified.",
                options.Directory);
        }
    }

    private async Task TryExportAsync(string trigger, CancellationToken cancellationToken)
    {
        try
        {
            using var credentialScope = BeginCredentialScope();
            var result = await archive.ExportAsync(MemoryTree, serializer, cancellationToken)
                .ConfigureAwait(false);

            switch (result.Outcome)
            {
                case RepoContextMemoryArchiveExportOutcome.Written:
                    logger.LogInformation(
                        "Durable memory archived ({Trigger}): {Records} record(s) written to {Path}.",
                        trigger,
                        result.RecordCount,
                        archive.SnapshotPath);
                    break;

                case RepoContextMemoryArchiveExportOutcome.RefusedEmptyOverNonEmpty:
                    logger.LogWarning(
                        "Durable memory export ({Trigger}) was refused: {Reason}. The existing archive at "
                            + "{Path} is unchanged and is still the newest copy of this store's memory.",
                        trigger,
                        result.Reason,
                        archive.SnapshotPath);
                    break;

                default:
                    LogIncompleteExport(trigger, result.Reason);
                    break;
            }
        }
        catch (Exception ex)
        {
            LogIncompleteExport(trigger, ex.Message);
        }
    }

    /// <summary>
    /// Reports an export that did not land. Deliberately loud, and deliberately
    /// explicit about the consequence: the archive is intact but is now older than the
    /// store, so memory authored since the last successful export exists only in the
    /// volume. That is precisely the state in which the next wipe loses data, and it
    /// must not be inferable only from the absence of a success line.
    /// </summary>
    private void LogIncompleteExport(string trigger, string? reason)
    {
        logger.LogWarning(
            "Durable memory was NOT archived ({Trigger}): {Reason}. The previous archive at {Path} is "
                + "intact and importable, but any memory authored since it was written now exists only "
                + "in this container's data volume and would be lost by a volume wipe.",
            trigger,
            reason ?? "the export did not complete",
            archive.SnapshotPath);
    }
}
