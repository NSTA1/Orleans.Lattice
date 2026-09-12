using System.Globalization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Owns the backup cadence for the durable agent-memory tree: one full capture at
/// startup, then a recurring incremental capture, with retention pruned after each
/// cycle. Wired by issue #2602 after a routine <c>docker compose down -v</c>
/// permanently destroyed several hundred agent-memory entries that had no copy
/// anywhere.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why the host owns the cadence instead of the reminder-driven schedule.</b>
/// <c>lattice.backup</c> ships a per-scope scheduler grain driven by Orleans
/// reminders, and it is the natural mechanism. It cannot be used here, for the
/// reason recorded in issue #2608. A reminder tick carries no ambient credential,
/// so the fail-closed capture authorizer resolves the caller as the anonymous
/// subject; this host registers a real access gate whose default effect is Deny,
/// so every reminder-driven capture would be refused. The failure is quiet in
/// exactly the way this issue exists to prevent: the schedule registers, reports
/// itself registered, and captures nothing - registration and capture being
/// different facts, of which only the first is observable there. Driving the
/// cadence from a hosted service lets every capture run inside an explicit
/// credential scope, which Orleans propagates across the grain call to the capture
/// path. <c>RepoContextBackupScheduleGuardTests</c> holds that arrangement in
/// place; do not replace this service with the two schedule flags until #2608 is
/// fixed.
/// </para>
/// <para>
/// <b>Why the bootstrap administrator.</b> Capture and restore are gated by the
/// dedicated <c>LatticeOperation.Backup</c> / <c>LatticeOperation.Restore</c>
/// capabilities, which the local agent's data-plane grant deliberately does not
/// include. Rather than widen a data-plane grant to cover an infrastructure
/// operation, the cadence runs under the same fixed bootstrap-administrator
/// identity the startup service already uses to seed policy, whose break-glass the
/// gate honours for every operation. This is a fixed container identity, not a
/// per-request credential, so scoping it here re-globalises no caller state.
/// </para>
/// <para>
/// <b>Why a full capture at startup, unconditionally.</b> Manifest validation
/// rejects an incremental whose base backup id is empty, so a full capture must
/// exist before any incremental can be taken. Capturing one on every start makes
/// that ordering hold without having to reason about what a previous container
/// left behind, and retention bounds the cost.
/// </para>
/// <para>
/// <b>This service never restores.</b> Restore is operator-driven and documented
/// separately. An automatic restore-on-empty behaviour is a different concern with
/// a different failure mode, owned by issue #2601.
/// </para>
/// </remarks>
public sealed class RepoContextBackupService : IHostedService
{
    /// <summary>The delay before the first capture attempt is retried after a failure.</summary>
    internal static readonly TimeSpan MinRetryDelay = TimeSpan.FromSeconds(5);

    /// <summary>The ceiling on the retry backoff between failed capture attempts.</summary>
    internal static readonly TimeSpan MaxRetryDelay = TimeSpan.FromMinutes(5);

    private readonly ILatticeBackupScheduler _scheduler;
    private readonly ILatticeBackupCatalogStore _catalog;
    private readonly ILatticeBackupColdRestoreService _restore;
    private readonly ILatticeBackupSink _sink;
    private readonly RepoContextBackupSettings _settings;
    private readonly RepoContextBackupStatus _status;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly ILogger<RepoContextBackupService> _logger;
    private readonly CancellationTokenSource _stopping = new();
    private Task? _loop;

    /// <summary>Initializes the backup cadence service.</summary>
    /// <param name="scheduler">The per-scope backup scheduler used to trigger captures and prune retention.</param>
    /// <param name="catalog">The backup catalog the captured manifest is read back from.</param>
    /// <param name="restore">The catalog-free cold restore service, used only when an operator names a backup id.</param>
    /// <param name="sink">The external sink, enumerated at startup to report which backups are restorable.</param>
    /// <param name="settings">The resolved backup settings.</param>
    /// <param name="status">The shared positive-statement status this service updates.</param>
    /// <param name="lifetime">The host application lifetime the cadence is anchored to.</param>
    /// <param name="logger">The logger.</param>
    /// <exception cref="ArgumentNullException">Any argument is null.</exception>
    public RepoContextBackupService(
        ILatticeBackupScheduler scheduler,
        ILatticeBackupCatalogStore catalog,
        ILatticeBackupColdRestoreService restore,
        ILatticeBackupSink sink,
        RepoContextBackupSettings settings,
        RepoContextBackupStatus status,
        IHostApplicationLifetime lifetime,
        ILogger<RepoContextBackupService> logger)
    {
        _scheduler = scheduler ?? throw new ArgumentNullException(nameof(scheduler));
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _restore = restore ?? throw new ArgumentNullException(nameof(restore));
        _sink = sink ?? throw new ArgumentNullException(nameof(sink));
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _status = status ?? throw new ArgumentNullException(nameof(status));
        _lifetime = lifetime ?? throw new ArgumentNullException(nameof(lifetime));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_settings.Enabled)
        {
            // Say so once, loudly, at the level an operator reads. A container with
            // no backup must not be silently indistinguishable from one with backup.
            _logger.LogWarning("{Status}", _status.Describe());
            return Task.CompletedTask;
        }

        _lifetime.ApplicationStarted.Register(() => _loop = RunAsync(_stopping.Token));
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stopping.CancelAsync().ConfigureAwait(false);

        if (_loop is not null)
        {
            try
            {
                await _loop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected when shutdown interrupts a capture or an interval wait.
            }
        }
    }

    /// <summary>
    /// Captures a full baseline, then captures an incremental on the configured
    /// interval until shutdown. Never throws: a failed cycle is recorded on the
    /// status, logged, and retried with backoff, because a backup service that
    /// takes the host down is worse than one that reports itself broken.
    /// </summary>
    /// <param name="cancellationToken">Cancelled when the host begins to stop.</param>
    internal async Task RunAsync(CancellationToken cancellationToken)
    {
        // Assert the property the whole design rests on against the sink that was
        // actually resolved, rather than trusting that configuring a connection
        // string produced an external sink. ILatticeBackupSink.IsDurable is the
        // sink's own statement about whether it stores payload outside the cluster
        // that captured it; the in-cluster default answers false precisely because
        // it dies with the store it protects.
        if (!_sink.IsDurable)
        {
            _status.RecordNonDurableSink(_sink.GetType().Name);
            _logger.LogError(
                "RepoContext memory backup resolved a NON-DURABLE sink ({Sink}), which stores backups inside "
                + "the same cluster they protect. Captures will run and will be destroyed by the same gesture "
                + "that destroys the '{Tree}' tree, so this container is NOT protected. Check {Key}.",
                _sink.GetType().Name,
                RepoContextHostTrees.Memory,
                RepoContextBackup.BlobConnectionStringKey);
        }

        await TryReportSinkInventoryAsync(cancellationToken).ConfigureAwait(false);
        await TryRestoreAsync(cancellationToken).ConfigureAwait(false);

        var delay = MinRetryDelay;
        while (!cancellationToken.IsCancellationRequested && _status.CaptureCount == 0)
        {
            if (await TryCaptureAsync(incremental: false, cancellationToken).ConfigureAwait(false))
            {
                _logger.LogInformation("{Status}", _status.Describe());
                break;
            }

            _logger.LogWarning(
                "RepoContext memory backup could not capture its initial full baseline; retrying in {Delay}. "
                + "Until it succeeds the '{Tree}' tree has NO backup from this container.",
                delay,
                RepoContextHostTrees.Memory);

            if (!await DelayAsync(delay, cancellationToken).ConfigureAwait(false))
            {
                return;
            }

            delay = delay >= MaxRetryDelay ? MaxRetryDelay : delay + delay;
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            if (!await DelayAsync(_settings.IncrementalInterval, cancellationToken).ConfigureAwait(false))
            {
                return;
            }

            var captured = await TryCaptureAsync(incremental: true, cancellationToken).ConfigureAwait(false);
            if (captured)
            {
                _logger.LogInformation("{Status}", _status.Describe());
            }

            await TryPruneAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Restores the backup an operator explicitly named, if any, before the
    /// capture cadence starts. A no-op when no id was supplied, which is the
    /// ordinary case: nothing here ever restores on its own initiative.
    /// </summary>
    /// <remarks>
    /// Uses the <b>cold</b> restore path deliberately.
    /// <c>ILatticeBackupRestoreService</c> resolves the target manifest from the
    /// <c>sys-backup-catalog</c> tree, which lives inside the Lattice store being
    /// protected - so the gesture that destroys the memory tree destroys the
    /// catalog too, and an ordinary restore cannot find the backup in precisely
    /// the disaster this wiring exists for. The cold path resolves the manifest
    /// and walks its base chain from the sink alone, bootstraps the reserved
    /// <c>sys-</c> trees when they are absent, and re-projects the catalog
    /// afterwards. It is also correct when the catalog did survive, because it
    /// never consults it, so there is no case where the ordinary path is the
    /// better choice here.
    /// </remarks>
    /// <param name="cancellationToken">Cancelled when the host begins to stop.</param>
    /// <returns><see langword="true"/> when a restore was requested and completed.</returns>
    internal async Task<bool> TryRestoreAsync(CancellationToken cancellationToken)
    {
        var backupId = _settings.RestoreBackupId;
        if (backupId is null)
        {
            return false;
        }

        _logger.LogWarning(
            "RepoContext memory backup is performing an OPERATOR-REQUESTED restore of backup '{BackupId}' "
            + "into tree '{Tree}' before starting the capture cadence.",
            backupId,
            RepoContextHostTrees.Memory);

        try
        {
            using (LatticeCredentialContext.Use(
                LocalTrustedAgent.BootstrapAdministrator,
                scheme: LocalTrustedAgent.Scheme))
            {
                var result = await _restore.ColdRestoreAsync(
                    new LatticeRestoreRequest(backupId, RepoContextHostTrees.Memory),
                    cancellationToken).ConfigureAwait(false);

                _logger.LogWarning(
                    "RepoContext memory restore of backup '{BackupId}' applied {Entries} entries to tree "
                    + "'{Tree}' over a chain of {ChainLength} manifest(s). UNSET {Key} now: leaving it set "
                    + "re-applies this restore on every restart, which resurrects entries deleted since it "
                    + "was captured. Entries written since the capture are NOT lost either way - the restore "
                    + "merges by hybrid-logical-clock order, so newer writes win.",
                    result.BackupId,
                    result.EntriesApplied,
                    result.TargetTreeId,
                    result.ManifestChain.Count,
                    RepoContextBackup.RestoreBackupIdKey);

                return true;
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Deliberately does not take the host down. An operator restoring
            // during an incident needs the container up to inspect the catalog and
            // try a different backup id, not a crash loop.
            _status.RecordFailure($"operator-requested restore of '{backupId}' failed: {ex.Message}");
            _logger.LogError(
                ex,
                "RepoContext memory restore of backup '{BackupId}' FAILED. The tree is unchanged by the "
                + "failed restore and the capture cadence will still start. The backup ids this sink holds "
                + "are listed above; set {Key} to one of them and restart.",
                backupId,
                RepoContextBackup.RestoreBackupIdKey);
            return false;
        }
    }

    /// <summary>
    /// Enumerates the external sink and records what it already holds for the
    /// memory tree, before this container has captured anything of its own.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the answer to the question an operator actually has in front of a
    /// freshly restarted, empty container: <i>what can I restore?</i> The catalog
    /// cannot answer it, because the catalog is stored in the destroyed tree; the
    /// sink can, because surviving is its entire job. Reporting it at startup also
    /// makes the health signal a positive statement from the first second of the
    /// process ("the sink holds 37 backups, newest ...") rather than only after
    /// the first capture completes.
    /// </para>
    /// <para>
    /// Failure here is logged and swallowed. Not being able to <i>list</i> the
    /// sink is a reporting problem, and must not stop the container capturing into
    /// it.
    /// </para>
    /// </remarks>
    /// <param name="cancellationToken">Cancelled when the host begins to stop.</param>
    internal async Task TryReportSinkInventoryAsync(CancellationToken cancellationToken)
    {
        try
        {
            using (LatticeCredentialContext.Use(
                LocalTrustedAgent.BootstrapAdministrator,
                scheme: LocalTrustedAgent.Scheme))
            {
                var count = 0;
                string? newestId = null;
                DateTimeOffset? newestAt = null;

                await foreach (var manifest in _sink.ListManifestsAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (!string.Equals(manifest.Scope.TreeId, RepoContextHostTrees.Memory, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    count++;
                    if (newestAt is null || manifest.CreatedAtUtc > newestAt)
                    {
                        newestAt = manifest.CreatedAtUtc;
                        newestId = manifest.Id;
                    }
                }

                _status.RecordSinkInventory(count, newestId, newestAt);
                _logger.LogInformation(
                    "RepoContext memory backup sink holds {Count} backup(s) of tree '{Tree}'; newest is "
                    + "'{NewestId}' from {NewestAt}. Set {Key} to one of these ids and restart to restore it.",
                    count,
                    RepoContextHostTrees.Memory,
                    newestId ?? "(none)",
                    newestAt?.ToString("O", CultureInfo.InvariantCulture) ?? "(never)",
                    RepoContextBackup.RestoreBackupIdKey);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "RepoContext memory backup could not enumerate the sink to report which backups exist. "
                + "Capture is unaffected.");
        }
    }

    /// <summary>
    /// Runs one capture under the bootstrap-administrator credential and records
    /// the manifest it produced on the shared status.
    /// </summary>
    /// <param name="incremental">Whether to request an incremental rather than a full capture.</param>
    /// <param name="cancellationToken">Cancelled when the host begins to stop.</param>
    /// <returns><see langword="true"/> when a manifest was captured and recorded.</returns>
    internal async Task<bool> TryCaptureAsync(bool incremental, CancellationToken cancellationToken)
    {
        try
        {
            using (LatticeCredentialContext.Use(
                LocalTrustedAgent.BootstrapAdministrator,
                scheme: LocalTrustedAgent.Scheme))
            {
                var scope = RepoContextBackup.MemoryScope;
                var backupId = incremental
                    ? await _scheduler.TriggerIncrementalBackupAsync(scope).ConfigureAwait(false)
                    : await _scheduler.TriggerFullBackupAsync(scope).ConfigureAwait(false);

                if (backupId is null)
                {
                    // The scheduler's overlap guard skipped this cycle because a
                    // capture for the scope was already running. Not a failure.
                    _logger.LogDebug(
                        "RepoContext memory backup skipped a {Kind} capture: one is already in flight.",
                        incremental ? "incremental" : "full");
                    return false;
                }

                var manifest = await _catalog.GetAsync(backupId, cancellationToken).ConfigureAwait(false);
                if (manifest is null)
                {
                    _status.RecordFailure(
                        $"capture '{backupId}' completed but no manifest is registered in the catalog");
                    return false;
                }

                _status.RecordCapture(
                    backupId,
                    manifest.Scope.TreeId,
                    manifest.KeyDescriptors.Count,
                    isFull: manifest.Kind == BackupKind.Full,
                    requestedIncremental: incremental,
                    manifest.CreatedAtUtc);

                return true;
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _status.RecordFailure(ex.Message);
            _logger.LogError(
                ex,
                "RepoContext memory backup {Kind} capture failed for tree '{Tree}'.",
                incremental ? "incremental" : "full",
                RepoContextHostTrees.Memory);
            return false;
        }
    }

    /// <summary>
    /// Applies the configured retention policy. Failures are logged and swallowed:
    /// an unpruned sink is a cost problem, never a data-loss one, so it must not
    /// interrupt the capture cadence.
    /// </summary>
    /// <param name="cancellationToken">Cancelled when the host begins to stop.</param>
    internal async Task TryPruneAsync(CancellationToken cancellationToken)
    {
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            using (LatticeCredentialContext.Use(
                LocalTrustedAgent.BootstrapAdministrator,
                scheme: LocalTrustedAgent.Scheme))
            {
                var report = await _scheduler.PruneAsync(RepoContextBackup.MemoryScope).ConfigureAwait(false);
                if (report.PrunedCount > 0)
                {
                    _logger.LogInformation(
                        "RepoContext memory backup retention removed {Removed} backup(s), retaining {Retained}.",
                        report.PrunedCount,
                        report.RetainedCount);
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RepoContext memory backup retention pass failed; captures are unaffected.");
        }
    }

    private static async Task<bool> DelayAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }
}
