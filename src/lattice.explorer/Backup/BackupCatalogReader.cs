using Grpc.Core;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// The default <see cref="IBackupCatalogReader"/> over an
/// <see cref="IBackupControlClient"/>. Every action catches a
/// <see cref="LatticeAuthorizationDeniedException"/> (the translated server
/// denial) and a residual <see cref="RpcException"/> and returns a non-success
/// result, so the Backups UI degrades cleanly and never leaks an exception even
/// when the advisory capability map believed an action was allowed.
/// </summary>
public sealed class BackupCatalogReader(IBackupControlClient client) : IBackupCatalogReader
{
    private readonly IBackupControlClient _client = client ?? throw new ArgumentNullException(nameof(client));

    /// <inheritdoc />
    public async Task<BackupListView> LoadPageAsync(int pageSize = 0, string? pageToken = null, BackupCatalogFilter? filter = null, CancellationToken cancellationToken = default)
    {
        filter ??= BackupCatalogFilter.None;
        var request = new BackupCatalogRequest
        {
            PageSize = pageSize,
            PageToken = string.IsNullOrEmpty(pageToken) ? null : pageToken,
            OrderByCreatedDescending = true,
            Kind = filter.Kind,
            TreeId = string.IsNullOrEmpty(filter.Scope) ? null : filter.Scope,
            NamePrefix = string.IsNullOrEmpty(filter.NamePrefix) ? null : filter.NamePrefix,
            CreatedPrefix = string.IsNullOrEmpty(filter.CreatedPrefix) ? null : filter.CreatedPrefix,
        };

        try
        {
            var page = await _client.ListBackupsAsync(request, cancellationToken).ConfigureAwait(false);
            return new BackupListView
            {
                Status = BackupOperationStatus.Succeeded,
                Entries = page.Entries,
                NextPageToken = page.NextPageToken,
            };
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            return new BackupListView
            {
                Status = BackupOperationStatus.Denied,
                Message = DenialMessage(ex),
            };
        }
        catch (RpcException ex)
        {
            return new BackupListView
            {
                Status = BackupOperationStatus.Failed,
                Message = FailureMessage(ex),
            };
        }
    }

    // A generous page size for the summary sweep, and a cap on the number of
    // pages, so a large catalog cannot turn facet-gathering into an unbounded
    // scan (the drop-downs stay accurate for any realistic backup count).
    private const int SummaryPageSize = 200;
    private const int SummaryMaxPages = 200;

    /// <inheritdoc />
    public async Task<BackupCatalogSummary> LoadSummaryAsync(CancellationToken cancellationToken = default)
    {
        var kinds = new HashSet<BackupKind>();
        var scopes = new HashSet<string>(StringComparer.Ordinal);
        var fullBackups = new List<BackupManifest>();

        try
        {
            string? token = null;
            var pages = 0;
            do
            {
                var request = new BackupCatalogRequest
                {
                    PageSize = SummaryPageSize,
                    PageToken = token,
                    OrderByCreatedDescending = true,
                };

                var page = await _client.ListBackupsAsync(request, cancellationToken).ConfigureAwait(false);
                foreach (var manifest in page.Entries)
                {
                    kinds.Add(manifest.Kind);
                    scopes.Add(manifest.Scope.TreeId);
                    if (manifest.Kind == BackupKind.Full && manifest.SetId is null)
                    {
                        fullBackups.Add(manifest);
                    }
                }

                token = page.NextPageToken;
            }
            while (!string.IsNullOrEmpty(token) && ++pages < SummaryMaxPages);
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return new BackupCatalogSummary { Status = BackupOperationStatus.Denied };
        }
        catch (RpcException)
        {
            return new BackupCatalogSummary { Status = BackupOperationStatus.Failed };
        }

        return new BackupCatalogSummary
        {
            Status = BackupOperationStatus.Succeeded,
            Kinds = kinds.OrderBy(k => k.ToString(), StringComparer.Ordinal).ToList(),
            Scopes = scopes.OrderBy(s => s, StringComparer.Ordinal).ToList(),
            FullBackups = fullBackups,
        };
    }

    /// <inheritdoc />
    public Task<BackupChainDescription?> DescribeAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        return _client.DescribeBackupAsync(backupId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<BackupOperationResult> TriggerFullAsync(string name, BackupScopeSelector scope, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(scope);
        return RunAsync(
            async () =>
            {
                var result = await _client
                    .CreateBackupAsync(new LatticeBackupCaptureRequest(name, scope), cancellationToken)
                    .ConfigureAwait(false);
                return BackupOperationResult.Success($"Captured full backup '{result.BackupId}'.");
            });
    }

    /// <inheritdoc />
    public Task<BackupOperationResult> TriggerSetAsync(string name, IReadOnlyList<BackupScopeSelector> scopes, bool crossTreeConsistent, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(scopes);
        return RunAsync(
            async () =>
            {
                var result = await _client
                    .CreateBackupSetAsync(new LatticeBackupSetCaptureRequest(name, scopes, crossTreeConsistent), cancellationToken)
                    .ConfigureAwait(false);
                return BackupOperationResult.Success(
                    $"Captured backup set '{result.SetManifest.SetId}' ({result.Members.Count} tree(s)).");
            });
    }

    /// <inheritdoc />
    public Task<BackupOperationResult> TriggerIncrementalAsync(string name, BackupScopeSelector scope, string baseBackupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentException.ThrowIfNullOrEmpty(baseBackupId);
        return RunAsync(
            async () =>
            {
                var result = await _client
                    .CreateIncrementalBackupAsync(new LatticeBackupIncrementalCaptureRequest(name, scope, baseBackupId), cancellationToken)
                    .ConfigureAwait(false);
                return BackupOperationResult.Success($"Captured incremental backup '{result.BackupId}'.");
            });
    }

    /// <inheritdoc />
    public Task<BackupOperationResult> RestoreAsync(string backupId, string targetTreeId, LatticeRestoreMode mode = LatticeRestoreMode.InPlace, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        ArgumentException.ThrowIfNullOrEmpty(targetTreeId);
        return RunAsync(
            async () =>
            {
                var result = await _client
                    .RestoreBackupAsync(new LatticeRestoreRequest(backupId, targetTreeId, mode: mode), cancellationToken)
                    .ConfigureAwait(false);
                return BackupOperationResult.Success($"Restored '{result.BackupId}' into '{result.TargetTreeId}' ({result.EntriesApplied} entries).");
            });
    }

    /// <inheritdoc />
    public Task<BackupOperationResult> DeleteAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        return RunAsync(
            async () =>
            {
                var deleted = await _client.DeleteBackupAsync(backupId, cancellationToken).ConfigureAwait(false);
                return BackupOperationResult.Success(deleted
                    ? $"Deleted backup '{backupId}'."
                    : $"Backup '{backupId}' was already absent.");
            });
    }

    /// <inheritdoc />
    public Task<BackupOperationResult> ScheduleAsync(BackupScopeSelector scope, bool incremental, TimeSpan interval, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(interval.Ticks);
        return RunAsync(
            async () =>
            {
                var effective = await _client
                    .ScheduleBackupAsync(scope, incremental, interval, cancellationToken)
                    .ConfigureAwait(false);
                var kind = incremental ? "incremental" : "full";
                return BackupOperationResult.Success(
                    $"Scheduled recurring {kind} backup every {FormatInterval(effective)}.");
            });
    }

    private static string FormatInterval(TimeSpan interval)
    {
        var parts = new List<string>();
        var wholeHours = (int)interval.TotalHours;
        if (wholeHours > 0)
        {
            parts.Add($"{wholeHours}h");
        }

        if (interval.Minutes > 0)
        {
            parts.Add($"{interval.Minutes}m");
        }

        return parts.Count > 0 ? string.Join(" ", parts) : $"{interval.TotalSeconds:0}s";
    }

    private static async Task<BackupOperationResult> RunAsync(Func<Task<BackupOperationResult>> action)
    {
        try
        {
            return await action().ConfigureAwait(false);
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            return BackupOperationResult.Denied(DenialMessage(ex));
        }
        catch (RpcException ex)
        {
            return BackupOperationResult.Failure(FailureMessage(ex));
        }
    }

    private static string DenialMessage(LatticeAuthorizationDeniedException ex) =>
        string.IsNullOrWhiteSpace(ex.Message)
            ? "You are not permitted to perform this backup operation."
            : ex.Message;

    private static string FailureMessage(RpcException ex)
    {
        // A restore of a replicated tree fans out to every peer cluster; if the
        // backup store is not actually shared across all of them (a common
        // misconfiguration - e.g. a per-cluster sink, or a filesystem path that
        // is not a shared mount) the peer cannot resolve the manifest / artifacts
        // and the coordinated restore aborts. The server reports these as
        // FailedPrecondition. Translate the sink-not-shared shapes into a clear,
        // actionable explanation instead of a raw status dump.
        if (ex.StatusCode == StatusCode.FailedPrecondition)
        {
            var detail = ex.Status.Detail;
            if (IndicatesUnsharedBackupStore(detail))
            {
                return "This restore could not be completed because the backup is not reachable from "
                    + "every cluster. A multi-cluster restore needs a single backup store that all "
                    + "clusters share - otherwise a backup captured on one cluster is invisible to its "
                    + "peers and the coordinated restore is aborted. Check that every cluster is "
                    + "configured with the same shared backup sink (for example one shared blob "
                    + $"account), then retry. Server detail: {detail}";
            }

            return $"The restore could not be completed: {detail}";
        }

        return $"The backup operation failed ({ex.StatusCode}): {ex.Status.Detail}";
    }

    private static bool IndicatesUnsharedBackupStore(string? detail) =>
        detail is not null
        && (detail.Contains("could not prepare", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("absent from the sink", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("catalog or sink", StringComparison.OrdinalIgnoreCase));
}
