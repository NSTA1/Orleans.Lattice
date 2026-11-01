using System.Globalization;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup;

/// <summary>
/// Serves the filtered, newest-first, paged backup-catalog listing from the
/// backup-catalog index materialised view. The index re-keys each catalogued
/// backup so a forward scan yields rows newest-first with the members of a backup
/// set contiguous; this query walks that order, evaluates the request's
/// name / kind / scope / created predicates inline (a push-down over the compact
/// index rows), collapses each set's contiguous members into one logical row,
/// drops any row whose backup no longer exists in the authoritative catalog (a
/// liveness check that heals stale index rows a value-less delete cannot re-key),
/// hides rows the caller may not read, and pages by an opaque index-key cursor.
/// Every catalogued backup is listed, the members of an incremental chain
/// included.
/// <para>
/// When the index view is not present (it is disabled, or the host does not run
/// the view infrastructure), or it cannot catch up to the catalog head within
/// <see cref="IndexCatchUpTimeout"/> (it is draining a burst or rebuilding), the
/// query is answered from a full catalog scan that produces the identical
/// ordering, filtering and cursor semantics, so a lagging index never hides a
/// catalogued backup.
/// </para>
/// </summary>
internal sealed class BackupCatalogIndexQuery
{
    private const char Separator = BackupConstants.KeySeparator;

    private static readonly ILatticeSerializer<BackupCatalogIndexRow> RowSerializer =
        JsonLatticeSerializer<BackupCatalogIndexRow>.Default;

    /// <summary>
    /// How long a listing waits for the index to reach the catalog head before it
    /// is answered from the authoritative catalog instead.
    /// </summary>
    internal static readonly TimeSpan IndexCatchUpTimeout = TimeSpan.FromSeconds(5);

    private readonly ILatticeBackupCatalogStore _catalog;
    private readonly ILatticeBackupSink _sink;
    private readonly ILatticeViewFactory? _viewFactory;

    /// <summary>Initializes a new <see cref="BackupCatalogIndexQuery"/>.</summary>
    /// <param name="catalog">The authoritative catalog store, read for liveness and to return full manifests.</param>
    /// <param name="sink">The durable sink, probed at selection time so an unresolvable backup is never surfaced.</param>
    /// <param name="viewFactory">The view factory used to open the index view, or <see langword="null"/> when views are not hosted.</param>
    public BackupCatalogIndexQuery(ILatticeBackupCatalogStore catalog, ILatticeBackupSink sink, ILatticeViewFactory? viewFactory)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(sink);
        _catalog = catalog;
        _sink = sink;
        _viewFactory = viewFactory;
    }

    /// <summary>
    /// Renders <paramref name="created"/> as the invariant UTC string the created
    /// starts-with filter matches against.
    /// </summary>
    /// <param name="created">The capture timestamp to format.</param>
    /// <returns>The <c>yyyy-MM-dd HH:mm:ss</c> UTC rendering.</returns>
    public static string FormatCreated(DateTimeOffset created) =>
        created.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    /// <summary>
    /// Runs the filtered, newest-first, paged query.
    /// </summary>
    /// <param name="request">The catalog request carrying the filters and the cursor.</param>
    /// <param name="pageSize">The resolved page size (already clamped by the facade).</param>
    /// <param name="isReadAuthorized">The fail-closed per-scope read gate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>One page of the catalog, newest-first, with the continuation cursor.</returns>
    public async Task<BackupCatalogPage> QueryAsync(
        BackupCatalogRequest request,
        int pageSize,
        Func<BackupScopeSelector, CancellationToken, ValueTask<bool>> isReadAuthorized,
        CancellationToken cancellationToken)
    {
        var startKey = request.PageToken is { Length: > 0 } token ? token + "\u0000" : null;

        var view = _viewFactory is null
            ? null
            : await _viewFactory.GetAsync(BackupConstants.CatalogIndexView, cancellationToken).ConfigureAwait(false);

        // Read-your-writes: the index is served only once it has caught up to the
        // catalog head. An index still draining a burst, or rebuilding after falling
        // off the catalog's write-ahead log, could lack a backup the catalog already
        // holds, so a catch-up that does not complete in time is answered from the
        // authoritative catalog instead (issue #5450). Both sources produce the same
        // keys, so a cursor taken from one resumes on the other.
        if (view is not null && !await IsCaughtUpAsync(view, cancellationToken).ConfigureAwait(false))
        {
            view = null;
        }

        var source = view is not null
            ? ScanIndexAsync(view, startKey, cancellationToken)
            : ScanFullScanAsync(startKey, cancellationToken);

        return await PageAsync(source, request, pageSize, isReadAuthorized, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Waits, bounded by <see cref="IndexCatchUpTimeout"/>, for the index to reflect
    /// every catalog write committed before the call.
    /// </summary>
    /// <returns><see langword="true"/> when the index caught up in time.</returns>
    private static async Task<bool> IsCaughtUpAsync(ILatticeView view, CancellationToken cancellationToken)
    {
        try
        {
            await view.WaitForSourceHeadAsync(IndexCatchUpTimeout, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    /// <summary>Streams index entries from the materialised view in newest-first key order.</summary>
    private static async IAsyncEnumerable<IndexEntry> ScanIndexAsync(
        ILatticeView view,
        string? startKey,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var entry in view.ScanEntriesAsync(startKey, null, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            yield return new IndexEntry(entry.Key, RowSerializer.Deserialize(entry.Value), Manifest: null);
        }
    }

    /// <summary>Streams synthesized index entries from a full catalog scan when the index view is absent or lagging.</summary>
    private async IAsyncEnumerable<IndexEntry> ScanFullScanAsync(
        string? startKey,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var synthesized = new List<IndexEntry>();
        await foreach (var manifest in _catalog.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            var key = BackupCatalogIndexKey.Encode(manifest);
            if (startKey is not null && string.CompareOrdinal(key, startKey) < 0)
            {
                continue;
            }

            synthesized.Add(new IndexEntry(key, ToRow(manifest), manifest));
        }

        synthesized.Sort(static (a, b) => string.CompareOrdinal(a.Key, b.Key));
        foreach (var entry in synthesized)
        {
            yield return entry;
        }
    }

    private static BackupCatalogIndexRow ToRow(BackupManifest manifest) => new()
    {
        BackupId = manifest.Id,
        Name = manifest.Name,
        Kind = manifest.Kind,
        TreeId = manifest.Scope.TreeId,
        CreatedAtUtc = manifest.CreatedAtUtc,
        SetId = manifest.SetId,
        SetName = manifest.SetName,
        BaseBackupId = manifest.BaseBackupId,
    };

    private async Task<BackupCatalogPage> PageAsync(
        IAsyncEnumerable<IndexEntry> source,
        BackupCatalogRequest request,
        int pageSize,
        Func<BackupScopeSelector, CancellationToken, ValueTask<bool>> isReadAuthorized,
        CancellationToken cancellationToken)
    {
        // Each accepted logical row and the index key that fully consumes it (the
        // group's highest key, so resuming after it skips the whole group).
        var matched = new List<(IReadOnlyList<BackupManifest> Manifests, string CursorKey)>();

        // Page-scoped guard so a backup that (transiently) holds more than one index
        // row - e.g. an orphan left by an older index generation before it rebuilds -
        // is still listed at most once per page.
        var emitted = new HashSet<string>(StringComparer.Ordinal);

        var group = new List<BackupCatalogIndexRow>();
        string? groupKey = null;
        string? groupLastKey = null;

        await foreach (var entry in source.ConfigureAwait(false))
        {
            var thisGroupKey = GroupKeyOf(entry.Key);
            if (groupKey is not null && !string.Equals(thisGroupKey, groupKey, StringComparison.Ordinal))
            {
                await FinalizeGroupAsync(group, groupLastKey!, request, isReadAuthorized, matched, emitted, cancellationToken)
                    .ConfigureAwait(false);
                group.Clear();

                // One accepted row beyond the page proves more remain; stop here.
                if (matched.Count > pageSize)
                {
                    break;
                }
            }

            groupKey = thisGroupKey;
            group.Add(entry.Row);
            groupLastKey = entry.Key;
            _preloaded[entry.Row.BackupId] = entry.Manifest;
        }

        if (group.Count > 0 && matched.Count <= pageSize)
        {
            await FinalizeGroupAsync(group, groupLastKey!, request, isReadAuthorized, matched, emitted, cancellationToken)
                .ConfigureAwait(false);
        }

        var pageRows = matched.Count > pageSize ? matched.GetRange(0, pageSize) : matched;
        var entries = new List<BackupManifest>();
        foreach (var row in pageRows)
        {
            entries.AddRange(row.Manifests);
        }

        var nextPageToken = matched.Count > pageSize ? pageRows[^1].CursorKey : null;
        return new BackupCatalogPage { Entries = entries, NextPageToken = nextPageToken };
    }

    // Carries the pre-loaded manifest (full-scan source) so the liveness read is
    // skipped for entries whose manifest is already in hand.
    private readonly Dictionary<string, BackupManifest?> _preloaded = new(StringComparer.Ordinal);

    private async Task FinalizeGroupAsync(
        List<BackupCatalogIndexRow> members,
        string groupLastKey,
        BackupCatalogRequest request,
        Func<BackupScopeSelector, CancellationToken, ValueTask<bool>> isReadAuthorized,
        List<(IReadOnlyList<BackupManifest> Manifests, string CursorKey)> matched,
        HashSet<string> emitted,
        CancellationToken cancellationToken)
    {
        // Every catalogued backup is its own row, incremental-chain members
        // included: a base an increment builds on is still a restorable backup the
        // caller must be able to find, filter for, choose as a base and delete
        // (issue #5450). The chain is described per backup, not by hiding rows.
        if (!MatchesFilters(members, request))
        {
            return;
        }

        var manifests = new List<BackupManifest>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var row in members)
        {
            if (!seen.Add(row.BackupId))
            {
                continue;
            }

            var manifest = _preloaded.TryGetValue(row.BackupId, out var preloaded) && preloaded is not null
                ? preloaded
                : await _catalog.GetAsync(row.BackupId, cancellationToken).ConfigureAwait(false);

            // Liveness: an index row whose backup was deleted from the catalog is
            // skipped, so a stale index entry never surfaces a phantom backup.
            if (manifest is null)
            {
                continue;
            }

            if (!await isReadAuthorized(manifest.Scope, cancellationToken).ConfigureAwait(false))
            {
                continue;
            }

            // Sink liveness: a catalogued backup whose sink manifest is gone (store
            // drift after a non-clean restart) is unresolvable and must not be
            // offered as a base or restore point, even though its catalog row - and
            // index row - still exist. A cheap manifest-presence probe on the sink,
            // the single source of truth, distinct from the catalog liveness above.
            if (!await _sink.ManifestExistsAsync(manifest.Id, cancellationToken).ConfigureAwait(false))
            {
                continue;
            }

            // Page-scoped de-duplication: never list the same backup twice on a page,
            // even if it holds more than one index row.
            if (!emitted.Add(row.BackupId))
            {
                continue;
            }

            manifests.Add(manifest);
        }

        if (manifests.Count > 0)
        {
            matched.Add((manifests, groupLastKey));
        }
    }

    private static bool MatchesFilters(List<BackupCatalogIndexRow> members, BackupCatalogRequest request)
    {
        if (request.NamePrefix is { Length: > 0 } namePrefix
            && !members[0].DisplayName.StartsWith(namePrefix, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (request.Kind is { } kind && !members.Exists(r => r.Kind == kind))
        {
            return false;
        }

        if (request.TreeId is { Length: > 0 } treeId
            && !members.Exists(r => string.Equals(r.TreeId, treeId, StringComparison.Ordinal)))
        {
            return false;
        }

        if (request.CreatedPrefix is { Length: > 0 } createdPrefix)
        {
            var created = members[0].CreatedAtUtc;
            foreach (var row in members)
            {
                if (row.CreatedAtUtc < created)
                {
                    created = row.CreatedAtUtc;
                }
            }

            if (!FormatCreated(created).StartsWith(createdPrefix, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Extracts the <c>{ticks}\u001f{groupId}</c> prefix that identifies one logical row.</summary>
    private static string GroupKeyOf(string indexKey)
    {
        var first = indexKey.IndexOf(Separator);
        if (first < 0)
        {
            return indexKey;
        }

        var second = indexKey.IndexOf(Separator, first + 1);
        return second < 0 ? indexKey : indexKey[..second];
    }

    private readonly record struct IndexEntry(string Key, BackupCatalogIndexRow Row, BackupManifest? Manifest);
}
