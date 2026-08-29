using System.Globalization;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Backup.Components;

/// <summary>
/// The Existing Backups concern: paging, the push-down filter, the display-row
/// projection, row selection, and the lazily-loaded incremental chains behind a
/// collapsed chain row.
/// </summary>
public partial class BackupsPanel
{
    /// <summary>
    /// The listing page size. The list is served newest-first with the filter
    /// predicates pushed into the server scan.
    /// </summary>
    internal const int ExistingPageSize = 5;

    /// <summary>The UTC format the created column and its filter agree on.</summary>
    internal const string CreatedFormat = "yyyy-MM-dd HH:mm:ss";

    private static readonly IReadOnlyList<BackupCatalogueRow> NoRows = Array.Empty<BackupCatalogueRow>();

    private BackupCatalogPager _pager = default!;
    private BackupCatalogSummary _summary = BackupCatalogSummary.Empty;

    // The current page's backups collapsed into display rows, folding each backup
    // set's per-tree members (sharing a stamped SetId) into a single row and
    // pre-rendering the cell text. Recomputed only when the page in view changes,
    // so a render reads a cached array instead of re-grouping the page and
    // re-formatting every cell.
    private BackupListView? _rowsProjectedFrom;
    private IReadOnlyList<BackupCatalogueRow> _rows = NoRows;

    // The Existing Backups row the user has selected (by display id); its
    // restore/delete controls are the only ones shown. Null when no row is
    // selected.
    private string? _selectedRowId;

    // Display ids of backups created by the most recent successful capture, so
    // the Existing Backups tab can highlight them right after creation.
    private readonly HashSet<string> _highlightedBackupIds = new(StringComparer.Ordinal);

    // Lazily-loaded incremental chains, keyed by the tip (chain-row) backup id:
    // the base-first ordered member manifests behind the collapsed row, and the
    // same members newest-first for the point-in-time picker. Populated when an
    // incremental-chain row is selected so the restore dropdown and the delete-all
    // prompt can enumerate the chain.
    private readonly Dictionary<string, IReadOnlyList<BackupManifest>> _chainCache = new(StringComparer.Ordinal);
    private readonly Dictionary<string, IReadOnlyList<BackupManifest>> _chainNewestFirst = new(StringComparer.Ordinal);

    private string _filterName = string.Empty;
    private string _filterCreated = string.Empty;
    private string _filterKind = string.Empty;
    private string _filterScope = string.Empty;

    /// <summary>The pager over the newest-first catalogue listing.</summary>
    internal BackupCatalogPager Pager => _pager;

    /// <summary>The catalogue-wide facets backing the filter drop-downs.</summary>
    internal BackupCatalogSummary Summary => _summary;

    /// <summary>The current page's display rows, in catalogue order.</summary>
    internal IReadOnlyList<BackupCatalogueRow> Rows
    {
        get
        {
            var page = _pager.Current;
            if (!ReferenceEquals(page, _rowsProjectedFrom))
            {
                _rowsProjectedFrom = page;
                _rows = Project(page);
            }

            return _rows;
        }
    }

    /// <summary>
    /// The row backing the Actions panel: the currently selected display row on
    /// the current page, or <see langword="null"/> when nothing is selected.
    /// </summary>
    internal BackupRow? SelectedRow
    {
        get
        {
            if (_selectedRowId is null)
            {
                return null;
            }

            var rows = Rows;
            for (var i = 0; i < rows.Count; i++)
            {
                if (string.Equals(rows[i].DisplayId, _selectedRowId, StringComparison.Ordinal))
                {
                    return rows[i].Row;
                }
            }

            return null;
        }
    }

    /// <summary>The name prefix filter currently applied.</summary>
    internal string FilterName => _filterName;

    /// <summary>The created-timestamp prefix filter currently applied.</summary>
    internal string FilterCreated => _filterCreated;

    /// <summary>The kind filter currently applied, or empty for any kind.</summary>
    internal string FilterKind => _filterKind;

    /// <summary>The scope filter currently applied, or empty for any scope.</summary>
    internal string FilterScope => _filterScope;

    /// <summary>The message shown when the current page holds no rows.</summary>
    internal string EmptyMessage => HasActiveFilter
        ? "No backups match the filters."
        : "No backups are visible.";

    /// <summary>Whether <paramref name="displayId"/> was created by the most recent capture.</summary>
    /// <param name="displayId">The display row id to test.</param>
    internal bool IsHighlighted(string displayId) => _highlightedBackupIds.Contains(displayId);

    /// <summary>Whether <paramref name="displayId"/> is the selected row.</summary>
    /// <param name="displayId">The display row id to test.</param>
    internal bool IsSelected(string displayId) =>
        string.Equals(_selectedRowId, displayId, StringComparison.Ordinal);

    private bool HasActiveFilter =>
        !string.IsNullOrEmpty(_filterName)
        || !string.IsNullOrEmpty(_filterCreated)
        || !string.IsNullOrEmpty(_filterKind)
        || !string.IsNullOrEmpty(_filterScope);

    private BackupCatalogFilter CurrentFilter => new()
    {
        Kind = Enum.TryParse<BackupKind>(_filterKind, out var kind) ? kind : null,
        Scope = string.IsNullOrEmpty(_filterScope) ? null : _filterScope,
        NamePrefix = string.IsNullOrEmpty(_filterName) ? null : _filterName,
        CreatedPrefix = string.IsNullOrEmpty(_filterCreated) ? null : _filterCreated,
    };

    /// <summary>
    /// Clicking a row selects it (revealing its restore/delete controls);
    /// clicking the selected row again clears the selection. Interacting with
    /// the table also drops the just-created highlight. Selecting an
    /// incremental-chain row lazily loads its chain so the point-in-time restore
    /// and delete-all prompt can enumerate the members folded behind the
    /// collapsed row.
    /// </summary>
    /// <param name="row">The display row that was activated.</param>
    internal async Task SelectRowAsync(BackupRow row)
    {
        if (string.Equals(_selectedRowId, row.DisplayId, StringComparison.Ordinal))
        {
            _selectedRowId = null;
            _highlightedBackupIds.Clear();
            NotifyStateChanged();
            return;
        }

        _selectedRowId = row.DisplayId;
        _highlightedBackupIds.Clear();

        if (row.IsIncrementalChain)
        {
            await EnsureChainLoadedAsync(row);
        }

        NotifyStateChanged();
    }

    /// <summary>Applies a new name prefix filter and reopens the listing.</summary>
    /// <param name="value">The committed prefix.</param>
    internal async Task OnNameFilterChangedAsync(string value)
    {
        _filterName = value;
        await ApplyFilterAsync();
    }

    /// <summary>Applies a new created-timestamp prefix filter and reopens the listing.</summary>
    /// <param name="value">The committed prefix.</param>
    internal async Task OnCreatedFilterChangedAsync(string value)
    {
        _filterCreated = value;
        await ApplyFilterAsync();
    }

    /// <summary>Applies a new kind filter and reopens the listing.</summary>
    /// <param name="value">The chosen kind, or empty for any kind.</param>
    internal async Task OnKindFilterChangedAsync(string? value)
    {
        _filterKind = value ?? string.Empty;
        await ApplyFilterAsync();
    }

    /// <summary>Applies a new scope filter and reopens the listing.</summary>
    /// <param name="value">The chosen scope tree id, or empty for any scope.</param>
    internal async Task OnScopeFilterChangedAsync(string? value)
    {
        _filterScope = value ?? string.Empty;
        await ApplyFilterAsync();
    }

    /// <summary>Advances to the next page, loading it at the frontier.</summary>
    internal async Task NextPageAsync()
    {
        BeginBusy();
        try
        {
            await _pager.NextAsync();
            InvalidateRows();
            await RefreshHealthAsync();
        }
        finally
        {
            EndBusy();
        }
    }

    /// <summary>Returns to the previous (already-visited) page.</summary>
    internal async Task PreviousPageAsync()
    {
        _pager.Previous();
        InvalidateRows();
        await RefreshHealthAsync();
        NotifyStateChanged();
    }

    // Re-runs the current filter from the first page, gathering fresh facets. Used
    // after a capture / restore / delete so the list and drop-downs stay accurate.
    private async Task RefreshListAsync()
    {
        _summary = await Reader.LoadSummaryAsync();
        await _pager.ResetAsync(ExistingPageSize, CurrentFilter);
        InvalidateRows();
        await RefreshBaseBackupsAsync();
        await RefreshHealthAsync();
    }

    // A filter change reopens the listing from the first page; the row selection
    // and just-created highlight are dropped since they may no longer be visible.
    private async Task ApplyFilterAsync()
    {
        _selectedRowId = null;
        _highlightedBackupIds.Clear();
        BeginBusy();
        try
        {
            await _pager.ResetAsync(ExistingPageSize, CurrentFilter);
            InvalidateRows();
            await RefreshHealthAsync();
        }
        finally
        {
            EndBusy();
        }
    }

    // After a successful capture, switch to the Existing Backups tab and
    // highlight (and select) the backup that was just created.
    private async Task RevealNewBackupAsync(HashSet<string> beforeIds)
    {
        if (_lastResult is not { Status: BackupOperationStatus.Succeeded })
        {
            return;
        }

        _highlightedBackupIds.Clear();
        var rows = Rows;
        for (var i = 0; i < rows.Count; i++)
        {
            var id = rows[i].DisplayId;
            if (!beforeIds.Contains(id))
            {
                _highlightedBackupIds.Add(id);
            }
        }

        _selectedRowId = null;
        foreach (var id in _highlightedBackupIds)
        {
            _selectedRowId = id;
            break;
        }

        _activeSubTab = BackupsSubTab.Existing;
        await _preferences.SetAsync(SubTabStateKey, _activeSubTab);
    }

    private HashSet<string> SnapshotRowIds()
    {
        var rows = Rows;
        var ids = new HashSet<string>(rows.Count, StringComparer.Ordinal);
        for (var i = 0; i < rows.Count; i++)
        {
            ids.Add(rows[i].DisplayId);
        }

        return ids;
    }

    // Drops the cached projection so the next read re-groups the page. Called
    // whenever the pager's page in view is replaced rather than merely moved,
    // because a reset can hand back a fresh view for the same page index.
    private void InvalidateRows()
    {
        _rowsProjectedFrom = null;
        _rows = NoRows;
    }

    private static IReadOnlyList<BackupCatalogueRow> Project(BackupListView page)
    {
        var grouped = BackupRowGrouping.Group(page.Entries);
        if (grouped.Count == 0)
        {
            return NoRows;
        }

        var rows = new BackupCatalogueRow[grouped.Count];
        for (var i = 0; i < grouped.Count; i++)
        {
            rows[i] = BackupCatalogueRow.From(grouped[i]);
        }

        return rows;
    }

    // Loads (once) the base-first member manifests of an incremental chain behind
    // its collapsed tip row, and caches the newest-first ordering the point-in-time
    // picker renders. A describe failure or denial folds to the tip alone, so the
    // row still restores/deletes its own backup.
    private async Task EnsureChainLoadedAsync(BackupRow row)
    {
        var tipId = row.Members[0].Id;
        if (_chainCache.ContainsKey(tipId))
        {
            return;
        }

        var members = new List<BackupManifest>();
        try
        {
            var description = await Reader.DescribeAsync(tipId);
            if (description is not null)
            {
                foreach (var id in description.ChainBackupIds)
                {
                    var memberDescription = await Reader.DescribeAsync(id);
                    if (memberDescription is not null)
                    {
                        members.Add(memberDescription.Manifest);
                    }
                }
            }
        }
        catch (LatticeAuthorizationDeniedException)
        {
            // Fall back to the tip alone below.
        }

        if (members.Count == 0)
        {
            members.Add(row.Members[0]);
        }

        _chainCache[tipId] = members;

        // Ordered once here rather than on every render of the point-in-time
        // picker and the restore prompt, both of which read it. OrderByDescending
        // is a stable sort, so members captured at the same instant keep their
        // base-first relative order exactly as the retired per-render ordering
        // produced.
        _chainNewestFirst[tipId] = members.OrderByDescending(m => m.CreatedAtUtc).ToList();

        _restorePointByTip[tipId] = tipId;
    }

    /// <summary>
    /// The chain members of a selected incremental-chain row, newest-first for
    /// the point-in-time restore dropdown; empty until the chain has loaded.
    /// </summary>
    /// <param name="row">The chain tip row.</param>
    internal IReadOnlyList<BackupManifest> ChainMembersNewestFirst(BackupRow row) =>
        _chainNewestFirst.TryGetValue(row.Members[0].Id, out var members) ? members : NoManifests;

    /// <summary>
    /// The number of backups a chain row's delete would remove: the loaded chain
    /// when it is known, otherwise the tip alone.
    /// </summary>
    /// <param name="row">The chain tip row.</param>
    internal int ChainMemberCount(BackupRow row) =>
        _chainCache.TryGetValue(row.Members[0].Id, out var members) ? members.Count : row.Members.Count;

    /// <summary>Formats a capture timestamp the way the catalogue renders it.</summary>
    /// <param name="createdAtUtc">The capture time.</param>
    internal static string FormatCreated(DateTimeOffset createdAtUtc) =>
        createdAtUtc.UtcDateTime.ToString(CreatedFormat, CultureInfo.InvariantCulture);
}
