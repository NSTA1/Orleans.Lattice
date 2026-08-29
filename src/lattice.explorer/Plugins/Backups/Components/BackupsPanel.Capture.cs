using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Backup.Components;

/// <summary>
/// The New Backup concern: the visible tree list, the trees a capture targets,
/// the capture form's fields, and the capture itself.
/// </summary>
public partial class BackupsPanel
{
    private static readonly IReadOnlyList<BackupTreeOption> NoTrees = Array.Empty<BackupTreeOption>();
    private static readonly IReadOnlyList<BackupManifest> NoManifests = Array.Empty<BackupManifest>();

    private readonly List<string> _selectedTrees = new();

    private IReadOnlyList<BackupTreeOption> _availableTrees = NoTrees;

    // The visible trees split once per discovery into the two lists the picker
    // renders, so the render path reads arrays instead of re-running a LINQ
    // pipeline (and re-grouping the shadows) on every keystroke.
    private IReadOnlyList<BackupTreeOption> _ordinaryTrees = NoTrees;
    private IReadOnlyList<BackupShadowTreeGroup> _restoreShadowGroups = Array.Empty<BackupShadowTreeGroup>();

    private string _captureName = string.Empty;
    private BackupKind _captureKind = BackupKind.Full;
    private string _incrementalBaseId = string.Empty;

    // The full backups of the single selected tree, loaded via the index-backed
    // catalog query, that an incremental capture may build on. Refreshed whenever
    // the incremental scope changes.
    private IReadOnlyList<BackupManifest> _baseBackups = NoManifests;

    private bool _scheduleEnabled;
    private int _scheduleHours;
    private int _scheduleMinutes;

    /// <summary>
    /// Ordinary (non-shadow) trees, rendered as flat options in the picker.
    /// </summary>
    internal IReadOnlyList<BackupTreeOption> OrdinaryTrees => _ordinaryTrees;

    /// <summary>
    /// Restore-shadow trees grouped under the logical tree they were restored
    /// for, keyed off the first-class restore-shadow marker rather than the tree
    /// name.
    /// </summary>
    internal IReadOnlyList<BackupShadowTreeGroup> RestoreShadowGroups => _restoreShadowGroups;

    /// <summary>The tree ids the next capture targets, in selection order.</summary>
    internal IReadOnlyList<string> SelectedTrees => _selectedTrees;

    /// <summary>The name the next capture is stamped with.</summary>
    internal string CaptureName => _captureName;

    /// <summary>Whether the next capture is a full or an incremental backup.</summary>
    internal BackupKind CaptureKind => _captureKind;

    /// <summary>The base backup an incremental capture builds on, or empty when unchosen.</summary>
    internal string IncrementalBaseId => _incrementalBaseId;

    /// <summary>The full backups the current incremental scope may build on, newest first.</summary>
    internal IReadOnlyList<BackupManifest> BaseBackups => _baseBackups;

    /// <summary>Whether a recurring schedule is registered alongside the capture.</summary>
    internal bool ScheduleEnabled => _scheduleEnabled;

    /// <summary>The hours component of the requested recurring cadence.</summary>
    internal int ScheduleHours => _scheduleHours;

    /// <summary>The minutes component of the requested recurring cadence.</summary>
    internal int ScheduleMinutes => _scheduleMinutes;

    /// <summary>
    /// The single tree an incremental would target, or <see langword="null"/>
    /// when the selection is not exactly one tree (incremental is a single-tree
    /// capture).
    /// </summary>
    internal string? IncrementalTree => _selectedTrees.Count == 1 ? _selectedTrees[0] : null;

    /// <summary>
    /// Scheduling attaches a recurring capture to a single tree; it is not
    /// offered for a multi-tree backup set (mirroring the incremental
    /// single-tree rule).
    /// </summary>
    internal bool CanSchedule => _selectedTrees.Count == 1;

    /// <summary>The requested recurring cadence.</summary>
    internal TimeSpan ScheduleInterval => new(hours: _scheduleHours, minutes: _scheduleMinutes, seconds: 0);

    /// <summary>Whether the capture form is complete enough to submit.</summary>
    internal bool CanBackup
    {
        get
        {
            if (_busy || _selectedTrees.Count == 0 || string.IsNullOrWhiteSpace(_captureName))
            {
                return false;
            }

            if (_scheduleEnabled && (!CanSchedule || ScheduleInterval <= TimeSpan.Zero))
            {
                return false;
            }

            if (_captureKind == BackupKind.Incremental)
            {
                return _selectedTrees.Count == 1
                    && !string.IsNullOrWhiteSpace(_incrementalBaseId);
            }

            return true;
        }
    }

    /// <summary>Whether <paramref name="treeId"/> is in the capture selection.</summary>
    /// <param name="treeId">The tree id to test.</param>
    internal bool IsTreeSelected(string treeId) => _selectedTrees.Contains(treeId);

    /// <summary>Sets the name the next capture is stamped with.</summary>
    /// <param name="value">The raw input value; <see langword="null"/> clears the name.</param>
    internal void SetCaptureName(string? value) => _captureName = value ?? string.Empty;

    /// <summary>Chooses the base backup an incremental capture builds on.</summary>
    /// <param name="value">The base backup id; <see langword="null"/> clears the choice.</param>
    internal void SetIncrementalBaseId(string? value) => _incrementalBaseId = value ?? string.Empty;

    /// <summary>Turns the recurring-schedule request on or off.</summary>
    /// <param name="enabled">Whether to register a schedule alongside the capture.</param>
    internal void SetScheduleEnabled(bool enabled) => _scheduleEnabled = enabled;

    /// <summary>Sets the hours component of the requested cadence.</summary>
    /// <param name="value">The raw input value, clamped to 0 - 168.</param>
    internal void SetScheduleHours(object? value) => _scheduleHours = ParseInterval(value, 0, 168);

    /// <summary>Sets the minutes component of the requested cadence.</summary>
    /// <param name="value">The raw input value, clamped to 0 - 59.</param>
    internal void SetScheduleMinutes(object? value) => _scheduleMinutes = ParseInterval(value, 0, 59);

    /// <summary>Adds a tree to the capture selection.</summary>
    /// <param name="id">The tree id to add.</param>
    internal async Task AddTreeAsync(string id)
    {
        if (!_selectedTrees.Contains(id))
        {
            _selectedTrees.Add(id);
        }

        if (!CanSchedule)
        {
            _scheduleEnabled = false;
        }

        await RefreshBaseBackupsAsync();
        NotifyStateChanged();
    }

    /// <summary>Removes a tree from the capture selection.</summary>
    /// <param name="id">The tree id to remove.</param>
    internal async Task RemoveTreeAsync(string id)
    {
        _selectedTrees.Remove(id);

        if (!CanSchedule)
        {
            _scheduleEnabled = false;
        }

        await RefreshBaseBackupsAsync();
        NotifyStateChanged();
    }

    /// <summary>Switches the capture between a full and an incremental backup.</summary>
    /// <param name="kind">The capture kind to compose.</param>
    internal async Task SetKindAsync(BackupKind kind)
    {
        _captureKind = kind;
        if (kind == BackupKind.Full)
        {
            // Full captures carry no base; clear the base selector.
            _incrementalBaseId = string.Empty;
        }

        await RefreshBaseBackupsAsync();
        NotifyStateChanged();
    }

    /// <summary>
    /// Captures the composed backup: a single-tree full or incremental capture,
    /// or a cross-tree consistent backup set for more than one tree. Reveals the
    /// new backup in the Existing Backups tab on success.
    /// </summary>
    internal async Task BackupAsync()
    {
        var trees = _selectedTrees.ToList();
        var scheduleThisRun = _scheduleEnabled && CanSchedule && ScheduleInterval > TimeSpan.Zero;

        // Snapshot the current rows so the newly created backup can be
        // highlighted once the list reloads after a successful capture.
        var beforeIds = SnapshotRowIds();

        if (_captureKind == BackupKind.Incremental)
        {
            var scope = BackupScopeSelector.WholeTree(trees[0]);
            var capture = (Func<Task<BackupOperationResult>>)(() => Reader.TriggerIncrementalAsync(
                _captureName, scope, _incrementalBaseId));
            await RunCaptureAsync(capture, scheduleThisRun, scope, incremental: true);
        }
        else if (trees.Count == 1)
        {
            var scope = BackupScopeSelector.WholeTree(trees[0]);
            var capture = (Func<Task<BackupOperationResult>>)(() => Reader.TriggerFullAsync(_captureName, scope));
            await RunCaptureAsync(capture, scheduleThisRun, scope, incremental: false);
        }
        else
        {
            // More than one tree implies a cross-tree consistent backup set.
            var scopes = trees.Select(BackupScopeSelector.WholeTree).ToList();
            await RunAsync(() => Reader.TriggerSetAsync(_captureName, scopes, crossTreeConsistent: true));
        }

        await RevealNewBackupAsync(beforeIds);
        NotifyStateChanged();
    }

    private async Task LoadTreesAsync()
    {
        _availableTrees = await _domain.LoadTreesAsync();
        ProjectTreeLists();
    }

    // Splits the discovered trees into the picker's two presentations once per
    // discovery. Restore shadows are grouped under the logical tree they were
    // restored for, keyed off the first-class RestoreShadowOfTreeId marker rather
    // than the tree name.
    private void ProjectTreeLists()
    {
        var trees = _availableTrees;
        if (trees.Count == 0)
        {
            _ordinaryTrees = NoTrees;
            _restoreShadowGroups = Array.Empty<BackupShadowTreeGroup>();
            return;
        }

        List<BackupTreeOption>? ordinary = null;
        SortedDictionary<string, List<BackupTreeOption>>? shadows = null;

        for (var i = 0; i < trees.Count; i++)
        {
            var tree = trees[i];
            if (tree.RestoreShadowOfTreeId is { } logical)
            {
                shadows ??= new SortedDictionary<string, List<BackupTreeOption>>(StringComparer.Ordinal);
                if (!shadows.TryGetValue(logical, out var members))
                {
                    members = new List<BackupTreeOption>();
                    shadows[logical] = members;
                }

                members.Add(tree);
            }
            else
            {
                ordinary ??= new List<BackupTreeOption>(trees.Count);
                ordinary.Add(tree);
            }
        }

        _ordinaryTrees = (IReadOnlyList<BackupTreeOption>?)ordinary ?? NoTrees;

        if (shadows is null)
        {
            _restoreShadowGroups = Array.Empty<BackupShadowTreeGroup>();
            return;
        }

        var groups = new BackupShadowTreeGroup[shadows.Count];
        var index = 0;
        foreach (var pair in shadows)
        {
            groups[index++] = new BackupShadowTreeGroup(pair.Key, pair.Value);
        }

        _restoreShadowGroups = groups;
    }

    // Reloads the base-backup candidates for the current incremental scope from the
    // index-backed catalog query, then drops a stale base selection. A no-op that
    // clears the list unless an incremental of exactly one tree is being composed.
    private async Task RefreshBaseBackupsAsync()
    {
        _baseBackups = _captureKind == BackupKind.Incremental && IncrementalTree is { } tree
            ? await Reader.LoadFullBackupsAsync(tree)
            : NoManifests;

        SyncIncrementalBase();
    }

    // Drop a chosen base backup that no longer belongs to the selected tree, so
    // the base dropdown never keeps a stale selection after the scope changes.
    private void SyncIncrementalBase()
    {
        if (_incrementalBaseId.Length == 0)
        {
            return;
        }

        for (var i = 0; i < _baseBackups.Count; i++)
        {
            if (string.Equals(_baseBackups[i].Id, _incrementalBaseId, StringComparison.Ordinal))
            {
                return;
            }
        }

        _incrementalBaseId = string.Empty;
    }

    // Captures now and, when a recurring schedule was requested, also registers
    // it for the same scope and kind, surfacing a combined result.
    private async Task RunCaptureAsync(
        Func<Task<BackupOperationResult>> capture,
        bool schedule,
        BackupScopeSelector scope,
        bool incremental)
    {
        if (!schedule)
        {
            await RunAsync(capture);
            return;
        }

        var interval = ScheduleInterval;
        await RunManyAsync(
            new[]
            {
                capture,
                (Func<Task<BackupOperationResult>>)(() => Reader.ScheduleAsync(scope, incremental, interval)),
            },
            $"Captured backup and scheduled a recurring {(incremental ? "incremental" : "full")} backup.");
    }
}
