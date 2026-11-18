using System.Globalization;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Suggestions;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// The Schedules page's backup-set section (#6067): lists the tenant's backup-set
/// schedules and their recent runs and retained sets, and registers, changes,
/// runs now, pauses, resumes and removes them. The cluster authorizes every
/// verb; the section only reports its answer. When the connection does not serve
/// backup-set schedules the section renders nothing.
/// </summary>
public partial class BackupSetSchedulesSection : IDisposable
{
    private const string NamedTrees = "named";
    private const string AllTrees = "all";
    private const string NotServedSetRestore = "This connection does not serve restoring a whole set.";

    private static readonly IReadOnlyList<LtSelectOption> MembershipOptions =
    [
        new(NamedTrees, "Named trees"),
        new(AllTrees, "All trees"),
    ];

    private readonly ComponentLifetime _load = new();
    private readonly List<string> _trees = [];
    private bool _served = true;
    private IReadOnlyList<BackupSetScheduleStatus>? _schedules;
    private string? _listError;
    private string? _selected;
    private string? _actionMessage;
    private string? _actionError;
    private bool _busy;

    private bool _formOpen;
    private bool _editing;
    private string? _name;
    private string _membership = NamedTrees;
    private string? _tree;
    private bool _includeNewTrees = true;
    private string? _excluded;
    private TimeSpan? _interval = TimeSpan.FromHours(24);
    private string? _keepLast = "7";
    private TimeSpan? _maxAge;
    private bool _crossTree = true;
    private string? _nameError;
    private string? _treeError;
    private string? _keepLastError;
    private string? _retentionError;
    private string? _formError;
    private LtComboBox? _addBox;
    private LtDurationInput? _intervalField;
    private LtDurationInput? _maxAgeField;

    private bool _confirmPause;
    private bool _confirmRemove;
    private bool _deleteRetainedSets;
    private string? _pending;

    private bool _setRestoreServed;
    private bool _confirmRestore;
    private ScheduledBackupSet? _pendingSet;
    private ClusterStatusPoller? _poller;
    private SetRestore? _restore;

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    [Inject(Key = ShellFacades.Key)]
    internal ILatticeBackupControl Control { get; set; } = default!;

    [Inject]
    internal BackupsAccess Access { get; set; } = default!;

    [Inject]
    internal ExplorerSuggestions Suggestions { get; set; } = default!;

    [Inject]
    internal ExplorerNavigator Navigator { get; set; } = default!;

    [Inject]
    internal TimeProvider Time { get; set; } = default!;

    private BackupSetScheduleStatus? Selected =>
        _selected is { } name ? _schedules?.FirstOrDefault(schedule => schedule.Name == name) : null;

    private bool AnyRunInFlight => _schedules?.Any(schedule => schedule.CurrentRun is not null) == true;

    /// <inheritdoc />
    public void Dispose()
    {
        _load.Leave();
        _poller?.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        _served = Access.BackupSetSchedulesServed != false;
        _setRestoreServed = Services.GetShellFacade<ILatticeTreeAdmin>() is not null;
        if (_served)
        {
            await LoadAsync(_load.Token);
        }
    }

    /// <summary>
    /// The request the form describes, or <see langword="null"/> with the fields'
    /// errors set when it describes none. Retention needs a keep-last count, a
    /// maximum age, or both - the same rule the request itself enforces.
    /// </summary>
    /// <param name="name">The schedule's name.</param>
    /// <param name="membership">Named trees or all trees.</param>
    /// <param name="trees">The named trees.</param>
    /// <param name="includeNewTrees">Whether a schedule of all trees includes trees created later.</param>
    /// <param name="excluded">The typed excluded tree prefixes.</param>
    /// <param name="interval">The interval.</param>
    /// <param name="keepLast">The typed keep-last count.</param>
    /// <param name="maxAge">The maximum age.</param>
    /// <param name="crossTree">Whether the set is captured at one consistency fence.</param>
    /// <param name="errors">The field errors, keyed by field.</param>
    internal static LatticeBackupSetScheduleRequest? Describe(
        string? name,
        string membership,
        IReadOnlyList<string> trees,
        bool includeNewTrees,
        string? excluded,
        TimeSpan interval,
        string? keepLast,
        TimeSpan? maxAge,
        bool crossTree,
        out FormErrors errors)
    {
        errors = new FormErrors();
        var trimmed = name?.Trim();
        if (string.IsNullOrEmpty(trimmed))
        {
            errors.Name = "Give the schedule a name.";
        }
        else
        {
            try
            {
                LatticeBackupSetScheduleRequest.ValidateName(trimmed);
            }
            catch (ArgumentException)
            {
                errors.Name = $"Use at most {LatticeBackupSetScheduleRequest.MaxNameLength} letters, digits, '-', '_' or '.'.";
            }
        }

        if (membership == NamedTrees && trees.Count == 0)
        {
            errors.Trees = "Add at least one tree.";
        }

        int? count = null;
        if (!string.IsNullOrWhiteSpace(keepLast))
        {
            if (int.TryParse(keepLast.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) && parsed >= 1)
            {
                count = parsed;
            }
            else
            {
                errors.KeepLast = "Give a whole number of sets, at least 1.";
            }
        }

        if (errors.KeepLast is null && count is null && maxAge is null)
        {
            errors.Retention = "Set how many sets to keep, how old a set may grow, or both.";
        }

        if (errors.Any)
        {
            return null;
        }

        try
        {
            return membership == NamedTrees
                ? LatticeBackupSetScheduleRequest.ForTrees(
                    trimmed!, [.. trees.Select(BackupScopeSelector.WholeTree)], interval, count, maxAge, crossTree)
                : LatticeBackupSetScheduleRequest.ForAllTrees(
                    trimmed!, interval, includeNewTrees, count, maxAge, BackupSetSchedulesText.Prefixes(excluded), crossTree);
        }
        catch (ArgumentException exception)
        {
            errors.Form = "The request was not accepted: " + exception.Message;
            return null;
        }
    }

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();

    private async Task LoadAsync(CancellationToken token)
    {
        try
        {
            var schedules = await Access.ListBackupSetSchedulesAsync(token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            if (schedules is null)
            {
                _served = false;
                return;
            }

            _schedules = schedules;
            _listError = null;
            FollowWhileRunning();
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                _listError = BackupsFaults.IsDenied(exception)
                    ? "You may not read the backup-set schedules."
                    : BackupsFaults.Describe(exception);
            }
        }
    }

    /// <summary>
    /// Starts reading the schedules again every <see cref="ClusterStatusPoller.Interval"/>
    /// while any of them reports a run in flight, so its progress stays live; the
    /// poller stops by itself once a read finds no run in flight, and with the component.
    /// </summary>
    private void FollowWhileRunning()
    {
        if (!AnyRunInFlight || _load.Token.IsCancellationRequested)
        {
            return;
        }

        _poller ??= new ClusterStatusPoller(Time);
        if (!_poller.IsFollowing)
        {
            _poller.Follow(PollAsync);
        }
    }

    private async Task<ClusterPollOutcome> PollAsync(CancellationToken cancellationToken)
    {
        IReadOnlyList<BackupSetScheduleStatus>? schedules;
        try
        {
            schedules = await Access.ListBackupSetSchedulesAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            return ClusterPollOutcome.Failed;
        }

        cancellationToken.ThrowIfCancellationRequested();
        var outcome = ClusterPollOutcome.Settled;
        await InvokeAsync(() =>
        {
            if (_load.Token.IsCancellationRequested)
            {
                return;
            }

            if (schedules is null)
            {
                _served = false;
            }
            else
            {
                _schedules = schedules;
                _listError = null;
                outcome = AnyRunInFlight ? ClusterPollOutcome.Running : ClusterPollOutcome.Settled;
            }

            StateHasChanged();
        }).ConfigureAwait(false);
        return outcome;
    }

    private async Task RefreshAsync()
    {
        _actionMessage = null;
        _actionError = null;
        await LoadAsync(_load.Token);
    }

    private void Select(string name)
    {
        _selected = name;
        _actionMessage = null;
        _actionError = null;
    }

    private void OpenNewForm()
    {
        ResetForm();
        _formOpen = true;
    }

    private void OpenEditForm(BackupSetScheduleStatus status)
    {
        ResetForm();
        var request = status.Request;
        _editing = true;
        _name = request.Name;
        _membership = request.Membership == BackupSetMembershipMode.AllTrees ? AllTrees : NamedTrees;
        _trees.AddRange(request.Scopes.Select(scope => scope.TreeId));
        _includeNewTrees = request.IncludeNewTrees;
        _excluded = string.Join(", ", request.ExcludedTreePrefixes);
        _interval = request.Interval;
        _keepLast = request.RetentionKeepLast?.ToString(CultureInfo.InvariantCulture);
        _maxAge = request.RetentionMaxAge;
        _crossTree = request.CrossTreeConsistent;
        _formOpen = true;
    }

    private void CloseForm()
    {
        ResetForm();
        _formOpen = false;
    }

    private void ResetForm()
    {
        _editing = false;
        _name = null;
        _membership = NamedTrees;
        _trees.Clear();
        _tree = null;
        _includeNewTrees = true;
        _excluded = null;
        _interval = TimeSpan.FromHours(24);
        _keepLast = "7";
        _maxAge = null;
        _crossTree = true;
        _nameError = null;
        _treeError = null;
        _keepLastError = null;
        _retentionError = null;
        _formError = null;
    }

    private async Task AddTreeAsync()
    {
        var tree = _tree?.Trim();
        if (string.IsNullOrEmpty(tree))
        {
            _treeError = "Name a tree to add.";
            return;
        }

        if (_trees.Contains(tree, StringComparer.Ordinal))
        {
            _treeError = "That tree is already in the set.";
            return;
        }

        _treeError = null;
        if (_addBox is not null && !await _addBox.ConfirmAsync().ConfigureAwait(true))
        {
            return;
        }

        _trees.Add(tree);
        _tree = null;
    }

    private void RemoveTree(string tree) => _trees.Remove(tree);

    private async Task SaveAsync()
    {
        if (_busy)
        {
            return;
        }

        var token = _load.Token;
        _actionMessage = null;
        _actionError = null;
        if (_intervalField is not null && !await _intervalField.ConfirmAsync())
        {
            return;
        }

        if (_maxAgeField is not null && !await _maxAgeField.ConfirmAsync())
        {
            return;
        }

        if (token.IsCancellationRequested || _interval is not { } interval)
        {
            return;
        }

        var request = Describe(_name, _membership, _trees, _includeNewTrees, _excluded, interval, _keepLast, _maxAge, _crossTree, out var errors);
        _nameError = errors.Name;
        _treeError = errors.Trees;
        _keepLastError = errors.KeepLast;
        _retentionError = errors.Retention;
        _formError = errors.Form;
        if (request is null)
        {
            return;
        }

        _busy = true;
        try
        {
            var status = await Control.ScheduleBackupSetAsync(request, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _formOpen = false;
            _selected = status.Name;
            _actionMessage = "Saved backup-set schedule " + status.Name + ". Next run: " + BackupSetSchedulesText.NextRun(status) + ".";
            ResetForm();
            await LoadAsync(token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (NotSupportedException)
        {
            Withdraw();
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                _formError = BackupsFaults.Describe(exception);
            }
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _busy = false;
            }
        }
    }

    private Task RunNowAsync(string name) =>
        ActAsync(name, async token =>
        {
            try
            {
                return await Control.RunBackupSetScheduleNowAsync(name, token)
                    ? "Started a run of " + name + "."
                    : "A run of " + name + " is already in progress.";
            }
            catch (KeyNotFoundException)
            {
                return Gone(name);
            }
        });

    private Task ResumeAsync(BackupSetScheduleStatus status) =>
        ActAsync(status.Name, async token =>
        {
            var resumed = await Control.ScheduleBackupSetAsync(status.Request, token);
            return "Resumed " + status.Name + ". Next run: " + BackupSetSchedulesText.NextRun(resumed) + ".";
        });

    private void AskPause(string name)
    {
        if (_busy)
        {
            return;
        }

        _pending = name;
        _confirmPause = true;
    }

    private Task PauseAsync()
    {
        _confirmPause = false;
        if (_pending is not { } name)
        {
            return Task.CompletedTask;
        }

        return ActAsync(name, async token =>
            await Control.CancelBackupSetScheduleAsync(name, token)
                ? "Paused " + name + ". Its retained sets are kept."
                : Gone(name));
    }

    private void AskRemove(string name)
    {
        if (_busy)
        {
            return;
        }

        _pending = name;
        _deleteRetainedSets = false;
        _confirmRemove = true;
    }

    private Task RemoveAsync()
    {
        if (_pending is not { } name)
        {
            return Task.CompletedTask;
        }

        var deleteSets = _deleteRetainedSets;
        return ActAsync(name, async token =>
        {
            if (!await Control.RemoveBackupSetScheduleAsync(name, deleteSets, token))
            {
                return Gone(name);
            }

            if (_selected == name)
            {
                _selected = null;
            }

            return "Removed " + name + (deleteSets ? " and deleted its retained sets' backups." : ". Its sets' backups stay in the catalogue.");
        });
    }

    private static string Gone(string name) => "No backup-set schedule is named " + name + " any more.";

    private async Task ActAsync(string name, Func<CancellationToken, Task<string>> act)
    {
        if (_busy)
        {
            return;
        }

        var token = _load.Token;
        _actionMessage = null;
        _actionError = null;
        _busy = true;
        try
        {
            var message = await act(token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _actionMessage = message;
            await LoadAsync(token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (NotSupportedException)
        {
            Withdraw();
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                _actionError = BackupsFaults.Describe(exception);
            }
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _busy = false;
            }
        }
    }

    private void Withdraw()
    {
        Access.MarkBackupSetSchedulesNotServed();
        _served = false;
        _poller?.Stop();
        _confirmPause = false;
        _confirmRemove = false;
        _confirmRestore = false;
    }

    /// <summary>
    /// The backup facade's form of one member of a whole-set restore, so the member
    /// can be reverted through <see cref="ILatticeBackupControl.RevertRestoreAsync"/>:
    /// every field carried across, as a shadow cut-over, which is the only mode a
    /// tree-administration restore applies.
    /// </summary>
    /// <param name="result">The member's restore result.</param>
    internal static LatticeRestoreResult ToRestoreResult(TreeRestoreResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new LatticeRestoreResult(
            result.BackupId,
            result.TargetTreeId,
            LatticeRestoreMode.ShadowCutover,
            result.OperationId,
            result.ManifestChain,
            result.EntriesApplied,
            result.ShadowPhysicalTreeId,
            result.PreviousPhysicalTreeId);
    }

    private void AskRestoreSet(ScheduledBackupSet set)
    {
        if (_busy)
        {
            return;
        }

        _pendingSet = set;
        _confirmRestore = true;
    }

    private async Task RestoreSetAsync()
    {
        if (_busy || _pendingSet is not { } set)
        {
            return;
        }

        if (Services.GetShellFacade<ILatticeTreeAdmin>() is not { } admin)
        {
            _setRestoreServed = false;
            return;
        }

        var token = _load.Token;
        _actionMessage = null;
        _actionError = null;
        _busy = true;
        try
        {
            var results = await admin.RestoreTreeSetAsync(set.SetId, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _restore = new SetRestore(set.SetId, set.Name, results);
            Services.GetService<ClusterTreeChanges>()?.Changed();
            _actionMessage = "Restored set " + set.Name + ": " + BackupSetSchedulesText.Trees(results.Count) + " switched to their copies in the set.";
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (NotSupportedException)
        {
            _setRestoreServed = false;
            _actionError = NotServedSetRestore;
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                _actionError = BackupsFaults.Describe(exception);
            }
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _busy = false;
            }
        }
    }

    private async Task RevertSetRestoreAsync()
    {
        if (_busy || _restore is not { } restore)
        {
            return;
        }

        var token = _load.Token;
        _actionMessage = null;
        _actionError = null;
        _busy = true;
        var revertedNow = 0;
        try
        {
            foreach (var result in restore.Results)
            {
                if (restore.Reverted.Contains(result.TargetTreeId))
                {
                    continue;
                }

                try
                {
                    await Control.RevertRestoreAsync(ToRestoreResult(result), token);
                    if (token.IsCancellationRequested)
                    {
                        return;
                    }

                    restore.Reverted.Add(result.TargetTreeId);
                    restore.Failures.Remove(result.TargetTreeId);
                    revertedNow++;
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception)
                {
                    restore.Failures[result.TargetTreeId] = BackupsFaults.Describe(exception);
                }
            }

            if (revertedNow > 0)
            {
                Services.GetService<ClusterTreeChanges>()?.Changed();
            }

            _actionMessage = restore.Failures.Count == 0
                ? "Reverted the restore of set " + restore.SetName + ": every tree holds the copy it held before."
                : null;
            _actionError = restore.Failures.Count == 0
                ? null
                : "Reverted " + restore.Reverted.Count.ToString(CultureInfo.InvariantCulture) + " of "
                    + BackupSetSchedulesText.Trees(restore.Results.Count) + "; "
                    + BackupSetSchedulesText.Trees(restore.Failures.Count) + " could not be reverted. Each tree's state is shown below.";
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _busy = false;
            }
        }
    }

    private void DismissSetRestore()
    {
        _restore = null;
        _actionMessage = null;
        _actionError = null;
    }

    /// <summary>A whole-set restore this page made, kept so it can be reverted until it is dismissed.</summary>
    /// <param name="setId">The restored set's id.</param>
    /// <param name="setName">The restored set's name.</param>
    /// <param name="results">One result per member tree.</param>
    private sealed class SetRestore(string setId, string setName, IReadOnlyList<TreeRestoreResult> results)
    {
        public string SetId { get; } = setId;

        public string SetName { get; } = setName;

        public IReadOnlyList<TreeRestoreResult> Results { get; } = results;

        /// <summary>The trees reverted so far.</summary>
        public HashSet<string> Reverted { get; } = new(StringComparer.Ordinal);

        /// <summary>Why each tree whose revert failed was not reverted.</summary>
        public Dictionary<string, string> Failures { get; } = new(StringComparer.Ordinal);
    }

    /// <summary>The backup-set form's field errors.</summary>
    internal sealed class FormErrors
    {
        /// <summary>The name's error.</summary>
        public string? Name { get; set; }

        /// <summary>The named trees' error.</summary>
        public string? Trees { get; set; }

        /// <summary>The keep-last count's error.</summary>
        public string? KeepLast { get; set; }

        /// <summary>The retention bound's error: neither a count nor an age.</summary>
        public string? Retention { get; set; }

        /// <summary>The whole request's error.</summary>
        public string? Form { get; set; }

        /// <summary>Whether any field has an error.</summary>
        public bool Any => Name is not null || Trees is not null || KeepLast is not null || Retention is not null || Form is not null;
    }
}
