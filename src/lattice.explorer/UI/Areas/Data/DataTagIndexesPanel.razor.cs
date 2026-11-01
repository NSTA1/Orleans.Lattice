using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Operations;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The Tag indexes tab: the indexes over this tree with their status, members
/// and reconcile action (the retired Tag Index plugin, plus the reconcile the
/// tree-administration facade offers). A reconcile runs on the cluster as a
/// tracked operation (#4124): the tab follows its progress, picks it up again when it
/// is opened later - after a reload or in another tab - and can ask it to stop.
/// </summary>
public partial class DataTagIndexesPanel : IDisposable
{
    /// <summary>How many members one page asks for.</summary>
    internal const int MemberPageSize = 50;

    private readonly ComponentLifetime _lifetime = new();
    private readonly ComponentLifetime _catalogue = new();
    private readonly ComponentLifetime _details = new();
    private readonly ComponentLifetime _memberReads = new();
    private readonly string _headingId = LtIds.Next("lt-data-tag-index");
    private readonly Dictionary<string, TreeTagIndexStatus?> _statuses = new(StringComparer.Ordinal);
    private IReadOnlyList<DataTagMember> _members = [];
    private string? _loadedFor;
    private (string? Index, string? Tag)? _selection;
    private IReadOnlyList<TagIndexRef>? _indexes;
    private IReadOnlyList<DataTreeEntry>? _covered;
    private int _hiddenCovered;
    private IReadOnlyList<string>? _tags;
    private string? _selected;
    private string? _tag;
    private string? _membersContinuation;
    private string? _membersError;
    private bool _loadingMembers;
    private bool _canReconcile;
    private bool _confirming;
    private string? _starting;
    private bool _cancelling;
    private TreeAdminOperationWatch _watch = default!;
    private DataStatusFollow _follow = default!;
    private string? _error;

    [CascadingParameter]
    internal DataWorkspace? Workspace { get; set; }

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    [Inject]
    internal DataAdminGate Gate { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    [Inject]
    internal TimeProvider Time { get; set; } = default!;

    private TagIndexRef? SelectedIndex => _indexes?.FirstOrDefault(index => index.IndexName == _selected);

    /// <summary>The index a reconcile is running on, or <see langword="null"/>.</summary>
    private string? Reconciling => _starting ?? (_watch.IsRunning ? _watch.Target : null);

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        _watch = new TreeAdminOperationWatch(Time);
        _watch.Changed += OnWatchChanged;
        _watch.Finished += OnWatchFinished;
        _follow = new DataStatusFollow(Time, InvokeAsync, FollowTickAsync);
    }

    /// <summary>
    /// Arms the follow while any index reads as reconciling - a reconcile this tab did
    /// not start, from another tab, session or caller - and disarms it once all are idle.
    /// </summary>
    private void UpdateFollow() =>
        _follow.Update(!_catalogue.IsLeft && _statuses.Values.Any(status => status is { ReconcileIdle: false }));

    /// <summary>
    /// One follow tick: re-reads only the indexes that read as reconciling and replaces
    /// each status in place. A failed re-read keeps the last answer, and a reload that
    /// starts while the tick reads wins.
    /// </summary>
    private async Task FollowTickAsync()
    {
        var token = _catalogue.Token;
        if (token.IsCancellationRequested || Gate.Admin is not { } admin)
        {
            return;
        }

        var changed = false;
        foreach (var (name, previous) in _statuses.ToArray())
        {
            if (previous is not { ReconcileIdle: false })
            {
                continue;
            }

            var status = await StatusAsync(admin, name, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            if (status is not null)
            {
                _statuses[name] = status;
                changed = true;
            }
        }

        UpdateFollow();
        if (changed)
        {
            StateHasChanged();
        }
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        if (_loadedFor != workspace.Tree.StateId)
        {
            _loadedFor = workspace.Tree.StateId;
            _lifetime.Renew();
            _watch.Clear();
            _starting = null;
            _cancelling = false;
            await ReloadAsync();
            return;
        }

        await LoadSelectionAsync();
    }

    private async Task LoadSelectionAsync()
    {
        if (Workspace is not { } workspace || _indexes is null)
        {
            return;
        }

        var selection = (workspace.Address.GetQuery(DataTabs.IndexQuery), workspace.Address.GetQuery(DataTabs.TagQuery));
        if (_selection != selection)
        {
            var indexChanged = _selection?.Index != selection.Item1;
            _selection = selection;
            _selected = selection.Item1;
            _tag = selection.Item2;
            var details = Task.CompletedTask;
            if (indexChanged)
            {
                _confirming = false;
                details = LoadSelectedIndexAsync(_details.Renew());
            }

            await Task.WhenAll(details, ResetMembersAsync());
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Leave();
        _catalogue.Leave();
        _details.Leave();
        _memberReads.Leave();
        _follow?.Dispose();
        _watch?.Dispose();
        GC.SuppressFinalize(this);
    }

    private ExplorerAddress IndexAddress(string indexName) =>
        Workspace!.ForTab(DataTabs.TagIndexes)
            .WithQuery(DataTabs.IndexQuery, indexName)
            .WithQuery(DataTabs.TagQuery, null);

    private LtStateRole ReconcileRole(TagIndexRef index) => Reconciling == index.IndexName ? LtStateRole.Lagging : _statuses.GetValueOrDefault(index.IndexName) switch
    {
        null => LtStateRole.Unknown,
        { ReconcileIdle: true } => LtStateRole.Healthy,
        _ => LtStateRole.Lagging,
    };

    private string ReconcileText(TagIndexRef index) => Reconciling == index.IndexName ? "Reconciling" : _statuses.GetValueOrDefault(index.IndexName) switch
    {
        null => "Unknown",
        { ReconcileIdle: true } => "Idle",
        _ => "Reconciling",
    };

    private string CoveredCount(TagIndexRef index) =>
        _statuses.GetValueOrDefault(index.IndexName) is { } status ? DataFormat.Count(status.CoveredTrees.Length) : "-";

    private async Task ReloadAsync()
    {
        var token = _catalogue.Renew();
        _details.Renew();
        _memberReads.Renew();
        _selection = null;
        _selected = null;
        _tag = null;
        _confirming = false;
        _canReconcile = false;
        _loadingMembers = false;
        _members = [];
        _membersContinuation = null;
        _membersError = null;
        _indexes = null;
        _error = null;
        _statuses.Clear();
        _follow.Stop();
        if (DataServices.Find<IDataReader>(Services) is not { } reader || Workspace is not { } workspace)
        {
            _error = "This Explorer has no state API to read tag indexes through.";
            return;
        }

        try
        {
            var indexes = await reader.ListTagIndexesForTreeAsync(workspace.Tree.StateId, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _indexes = indexes;
            if (Gate.Admin is { } admin)
            {
                foreach (var index in indexes)
                {
                    var status = await StatusAsync(admin, index.IndexName, token);
                    if (token.IsCancellationRequested)
                    {
                        return;
                    }

                    _statuses[index.IndexName] = status;
                }
            }

            UpdateFollow();
            await LoadSelectionAsync();
            if (!token.IsCancellationRequested)
            {
                await ResumeAsync(token);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            return;
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                _error = DataErrors.Describe(exception, "read the tag indexes over this tree");
            }
        }
    }

    private static async Task<TreeTagIndexStatus?> StatusAsync(ILatticeTreeAdmin admin, string indexName, CancellationToken token)
    {
        try
        {
            return await admin.GetTagIndexStatusAsync(indexName, token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            return null;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            // A status the caller may not read reads as unknown.
            return null;
        }
    }

    private async Task LoadSelectedIndexAsync(CancellationToken token)
    {
        _covered = null;
        _tags = null;
        _hiddenCovered = 0;
        _canReconcile = false;
        if (SelectedIndex is not { } index || DataServices.Find<IDataReader>(Services) is not { } reader || Workspace is not { } workspace)
        {
            return;
        }

        try
        {
            var allowed = workspace.OffersAdministration && TreeAdminOperationsAccess.Of(Gate.Admin) is not null && await Gate.CanAdministerAsync(index.TreeId, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _canReconcile = allowed;
            var covered = await reader.ListCoveredTreesForIndexAsync(index.IndexName, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            var visible = new List<DataTreeEntry>(covered.Count);
            foreach (var stateId in covered)
            {
                if (workspace.Directory.FindByStateId(stateId) is { } tree)
                {
                    visible.Add(tree);
                }
            }

            _covered = visible;
            _hiddenCovered = covered.Count - visible.Count;
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            return;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            if (token.IsCancellationRequested)
            {
                return;
            }

            _covered = [];
        }

        try
        {
            var tags = await reader.ListTagsForIndexAsync(index.IndexName, token);
            if (!token.IsCancellationRequested)
            {
                _tags = tags;
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            if (!token.IsCancellationRequested)
            {
                _tags = [];
            }
        }
    }

    private async Task ResetMembersAsync()
    {
        _memberReads.Renew();
        _loadingMembers = false;
        _members = [];
        _membersContinuation = null;
        _membersError = null;
        if (_tag is not null)
        {
            await LoadMembersAsync();
        }
    }

    private async Task LoadMembersAsync()
    {
        if (_loadingMembers || SelectedIndex is not { } index || _tag is not { } tag || DataServices.Find<IDataReader>(Services) is not { } reader || Workspace is not { } workspace)
        {
            return;
        }

        _loadingMembers = true;
        _membersError = null;
        var token = _memberReads.Token;
        try
        {
            var page = await reader.ScanTagMembersAsync(index.IndexName, tag, MemberPageSize, _membersContinuation, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _members = [.. _members, .. page.Members.Select(member => new DataTagMember(workspace.Directory.FindByStateId(member.TreeId), member.Key, member.TreeId + "\n" + member.Key))];

            _membersContinuation = page.HasMore ? page.ContinuationToken : null;
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                _membersError = DataErrors.Describe(exception, "read the keys carrying this tag");
            }
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _loadingMembers = false;
            }
        }
    }

    private async Task ResumeAsync(CancellationToken token)
    {
        if (_indexes is not { Count: > 0 } indexes || TreeAdminOperationsAccess.Of(Gate.Admin) is not { } operations)
        {
            return;
        }

        var candidates = indexes.Select(index => (TreeAdminOperationKinds.TagIndexReconcile, index.IndexName)).ToList();
        try
        {
            await _watch.ResumeAsync(operations, candidates, token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
    }

    private async Task ReconcileAsync()
    {
        if (!_confirming)
        {
            return;
        }

        _confirming = false;
        if (!_canReconcile || SelectedIndex is not { } index || TreeAdminOperationsAccess.Of(Gate.Admin) is not { } operations || Reconciling is not null)
        {
            return;
        }

        _starting = index.IndexName;
        var token = _lifetime.Token;
        StateHasChanged();
        try
        {
            await _watch.StartAsync(
                operations,
                TreeAdminOperationKinds.TagIndexReconcile,
                index.IndexName,
                (operationId, ct) => operations.StartTagIndexReconcileAsync(index.IndexName, operationId, ct),
                token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                Toasts.Show(DataErrors.Describe(exception, "reconcile this tag index"), LtToastTone.Danger);
            }
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _starting = null;
            }
        }
    }

    private async Task CancelAsync()
    {
        var token = _lifetime.Token;
        _cancelling = true;
        try
        {
            await _watch.CancelAsync(token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                Toasts.Show(DataErrors.Describe(exception, "stop this reconcile"), LtToastTone.Danger);
            }
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _cancelling = false;
            }
        }
    }

    private void OnWatchChanged() => _ = InvokeAsync(StateHasChanged);

    private void OnWatchFinished(LatticeOperationStatus status)
    {
        var indexName = _watch.Target ?? string.Empty;
        var token = _lifetime.Token;
        _ = InvokeAsync(async () =>
        {
            if (token.IsCancellationRequested || _watch.OperationId != status.OperationId)
            {
                return;
            }

            switch (status.State)
            {
                case LatticeOperationState.Succeeded:
                    Toasts.Show(
                        $"Reconciled {indexName}: scanned {DataArea.Plural(ResultCount(status, TreeAdminOperationResultKeys.KeysScanned), "key")} and removed {DataArea.Plural(ResultCount(status, TreeAdminOperationResultKeys.OrphanRowsRemoved), "orphaned row")}.",
                        LtToastTone.Success);
                    _watch.Clear();
                    if (Gate.Admin is { } admin)
                    {
                        var current = await StatusAsync(admin, indexName, token);
                        if (token.IsCancellationRequested)
                        {
                            return;
                        }

                        _statuses[indexName] = current;
                        UpdateFollow();
                    }

                    if (string.Equals(SelectedIndex?.IndexName, indexName, StringComparison.Ordinal))
                    {
                        await ResetMembersAsync();
                    }

                    break;
                case LatticeOperationState.Cancelled:
                    Toasts.Show($"Stopped the reconcile of {indexName}.", LtToastTone.Warning);
                    break;
                default:
                    Toasts.Show($"The reconcile of {indexName} failed.", LtToastTone.Danger);
                    break;
            }

            StateHasChanged();
        });
    }

    private static long ResultCount(LatticeOperationStatus status, string key) =>
        status.Result.TryGetValue(key, out var text) && long.TryParse(text, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var value)
            ? value
            : 0;
}