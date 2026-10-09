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
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        var treeChanged = _loadedFor != workspace.Tree.StateId;
        if (treeChanged || _indexes is null)
        {
            var cancellationToken = _memberReads.Renew();
            if (treeChanged)
            {
                _loadedFor = workspace.Tree.StateId;
                _selection = null;
                _members = [];
                _membersContinuation = null;
                _membersError = null;
                _loadingMembers = false;
                _watch.Clear();
            }

            await ReloadAsync(cancellationToken);
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            if (treeChanged)
            {
                await ResumeAsync();
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }
        }

        var selection = (workspace.Address.GetQuery(DataTabs.IndexQuery), workspace.Address.GetQuery(DataTabs.TagQuery));
        if (_selection != selection)
        {
            var cancellationToken = _memberReads.Renew();
            var indexChanged = _selection?.Index != selection.Item1;
            _selection = selection;
            _selected = selection.Item1;
            _tag = selection.Item2;
            if (indexChanged || _covered is null || _tags is null)
            {
                try
                {
                    await LoadSelectedIndexAsync(cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
            }

            await ResetMembersAsync();
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Leave();
        _memberReads.Leave();
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

    private Task ReloadAsync() => ReloadAsync(_lifetime.Token);

    private async Task ReloadAsync(CancellationToken cancellationToken)
    {
        _indexes = null;
        _error = null;
        _statuses.Clear();
        if (DataServices.Find<IDataReader>(Services) is not { } reader || Workspace is not { } workspace)
        {
            _error = "This Explorer has no state API to read tag indexes through.";
            return;
        }

        try
        {
            var indexes = await reader.ListTagIndexesForTreeAsync(workspace.Tree.StateId, cancellationToken);
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            _indexes = indexes;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception exception)
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                _error = DataErrors.Describe(exception, "read the tag indexes over this tree");
            }

            return;
        }

        if (Gate.Admin is { } admin)
        {
            foreach (var index in _indexes)
            {
                var status = await StatusAsync(admin, index.IndexName);
                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                _statuses[index.IndexName] = status;
            }
        }
    }

    private async Task<TreeTagIndexStatus?> StatusAsync(ILatticeTreeAdmin admin, string indexName)
    {
        try
        {
            return await admin.GetTagIndexStatusAsync(indexName, _lifetime.Token);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            // A status the caller may not read reads as unknown.
            return null;
        }
    }

    private async Task LoadSelectedIndexAsync(CancellationToken cancellationToken)
    {
        _covered = null;
        _tags = null;
        _hiddenCovered = 0;
        _canReconcile = false;
        if (SelectedIndex is not { } index || DataServices.Find<IDataReader>(Services) is not { } reader || Workspace is not { } workspace)
        {
            return;
        }

        var canReconcile = workspace.OffersAdministration && TreeAdminOperationsAccess.Of(Gate.Admin) is not null && await Gate.CanAdministerAsync(index.TreeId, cancellationToken);
        if (cancellationToken.IsCancellationRequested)
        {
            return;
        }

        _canReconcile = canReconcile;
        try
        {
            var covered = await reader.ListCoveredTreesForIndexAsync(index.IndexName, cancellationToken);
            if (cancellationToken.IsCancellationRequested)
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
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            _covered = [];
        }

        try
        {
            var tags = await reader.ListTagsForIndexAsync(index.IndexName, cancellationToken);
            if (!cancellationToken.IsCancellationRequested)
            {
                _tags = tags;
            }
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                _tags = [];
            }
        }
    }

    private async Task ResetMembersAsync()
    {
        _memberReads.Renew();
        _members = [];
        _membersContinuation = null;
        _membersError = null;
        _loadingMembers = false;
        if (_tag is not null)
        {
            await LoadMembersAsync();
        }
    }

    private async Task LoadMembersAsync()
    {
        if (SelectedIndex is not { } index || _tag is null || DataServices.Find<IDataReader>(Services) is not { } reader || Workspace is not { } workspace)
        {
            return;
        }

        _loadingMembers = true;
        _membersError = null;
        var cancellationToken = _memberReads.Token;
        try
        {
            var page = await reader.ScanTagMembersAsync(index.IndexName, _tag, MemberPageSize, _membersContinuation, cancellationToken);
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            _members = [.. _members, .. page.Members.Select(member => new DataTagMember(workspace.Directory.FindByStateId(member.TreeId), member.Key, member.TreeId + "\n" + member.Key))];

            _membersContinuation = page.HasMore ? page.ContinuationToken : null;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                _membersError = DataErrors.Describe(exception, "read the keys carrying this tag");
            }
        }
        finally
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                _loadingMembers = false;
            }
        }
    }

    private async Task ResumeAsync()
    {
        if (_indexes is not { Count: > 0 } indexes || TreeAdminOperationsAccess.Of(Gate.Admin) is not { } operations)
        {
            return;
        }

        var candidates = indexes.Select(index => (TreeAdminOperationKinds.TagIndexReconcile, index.IndexName)).ToList();
        try
        {
            await _watch.ResumeAsync(operations, candidates, _lifetime.Token);
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
        }
    }

    private async Task ReconcileAsync()
    {
        _confirming = false;
        if (SelectedIndex is not { } index || TreeAdminOperationsAccess.Of(Gate.Admin) is not { } operations || Reconciling is not null)
        {
            return;
        }

        _starting = index.IndexName;
        StateHasChanged();
        try
        {
            await _watch.StartAsync(
                operations,
                TreeAdminOperationKinds.TagIndexReconcile,
                index.IndexName,
                (operationId, ct) => operations.StartTagIndexReconcileAsync(index.IndexName, operationId, ct),
                _lifetime.Token);
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
        }
        catch (Exception exception)
        {
            Toasts.Show(DataErrors.Describe(exception, "reconcile this tag index"), LtToastTone.Danger);
        }
        finally
        {
            _starting = null;
        }
    }

    private async Task CancelAsync()
    {
        _cancelling = true;
        try
        {
            await _watch.CancelAsync(_lifetime.Token);
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
        }
        catch (Exception exception)
        {
            Toasts.Show(DataErrors.Describe(exception, "stop this reconcile"), LtToastTone.Danger);
        }
        finally
        {
            _cancelling = false;
        }
    }

    private void OnWatchChanged() => _ = InvokeAsync(StateHasChanged);

    private void OnWatchFinished(LatticeOperationStatus status)
    {
        var indexName = _watch.Target ?? string.Empty;
        _ = InvokeAsync(async () =>
        {
            if (_lifetime.IsLeft)
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
                        _statuses[indexName] = await StatusAsync(admin, indexName);
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