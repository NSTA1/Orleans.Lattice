using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The Tag indexes tab: the indexes over this tree with their status, members
/// and reconcile action (the retired Tag Index plugin, plus the reconcile the
/// tree-administration facade offers).
/// </summary>
public partial class DataTagIndexesPanel : IDisposable
{
    /// <summary>How many members one page asks for.</summary>
    internal const int MemberPageSize = 50;

    private readonly CancellationTokenSource _lifetime = new();
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
    private bool _reconciling;
    private string? _error;

    [CascadingParameter]
    internal DataWorkspace? Workspace { get; set; }

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    [Inject]
    internal DataAdminGate Gate { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    private TagIndexRef? SelectedIndex => _indexes?.FirstOrDefault(index => index.IndexName == _selected);

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
            _selection = null;
            await ReloadAsync();
        }

        var selection = (workspace.Address.GetQuery(DataTabs.IndexQuery), workspace.Address.GetQuery(DataTabs.TagQuery));
        if (_selection != selection)
        {
            var indexChanged = _selection?.Index != selection.Item1;
            _selection = selection;
            _selected = selection.Item1;
            _tag = selection.Item2;
            if (indexChanged)
            {
                await LoadSelectedIndexAsync();
            }

            await ResetMembersAsync();
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Cancel();
        _lifetime.Dispose();
        GC.SuppressFinalize(this);
    }

    private ExplorerAddress IndexAddress(string indexName) =>
        Workspace!.ForTab(DataTabs.TagIndexes)
            .WithQuery(DataTabs.IndexQuery, indexName)
            .WithQuery(DataTabs.TagQuery, null);

    private LtStateRole ReconcileRole(TagIndexRef index) => _statuses.GetValueOrDefault(index.IndexName) switch
    {
        null => LtStateRole.Unknown,
        { ReconcileIdle: true } => LtStateRole.Healthy,
        _ => LtStateRole.Lagging,
    };

    private string ReconcileText(TagIndexRef index) => _statuses.GetValueOrDefault(index.IndexName) switch
    {
        null => "Unknown",
        { ReconcileIdle: true } => "Idle",
        _ => "Reconciling",
    };

    private string CoveredCount(TagIndexRef index) =>
        _statuses.GetValueOrDefault(index.IndexName) is { } status ? DataFormat.Count(status.CoveredTrees.Length) : "-";

    private async Task ReloadAsync()
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
            _indexes = await reader.ListTagIndexesForTreeAsync(workspace.Tree.StateId, _lifetime.Token);
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
            return;
        }
        catch (Exception exception)
        {
            _error = DataErrors.Describe(exception, "read the tag indexes over this tree");
            return;
        }

        if (Gate.Admin is { } admin)
        {
            foreach (var index in _indexes)
            {
                _statuses[index.IndexName] = await StatusAsync(admin, index.IndexName);
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

    private async Task LoadSelectedIndexAsync()
    {
        _covered = null;
        _tags = null;
        _hiddenCovered = 0;
        _canReconcile = false;
        if (SelectedIndex is not { } index || DataServices.Find<IDataReader>(Services) is not { } reader || Workspace is not { } workspace)
        {
            return;
        }

        _canReconcile = workspace.OffersAdministration && Gate.Admin is not null && await Gate.CanAdministerAsync(index.TreeId, _lifetime.Token);
        try
        {
            var covered = await reader.ListCoveredTreesForIndexAsync(index.IndexName, _lifetime.Token);
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
            _covered = [];
        }

        try
        {
            _tags = await reader.ListTagsForIndexAsync(index.IndexName, _lifetime.Token);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _tags = [];
        }
    }

    private async Task ResetMembersAsync()
    {
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
        if (SelectedIndex is not { } index || _tag is null || DataServices.Find<IDataReader>(Services) is not { } reader || Workspace is not { } workspace)
        {
            return;
        }

        _loadingMembers = true;
        try
        {
            var page = await reader.ScanTagMembersAsync(index.IndexName, _tag, MemberPageSize, _membersContinuation, _lifetime.Token);
            _members = [.. _members, .. page.Members.Select(member => new DataTagMember(workspace.Directory.FindByStateId(member.TreeId), member.Key, member.TreeId + "\n" + member.Key))];

            _membersContinuation = page.HasMore ? page.ContinuationToken : null;
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            _membersError = DataErrors.Describe(exception, "read the keys carrying this tag");
        }
        finally
        {
            _loadingMembers = false;
        }
    }

    private async Task ReconcileAsync()
    {
        if (SelectedIndex is not { } index || Gate.Admin is not { } admin)
        {
            return;
        }

        _reconciling = true;
        _confirming = false;
        StateHasChanged();
        try
        {
            var report = await admin.ReconcileTagIndexAsync(index.IndexName, _lifetime.Token);
            Toasts.Show(
                $"Reconciled {index.IndexName}: scanned {DataArea.Plural(report.KeysScanned, "key")} and removed {DataArea.Plural(report.OrphanRowsRemoved, "orphaned row")}.",
                LtToastTone.Success);
            _statuses[index.IndexName] = await StatusAsync(admin, index.IndexName);
            await ResetMembersAsync();
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            Toasts.Show(DataErrors.Describe(exception, "reconcile this tag index"), LtToastTone.Danger);
        }
        finally
        {
            _reconciling = false;
        }
    }
}
