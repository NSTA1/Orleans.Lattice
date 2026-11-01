using System.Globalization;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// The create-tree dialog (G4, issue #5368): the tree's name, its physical shard
/// count and its node sizes, each starting at the library default and explained; a
/// review that checks the exact name before anything is written; then the create.
/// By default a created tree opens on its Summary tab, where the page offers what
/// to do next; a host that sets <see cref="Created"/> takes the tree instead, as
/// the Data area's <b>New tree from file...</b> does to seed it.
/// </summary>
/// <remarks>
/// The dialog checks what it can before the cluster is asked - a reserved or
/// unaddressable name, a size out of range, a name already taken, a grant the
/// caller lacks, a tenant at its tree quota - and the cluster refuses the rest:
/// every outcome is one sentence beside the step that caused it.
/// </remarks>
public partial class ClusterTreeCreateDialog : IDisposable
{
    private readonly ComponentLifetime _lifetime = new();
    private readonly string _id = LtIds.Next("lt-cluster-create");
    private Stage _stage;
    private bool _wasOpen;
    private bool _busy;
    private string? _treeId;
    private string? _shards;
    private string? _leaf;
    private string? _internal;
    private string? _treeIdError;
    private string? _shardsError;
    private string? _leafError;
    private string? _internalError;
    private string? _refusal;
    private string? _tenant;
    private ClusterTreeCreationRequest? _request;

    private enum Stage
    {
        Compose,
        Review,
    }

    /// <summary>Whether the dialog is showing.</summary>
    [Parameter]
    public bool Open { get; set; }

    /// <summary>Raised with <see langword="false"/> when the dialog closes, after a create or on request.</summary>
    [Parameter]
    public EventCallback<bool> OpenChanged { get; set; }

    /// <summary>The dialog's title.</summary>
    [Parameter]
    public string Title { get; set; } = "New tree";

    /// <summary>A name to start the form with, or <see langword="null"/> for an empty one.</summary>
    [Parameter]
    public string? InitialTreeId { get; set; }

    /// <summary>Where the dialog sits: centred, or as a sheet on a narrow screen.</summary>
    [Parameter]
    public LtDialogPlacement Placement { get; set; } = LtDialogPlacement.Center;

    /// <summary>
    /// Raised with the cluster's answer once a new tree is registered. When set, the
    /// host decides where to go next; when not, the tree's Summary tab opens.
    /// </summary>
    [Parameter]
    public EventCallback<TreeCreationResult> Created { get; set; }

    /// <summary>The tenant a tenant-rooted address names, or <see langword="null"/> on a cluster-wide address; the tree lands in it.</summary>
    [CascadingParameter(Name = ClusterScope.CascadeName)]
    internal string? Scope { get; set; }

    [Inject]
    private ClusterTreeCreation Creation { get; set; } = default!;

    [Inject]
    private ExplorerNavigator Navigator { get; set; } = default!;

    [Inject]
    private LtToastService Toasts { get; set; } = default!;

    private string ReviewHeadingId => _id + "-review";

    private string NameHint => Creation.TenantFor(Scope) is { } tenant
        ? $"Its logical name, such as factory/orders. It is created in tenant {tenant}. Names beginning _lattice_ or t/ are reserved."
        : "Its logical name, such as factory/orders. Names beginning _lattice_ or t/ are reserved.";

    /// <inheritdoc />
    public void Dispose() => _lifetime.Leave();

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        // Every opening starts afresh from the defaults, so a dialog closed half-way
        // never reopens on a stale name or a refusal meant for another.
        if (Open && !_wasOpen)
        {
            Reset();
        }

        _wasOpen = Open;
    }

    private void Reset()
    {
        _stage = Stage.Compose;
        _busy = false;
        _treeId = InitialTreeId;
        _shards = ClusterTreeCreation.DefaultShardCount.ToString(CultureInfo.InvariantCulture);
        _leaf = ClusterTreeCreation.DefaultMaxLeafKeys.ToString(CultureInfo.InvariantCulture);
        _internal = ClusterTreeCreation.DefaultMaxInternalChildren.ToString(CultureInfo.InvariantCulture);
        _treeIdError = _shardsError = _leafError = _internalError = _refusal = null;
        _request = null;
        _tenant = null;
    }

    private async Task ReviewAsync()
    {
        if (_busy)
        {
            return;
        }

        _refusal = null;
        _treeIdError = ClusterTreeCreation.ValidateTreeId(_treeId);
        _shardsError = ClusterTreeCreation.TryParseSize(_shards, 1, ClusterTreeCreation.MaximumShardCount, out var shards)
            ? null
            : $"Enter a whole number of shards, from 1 to {ClusterFormat.Count(ClusterTreeCreation.MaximumShardCount)}.";
        _leafError = ClusterTreeCreation.TryParseSize(_leaf, ClusterTreeCreation.MinimumMaxLeafKeys, int.MaxValue, out var leaf)
            ? null
            : $"Enter a whole number of keys, at least {ClusterTreeCreation.MinimumMaxLeafKeys}.";
        _internalError = ClusterTreeCreation.TryParseSize(_internal, ClusterTreeCreation.MinimumMaxInternalChildren, int.MaxValue, out var children)
            ? null
            : $"Enter a whole number of children, at least {ClusterTreeCreation.MinimumMaxInternalChildren}.";
        if (_treeIdError is not null || _shardsError is not null || _leafError is not null || _internalError is not null)
        {
            return;
        }

        var request = new ClusterTreeCreationRequest(_treeId!.Trim(), shards, leaf, children);
        _busy = true;
        string? refusal;
        try
        {
            refusal = await Creation.CheckAsync(request, Scope, _lifetime.Token);
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
            return;
        }

        if (_lifetime.IsLeft)
        {
            return;
        }

        _busy = false;
        if (refusal is not null)
        {
            _refusal = refusal;
            return;
        }

        _request = request;
        _tenant = Creation.TenantFor(Scope);
        _stage = Stage.Review;
    }

    private void Back()
    {
        _refusal = null;
        _stage = Stage.Compose;
    }

    private async Task CreateAsync()
    {
        if (_busy || _request is not { } request)
        {
            return;
        }

        _busy = true;
        _refusal = null;
        ClusterTreeCreationOutcome outcome;
        try
        {
            outcome = await Creation.CreateAsync(request, Scope, _lifetime.Token);
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
            return;
        }

        if (_lifetime.IsLeft)
        {
            return;
        }

        _busy = false;
        if (outcome.Result is not { } result)
        {
            _refusal = outcome.Refusal;
            return;
        }

        Toasts.Show($"Tree {result.TreeId} created.", LtToastTone.Success);
        await OpenChanged.InvokeAsync(false);
        if (Created.HasDelegate)
        {
            await Created.InvokeAsync(result);
        }
        else if (ClusterAddresses.TryTree(result.TreeId, ClusterTreeView.Overview, out var address))
        {
            Navigator.NavigateTo(address.WithTenant(Scope));
        }
    }

    private Task OnCancelAsync() => OnOpenChangedAsync(false);

    private Task OnOpenChangedAsync(bool open)
    {
        if (!open && _busy && _stage == Stage.Review)
        {
            // The create is already on its way; closing now would hide its answer.
            return Task.CompletedTask;
        }

        return OpenChanged.InvokeAsync(open);
    }

    private string ReviewSentence(ClusterTreeCreationRequest request) =>
        $"Create the empty tree {request.TreeId}{(_tenant is { } tenant ? " in tenant " + tenant : string.Empty)} with "
        + $"{ClusterFormat.Plural(request.ShardCount, "physical shard")}, {ClusterFormat.Count(request.MaxLeafKeys)} keys per leaf "
        + $"and {ClusterFormat.Count(request.MaxInternalChildren)} children per internal node. "
        + "Reshard and resize can change these later; the name cannot.";
}
