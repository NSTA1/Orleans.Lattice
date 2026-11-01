using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// <c>/cluster/trees</c>: every tree by logical name, its owners, shards, WAL
/// partitions and lifecycle state, each linking to its administration page. It
/// owns the visible controls of the "Reshard tree..." and "New tree..." palette
/// commands; "New tree..." is drawn only for a caller the create-tree probe admits,
/// and the command is refused, with a sentence, for one it does not.
/// </summary>
public partial class ClusterTreeList : IDisposable
{
    /// <summary>What a caller the create-tree probe refuses is told when the palette asks for a new tree.</summary>
    internal const string CreateRefusedText = "Your identity holds no grant to create trees on this cluster.";

    private readonly ComponentLifetime _lifetime = new();
    private ClusterLoad<IReadOnlyList<ClusterTreeEntry>> _trees = ClusterLoad<IReadOnlyList<ClusterTreeEntry>>.Loading;
    private string? _filter;
    private bool _reshardOpen;
    private string? _reshardTree;
    private string? _reshardError;
    private Task<bool>? _canCreateProbe;
    private bool _canCreate;
    private bool _createOpen;
    private string? _createRefusal;

    [Inject]
    private ClusterTreeCatalog Catalog { get; set; } = default!;

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

    [Inject]
    internal ExplorerSuggestions Suggestions { get; set; } = default!;

    [Inject]
    private ClusterCommandSignals Signals { get; set; } = default!;

    [Inject]
    private ExplorerNavigator Navigator { get; set; } = default!;

    [Inject]
    private ClusterTreeCreation Creation { get; set; } = default!;

    /// <summary>The tenant a tenant-rooted address names, whose own trees are all the list shows; <see langword="null"/> on the cluster-wide address.</summary>
    [CascadingParameter(Name = ClusterScope.CascadeName)]
    internal string? Scope { get; set; }

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    private LtDialogPlacement SheetPlacement => Breakpoint == LtBreakpoint.Compact ? LtDialogPlacement.End : LtDialogPlacement.Center;

    /// <inheritdoc />
    public void Dispose()
    {
        Signals.Requested -= OnCommand;
        _lifetime.Leave();
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        Signals.Requested += OnCommand;
        _canCreateProbe = ProbeCreateAsync();
        if (Signals.TryTake(ClusterArea.ReshardCommandId))
        {
            OpenReshard();
        }

        if (Signals.TryTake(ClusterArea.CreateTreeCommandId))
        {
            _ = InvokeAsync(RequestCreateAsync);
        }

        await LoadAsync(refresh: false);
        await _canCreateProbe;
    }

    // The probe fails closed: no facade, a refusal or a fault leaves New tree... hidden.
    private async Task<bool> ProbeCreateAsync()
    {
        bool answer;
        try
        {
            answer = await Creation.CanCreateAsync(_lifetime.Token);
        }
        catch (OperationCanceledException)
        {
            return false;
        }

        if (!_lifetime.IsLeft)
        {
            _canCreate = answer;
        }

        return answer;
    }

    private async Task LoadAsync(bool refresh)
    {
        _trees = ClusterLoad<IReadOnlyList<ClusterTreeEntry>>.Loading;
        _trees = await ClusterLoad<IReadOnlyList<ClusterTreeEntry>>.RunAsync(
            async ct => ClusterTreeCatalog.InScope(await Catalog.GetAsync(refresh, ct), Scope),
            _lifetime.Token);
    }

    private IReadOnlyList<ClusterTreeEntry> Filtered(IReadOnlyList<ClusterTreeEntry> trees) =>
        string.IsNullOrWhiteSpace(_filter)
            ? trees
            : [.. trees.Where(tree => Matches(tree, _filter.Trim()))];

    /// <summary>
    /// Whether the filter text names the tree: a part of its logical name (which carries its
    /// tenant and app), of its owner as the Owner column reads it, or of its state as the State
    /// column reads it (so "deleted" or "aliased" narrows the list to those trees).
    /// </summary>
    /// <param name="tree">The tree.</param>
    /// <param name="text">The filter text.</param>
    /// <returns>Whether the tree is listed.</returns>
    internal static bool Matches(ClusterTreeEntry tree, string text) =>
        tree.TreeId.Contains(text, StringComparison.OrdinalIgnoreCase)
        || (tree.Name.Ownership?.Contains(text, StringComparison.OrdinalIgnoreCase) ?? false)
        || StateText(tree).Contains(text, StringComparison.OrdinalIgnoreCase);

    private void OnCommand(string commandId)
    {
        if (string.Equals(commandId, ClusterArea.ReshardCommandId, StringComparison.Ordinal))
        {
            _ = InvokeAsync(() =>
            {
                OpenReshard();
                StateHasChanged();
            });
        }
        else if (string.Equals(commandId, ClusterArea.CreateTreeCommandId, StringComparison.Ordinal))
        {
            _ = InvokeAsync(RequestCreateAsync);
        }
    }

    // The palette can ask before the probe has answered, so the request waits for
    // it; a caller the probe refuses is told so rather than shown a dialog the
    // cluster would refuse.
    private async Task RequestCreateAsync()
    {
        var admitted = _canCreateProbe is { } probe && await probe;
        if (_lifetime.IsLeft)
        {
            return;
        }

        if (admitted)
        {
            OpenCreate();
        }
        else
        {
            _createRefusal = CreateRefusedText;
        }

        StateHasChanged();
    }

    private void OpenCreate()
    {
        _createRefusal = null;
        _createOpen = true;
    }

    private void OpenReshard()
    {
        _reshardError = null;
        _reshardOpen = true;
    }

    private void ContinueReshard()
    {
        var name = _reshardTree?.Trim();
        if (string.IsNullOrEmpty(name))
        {
            _reshardError = "Name the tree to reshard.";
            return;
        }

        if (_trees.Value is { } trees && !trees.Any(tree => string.Equals(tree.TreeId, name, StringComparison.Ordinal)))
        {
            _reshardError = $"No tree is named {name}.";
            return;
        }

        if (!ClusterAddresses.TryTree(name, ClusterTreeView.Reshard, out var address))
        {
            _reshardError = "That tree has no address the Explorer can open.";
            return;
        }

        _reshardOpen = false;
        Navigator.NavigateTo(address.WithTenant(Scope));
    }

    internal static LtStateRole StateOf(ClusterTreeEntry tree) =>
        tree.Lifecycle == TreeLifecycleState.Active ? LtStateRole.Enabled : LtStateRole.Disabled;

    internal static string StateText(ClusterTreeEntry tree) => tree.Lifecycle switch
    {
        TreeLifecycleState.Active when tree.IsAliased => "Live, aliased",
        TreeLifecycleState.Active => "Live",
        _ => "Deleted",
    };

    private static string Summary(ClusterTreeEntry tree) =>
        (tree.Name.Ownership is { } owners ? owners + ", " : string.Empty) + ClusterFormat.Plural(tree.ShardCount, "shard");

    /// <summary>
    /// The sentence that counts the list, in the words the Home status and the
    /// directory badge use, and says what it leaves out.
    /// </summary>
    /// <param name="count">The trees listed.</param>
    /// <param name="assertedTenant">The tenant the circuit asserts, or <see langword="null"/>.</param>
    /// <returns>The sentence.</returns>
    /// <param name="scope">The tenant a tenant-rooted address names, or <see langword="null"/> on the cluster-wide address.</param>
    internal static string CountLine(int count, string? assertedTenant, string? scope = null) =>
        (scope ?? ClusterTreeCatalog.NarrowingTenant(assertedTenant)) is { } tenant
            ? $"{ClusterFormat.Plural(count, "tree")} of tenant {tenant}. Other tenants' trees and system trees are not listed."
            : $"{ClusterFormat.Plural(count, "tree")}. System trees are not listed.";
}
