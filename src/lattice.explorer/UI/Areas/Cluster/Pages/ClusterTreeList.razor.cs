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
/// owns the visible control of the "Reshard tree..." palette command.
/// </summary>
public partial class ClusterTreeList : IDisposable
{
    private readonly ComponentLifetime _lifetime = new();
    private ClusterLoad<IReadOnlyList<ClusterTreeEntry>> _trees = ClusterLoad<IReadOnlyList<ClusterTreeEntry>>.Loading;
    private string? _filter;
    private bool _reshardOpen;
    private string? _reshardTree;
    private string? _reshardError;

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
        if (Signals.TryTake(ClusterArea.ReshardCommandId))
        {
            OpenReshard();
        }

        await LoadAsync(refresh: false);
    }

    private async Task LoadAsync(bool refresh)
    {
        _trees = ClusterLoad<IReadOnlyList<ClusterTreeEntry>>.Loading;
        _trees = await ClusterLoad<IReadOnlyList<ClusterTreeEntry>>.RunAsync(
            async ct => await Catalog.GetAsync(refresh, ct),
            _lifetime.Token);
    }

    private IReadOnlyList<ClusterTreeEntry> Filtered(IReadOnlyList<ClusterTreeEntry> trees) =>
        string.IsNullOrWhiteSpace(_filter)
            ? trees
            : [.. trees.Where(tree => tree.TreeId.Contains(_filter.Trim(), StringComparison.OrdinalIgnoreCase))];

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
        Navigator.NavigateTo(address);
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
    internal static string CountLine(int count, string? assertedTenant) =>
        ClusterTreeCatalog.NarrowingTenant(assertedTenant) is { } tenant
            ? $"{ClusterFormat.Plural(count, "tree")} of tenant {tenant}. Other tenants' trees and system trees are not listed."
            : $"{ClusterFormat.Plural(count, "tree")}. System trees are not listed.";
}
