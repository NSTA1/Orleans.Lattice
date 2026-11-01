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
/// and "Reshard tree..." only for one who may manage some tree's lifecycle. Either
/// command is refused, with a sentence, for a caller it does not admit.
/// </summary>
public partial class ClusterTreeList : IDisposable
{
    /// <summary>What a caller the create-tree probe refuses is told when the palette asks for a new tree.</summary>
    internal const string CreateRefusedText = "Your identity holds no grant to create trees on this cluster.";

    /// <summary>What a caller who may reshard no tree is told when the palette asks to reshard one.</summary>
    internal const string ReshardRefusedText = "Your identity holds no grant to reshard a tree on this cluster.";

    /// <summary>The most listed trees probed for a reshard grant when the area-wide probe grants none.</summary>
    internal const int MaximumReshardProbes = 64;

    /// <summary>How many listed trees are probed for a reshard grant at once.</summary>
    internal const int ReshardProbeConcurrency = 8;

    private readonly ComponentLifetime _lifetime = new();
    private ClusterLoad<IReadOnlyList<ClusterTreeEntry>> _trees = ClusterLoad<IReadOnlyList<ClusterTreeEntry>>.Loading;
    private string? _filter;
    private bool _reshardOpen;
    private string? _reshardTree;
    private string? _reshardError;
    private Task<bool>? _canCreateProbe;
    private bool _canCreate;
    private Task<bool>? _canReshardProbe;
    private bool _canReshard;
    private string? _reshardRefusal;
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
        _canReshardProbe = ProbeReshardAsync();
        if (Signals.TryTake(ClusterArea.ReshardCommandId))
        {
            _ = InvokeAsync(RequestReshardAsync);
        }

        if (Signals.TryTake(ClusterArea.CreateTreeCommandId))
        {
            _ = InvokeAsync(RequestCreateAsync);
        }

        await LoadAsync(refresh: false);
        await _canCreateProbe;
        await _canReshardProbe;
    }

    // Fails closed like the create probe: no facade, a refusal or a fault leaves
    // Reshard tree... hidden. A caller who may manage some tree's lifecycle - the
    // grant a reshard needs - sees it; the reshard page still gates the tree chosen.
    private async Task<bool> ProbeReshardAsync()
    {
        bool answer;
        try
        {
            answer = await ManagesAnyTreeLifecycleAsync(_lifetime.Token);
        }
        catch (OperationCanceledException)
        {
            return false;
        }

        if (!_lifetime.IsLeft)
        {
            _canReshard = answer;
        }

        return answer;
    }

    private async Task<bool> ManagesAnyTreeLifecycleAsync(CancellationToken cancellationToken)
    {
        if (Facades.TreeAdmin is not { } admin || !Facades.IsConnected)
        {
            return false;
        }

        if (await ManagesLifecycleAsync(admin, ClusterArea.ProbeTreeId, cancellationToken))
        {
            return true;
        }

        string[] trees;
        try
        {
            trees = [.. ClusterTreeCatalog.InScope(await Catalog.GetAsync(refresh: false, cancellationToken), Scope)
                .Where(tree => tree.Lifecycle == TreeLifecycleState.Active)
                .Select(tree => tree.TreeId)
                .Take(MaximumReshardProbes)];
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return false;
        }

        for (var start = 0; start < trees.Length; start += ReshardProbeConcurrency)
        {
            var wave = new Task<bool>[Math.Min(ReshardProbeConcurrency, trees.Length - start)];
            for (var i = 0; i < wave.Length; i++)
            {
                wave[i] = ManagesLifecycleAsync(admin, trees[start + i], cancellationToken);
            }

            if ((await Task.WhenAll(wave)).Contains(true))
            {
                return true;
            }
        }

        return false;
    }

    private static async Task<bool> ManagesLifecycleAsync(Orleans.Lattice.Api.TreeAdmin.ILatticeTreeAdmin admin, string treeId, CancellationToken cancellationToken)
    {
        try
        {
            var capabilities = await admin.ProbeCapabilitiesAsync(treeId, cancellationToken);
            return capabilities?.CanManageTreeLifecycle == true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return false;
        }
    }

    // The palette can ask before the probe has answered, so the request waits for it;
    // a caller who may reshard nothing is told so rather than shown a picker.
    private async Task RequestReshardAsync()
    {
        var admitted = _canReshardProbe is { } probe && await probe;
        if (_lifetime.IsLeft)
        {
            return;
        }

        if (admitted)
        {
            OpenReshard();
        }
        else
        {
            _reshardRefusal = ReshardRefusedText;
        }

        StateHasChanged();
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
            _ = InvokeAsync(RequestReshardAsync);
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
        _reshardRefusal = null;
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
