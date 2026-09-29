using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// A tree's lifecycle tab: its deletion status, and delete, recover, purge and
/// alias - each behind a typed confirmation that states its consequence, and each
/// hidden unless the capability probe grants it (delete, recover and purge need
/// the TreeLifecycle grant; alias needs whole-tree admin authority).
/// </summary>
public partial class ClusterTreeLifecycle : IDisposable
{
    /// <summary>What the page says about purge on every tree.</summary>
    internal const string PurgeRule =
        "Purge requires the TreeLifecycle grant and is not an app operation: no app role can purge a tree, even one its app owns.";

    private readonly CancellationTokenSource _lifetime = new();
    private ClusterLoad<TreeDeletionStatus> _status = ClusterLoad<TreeDeletionStatus>.Loading;
    private Verb _confirm;
    private bool _busy;
    private string? _aliasTarget;
    private string? _aliasError;

    private enum Verb
    {
        None,
        Delete,
        Recover,
        Purge,
        Alias,
    }

    /// <summary>The logical tree id.</summary>
    [Parameter, EditorRequired]
    public string TreeId { get; set; } = string.Empty;

    /// <summary>What the caller may do to the tree.</summary>
    [Parameter, EditorRequired]
    public LatticeTreeAdminCapabilities Capabilities { get; set; } = default!;

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

    [Inject]
    private ClusterTreeCatalog Catalog { get; set; } = default!;

    [Inject]
    private LtToastService Toasts { get; set; } = default!;

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Cancel();
        _lifetime.Dispose();
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync() =>
        _status = await ClusterLoad<TreeDeletionStatus>.RunAsync(
            ct => Facades.RequireTreeAdmin().GetTreeDeletionStatusAsync(TreeId, ct),
            _lifetime.Token);

    private void Close(bool open)
    {
        if (!open)
        {
            _confirm = Verb.None;
        }
    }

    private void ReviewAlias()
    {
        var target = _aliasTarget?.Trim();
        _aliasError = string.IsNullOrEmpty(target)
            ? "Name the tree this name should reach."
            : string.Equals(target, TreeId, StringComparison.Ordinal)
                ? "A tree cannot alias itself."
                : null;
        if (_aliasError is null)
        {
            _aliasTarget = target;
            _confirm = Verb.Alias;
        }
    }

    private Task DeleteAsync() =>
        RunAsync(ct => Facades.RequireTreeAdmin().DeleteTreeAsync(TreeId, ct), "Tree deleted. It can be recovered until its window closes.");

    private Task RecoverAsync() =>
        RunAsync(ct => Facades.RequireTreeAdmin().RecoverTreeAsync(TreeId, ct), "Tree recovered.");

    private Task PurgeAsync() =>
        RunAsync(ct => Facades.RequireTreeAdmin().PurgeTreeAsync(TreeId, confirm: true, ct), "Tree purged.");

    private async Task SetAliasAsync()
    {
        _confirm = Verb.None;
        _busy = true;
        var target = _aliasTarget!;
        var result = await ClusterLoad<TreeAliasResolution>.RunAsync(
            ct => Facades.RequireTreeAdmin().SetTreeAliasAsync(TreeId, target, ct),
            _lifetime.Token);
        _busy = false;

        if (result.Value is not null)
        {
            Catalog.Invalidate();
            Toasts.Show("Alias set.", LtToastTone.Success);
        }
        else
        {
            Toasts.Show(result.Error!, LtToastTone.Danger);
        }
    }

    private async Task RunAsync(Func<CancellationToken, Task<TreeDeletionStatus>> verb, string done)
    {
        _confirm = Verb.None;
        _busy = true;
        var result = await ClusterLoad<TreeDeletionStatus>.RunAsync(verb, _lifetime.Token);
        _busy = false;

        if (result.Value is not null)
        {
            _status = result;
            Catalog.Invalidate();
            Toasts.Show(done, LtToastTone.Success);
        }
        else
        {
            Toasts.Show(result.Error!, LtToastTone.Danger);
        }
    }

    private static string StateText(TreeDeletionStatus status) => status switch
    {
        { PurgeComplete: true } => "Purged",
        { PurgeInProgress: true } => "Purging",
        { IsDeleted: true, CanRecover: true } => "Deleted, recoverable",
        { IsDeleted: true } => "Deleted",
        _ => "Live",
    };
}
