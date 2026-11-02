using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The tree's lifecycle on its Data page (issue #5477): Delete tree... and, once deleted,
/// Recover tree..., for a caller the tree-administration probe says may manage the tree's
/// lifecycle. A data steward holds that grant on the trees she creates without holding the
/// cluster-wide telemetry the Cluster area needs, so without this the Data area offered her
/// no way to delete a tree she may delete. The facade authorizes every call; the gate only
/// decides what is drawn, and fails closed.
/// </summary>
public partial class DataTreePage
{
    /// <summary>The catalogue's lifecycle text for a soft-deleted tree.</summary>
    internal const string SoftDeletedLifecycle = "SoftDeleted";

    private bool _canManageLifecycle;
    private bool _lifecycleBusy;
    private LifecycleVerb _lifecycleConfirm;
    private TreeDeletionStatus? _deletion;

    private enum LifecycleVerb
    {
        None,
        Delete,
        Recover,
    }

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    private bool IsDeleted => _deletion is { IsDeleted: true };

    /// <summary>What the page says about a deleted tree: whether, and until when, it can be recovered.</summary>
    /// <param name="status">The tree's deletion status.</param>
    internal static string DeletedText(TreeDeletionStatus status) => status switch
    {
        { CanRecover: true, RecoveryDeadlineUtc: { } deadline } =>
            $"Every read and write on it fails. It can be recovered until {DataFormat.Time(deadline)}, when it is purged.",
        { CanRecover: true } => "Every read and write on it fails. It can be recovered until the cluster purges it.",
        _ => "Every read and write on it fails, and it can no longer be recovered.",
    };

    private void ResetLifecycle()
    {
        _canManageLifecycle = false;
        _lifecycleBusy = false;
        _lifecycleConfirm = LifecycleVerb.None;
        _deletion = null;
    }

    /// <summary>
    /// Draws the lifecycle control once the tree-administration probe says the caller may
    /// manage <paramref name="tree"/>'s lifecycle. A view, a shared tree, no facade, a refusal
    /// or a fault all draw nothing.
    /// </summary>
    private async Task LoadLifecycleGateAsync(DataTreeEntry tree)
    {
        if (tree.Kind != DataTreeKind.Tree || tree.IsShared)
        {
            return;
        }

        LatticeTreeAdminCapabilities? capabilities;
        try
        {
            if (Services.GetShellFacade<ILatticeTreeAdmin>() is not { } admin)
            {
                return;
            }

            capabilities = await admin.ProbeCapabilitiesAsync(tree.StateId, _lifetime.Token);
        }
        catch (Exception)
        {
            return;
        }

        if (capabilities?.CanManageTreeLifecycle != true || !ReferenceEquals(tree, _tree))
        {
            return;
        }

        _canManageLifecycle = true;
        if (string.Equals(tree.Lifecycle, SoftDeletedLifecycle, StringComparison.Ordinal))
        {
            // The catalogue lists a soft-deleted tree until it is purged: opened again, it
            // shows its deletion and the way back rather than tabs whose reads all fail.
            try
            {
                var status = await Services.GetShellFacade<ILatticeTreeAdmin>()!.GetTreeDeletionStatusAsync(tree.LogicalId, _lifetime.Token);
                if (ReferenceEquals(tree, _tree))
                {
                    _deletion = status;
                }
            }
            catch (Exception)
            {
                // The status is a courtesy; the tabs report a failed read in their own words.
            }
        }

        await InvokeAsync(StateHasChanged);
    }

    private void AskDelete() => _lifecycleConfirm = LifecycleVerb.Delete;

    private void AskRecover() => _lifecycleConfirm = LifecycleVerb.Recover;

    private void CloseLifecycleConfirm(bool open)
    {
        if (!open)
        {
            _lifecycleConfirm = LifecycleVerb.None;
        }
    }

    private Task DeleteTreeAsync() =>
        RunLifecycleAsync(
            (admin, treeId, token) => admin.DeleteTreeAsync(treeId, token),
            "delete this tree",
            tree => $"Tree {tree} deleted. It can be recovered until its window closes.");

    private Task RecoverTreeAsync() =>
        RunLifecycleAsync(
            (admin, treeId, token) => admin.RecoverTreeAsync(treeId, token),
            "recover this tree",
            tree => $"Tree {tree} recovered.");

    private async Task RunLifecycleAsync(
        Func<ILatticeTreeAdmin, string, CancellationToken, Task<TreeDeletionStatus>> verb,
        string action,
        Func<string, string> done)
    {
        _lifecycleConfirm = LifecycleVerb.None;
        if (_tree is not { } tree || _lifecycleBusy)
        {
            return;
        }

        var token = _lifetime.Token;
        _lifecycleBusy = true;
        try
        {
            var admin = Services.GetShellFacade<ILatticeTreeAdmin>()
                ?? throw new InvalidOperationException("This Explorer has no tree-administration facade.");

            // The logical id, as the Cluster area's lifecycle tab passes it: on an aliased tree
            // the facade deletes (and recovers) the live copy the alias targets.
            var status = await verb(admin, tree.LogicalId, token);
            if (token.IsCancellationRequested || !ReferenceEquals(tree, _tree))
            {
                return;
            }

            _deletion = status;
            Services.GetService<ClusterTreeChanges>()?.Changed();
            Toasts.Show(done(tree.DisplayName), LtToastTone.Success);
            if (!status.IsDeleted)
            {
                // Recovered: the tree serves again, so its tabs are read afresh.
                _liveKeys = null;
                _ = LoadKeyCountAsync(tree);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                Toasts.Show(DataErrors.Describe(exception, action), LtToastTone.Danger);
            }
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _lifecycleBusy = false;
            }
        }
    }
}
