using System.Globalization;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Shell.Design.Components;

namespace Orleans.Lattice.Explorer.Shell.Areas.Cluster.Pages;

/// <summary>
/// <c>/cluster/trees/{tree-path}/resize</c>: the resumable status page of an
/// online resize (E15). It stages a resize - capacity, review, typed
/// confirmation - and its undo for a caller holding the TreeLifecycle grant, and
/// follows the cluster's status while one runs.
/// </summary>
public partial class ClusterResizePage : IDisposable
{
    private readonly CancellationTokenSource _lifetime = new();
    private ClusterLoad<TreeResizeStatus> _status = ClusterLoad<TreeResizeStatus>.Loading;
    private LatticeTreeAdminCapabilities _access = default!;
    private ClusterStatusPoller? _poller;
    private string? _leaf;
    private string? _internal;
    private string? _leafError;
    private string? _internalError;
    private (int Leaf, int Internal)? _reviewing;
    private Verb _confirm;
    private bool _busy;

    private enum Verb
    {
        None,
        Resize,
        Undo,
    }

    /// <summary>The logical tree id.</summary>
    [Parameter, EditorRequired]
    public string TreeId { get; set; } = string.Empty;

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

    [Inject]
    private TimeProvider Time { get; set; } = default!;

    [Inject]
    private LtToastService Toasts { get; set; } = default!;

    private ClusterStatusPoller Poller => _poller ??= new ClusterStatusPoller(Time);

    /// <inheritdoc />
    public void Dispose()
    {
        _poller?.Dispose();
        _lifetime.Cancel();
        _lifetime.Dispose();
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        _access = ClusterTreeAccess.None(TreeId);
        var token = _lifetime.Token;
        var access = ClusterLoad<LatticeTreeAdminCapabilities>.RunAsync(ct => ClusterTreeAccess.ProbeAsync(Facades.RequireTreeAdmin(), TreeId, ct), token);
        var status = ClusterLoad<TreeResizeStatus>.RunAsync(ct => Facades.RequireTreeAdmin().GetResizeStatusAsync(TreeId, ct), token);
        _access = (await access).Value ?? _access;
        Show(await status);
    }

    private void Show(ClusterLoad<TreeResizeStatus> status)
    {
        _status = status;
        if (status.Value is { InProgress: true })
        {
            Poller.Follow(RefreshAsync);
        }
        else
        {
            _poller?.Stop();
        }
    }

    private async Task<bool> RefreshAsync(CancellationToken cancellationToken)
    {
        var status = await ClusterLoad<TreeResizeStatus>.RunAsync(
            ct => Facades.RequireTreeAdmin().GetResizeStatusAsync(TreeId, ct),
            cancellationToken);
        var running = status.Value?.InProgress ?? !status.Denied;
        await InvokeAsync(() =>
        {
            if (status.Value is { } value)
            {
                if (!value.InProgress && _status.Value is { InProgress: true })
                {
                    Toasts.Show("Resize complete.", LtToastTone.Success);
                }

                _status = status;
            }

            StateHasChanged();
        });
        return running;
    }

    private void Close(bool open)
    {
        if (!open)
        {
            _confirm = Verb.None;
        }
    }

    private void Review()
    {
        _leafError = Parse(_leaf, 2, out var leaf) ? null : "Enter a whole number of keys, at least 2.";
        _internalError = Parse(_internal, 3, out var children) ? null : "Enter a whole number of children, at least 3.";
        if (_leafError is null && _internalError is null)
        {
            _reviewing = (leaf, children);
        }
    }

    private async Task StartAsync()
    {
        _confirm = Verb.None;
        if (_reviewing is not { } target)
        {
            return;
        }

        _busy = true;
        var started = await ClusterLoad<TreeResizeStatus>.RunAsync(
            ct => Facades.RequireTreeAdmin().ResizeTreeAsync(TreeId, target.Leaf, target.Internal, ct),
            _lifetime.Token);
        _busy = false;

        if (started.Value is not null)
        {
            _reviewing = null;
            Toasts.Show("Resize started.", LtToastTone.Info);
            Show(started);
        }
        else
        {
            Toasts.Show(started.Error!, LtToastTone.Danger);
        }
    }

    private async Task UndoAsync()
    {
        _confirm = Verb.None;
        _busy = true;
        var undone = await ClusterLoad<TreeResizeStatus>.RunAsync(
            ct => Facades.RequireTreeAdmin().UndoTreeResizeAsync(TreeId, ct),
            _lifetime.Token);
        _busy = false;

        if (undone.Value is not null)
        {
            Toasts.Show("Resize undone.", LtToastTone.Success);
            Show(undone);
        }
        else
        {
            Toasts.Show(undone.Error!, LtToastTone.Danger);
        }
    }

    private static bool Parse(string? text, int minimum, out int value) =>
        int.TryParse(text?.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out value) && value >= minimum;

    private static string StatusSentence(TreeResizeStatus status) =>
        status.InProgress
            ? $"Resizing to {ClusterFormat.Count(status.RequestedMaxLeafKeys ?? status.CurrentMaxLeafKeys)} keys per leaf and {ClusterFormat.Count(status.RequestedMaxInternalChildren ?? status.CurrentMaxInternalChildren)} children per node."
            : "No resize is running.";
}
