using System.Globalization;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// <c>/cluster/trees/{tree-path}/resize</c>: the resumable status page of an
/// online resize (E15). It stages a resize - capacity, review, typed
/// confirmation - and its undo for a caller holding the TreeLifecycle grant, and
/// follows the cluster's status while one runs, with its progress. Undo is
/// accept-then-poll: an accepted undo is followed until it has unwound.
/// </summary>
public partial class ClusterResizePage : IDisposable
{
    private readonly ComponentLifetime _lifetime = new();
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
        _lifetime.Leave();
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
        if (status.Value is { } value && IsActive(value))
        {
            Poller.Follow(RefreshAsync);
        }
        else
        {
            _poller?.Stop();
        }
    }

    /// <summary>
    /// Whether a resize or an accepted undo is still under way. The undo flag is
    /// read first: an undo of a finished resize unwinds with InProgress false.
    /// </summary>
    private static bool IsActive(TreeResizeStatus status) => status.UndoRequested || status.InProgress;

    private async Task<ClusterPollOutcome> RefreshAsync(CancellationToken cancellationToken)
    {
        var status = await ClusterLoad<TreeResizeStatus>.RunAsync(
            ct => Facades.RequireTreeAdmin().GetResizeStatusAsync(TreeId, ct),
            cancellationToken);
        if (status.Value is not { } value)
        {
            return status.Denied ? ClusterPollOutcome.Settled : ClusterPollOutcome.Failed;
        }

        await InvokeAsync(() =>
        {
            var previous = _status.Value;
            if (previous is { UndoRequested: true } && !value.UndoRequested)
            {
                if (value.InProgress)
                {
                    Toasts.Show("The undo could not be applied, so the resize carries on. Try the undo again, or let the resize finish.", LtToastTone.Warning);
                }
                else
                {
                    Toasts.Show("Resize undone.", LtToastTone.Success);
                }
            }
            else if (previous is { InProgress: true, UndoRequested: false } && !IsActive(value))
            {
                Toasts.Show("Resize complete.", LtToastTone.Success);
            }

            _status = ClusterLoad<TreeResizeStatus>.Loaded(KeepRequested(previous, value));
            StateHasChanged();
        });
        return IsActive(value) ? ClusterPollOutcome.Running : ClusterPollOutcome.Settled;
    }

    /// <summary>
    /// A standalone status read does not echo the target a trigger asked for, so
    /// a running resize keeps the target this page last saw rather than losing it.
    /// </summary>
    private static TreeResizeStatus KeepRequested(TreeResizeStatus? previous, TreeResizeStatus current) =>
        current.InProgress && current.RequestedMaxLeafKeys is null && previous?.RequestedMaxLeafKeys is not null
            ? current with { RequestedMaxLeafKeys = previous.RequestedMaxLeafKeys, RequestedMaxInternalChildren = previous.RequestedMaxInternalChildren }
            : current;

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
        var previous = _status.Value;
        var undone = await ClusterLoad<TreeResizeStatus>.RunAsync(
            ct => Facades.RequireTreeAdmin().UndoTreeResizeAsync(TreeId, ct),
            _lifetime.Token);
        _busy = false;

        if (undone.Value is { } value)
        {
            // Undo is accept-then-poll: the call returns once the undo is
            // persisted, and UndoRequested says whether it is still unwinding.
            Toasts.Show(
                value.UndoRequested ? "Undo accepted. It is unwinding the resize; this page follows it." : "Resize undone.",
                value.UndoRequested ? LtToastTone.Info : LtToastTone.Success);
            Show(ClusterLoad<TreeResizeStatus>.Loaded(KeepRequested(previous, value)));
        }
        else
        {
            Toasts.Show(undone.Error!, LtToastTone.Danger);
        }
    }

    private static bool Parse(string? text, int minimum, out int value) =>
        int.TryParse(text?.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out value) && value >= minimum;

    private static (LtStateRole State, string Text) Stage(TreeResizeStatus status) => status switch
    {
        { UndoRequested: true } => (LtStateRole.Lagging, "Undoing"),
        { InProgress: true } => (LtStateRole.Lagging, "Running"),
        _ => (LtStateRole.Healthy, "Idle"),
    };

    private static string StatusSentence(TreeResizeStatus status) => status switch
    {
        { UndoRequested: true } => "An undo was accepted and is unwinding the resize. The tree returns to its old size when it finishes.",
        { InProgress: true, RequestedMaxLeafKeys: { } leaf } =>
            $"Resizing to {ClusterFormat.Count(leaf)} keys per leaf and {ClusterFormat.Count(status.RequestedMaxInternalChildren ?? status.CurrentMaxInternalChildren)} children per node.",
        { InProgress: true } => "A resize is running. The new size takes effect when the copy is swapped in.",
        _ => "No resize is running.",
    };
}
