using System.Globalization;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Shell.Design.Components;

namespace Orleans.Lattice.Explorer.Shell.Areas.Cluster.Pages;

/// <summary>
/// <c>/cluster/trees/{tree-path}/snapshot</c>: the resumable status page of a
/// point-in-time snapshot into a new tree (E15). It stages a capture -
/// destination, mode and sizing, review, typed confirmation - for a caller with
/// whole-tree admin authority, and follows it while it runs.
/// </summary>
public partial class ClusterSnapshotPage : IDisposable
{
    private static readonly IReadOnlyList<LtSelectOption> Modes =
    [
        new(nameof(TreeSnapshotMode.Online), "Online"),
        new(nameof(TreeSnapshotMode.Offline), "Offline"),
    ];

    private readonly CancellationTokenSource _lifetime = new();
    private ClusterLoad<TreeSnapshotStatus> _status = ClusterLoad<TreeSnapshotStatus>.Loading;
    private LatticeTreeAdminCapabilities _access = default!;
    private ClusterStatusPoller? _poller;
    private string? _destination;
    private string? _destinationError;
    private string? _mode = nameof(TreeSnapshotMode.Online);
    private string? _leaf;
    private string? _internal;
    private string? _sizingError;
    private bool _reviewing;
    private bool _confirm;
    private bool _busy;

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

    private bool IsOffline => string.Equals(_mode, nameof(TreeSnapshotMode.Offline), StringComparison.Ordinal);

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
        var status = ClusterLoad<TreeSnapshotStatus>.RunAsync(ct => Facades.RequireTreeAdmin().GetSnapshotStatusAsync(TreeId, ct), token);
        _access = (await access).Value ?? _access;
        Show(await status);
    }

    private void Show(ClusterLoad<TreeSnapshotStatus> status)
    {
        _status = status;
        if (status.Value is { InProgress: true })
        {
            Poller.Follow(RefreshAsync);
        }
    }

    private async Task<bool> RefreshAsync(CancellationToken cancellationToken)
    {
        var status = await ClusterLoad<TreeSnapshotStatus>.RunAsync(
            ct => Facades.RequireTreeAdmin().GetSnapshotStatusAsync(TreeId, ct),
            cancellationToken);
        var running = status.Value?.InProgress ?? !status.Denied;
        await InvokeAsync(() =>
        {
            if (status.Value is { } value)
            {
                if (!value.InProgress && _status.Value is { InProgress: true })
                {
                    Toasts.Show("Snapshot complete.", LtToastTone.Success);
                }

                _status = status;
            }

            StateHasChanged();
        });
        return running;
    }

    private void Review()
    {
        var destination = _destination?.Trim();
        _destinationError = string.IsNullOrEmpty(destination)
            ? "Name the new tree to copy into."
            : string.Equals(destination, TreeId, StringComparison.Ordinal)
                ? "A snapshot cannot copy a tree into itself."
                : null;
        _sizingError = TryOptional(_leaf, 2, out _) && TryOptional(_internal, 3, out _)
            ? null
            : "Sizing must be whole numbers: at least 2 keys per leaf and 3 children per node.";

        if (_destinationError is null && _sizingError is null)
        {
            _destination = destination;
            _reviewing = true;
        }
    }

    private async Task StartAsync()
    {
        TryOptional(_leaf, 2, out var leaf);
        TryOptional(_internal, 3, out var children);
        var mode = IsOffline ? TreeSnapshotMode.Offline : TreeSnapshotMode.Online;
        var destination = _destination!;

        _busy = true;
        var started = await ClusterLoad<TreeSnapshotStatus>.RunAsync(
            ct => Facades.RequireTreeAdmin().SnapshotTreeAsync(TreeId, destination, mode, leaf, children, ct),
            _lifetime.Token);
        _busy = false;

        if (started.Value is not null)
        {
            _reviewing = false;
            Toasts.Show("Snapshot started.", LtToastTone.Info);
            Show(started);
        }
        else
        {
            Toasts.Show(started.Error!, LtToastTone.Danger);
        }
    }

    private static bool TryOptional(string? text, int minimum, out int? value)
    {
        value = null;
        if (string.IsNullOrWhiteSpace(text))
        {
            return true;
        }

        if (int.TryParse(text.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) && parsed >= minimum)
        {
            value = parsed;
            return true;
        }

        return false;
    }

    private static string StatusSentence(TreeSnapshotStatus status) =>
        status.InProgress
            ? $"Copying into {status.RequestedDestinationTreeId}, {(status.RequestedMode == TreeSnapshotMode.Offline ? "offline" : "online")}."
            : "No snapshot is running.";
}
