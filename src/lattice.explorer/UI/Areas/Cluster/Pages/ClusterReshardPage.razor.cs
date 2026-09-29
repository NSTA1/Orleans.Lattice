using System.Globalization;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// <c>/cluster/trees/{tree-path}/reshard</c>: the resumable status page of an
/// online reshard (E15). It stages a reshard - target, review, typed
/// confirmation - for a caller holding the TreeLifecycle grant, and follows the
/// cluster's status while one runs, so leaving and returning resumes it.
/// </summary>
public partial class ClusterReshardPage : IDisposable
{
    /// <summary>The largest shard count a reshard accepts.</summary>
    internal const int MaximumShards = 4096;

    private readonly CancellationTokenSource _lifetime = new();
    private ClusterLoad<TreeReshardStatus> _status = ClusterLoad<TreeReshardStatus>.Loading;
    private LatticeTreeAdminCapabilities _access = default!;
    private ClusterStatusPoller? _poller;
    private string? _target;
    private string? _error;
    private int? _reviewing;
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
        var status = ClusterLoad<TreeReshardStatus>.RunAsync(ct => Facades.RequireTreeAdmin().GetReshardStatusAsync(TreeId, ct), token);
        _access = (await access).Value ?? _access;
        Show(await status);
    }

    private void Show(ClusterLoad<TreeReshardStatus> status)
    {
        _status = status;
        if (status.Value is { InProgress: true })
        {
            Poller.Follow(RefreshAsync);
        }
    }

    private async Task<bool> RefreshAsync(CancellationToken cancellationToken)
    {
        var status = await ClusterLoad<TreeReshardStatus>.RunAsync(
            ct => Facades.RequireTreeAdmin().GetReshardStatusAsync(TreeId, ct),
            cancellationToken);
        var running = status.Value?.InProgress ?? !status.Denied;
        await InvokeAsync(() =>
        {
            if (status.Value is { } value)
            {
                if (!value.InProgress && _status.Value is { InProgress: true })
                {
                    Toasts.Show($"Reshard complete: {ClusterFormat.Plural(value.CurrentPhysicalShardCount, "physical shard")}.", LtToastTone.Success);
                }

                _status = status;
            }

            StateHasChanged();
        });
        return running;
    }

    private void Review(TreeReshardStatus status)
    {
        _error = null;
        if (!int.TryParse(_target?.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var target))
        {
            _error = "Enter a whole number of shards.";
        }
        else if (target <= status.CurrentPhysicalShardCount)
        {
            _error = $"Resharding only grows: enter more than {status.CurrentPhysicalShardCount}.";
        }
        else if (target > MaximumShards)
        {
            _error = $"A tree can have at most {MaximumShards} physical shards.";
        }
        else
        {
            _reviewing = target;
        }
    }

    private async Task StartAsync()
    {
        if (_reviewing is not { } target)
        {
            return;
        }

        _busy = true;
        var started = await ClusterLoad<TreeReshardStatus>.RunAsync(
            ct => Facades.RequireTreeAdmin().ReshardTreeAsync(TreeId, target, ct),
            _lifetime.Token);
        _busy = false;

        if (started.Value is not null)
        {
            _reviewing = null;
            _target = null;
            Toasts.Show("Reshard started.", LtToastTone.Info);
            Show(started);
        }
        else
        {
            Toasts.Show(started.Error!, LtToastTone.Danger);
        }
    }

    private static string StatusSentence(TreeReshardStatus status) =>
        status.InProgress
            ? $"Resharding to {ClusterFormat.Plural(status.RequestedShardCount ?? status.CurrentPhysicalShardCount, "physical shard")}."
            : "No reshard is running.";
}
