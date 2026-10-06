using System.Globalization;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// <c>/cluster/trees/{tree-path}/reshard</c>: the resumable status page of an
/// online reshard (E15), which grows or shrinks a tree's physical shard count.
/// It stages a reshard - target, review, typed confirmation - for a caller
/// holding the TreeLifecycle grant, explains a shrink's throughput trade-off
/// before it is submitted, and follows the cluster's status while one runs, so
/// leaving and returning resumes it.
/// </summary>
public partial class ClusterReshardPage : IDisposable
{
    /// <summary>The largest shard count a reshard accepts, whatever the tree's virtual slot count.</summary>
    internal const int MaximumShards = 4096;

    /// <summary>The smallest shard count a reshard accepts.</summary>
    internal const int MinimumShards = 2;

    private readonly ComponentLifetime _lifetime = new();
    private ClusterLoad<TreeReshardStatus> _status = ClusterLoad<TreeReshardStatus>.Loading;
    private LatticeTreeAdminCapabilities _access = default!;
    private ClusterStatusPoller? _poller;
    private string? _target;
    private string? _error;
    private ReshardPlan? _reviewing;
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

    [Inject]
    private ClusterTreeChanges TreeChanges { get; set; } = default!;

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
        var status = ClusterLoad<TreeReshardStatus>.RunAsync(ct => ReadAsync(admin => admin.GetReshardStatusAsync(TreeId, ct), ct), token);
        _access = (await access).Value ?? _access;
        Show(await status);
    }

    /// <summary>
    /// Reads a reshard status. A tree no reshard has given a map of its own routes
    /// over the default map, which the status reports as zero shards and zero
    /// slots (issue #4146); the live map then supplies the figures, so the page
    /// never says the tree has no shards. A failed map read keeps the status as read.
    /// </summary>
    /// <param name="read">The status read.</param>
    /// <param name="cancellationToken">Cancels the reads.</param>
    /// <returns>The status, with the live map's figures when it has none.</returns>
    private async Task<TreeReshardStatus> ReadAsync(Func<ILatticeTreeAdmin, Task<TreeReshardStatus>> read, CancellationToken cancellationToken)
    {
        var admin = Facades.RequireTreeAdmin();
        var status = await read(admin);
        if (status.VirtualShardCount > 0)
        {
            return status;
        }

        var map = await ClusterLoad<ShardMapInspection>.RunAsync(ct => admin.InspectShardMapAsync(TreeId, ct), cancellationToken);
        return map.Value is { } live
            ? status with { CurrentPhysicalShardCount = live.PhysicalShardCount, VirtualShardCount = live.VirtualShardCount, MapVersion = live.MapVersion }
            : status;
    }

    private void Show(ClusterLoad<TreeReshardStatus> status)
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

    private async Task<ClusterPollOutcome> RefreshAsync(CancellationToken cancellationToken)
    {
        var status = await ClusterLoad<TreeReshardStatus>.RunAsync(
            ct => ReadAsync(admin => admin.GetReshardStatusAsync(TreeId, ct), ct),
            cancellationToken);
        if (status.Value is not { } value)
        {
            return status.Denied ? ClusterPollOutcome.Settled : ClusterPollOutcome.Failed;
        }

        await InvokeAsync(() =>
        {
            if (!value.InProgress && _status.Value is { InProgress: true })
            {
                // The tree's shard count changed: every list of trees read before it is stale.
                TreeChanges.Changed();
                Toasts.Show($"Reshard complete: {ClusterFormat.Plural(value.CurrentPhysicalShardCount, "physical shard")}.", LtToastTone.Success);
            }

            _status = status;
            StateHasChanged();
        });
        return value.InProgress ? ClusterPollOutcome.Running : ClusterPollOutcome.Settled;
    }

    private void Review(TreeReshardStatus status)
    {
        _error = null;
        var current = status.CurrentPhysicalShardCount;
        var maximum = MaximumFor(status);
        if (!int.TryParse(_target?.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var target))
        {
            _error = "Enter a whole number of shards.";
        }
        else if (target < MinimumShards)
        {
            _error = $"A tree needs at least {MinimumShards} physical shards.";
        }
        else if (target > maximum)
        {
            _error = maximum < MaximumShards
                ? $"This tree can have at most {maximum} physical shards: one per virtual slot."
                : $"A tree can have at most {MaximumShards} physical shards.";
        }
        else if (target == current)
        {
            _error = $"The tree already has {ClusterFormat.Plural(current, "physical shard")}: enter a larger count to split shards or a smaller one to fold them together.";
        }
        else
        {
            _reviewing = new ReshardPlan(current, target);
        }
    }

    /// <summary>The largest target a tree accepts: its virtual slot count, and never more than <see cref="MaximumShards"/>.</summary>
    /// <param name="status">The reshard status.</param>
    /// <returns>The largest target.</returns>
    internal static int MaximumFor(TreeReshardStatus status) =>
        status.VirtualShardCount > 0 ? Math.Min(MaximumShards, status.VirtualShardCount) : MaximumShards;

    private static string Hint(TreeReshardStatus status) =>
        $"From {MinimumShards} to {MaximumFor(status)}. The tree has {status.CurrentPhysicalShardCount} now: a larger count splits shards, a smaller one folds adjacent shards together.";

    private async Task StartAsync()
    {
        if (_reviewing is not { } plan)
        {
            return;
        }

        _busy = true;
        var started = await ClusterLoad<TreeReshardStatus>.RunAsync(
            ct => ReadAsync(admin => admin.ReshardTreeAsync(TreeId, plan.Target, ct), ct),
            _lifetime.Token);
        _busy = false;

        if (started.Value is not null)
        {
            _reviewing = null;
            _target = null;
            TreeChanges.Changed();
            Toasts.Show("Reshard started.", LtToastTone.Info);
            Show(started);
        }
        else
        {
            Toasts.Show(started.Error!, LtToastTone.Danger);
        }
    }

    private static string StatusSentence(TreeReshardStatus status) =>
        !status.InProgress
            ? "No reshard is running."
            : (status.TargetShardCount ?? status.RequestedShardCount) is { } target
                ? $"Resharding to {ClusterFormat.Plural(target, "physical shard")}."
                : "A reshard is running.";

    /// <summary>A reshard under review: the count it starts from and the count it goes to.</summary>
    /// <param name="From">The physical shard count now.</param>
    /// <param name="Target">The physical shard count asked for.</param>
    private readonly record struct ReshardPlan(int From, int Target)
    {
        /// <summary>Whether the reshard folds shards together rather than splitting them.</summary>
        public bool Shrinks => Target < From;
    }
}
