using Grpc.Core;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Metrics;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>The Metrics tab: the per-tree measures of the retired Metrics plugin, drawn as booktabs figures.</summary>
public partial class DataMetricsPanel : IDisposable
{
    private const string Paused = "Paused";

    private readonly ComponentLifetime _lifetime = new();
    private DataStatusFollow? _follow;
    private string? _loadedFor;
    private TreeMetrics? _metrics;
    private IReadOnlyList<DataMeasure> _measures = [];
    private DateTimeOffset? _sampledAt;
    private bool _loading;
    private string? _error;
    private bool _withheld;

    [CascadingParameter]
    internal DataWorkspace? Workspace { get; set; }

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    [Inject]
    internal TimeProvider Time { get; set; } = default!;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Workspace is { } workspace && _loadedFor != workspace.Tree.StateId)
        {
            _loadedFor = workspace.Tree.StateId;
            await LoadAsync();
        }
    }

    /// <inheritdoc />
    protected override void OnInitialized() =>
        _follow = new DataStatusFollow(Time, InvokeAsync, FollowTickAsync);

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Leave();
        _follow?.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>Follows the measures while the views over the tree are behind, so the lag moves without a Refresh.</summary>
    private void UpdateFollow() =>
        _follow?.Update(!_lifetime.IsLeft && _metrics is { ViewLagTotal: > 0 });

    /// <summary>
    /// One follow tick: re-reads the measures and replaces them in place. A failed
    /// re-read keeps the last answer, and a Refresh that starts while the tick reads wins.
    /// </summary>
    private async Task FollowTickAsync()
    {
        var token = _lifetime.Token;
        if (_loading || token.IsCancellationRequested || Workspace is not { } workspace
            || DataServices.Find<IMetricsReader>(Services) is not { } reader)
        {
            return;
        }

        try
        {
            var metrics = await reader.GetAsync(workspace.Tree.StateId, token);
            if (token.IsCancellationRequested || _loading)
            {
                return;
            }

            if (metrics is not null)
            {
                _metrics = metrics;
                _measures = Measures(metrics);
                _sampledAt = Time.GetUtcNow();
                StateHasChanged();
            }
        }
        catch (Exception)
        {
            // A failed or cancelled re-read keeps the last answer; Refresh still reports a failure.
        }

        if (!token.IsCancellationRequested)
        {
            UpdateFollow();
        }
    }

    internal static IReadOnlyList<DataMeasure> Measures(TreeMetrics metrics)
    {
        ArgumentNullException.ThrowIfNull(metrics);
        var paused = metrics.DetailPaused;
        var measures = new List<DataMeasure>(8)
        {
            new("Lifecycle", metrics.Lifecycle switch
            {
                TreeLifecycleState.SoftDeleted => "Soft-deleted",
                TreeLifecycleState.Purging => "Purging",
                _ => "Active",
            }),
            new("Shards", DataFormat.Count(metrics.ShardCount)),
            new("Live keys", paused ? Paused : DataFormat.Count(metrics.LiveKeys)),
            new("Tombstones", paused ? Paused : DataFormat.Count(metrics.Tombstones)),
            new("Depth (min to max)", paused ? Paused : $"{metrics.MinDepth} to {metrics.MaxDepth}"),
            new("Shards splitting", paused ? Paused : DataFormat.Count(metrics.ShardsSplitting)),
        };

        if (metrics.ViewCount is { } views)
        {
            measures.Add(new DataMeasure("Views", DataFormat.Count(views)));
        }

        if (metrics.ViewLagTotal is { } lag)
        {
            measures.Add(new DataMeasure("View lag (entries, total)", DataFormat.Count(lag)));
        }

        return measures;
    }

    /// <summary>
    /// Whether the cluster refused a signed-in caller the grant, as opposed to
    /// refusing an anonymous one, which the shared sentence and sign-in describe.
    /// </summary>
    internal static bool IsGrantRefusal(Exception exception) => exception switch
    {
        LatticeAuthorizationDeniedException => true,
        LatticeStateApiException state => state.IsPermissionDenied,
        RpcException rpc => rpc.StatusCode == StatusCode.PermissionDenied,
        _ => false,
    };

    private async Task LoadAsync()
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        var token = _lifetime.Renew();
        _follow?.Stop();
        _metrics = null;
        _measures = [];
        _sampledAt = null;
        if (DataServices.Find<IMetricsReader>(Services) is not { } reader)
        {
            _error = "This Explorer has no state API to read metrics through.";
            return;
        }

        _loading = true;
        _error = null;
        _withheld = false;
        try
        {
            var metrics = await reader.GetAsync(workspace.Tree.StateId, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _metrics = metrics;
            _measures = _metrics is null ? [] : Measures(_metrics);
            _sampledAt = Time.GetUtcNow();
            UpdateFollow();
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                _metrics = null;
                if (IsGrantRefusal(exception))
                {
                    // The measures count every key in the tree, so the cluster
                    // withholds them from a grant that covers only part of it. That
                    // is an answer, not a failure: retrying cannot change it.
                    _withheld = true;
                }
                else
                {
                    _error = DataErrors.Describe(exception, "read this tree's metrics");
                }
            }
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _loading = false;
            }
        }
    }
}
