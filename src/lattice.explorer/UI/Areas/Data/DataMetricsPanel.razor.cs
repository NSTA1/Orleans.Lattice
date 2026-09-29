using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Metrics;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>The Metrics tab: the per-tree measures of the retired Metrics plugin, drawn as booktabs figures.</summary>
public partial class DataMetricsPanel : IDisposable
{
    private const string Paused = "Paused";

    private readonly CancellationTokenSource _lifetime = new();
    private string? _loadedFor;
    private TreeMetrics? _metrics;
    private IReadOnlyList<DataMeasure> _measures = [];
    private DateTimeOffset? _sampledAt;
    private bool _loading;
    private string? _error;

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
    public void Dispose()
    {
        _lifetime.Cancel();
        _lifetime.Dispose();
        GC.SuppressFinalize(this);
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

    private async Task LoadAsync()
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        if (DataServices.Find<IMetricsReader>(Services) is not { } reader)
        {
            _error = "This Explorer has no state API to read metrics through.";
            return;
        }

        _loading = true;
        _error = null;
        try
        {
            _metrics = await reader.GetAsync(workspace.Tree.StateId, _lifetime.Token);
            _measures = _metrics is null ? [] : Measures(_metrics);
            _sampledAt = Time.GetUtcNow();
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            _metrics = null;
            _error = DataErrors.Describe(exception, "read this tree's metrics");
        }
        finally
        {
            _loading = false;
        }
    }
}
