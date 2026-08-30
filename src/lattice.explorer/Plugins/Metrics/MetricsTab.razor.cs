using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.DesignSystem.Layout;

namespace Orleans.Lattice.Explorer.Plugins.Metrics;

/// <summary>
/// The live-metrics surface's state and load path. The markup lives beside this
/// file in <c>MetricsTab.razor</c>; the two are one partial class, so the view
/// keeps its rendering and its behaviour in separate, readable files.
/// </summary>
public partial class MetricsTab
{
    private const string PausedValue = "Paused";

    // Composed once for the type rather than per render: the column set never
    // varies, so rebuilding it on each pass would allocate a list, four column
    // descriptors and four render fragments for every re-render.
    private static readonly LatticeTableColumn<ShardHotness>[] HotnessColumns =
    [
        new()
        {
            Header = "Shard",
            IsPrimary = true,
            IsNumericOrCode = true,
            Cell = static shard => builder => builder.AddContent(0, shard.ShardIndex),
        },
        new()
        {
            Header = "Ops/sec",
            IsNumericOrCode = true,
            Cell = static shard => builder => builder.AddContent(0, shard.OpsPerSecond.ToString("N1")),
        },
        new()
        {
            Header = "Live keys",
            IsNumericOrCode = true,
            Cell = static shard => builder => builder.AddContent(0, shard.LiveKeys.ToString("N0")),
        },
        new()
        {
            Header = "Splitting",
            IsNumericOrCode = true,
            Cell = static shard => builder => builder.AddContent(0, shard.SplitInProgress ? "yes" : "-"),
        },
    ];

    // A reference key per row, so the adaptive table's @key does not box the
    // shard index on every row of every render.
    private static readonly Func<ShardHotness, object> HotnessKeySelector =
        static shard => shard.ShardIndex.ToString();

    private readonly Func<ShardHotness, object> _hotnessKey = HotnessKeySelector;

    private EventCallback<string> _goToSourceTree;

    private TreeMetrics? _metrics;
    private DateTimeOffset _sampledAt;
    private bool _loading;
    private string? _error;

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        // Bound once rather than per render, so the guidance card's callback does
        // not allocate a delegate on every pass.
        _goToSourceTree = EventCallback.Factory.Create<string>(this, GoToSourceTree);

        // A change-history view has no metrics of its own; skip the load and
        // render the source-table guidance instead of a bare empty state.
        if (Selection.IsHistory)
        {
            return;
        }

        await LoadAsync();
    }

    private void GoToSourceTree(string sourceTreeId) => Surface.GoToTree(sourceTreeId);

    private async Task LoadAsync()
    {
        _loading = true;
        _error = null;
        StateHasChanged();

        try
        {
            _metrics = await Surface.GetAsync(Selection.Id, TabToken);
            _sampledAt = DateTimeOffset.UtcNow;
        }
        catch (OperationCanceledException) when (TabToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            _error = ex.Message;
        }
        finally
        {
            _loading = false;
            StateHasChanged();
        }
    }
}
