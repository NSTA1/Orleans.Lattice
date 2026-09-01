using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Vocabulary;
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

    /// <summary>
    /// What the surface says when the cluster answered but reported no metrics
    /// for the selection. Built once. It says explicitly that nothing is being
    /// withheld, because "No metrics reported for this id." read equally well as
    /// a missing grant, which is the confusion this epic removes.
    /// </summary>
    private static readonly ExplorerStateMessage NoMetricsReported =
        ExplorerStateCopy.Empty(ExplorerSubjects.Metrics) with
        {
            Headline = "No metrics for this selection",
            Explanation = "The cluster answered but reported no metrics for this id. Nothing is "
                + "being hidden from you: the table may have been created moments ago, or "
                + "removed since the catalog was listed.",
            Remedy = "Refresh, or check that the table still exists in the catalog.",
            ActionLabel = ExplorerVocabulary.RetryAction,
        };

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
    private EventCallback _reload;

    private TreeMetrics? _metrics;
    private DateTimeOffset _sampledAt;
    private bool _loading;
    private string? _error;
    private ExplorerStateMessage? _failure;

    /// <summary>
    /// The state the surface is in, or <see langword="null"/> when it has a
    /// snapshot to render. Three distinct situations, each named: a read in
    /// flight, a read the cluster refused or could not answer, and a cluster
    /// that answered with nothing.
    /// </summary>
    /// <remarks>
    /// Read on the render path, and twice per pass, so it only ever selects an
    /// already-built message. The failure copy quotes the cluster's own words
    /// and therefore has to be composed; it is composed once, where the failure
    /// is caught.
    /// </remarks>
    private ExplorerStateMessage? State
    {
        get
        {
            if (_failure is not null)
            {
                return _failure;
            }

            if (_metrics is not null)
            {
                return null;
            }

            return _loading
                ? ExplorerStateCopy.Loading(ExplorerSubjects.Metrics)
                : NoMetricsReported;
        }
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        // Bound once rather than per render, so the guidance card's callback does
        // not allocate a delegate on every pass.
        _goToSourceTree = EventCallback.Factory.Create<string>(this, GoToSourceTree);
        _reload = EventCallback.Factory.Create(this, LoadAsync);

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
        _failure = null;
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

            // Composed here, not in the State property: the failure copy quotes
            // the cluster's words and so has to be built, and State is read on
            // every render pass.
            _failure = ExplorerStateCopy.Failed(ExplorerSubjects.Metrics, _error);
        }
        finally
        {
            _loading = false;
            StateHasChanged();
        }
    }
}
