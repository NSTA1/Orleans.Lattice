using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Telemetry;

/// <summary>
/// The Telemetry area (epic #3807, A9): the cluster's metric catalogue drawn as
/// boards of order-diagram charts at <c>/telemetry</c>, rooted at the active
/// tenant (<c>/t/{tenant}/telemetry</c>) while tenancy is on.
/// </summary>
/// <remarks>
/// Visibility follows the telemetry facade's own gate and fails closed: the area
/// is shown only when the catalogue read succeeds. A refused caller, a cluster
/// that serves no telemetry, and an unreachable one all hide it; an empty
/// catalogue - no backend configured, or nothing this caller may read, which the
/// facade deliberately does not tell apart - shows it with an explanation.
/// </remarks>
internal sealed class TelemetryArea(TelemetryCatalogCache catalog, ExplorerTenancy tenancy) : IExplorerArea
{
    /// <summary>The area's key and address segment.</summary>
    public const string AreaKey = "telemetry";

    /// <summary>The palette command that re-reads the catalogue and every chart.</summary>
    public const string RefreshCommandId = "telemetry.refresh";

    private readonly TelemetryCompletionSource _completions = new(catalog, tenancy);

    /// <inheritdoc />
    public string Key => AreaKey;

    /// <inheritdoc />
    public string DisplayName => "Telemetry";

    /// <inheritdoc />
    public int DirectoryOrder => 80;

    /// <inheritdoc />
    public IAddressCompletionSource? Completions => _completions;

    /// <inheritdoc />
    public IReadOnlyList<ExplorerCommand> Commands =>
    [
        new(RefreshCommandId, "Refresh telemetry")
        {
            Detail = "Re-read the metric catalogue and every chart.",
            Target = ExplorerAddress.ForArea(AreaKey),
            InvokeAsync = _ =>
            {
                catalog.Invalidate();
                return ValueTask.CompletedTask;
            },
        },
    ];

    /// <inheritdoc />
    public async ValueTask<AreaAvailability> GetAvailabilityAsync(CancellationToken cancellationToken)
    {
        try
        {
            await catalog.GetAsync(cancellationToken).ConfigureAwait(false);
            return AreaAvailability.Visible;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return AreaAvailability.Hidden;
        }
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetHomeStatusAsync(CancellationToken cancellationToken)
    {
        var read = await catalog.GetAsync(cancellationToken).ConfigureAwait(false);
        if (read.Count == 0)
        {
            return "No metrics are available to you.";
        }

        var boards = TelemetryBoards.Plan(read, tenancy.IsActive).Where(plan => plan.HasCharts).ToArray();
        var charts = boards.Sum(plan => plan.Charts.Count);
        return $"{charts} {(charts == 1 ? "chart" : "charts")} on {boards.Length} {(boards.Length == 1 ? "board" : "boards")}.";
    }
}
