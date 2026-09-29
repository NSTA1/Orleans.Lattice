using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Telemetry;

/// <summary>
/// Completes free text in the address line against the Telemetry area's boards
/// and the charts on them, so <c>latency</c> goes straight to the Latency board.
/// </summary>
internal sealed class TelemetryCompletionSource(TelemetryCatalogCache catalog, ExplorerTenancy tenancy) : IAddressCompletionSource
{
    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<AddressCompletion>> CompleteAsync(AddressQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);

        var text = query.Text.Trim();
        if (query.Mode != AddressQueryMode.Search || text.Length == 0)
        {
            return [];
        }

        var read = await catalog.GetAsync(cancellationToken).ConfigureAwait(false);
        var results = new List<AddressCompletion>();
        foreach (var plan in TelemetryBoards.Plan(read, tenancy.IsActive))
        {
            if (!plan.HasCharts)
            {
                continue;
            }

            var target = ExplorerAddress.ForArea(TelemetryArea.AreaKey, plan.Board.Key);
            var label = TelemetryArea.AreaKey + "/" + plan.Board.Key;
            if (Matches(plan.Board.Title, text) || Matches(plan.Board.Key, text))
            {
                results.Add(new AddressCompletion(label, target, $"{plan.Board.Title} board - {plan.Board.Summary}"));
            }

            foreach (var chart in plan.Charts)
            {
                if (Matches(chart.Title, text) || Matches(chart.QueryId, text))
                {
                    results.Add(new AddressCompletion(label, target, $"{chart.Title}, on the {plan.Board.Title} board"));
                }
            }

            if (results.Count >= query.Limit)
            {
                break;
            }
        }

        return results.Count > query.Limit ? results.GetRange(0, query.Limit) : results;
    }

    private static bool Matches(string candidate, string text) =>
        candidate.Contains(text, StringComparison.OrdinalIgnoreCase);
}
