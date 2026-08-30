namespace Orleans.Lattice.Explorer.Plugins.Telemetry.Workspace;

/// <summary>
/// The workspace's catalogue half: discovering what the cluster offers, and
/// selecting one of the entries it offered.
/// </summary>
/// <remarks>
/// <b>A panel is built from the catalogue, not from a list of its own.</b> There
/// is no query id anywhere in this package outside what discovery returned, so a
/// panel cannot describe something the server does not measure, and a title,
/// unit, or bound cannot drift from the instrument behind it.
/// </remarks>
public sealed partial class TelemetryWorkspace
{
    private static readonly TelemetryDurationChoice[] NoChoices = [];
    private static readonly string[] NoTrees = [];

    private bool _initialized;

    /// <summary>
    /// The entries the cluster offers this caller, in the server's own order.
    /// Empty until discovery completes, and legitimately empty afterwards on a
    /// cluster that offers this caller nothing.
    /// </summary>
    public ExplorerTelemetryCatalog Catalog { get; private set; } = ExplorerTelemetryCatalog.Empty;

    /// <summary>
    /// The entry currently selected, or <see langword="null"/> before discovery
    /// completes or when the catalogue is empty.
    /// </summary>
    public ExplorerTelemetryQuery? Selected { get; private set; }

    /// <summary>
    /// The selected entry's instrument names, joined once per selection change,
    /// or <see langword="null"/> when the entry names none.
    /// </summary>
    public string? SelectedInstruments { get; private set; }

    /// <summary>The last outcome to report, or <see langword="null"/> when the last one succeeded.</summary>
    public TelemetryNotice? Notice { get; private set; }

    /// <summary>
    /// The time-range choices legal for the selected entry, recomputed when the
    /// selection or the step changes and never per render.
    /// </summary>
    public IReadOnlyList<TelemetryDurationChoice> RangeChoices { get; private set; } = NoChoices;

    /// <summary>
    /// The step choices legal for the selected entry, recomputed when the
    /// selection changes and never per render.
    /// </summary>
    public IReadOnlyList<TelemetryDurationChoice> StepChoices { get; private set; } = NoChoices;

    /// <summary>
    /// The tree ids the last answer contained, which is the whole of what the
    /// tree filter may narrow to.
    /// </summary>
    public IReadOnlyList<string> TreeChoices { get; private set; } = NoTrees;

    /// <summary>
    /// Reads the catalogue and evaluates the selected entry once. A no-op
    /// beyond the gate read when the gate denies, and idempotent, so a
    /// re-render cannot cause a second discovery.
    /// </summary>
    /// <returns>A task that completes when the first result has been rendered.</returns>
    public async Task InitializeAsync()
    {
        if (_initialized || !Allowed)
        {
            return;
        }

        _initialized = true;
        var discovery = await LoadCatalogAsync(refresh: false).ConfigureAwait(false);
        await EvaluateAsync().ConfigureAwait(false);
        Restore(discovery);
        RaiseChanged();
    }

    /// <summary>
    /// Re-reads the catalogue from the cluster and re-evaluates. This is the
    /// surface's Refresh command: it is what a caller presses after a reconnect
    /// or a sign-in, when what they are offered may have changed.
    /// </summary>
    /// <returns>A task that completes when the refreshed result has been rendered.</returns>
    public async Task RefreshAsync()
    {
        if (!Allowed)
        {
            return;
        }

        var discovery = await LoadCatalogAsync(refresh: true).ConfigureAwait(false);
        await EvaluateAsync().ConfigureAwait(false);
        Restore(discovery);
        RaiseChanged();
    }

    /// <summary>
    /// Re-evaluates the selected entry without re-reading the catalogue. This
    /// is the cheap refresh a caller presses to see the latest data, and the
    /// one a retryable backend fault offers.
    /// </summary>
    /// <returns>A task that completes when the new result has been rendered.</returns>
    public async Task ReevaluateAsync()
    {
        if (!Allowed)
        {
            return;
        }

        await EvaluateAsync().ConfigureAwait(false);
        RaiseChanged();
    }

    /// <summary>
    /// Selects the catalogue entry with id <paramref name="queryId"/> and
    /// evaluates it. An id the catalogue does not offer is ignored, so a value
    /// that was never rendered - including one edited into the DOM - cannot
    /// change the selection.
    /// </summary>
    /// <param name="queryId">The catalogue id to select.</param>
    /// <returns>A task that completes when the new entry's result has been rendered.</returns>
    public async Task SelectQueryAsync(string? queryId)
    {
        if (queryId is null
            || !Catalog.TryGetQuery(queryId, out var query)
            || string.Equals(Selected?.QueryId, queryId, StringComparison.Ordinal))
        {
            return;
        }

        Select(query);
        await EvaluateAsync().ConfigureAwait(false);
        RaiseChanged();
    }

    private async Task<TelemetryNotice?> LoadCatalogAsync(bool refresh)
    {
        Busy = true;
        RaiseChanged();
        try
        {
            var result = refresh
                ? await _domain.Queries.RefreshCatalogAsync().ConfigureAwait(false)
                : await _domain.Queries.GetCatalogAsync().ConfigureAwait(false);

            Notice = TelemetryNotice.For(result);
            if (!result.IsSuccess || result.Value is not { } catalog)
            {
                // A failed discovery leaves the previous catalogue in place
                // rather than blanking the surface: the entries a caller was
                // just looking at are still the entries the cluster offers, and
                // a transient outage should not empty the picker.
                return Notice;
            }

            Catalog = catalog;
            ReconcileSelection();
            return null;
        }
        finally
        {
            Busy = false;
        }
    }

    /// <summary>
    /// Puts a discovery failure back after the evaluation that followed it,
    /// which would otherwise have replaced it with its own (possibly
    /// successful) outcome.
    /// </summary>
    /// <remarks>
    /// A refresh that could not re-read the catalogue but could still evaluate
    /// the entry it already had is charting data from a stale offering. Letting
    /// the evaluation's success clear the discovery banner would hide the one
    /// fact the caller pressed Refresh to learn.
    /// </remarks>
    private void Restore(TelemetryNotice? discovery)
    {
        if (discovery is not null)
        {
            Notice = discovery;
        }
    }

    /// <summary>
    /// Keeps the selection valid across a catalogue change: an entry the
    /// cluster still offers is kept - so a refresh does not throw a caller back
    /// to the first panel - and anything else falls back to the first entry, or
    /// to nothing when the catalogue is empty.
    /// </summary>
    private void ReconcileSelection()
    {
        if (Selected is { } current && Catalog.TryGetQuery(current.QueryId, out var stillOffered))
        {
            // Re-taken from the new catalogue rather than kept: the entry's
            // bounds, unit, or title may have changed even though its id did.
            Select(stillOffered);
            return;
        }

        if (Catalog.Count == 0)
        {
            Selected = null;
            SelectedInstruments = null;
            RangeChoices = NoChoices;
            StepChoices = NoChoices;
            return;
        }

        Select(Catalog.Queries[0]);
    }

    private void Select(ExplorerTelemetryQuery query)
    {
        Selected = query;
        SelectedInstruments = DescribeInstruments(query);
        StepChoices = TelemetryDurationChoices.StepsFor(query.Bounds);

        // A step chosen against another entry's bounds may be illegal here, and
        // sending it would earn a refusal the caller did not ask for. Dropping
        // it back to the server default is the fail-safe direction: the facade
        // then picks a step it is certain to accept.
        if (!TelemetryDurationChoices.IsOffered(StepChoices, Step))
        {
            Step = TimeSpan.Zero;
        }

        RangeChoices = TelemetryDurationChoices.RangesFor(query.Bounds, Step);
        if (!TelemetryDurationChoices.IsOffered(RangeChoices, Range))
        {
            Range = TimeSpan.Zero;
        }
    }

    /// <summary>
    /// The instrument names of <paramref name="query"/>, joined once when the
    /// selection changes rather than on every render. A panel re-renders on
    /// every refresh, and a joined list that never changes between them has no
    /// business being rebuilt each time.
    /// </summary>
    private static string? DescribeInstruments(ExplorerTelemetryQuery query)
    {
        var instruments = query.Instruments;
        if (instruments.Count == 0)
        {
            return null;
        }

        if (instruments.Count == 1)
        {
            return instruments[0].Name;
        }

        var builder = new System.Text.StringBuilder(instruments.Count * 24);
        for (var i = 0; i < instruments.Count; i++)
        {
            if (i > 0)
            {
                builder.Append(", ");
            }

            builder.Append(instruments[i].Name);
        }

        return builder.ToString();
    }
}
