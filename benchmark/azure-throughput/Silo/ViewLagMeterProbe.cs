// Passive apply-lag observer for the set-point-mv benchmark cohort.
//
// The materialised-view maintainer already records its apply lag and per-pass
// backlog depth onto the public `orleans.lattice` meter once per drain pass
// (orleans.lattice.view.apply_lag / orleans.lattice.view.backlog_depth). This
// probe tails those instruments with a MeterListener and keeps the most-recent
// sample per view in a volatile field. The set-point-mv reporter reads those
// fields instead of calling ILatticeView.GetLagAsync on its cadence, so
// surfacing the lag adds *zero* grain traffic to the very tree the cohort is
// measuring - the whole point of set-point-mv is that the view is maintained
// without taxing the source tree, and a polling RPC on the reporter cadence
// would itself perturb the numbers (and, on a saturated silo, time out and
// pollute the cohort's exception tally).

using System.Diagnostics.Metrics;
using Orleans.Lattice;

namespace VehicleFleetSimulator.AzureThroughput.Silo;

/// <summary>
/// Read-only, allocation-light observer of a materialised view's apply lag and
/// backlog depth, sourced from the <c>orleans.lattice</c> metrics surface the
/// maintainer already publishes. Adds no grain RPC to the tree under test.
/// </summary>
internal sealed class ViewLagMeterProbe : IDisposable
{
    private const string ApplyLagInstrument = "orleans.lattice.view.apply_lag";
    private const string BacklogDepthInstrument = "orleans.lattice.view.backlog_depth";

    private readonly string _viewName;
    private readonly MeterListener _listener;

    // -1 sentinel = no sample observed yet this run.
    private long _latestApplyLag = -1;
    private long _latestBacklogDepth = -1;

    public ViewLagMeterProbe(string viewName)
    {
        _viewName = viewName;
        _listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (!ReferenceEquals(instrument.Meter, LatticeMetrics.Meter))
                {
                    return;
                }

                if (instrument.Name is ApplyLagInstrument or BacklogDepthInstrument)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };
        _listener.SetMeasurementEventCallback<long>(OnMeasurement);
        _listener.Start();
    }

    /// <summary>
    /// Most recent apply-lag sample (source WAL entries committed but not yet
    /// applied to the view) for the observed view, or <c>-1</c> if the
    /// maintainer has not published a sample yet.
    /// </summary>
    public long LatestApplyLag => Volatile.Read(ref _latestApplyLag);

    /// <summary>
    /// Most recent per-pass backlog-depth sample for the observed view, or
    /// <c>-1</c> if the maintainer has not published a sample yet.
    /// </summary>
    public long LatestBacklogDepth => Volatile.Read(ref _latestBacklogDepth);

    private void OnMeasurement(
        Instrument instrument,
        long measurement,
        ReadOnlySpan<KeyValuePair<string, object?>> tags,
        object? state)
    {
        // The instruments are tagged with the view name; only honour the view
        // this probe was created for so a future multi-view silo cannot cross
        // a second view's samples into this cohort's reading.
        if (!MatchesView(tags))
        {
            return;
        }

        switch (instrument.Name)
        {
            case ApplyLagInstrument:
                Volatile.Write(ref _latestApplyLag, measurement);
                break;
            case BacklogDepthInstrument:
                Volatile.Write(ref _latestBacklogDepth, measurement);
                break;
        }
    }

    private bool MatchesView(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        foreach (var tag in tags)
        {
            if (string.Equals(tag.Key, LatticeMetrics.TagView, StringComparison.Ordinal))
            {
                return string.Equals(tag.Value as string, _viewName, StringComparison.Ordinal);
            }
        }

        return false;
    }

    public void Dispose() => _listener.Dispose();
}
