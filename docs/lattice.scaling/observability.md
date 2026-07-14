# Observability

The `Orleans.Lattice.Scaling` package emits its signal as OpenTelemetry metrics
on a dedicated meter and ships a bundled Grafana dashboard, so the same scale
value the HTTP endpoint serves can be scraped by Prometheus and charted
alongside the rest of the cluster's telemetry.

## The `orleans.lattice.scaling` meter

`LatticeScalingMetrics.MeterName` is `orleans.lattice.scaling`. Every instrument
is an observable gauge published from the cached `ScalingSignal` on the silo's
sampling timer, so scraping them is as cheap as scraping the HTTP endpoint - the
gauge callbacks read a published scalar and never recompute the signal.

| Instrument | Unit | Source |
|---|---|---|
| `orleans.lattice.scaling.scale_value` | `{replica}` | `ScalingSignal.ScaleValue` (the value an autoscaler acts on) |
| `orleans.lattice.scaling.raw_scale_value` | `{replica}` | `ScalingSignal.RawScaleValue` (before smoothing and gating) |
| `orleans.lattice.scaling.compute.activation_pressure` | `1` | `ComputePressure.Activation` |
| `orleans.lattice.scaling.compute.resource_pressure` | `1` | `ComputePressure.Resource` |
| `orleans.lattice.scaling.compute.wal_dispatch_pressure` | `1` | `ComputePressure.WalDispatch` |
| `orleans.lattice.scaling.compute.replicas` | `{replica}` | `ScalingSignal.RecommendedReplicas` |
| `orleans.lattice.scaling.storage.accounts_over_threshold` | `{account}` | count of over-threshold `WalAccountPressure` entries |
| `orleans.lattice.scaling.storage.rebalance_recommendations` | `{recommendation}` | `1` when a rebalance is recommended, else `0` |

The instrument-name constants are exposed on `LatticeScalingMetrics`
(`ScaleValueName`, `RawScaleValueName`, `ComputeActivationPressureName`, and so
on) so a `MeterListener` or a test can reference them without hard-coding
strings.

When these names are exported through the OpenTelemetry Prometheus exporter the
dots become underscores, so `orleans.lattice.scaling.scale_value` is scraped as
`orleans_lattice_scaling_scale_value`.

## Reading the meter in-process

```csharp verify
using System.Diagnostics.Metrics;
using Orleans.Lattice.Scaling;

var listener = new MeterListener();
listener.InstrumentPublished = (instrument, l) =>
{
    if (instrument.Meter.Name == LatticeScalingMetrics.MeterName)
    {
        l.EnableMeasurementEvents(instrument);
    }
};
listener.SetMeasurementEventCallback<double>((instrument, value, tags, state) =>
{
    if (instrument.Name == LatticeScalingMetrics.ScaleValueName)
    {
        Console.WriteLine($"scaleValue={value}");
    }
});
listener.Start();
listener.RecordObservableInstruments();
```

## The bundled dashboard

`Orleans.Lattice.Dashboards` bundles an **Autoscaling Signal** Grafana dashboard
(`LatticeDashboardKind.Scaling`) with panels for the smoothed-versus-raw scale
value, compute pressure by dimension, the recommended replica count, WAL accounts
over threshold, and whether a rebalance is recommended. Every instrument on the
meter is charted by a panel; the mapping is recorded in
[`docs/lattice.dashboards/metrics-to-panel-map.md`](../lattice.dashboards/metrics-to-panel-map.md#orleanslatticescaling-meter)
and enforced by a coverage test in `Orleans.Lattice.Scaling.Tests`.

Load it the same way as the other bundled dashboards:

```csharp
using Orleans.Lattice.Dashboards;

string json = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Scaling);
```
