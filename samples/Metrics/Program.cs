using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// Metrics sample
// ==============
// Orleans.Lattice publishes runtime telemetry through System.Diagnostics.Metrics
// on a single meter named "orleans.lattice" (LatticeMetrics.MeterName). Any
// OpenTelemetry-compatible exporter can subscribe to that meter and receive every
// instrument. This sample attaches a MeterListener directly so it can print the
// instrument measurements the library emits while a few operations run - no
// exporter, collector, or external process required.

// Accumulate every measurement per instrument so we can print a stable summary.
var totals = new ConcurrentDictionary<string, (long Count, double Sum)>();

void Record(Instrument instrument, double value)
{
    totals.AddOrUpdate(
        instrument.Name,
        (1, value),
        (_, prev) => (prev.Count + 1, prev.Sum + value));
}

using var listener = new MeterListener();
listener.InstrumentPublished = (instrument, l) =>
{
    // Subscribe only to the Lattice meter, ignoring Orleans/runtime meters.
    if (instrument.Meter.Name == LatticeMetrics.MeterName)
    {
        l.EnableMeasurementEvents(instrument);
    }
};
listener.SetMeasurementEventCallback<long>((inst, m, _, _) => Record(inst, m));
listener.SetMeasurementEventCallback<int>((inst, m, _, _) => Record(inst, m));
listener.SetMeasurementEventCallback<double>((inst, m, _, _) => Record(inst, m));
listener.Start();

using var host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.SetMinimumLevel(LogLevel.None);
    })
    .UseOrleans(silo =>
    {
        silo.UseLocalhostClustering();
        silo.AddMemoryGrainStorageAsDefault();
        silo.UseInMemoryReminderService();
        silo.AddLattice((s, name) => s.AddMemoryGrainStorage(name));
    })
    .Build();

Console.Write("Silo starting...");
await host.StartAsync();
Console.WriteLine(" ready.");
Console.WriteLine($"Listening on meter '{LatticeMetrics.MeterName}'.");
Console.WriteLine();

var grains = host.Services.GetRequiredService<IGrainFactory>();
var tree = grains.GetGrain<ILattice>("telemetry");

// Drive a mix of writes and reads so several instruments record measurements.
Console.WriteLine("Driving 5 writes, 5 reads, 2 deletes...");
for (var i = 0; i < 5; i++)
{
    await tree.SetAsync($"key-{i}", Encoding.UTF8.GetBytes($"value-{i}"));
}
for (var i = 0; i < 5; i++)
{
    _ = await tree.GetAsync($"key-{i}");
}
await tree.DeleteAsync("key-0");
await tree.DeleteAsync("key-1");
Console.WriteLine();

// Give the async commit/digest paths a moment to record their measurements, then
// flush any observable (gauge) instruments so they contribute a reading too.
await Task.Delay(250);
listener.RecordObservableInstruments();

Console.WriteLine("Recorded Lattice instrument measurements:");
Console.WriteLine($"  {"instrument",-45} {"measurements",12}  {"total",12}");
foreach (var (name, agg) in totals.OrderBy(kvp => kvp.Key))
{
    Console.WriteLine($"  {name,-45} {agg.Count,12}  {agg.Sum,12:F2}");
}
Console.WriteLine();
Console.WriteLine($"{totals.Count} distinct instrument(s) recorded.");
Console.WriteLine("(Counter totals are exact; histogram/gauge 'total' sums vary per run.)");

await host.StopAsync();
