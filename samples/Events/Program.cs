using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// Events sample
// =============
// Orleans.Lattice publishes metadata-only event notifications on a per-tree
// Orleans stream so caches, projections, and dashboards can react to mutations
// without polling. This sample turns publication on, subscribes to a tree's
// event stream, performs a few writes, and prints the LatticeTreeEvent records
// as they arrive.
//
// Events carry only metadata - the key name and operation kind, never the value
// bytes - so a subscriber that needs the new value issues its own GetAsync.
//
// Setup requires: PublishEvents = true, a named stream provider, and a matching
// AddMemoryStreams(...) provider plus its PubSubStore.

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
        silo.ConfigureLattice(o =>
        {
            o.PublishEvents = true;
            o.EventStreamProviderName = "Default";
        });
        // Lattice never registers a stream provider for you - add one explicitly
        // (plus the PubSubStore the stream runtime needs) and name it above.
        silo.AddMemoryStreams("Default");
        silo.AddMemoryGrainStorage("PubSubStore");
    })
    .Build();

Console.Write("Silo starting...");
await host.StartAsync();
Console.WriteLine(" ready.");
Console.WriteLine();

var client = host.Services.GetRequiredService<IClusterClient>();
var tree = client.GetGrain<ILattice>("catalog");

// Buffer received events so we can wait for them deterministically (delivery is
// best-effort and asynchronous).
var received = new ConcurrentQueue<LatticeTreeEvent>();
var handle = await tree.SubscribeToEventsAsync(
    client,
    evt =>
    {
        received.Enqueue(evt);
        return Task.CompletedTask;
    },
    providerName: "Default",
    CancellationToken.None);

Console.WriteLine("Subscribed to stream 'orleans.lattice.events' for tree 'catalog'.");
Console.WriteLine();

// Perform a handful of mutations. Each emits one metadata-only event.
Console.WriteLine("Performing writes:");
await tree.SetAsync("sku-1", Encoding.UTF8.GetBytes("widget"));
Console.WriteLine("  set    sku-1");
await tree.SetAsync("sku-2", Encoding.UTF8.GetBytes("gadget"));
Console.WriteLine("  set    sku-2");
await tree.SetAsync("sku-1", Encoding.UTF8.GetBytes("widget-v2"));
Console.WriteLine("  set    sku-1 (update)");
await tree.DeleteAsync("sku-2");
Console.WriteLine("  delete sku-2");
Console.WriteLine();

// Wait (bounded) for all four events to be delivered.
const int expected = 4;
var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
while (received.Count < expected && DateTime.UtcNow < deadline)
{
    await Task.Delay(50);
}

Console.WriteLine($"Received {received.Count} event(s):");
foreach (var evt in received)
{
    Console.WriteLine($"  {evt.Kind,-8} tree={evt.TreeId} key={evt.Key ?? "<none>"} shard={evt.ShardIndex?.ToString() ?? "-"}");
}

await handle.UnsubscribeAsync();
await host.StopAsync();
