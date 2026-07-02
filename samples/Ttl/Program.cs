using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// ---------------------------------------------------------------------------
// Ttl - per-entry time-to-live on SetAsync.
//
// A key written with a TimeSpan TTL is visible to every read until its absolute
// expiry instant (resolved server-side as UtcNow + ttl), after which every read
// path treats it as absent. A plain write carries no expiry and never lapses.
// This sample writes one of each, reads them before expiry, waits past the TTL,
// and reads again to show the expiring key disappear while the durable key
// stays.
// ---------------------------------------------------------------------------

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

await host.StartAsync();
var grainFactory = host.Services.GetRequiredService<IGrainFactory>();

Console.WriteLine("== Ttl sample ==");
Console.WriteLine();

var sessions = grainFactory.GetGrain<ILattice>("sessions");
var ttl = TimeSpan.FromSeconds(2);

// A short-lived session token (expires) alongside a durable account record
// (no TTL). Both are written the same way; only the token carries a TimeSpan.
await sessions.SetAsync("session:token", Encoding.UTF8.GetBytes("abc123"), ttl);
await sessions.SetAsync("account:alice", Encoding.UTF8.GetBytes("Alice"));

Console.WriteLine($"Wrote 'session:token' with a {ttl.TotalSeconds:F0}s TTL and 'account:alice' with no TTL.");
Console.WriteLine();

// Before expiry: both keys are visible.
Console.WriteLine("Immediately after write:");
await Print(sessions, "session:token");
await Print(sessions, "account:alice");
Console.WriteLine();

// Wait until the TTL has certainly elapsed.
var wait = ttl + TimeSpan.FromSeconds(1);
Console.WriteLine($"Waiting {wait.TotalSeconds:F0}s for the TTL to elapse...");
await Task.Delay(wait);
Console.WriteLine();

// After expiry: the TTL key reads back as <not found>; the durable key remains.
Console.WriteLine("After the TTL elapsed:");
await Print(sessions, "session:token");
await Print(sessions, "account:alice");
Console.WriteLine();
Console.WriteLine("-> the expiring key vanished from reads; the durable key stayed.");

Console.WriteLine();
Console.WriteLine("Done.");
await host.StopAsync();

static async Task Print(ILattice tree, string key)
{
    var bytes = await tree.GetAsync(key);
    var shown = bytes is null ? "<not found>" : Encoding.UTF8.GetString(bytes);
    Console.WriteLine($"   {key} = {shown}");
}
