using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// ---------------------------------------------------------------------------
// AtomicWrites - all-or-nothing multi-key writes with SetManyAtomicAsync.
//
// The point of this sample is the *atomicity boundary*: a batch either lands in
// full or leaves the tree exactly as it was. We demonstrate three things:
//   1. A successful single-tree atomic batch (every key becomes visible).
//   2. A guarded atomic batch whose precondition fails - proving that NO key is
//      written when the guard rejects the batch (no partial state is observable).
//   3. The cross-tree IGrainFactory overload, which extends the same guarantee
//      across two independent trees.
// ---------------------------------------------------------------------------

using var host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(logging =>
    {
        // Silence Orleans so the console shows only the feature narration.
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

Console.WriteLine("== AtomicWrites sample ==");
Console.WriteLine();

// --- 1. Successful all-or-nothing batch (byte[] surface) -------------------
// These three keys describe one logical change ("order 42 shipped"). Writing
// them one-by-one would let a reader observe a half-shipped order. The atomic
// batch flips all three into visibility together.
var orders = grainFactory.GetGrain<ILattice>("orders");
var shipment = new List<KeyValuePair<string, byte[]>>
{
    new("order:42/status", Encoding.UTF8.GetBytes("shipped")),
    new("order:42/tracking", Encoding.UTF8.GetBytes("1Z999")),
    new("customer:alice/last-order", Encoding.UTF8.GetBytes("42")),
};

Console.WriteLine("1) Committing a 3-key shipment batch atomically...");
await orders.SetManyAtomicAsync(shipment);
foreach (var kv in shipment)
{
    var stored = await orders.GetAsync(kv.Key);
    Console.WriteLine($"   {kv.Key} = {Encoding.UTF8.GetString(stored!)}");
}
Console.WriteLine("   -> all three keys are visible together.");
Console.WriteLine();

// --- 2. Guarded batch whose precondition fails -> nothing is written --------
// Seed two orders with their current totals. The atomic guard is evaluated once
// against the PRE-saga snapshot of every target key. If any key fails the guard,
// the whole batch is rejected and no key changes - this is the "no partial
// state" property that makes atomic writes safe.
await orders.SetAsync("order:1", new Order("order:1", 120m));
await orders.SetAsync("order:2", new Order("order:2", 80m));
Console.WriteLine("2) Seeded order:1=120, order:2=80.");

var guardedUpdate = new List<KeyValuePair<string, Order>>
{
    new("order:1", new Order("order:1", 999m)),
    new("order:2", new Order("order:2", 5m)),
};

// Guard requires every current total to be >= 100. order:2 is 80, so it fails.
var rejected = await orders.SetManyAtomicAsync<Order>(
    guardedUpdate, current => current.Total >= 100m);

Console.WriteLine($"   Guard 'current.Total >= 100' outcome: {rejected}");
var afterReject1 = await orders.GetAsync<Order>("order:1");
var afterReject2 = await orders.GetAsync<Order>("order:2");
Console.WriteLine($"   order:1 = {afterReject1!.Total}, order:2 = {afterReject2!.Total}");
Console.WriteLine("   -> both keys keep their ORIGINAL totals: no partial write leaked.");
Console.WriteLine();

// The same batch with a guard every key satisfies commits in full.
var accepted = await orders.SetManyAtomicAsync<Order>(
    guardedUpdate, current => current.Total > 0m);
Console.WriteLine($"3) Guard 'current.Total > 0' outcome: {accepted}");
var afterCommit1 = await orders.GetAsync<Order>("order:1");
var afterCommit2 = await orders.GetAsync<Order>("order:2");
Console.WriteLine($"   order:1 = {afterCommit1!.Total}, order:2 = {afterCommit2!.Total}");
Console.WriteLine("   -> both keys now hold the new totals: the batch committed as a unit.");
Console.WriteLine();

// --- 4. Cross-tree atomic write via IGrainFactory ---------------------------
// A single tree's saga cannot span two trees. To flip keys across two distinct
// trees all-or-nothing, use the IGrainFactory overload, which layers one global
// decision over each tree's per-tree saga.
var east = grainFactory.GetGrain<ILattice>("orders-east");
var inventory = grainFactory.GetGrain<ILattice>("inventory");
var batches = new List<LatticeTreeBatch>
{
    new("orders-east", new List<KeyValuePair<string, byte[]>>
    {
        new("order:42/status", Encoding.UTF8.GetBytes("fulfilled")),
    }),
    new("inventory", new List<KeyValuePair<string, byte[]>>
    {
        new("sku:99/reserved", Encoding.UTF8.GetBytes("0")),
    }),
};

Console.WriteLine("4) Committing a batch spanning 'orders-east' and 'inventory'...");
var crossOutcome = await grainFactory.SetManyAtomicAsync(batches, "fulfil:order-42");
Console.WriteLine($"   Cross-tree outcome: {crossOutcome}");
var eastStatus = await east.GetAsync("order:42/status");
var invReserved = await inventory.GetAsync("sku:99/reserved");
Console.WriteLine($"   orders-east/order:42/status = {Encoding.UTF8.GetString(eastStatus!)}");
Console.WriteLine($"   inventory/sku:99/reserved   = {Encoding.UTF8.GetString(invReserved!)}");
Console.WriteLine("   -> keys on both trees flipped together.");
Console.WriteLine();

Console.WriteLine("Done.");
await host.StopAsync();
