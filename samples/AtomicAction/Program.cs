using System.Globalization;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// ---------------------------------------------------------------------------
// AtomicAction - a generic saga / TCC coordinator that runs an ordered plan
// all-or-nothing, mixing a *Lattice tree write* with a *custom external effect*
// in one transaction.
//
// The point of this sample is that a Lattice-tree mutation can be one step of a
// larger business transaction without giving up the tree's atomicity: the built-in
// .TreeWrite step delegates to the verified atomic-write machinery, and its
// compensation is library-synthesized from captured pre-images. We demonstrate:
//   1. A committing plan: decrement stock in a tree AND reserve credit in an
//      external ledger, together.
//   2. A rolling-back plan: the same shape, but a later step faults - so the tree
//      write is restored to its pre-saga value AND the external reservation is
//      released, leaving no partial effect behind.
// ---------------------------------------------------------------------------

// The "external system" the custom step touches. In a single process here; in
// production it would be a payment gateway or another service.
var ledger = new CreditLedger();

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

        // Register the custom handlers once at silo start. A saga step never carries
        // a delegate - it names one of these handlers by id, and resolution fails
        // closed for any id that was not registered here.
        silo.AddLatticeAtomicAction(handlers => handlers
            // Reserves credit in the external ledger; compensation releases it.
            // Both effects are idempotent keyed on ctx.OperationId.
            .AddHandler(
                "reserve-credit",
                versionTag: "v1",
                forward: ctx =>
                {
                    var (account, amount) = ParseReservation(ctx.Args.Span);
                    ledger.Reserve(ctx.OperationId, account, amount);
                    return Task.CompletedTask;
                },
                compensate: ctx =>
                {
                    ledger.Release(ctx.OperationId);
                    return Task.CompletedTask;
                })
            // A step that always faults, standing in for an external effect that
            // fails (a carrier rejecting a shipment). Its fault is what drives the
            // saga into reverse-order compensation of the earlier steps.
            .AddHandler(
                "flaky-carrier",
                versionTag: "v1",
                forward: _ => throw new InvalidOperationException("carrier rejected the shipment"),
                compensate: _ => Task.CompletedTask));
    })
    .Build();

await host.StartAsync();
var grainFactory = host.Services.GetRequiredService<IGrainFactory>();
var inventory = grainFactory.GetGrain<ILattice>("inventory");

Console.WriteLine("== AtomicAction sample ==");
Console.WriteLine();

// --- 1. A committing plan: tree write + external reservation, together ------
// Seed the on-hand stock so we can see it move, then run a saga that decrements
// it AND reserves credit. Both land, or neither would.
await inventory.SetAsync("sku-42/onhand", Encoding.UTF8.GetBytes("41"));
Console.WriteLine("1) Seeded inventory 'sku-42/onhand' = 41, ledger reservation = 0.");

var commitPlan = new AtomicActionPlanBuilder()
    .TreeWrite("inventory", w => w.Upsert("sku-42/onhand", Encoding.UTF8.GetBytes("40")))
    .Step("reserve-credit", Encoding.UTF8.GetBytes("alice:100"))
    .Build();

var committing = grainFactory.GetGrain<IAtomicActionGrain>("order-1001");
var commitOutcome = await committing.ExecuteAsync(commitPlan);

Console.WriteLine($"   Outcome: {commitOutcome.Status}");
Console.WriteLine($"   inventory 'sku-42/onhand' = {await ReadAsync(inventory, "sku-42/onhand")}");
Console.WriteLine($"   ledger reservation for order-1001 = {ledger.Reserved("order-1001")}");
Console.WriteLine("   -> the tree write and the external reservation committed together.");
Console.WriteLine();

// --- 2. A rolling-back plan: a later step faults -> everything is undone -----
// Same shape, but a third step ('flaky-carrier') faults. The saga compensates in
// strict reverse order: it releases the credit reservation and restores the tree
// key's pre-saga value from the captured pre-image.
await inventory.SetAsync("sku-99/onhand", Encoding.UTF8.GetBytes("5"));
Console.WriteLine("2) Seeded inventory 'sku-99/onhand' = 5, ledger reservation = 0.");

var rollbackPlan = new AtomicActionPlanBuilder()
    .TreeWrite("inventory", w => w.Upsert("sku-99/onhand", Encoding.UTF8.GetBytes("4")))
    .Step("reserve-credit", Encoding.UTF8.GetBytes("bob:50"))
    .Step("flaky-carrier")
    .Build();

var rollingBack = grainFactory.GetGrain<IAtomicActionGrain>("order-2002");
var rollbackOutcome = await rollingBack.ExecuteAsync(rollbackPlan);

Console.WriteLine($"   Outcome: {rollbackOutcome.Status} (faulted at step {rollbackOutcome.FailedStepIndex}: {rollbackOutcome.FailureMessage})");
Console.WriteLine($"   inventory 'sku-99/onhand' = {await ReadAsync(inventory, "sku-99/onhand")}");
Console.WriteLine($"   ledger reservation for order-2002 = {ledger.Reserved("order-2002")}");
Console.WriteLine("   -> the tree write was restored and the reservation released: no partial effect.");
Console.WriteLine();

// --- 3. Idempotent retry: re-issuing a terminal operation id is memoized -----
var replay = await committing.ExecuteAsync(commitPlan);
Console.WriteLine($"3) Re-issuing operation 'order-1001' returns the memoized outcome: {replay.Status}");
Console.WriteLine("   -> a client retry after a timeout observes the original result, not a double-apply.");
Console.WriteLine();

Console.WriteLine("Done.");
await host.StopAsync();

static (string Account, decimal Amount) ParseReservation(ReadOnlySpan<byte> args)
{
    var text = Encoding.UTF8.GetString(args);
    var parts = text.Split(':', 2);
    return (parts[0], decimal.Parse(parts[1], CultureInfo.InvariantCulture));
}

static async Task<string> ReadAsync(ILattice tree, string key)
{
    var bytes = await tree.GetAsync(key);
    return bytes is null ? "(absent)" : Encoding.UTF8.GetString(bytes);
}
