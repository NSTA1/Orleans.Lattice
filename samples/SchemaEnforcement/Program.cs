using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Schema;

// ---------------------------------------------------------------------------
// SchemaEnforcement - server-side validation and per-value versioning.
//
// The core lattice stores every value as an opaque byte[] and never looks
// inside it. The companion Orleans.Lattice.Schema package adds two opt-in,
// composable capabilities:
//
//   1. Enforcement - a per-tree policy validates every write. A non-compliant
//      local write fails fast with LatticeSchemaViolationException and is never
//      persisted.
//   2. Versioning  - each value is stamped with a schema version; stale values
//      are upcast to the tree's current target version on read. Advancing the
//      target version is an admin action; existing values migrate lazily.
//
// This sample installs both on one silo, then exercises each in turn.
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

        // Enforcement first, then versioning (versioning composes the concrete
        // enforcement write interceptor, so the order matters).
        silo.AddLatticeSchemaEnforcement();
        silo.AddLatticeSchemaVersioning(registry =>
        {
            registry.AddSchema(schemaId: 1, version: 1, name: "order");
            registry.AddSchema(schemaId: 1, version: 2, name: "order");

            // v1 -> v2 adds a default "status": "open" member.
            registry.AddUpcaster(
                schemaId: 1,
                fromVersion: 1,
                toVersion: 2,
                transform: LatticeValueTransform.Passthrough(
                    LatticeValueTransform.SetMember(
                        "status", LatticeValueTransform.Const(LatticeConstant.Text("open")))));
        });
    })
    .Build();

await host.StartAsync();
var grainFactory = host.Services.GetRequiredService<IGrainFactory>();
var schemaAdmin = host.Services.GetRequiredService<ILatticeSchemaAdmin>();
var versionAdmin = host.Services.GetRequiredService<ILatticeSchemaVersionAdmin>();

Console.WriteLine("== SchemaEnforcement sample ==");
Console.WriteLine();

// -- Part 1: enforcement ----------------------------------------------------

Console.WriteLine("Part 1: enforcement");
Console.WriteLine();

var orders = grainFactory.GetGrain<ILattice>("orders");

// Require every value written to "orders" to be well-formed JSON.
await schemaAdmin.SetPolicyAsync("orders", new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() }));
Console.WriteLine("Installed a JSON policy on 'orders'.");

// A well-formed JSON value is accepted.
await orders.SetAsync("order:1", Encoding.UTF8.GetBytes("{\"id\":\"order:1\",\"quantity\":3}"));
Console.WriteLine("   accepted a well-formed JSON write.");

// A malformed value is rejected before it is persisted.
try
{
    await orders.SetAsync("order:2", Encoding.UTF8.GetBytes("not json at all"));
    Console.WriteLine("   ERROR: the malformed write was unexpectedly accepted.");
}
catch (LatticeSchemaViolationException ex)
{
    Console.WriteLine($"   rejected a malformed write: {ex.Message}");
}

// The rejected key was never stored.
var order2 = await orders.GetAsync("order:2");
Console.WriteLine($"   'order:2' is {(order2 is null ? "<not found>" : "present")} after the rejected write.");
Console.WriteLine();

// -- Part 2: versioning -----------------------------------------------------

Console.WriteLine("Part 2: versioning");
Console.WriteLine();

var catalog = grainFactory.GetGrain<ILattice>("catalog");

// Opt "catalog" in at schema 1, version 1; new writes are stamped v1.
await versionAdmin.SetVersionConfigAsync("catalog", new LatticeSchemaVersionConfig(schemaId: 1, targetVersion: 1));
await catalog.SetAsync("sku:42", Encoding.UTF8.GetBytes("{\"id\":\"sku:42\",\"quantity\":7}"));
Console.WriteLine("Wrote 'sku:42' as schema 1, version 1.");
Console.WriteLine($"   read at v1 target: {Decode(await catalog.GetAsync("sku:42"))}");

// Advance the target to v2. Existing v1 values upcast on the next read.
await versionAdmin.AdvanceTargetVersionAsync("catalog", newTargetVersion: 2);
Console.WriteLine("Advanced 'catalog' target version to 2.");
Console.WriteLine($"   read at v2 target: {Decode(await catalog.GetAsync("sku:42"))}");
Console.WriteLine("   -> the stored v1 value was upcast on read; 'status' appeared.");
Console.WriteLine();

Console.WriteLine("Done.");
await host.StopAsync();

static string Decode(byte[]? bytes) =>
    bytes is null ? "<not found>" : Encoding.UTF8.GetString(bytes);
