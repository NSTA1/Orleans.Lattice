using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Replication;

// Runtime per-tree replication configuration sample.
//
// This sample hosts a single-silo Orleans cluster and drives replication
// enablement at RUNTIME through the control facade instead of the static
// LatticeReplicationOptions.ReplicatedTrees options map. The facade writes to the replicated
// sys-replication-config CRDT system tree; every cluster that enrolls that
// tree converges on the same per-tree enabled/mode decision.
//
// The flow is scripted and non-interactive: it enables a tree, reports the
// live config, demonstrates that an in-place mode change is rejected, then
// disables the tree and exits. There is no auth stack registered, so the
// default allow-all access gate authorizes every facade call; a production
// deployment would register Orleans.Lattice.Auth and author through the
// fail-closed API access gate.

const string TreeName = "orders";

using var host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.SetMinimumLevel(LogLevel.None);
    })
    .UseOrleans(silo =>
    {
        silo.UseLocalhostClustering(serviceId: "runtime-replication-config-sample", clusterId: "site-a");
        silo.AddMemoryGrainStorageAsDefault();
        silo.UseInMemoryReminderService();
        silo.AddLattice((services, name) => services.AddMemoryGrainStorage(name));

        // Enable the replication engine for this cluster. No peers are declared:
        // this single-silo sample exercises the local authoring and reporting
        // path, not cross-cluster shipping. ClusterId stamps this cluster's
        // origin and satisfies the runtime preconditions for enablement.
        silo.AddLatticeReplication(opts =>
        {
            opts.ClusterId = "site-a";
        });

        // Statically anchor the sys-replication-config tree so runtime
        // enablement decisions are themselves a replicated CRDT. This is the
        // one static enrolment the runtime-config model requires; every other
        // tree is enabled dynamically through the facade below.
        silo.ReplicateLatticeReplicationConfig();

        // Register the runtime replication control API. This binds
        // ILatticeReplicationControl over the config authority.
        silo.AddLatticeReplicationApi();
    })
    .Build();

Console.Write("Silo starting...");
await host.StartAsync();
Console.WriteLine(" ready.");
Console.WriteLine();

var control = host.Services.GetRequiredService<ILatticeReplicationControl>();

// 1. Enable replication for the "orders" tree under an OrSet merge mode. The
//    mode is fixed at enable-time and cannot be changed in place afterwards.
Console.WriteLine($"Enabling replication for tree '{TreeName}' under {LatticeMergeMode.OrSet}...");
var enabled = await control.EnableReplicationAsync(TreeName, LatticeMergeMode.OrSet);
Console.WriteLine(
    $"  enabled: tree={enabled.TreeId} mode={enabled.Mode} " +
    $"alreadyEnabled={enabled.AlreadyEnabled} bootstrapRequested={enabled.BootstrapRequested}");
Console.WriteLine();

// 2. Report the live per-tree configuration as converged in the config tree.
await PrintConfigAsync(control);

// 3. An in-place mode change is rejected by design. The sanctioned path to
//    change a tree's merge mode is disable-then-re-enable, which re-bootstraps.
Console.WriteLine($"Attempting an in-place mode change to {LatticeMergeMode.LwwRegister} (expected to be rejected)...");
try
{
    await control.EnableReplicationAsync(TreeName, LatticeMergeMode.LwwRegister);
    Console.WriteLine("  unexpected: the mode change was NOT rejected.");
}
catch (LatticeReplicationModeChangeRejectedException ex)
{
    Console.WriteLine($"  rejected as expected: {ex.Message}");
}
Console.WriteLine();

// 4. Disable replication for the tree. This stops shipping without purging any
//    data already replicated to peers.
Console.WriteLine($"Disabling replication for tree '{TreeName}'...");
var disabled = await control.DisableReplicationAsync(TreeName);
Console.WriteLine($"  disabled: tree={disabled.TreeId} alreadyDisabled={disabled.AlreadyDisabled}");
Console.WriteLine();

// 5. Report the config again to show the tree is now disabled.
await PrintConfigAsync(control);

Console.WriteLine("Sample complete. Stopping silo...");
await host.StopAsync();
Console.WriteLine("Done.");

static async Task PrintConfigAsync(ILatticeReplicationControl control)
{
    var report = await control.GetReplicationConfigAsync();
    Console.WriteLine($"Replication config ({report.Trees.Count} tree(s)):");
    foreach (var tree in report.Trees)
    {
        var mode = tree.Mode?.ToString() ?? "(none)";
        Console.WriteLine(
            $"  tree={tree.TreeId} enabled={tree.Enabled} mode={mode} ambiguous={tree.Ambiguous}");
    }

    Console.WriteLine();
}
