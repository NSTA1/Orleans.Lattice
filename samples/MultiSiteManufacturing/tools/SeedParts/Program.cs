using System.Globalization;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Lattice;
using Orleans.Lattice;

// ---------------------------------------------------------------------------
// SeedParts - one-off dev tool (NOT part of the sample build, do not commit).
//
// Connects to a running MultiSiteManufacturing cluster as an Orleans client
// and inserts a batch of synthetic parts into a lattice fact tree so the
// Orleans.Lattice.Explorer topology panel has enough fan-out to render a
// multi-layer graph. Every run inserts a fresh, collision-free batch.
//
// It reuses the sample's own LatticeFactBackend + FactJsonCodec, so the keys
// ({serial}/{hlc}/{factId}) and JSON values are identical to what the sample
// writes itself - the cluster's compliance fold and dashboard stay valid.
//
// Reachability: the silo gateways and Azurite are only published on the
// cluster's internal Docker network (msmfg_us-net / msmfg_eu-net), so this
// tool must run inside that network. See seed.ps1 for the docker run wrapper.
// ---------------------------------------------------------------------------

var options = SeedOptions.Parse(args);

Console.WriteLine($"SeedParts -> cluster '{options.Cluster}' (ClusterId {options.ClusterId}), " +
                  $"tree '{options.TreeId}', inserting {options.Count} parts.");

var host = new HostBuilder()
    .UseOrleansClient(client =>
    {
        client.UseAzureStorageClustering(o =>
            o.TableServiceClient = new TableServiceClient(options.TableConnectionString));
        client.Configure<Orleans.Configuration.ClusterOptions>(o =>
        {
            o.ClusterId = options.ClusterId;
            o.ServiceId = options.ServiceId;
        });
    })
    .Build();

await host.StartAsync();
Console.WriteLine("Connected to the cluster.");

try
{
    var clusterClient = host.Services.GetRequiredService<IClusterClient>();
    var tree = clusterClient.GetGrain<ILattice>(options.TreeId);

    // A per-run token keeps every batch's serials distinct from prior runs,
    // so re-running the tool always adds brand-new parts rather than
    // re-touching existing ones.
    var runToken = DateTime.UtcNow.ToString("yyMMddHHmmss", CultureInfo.InvariantCulture);
    var family = new PartFamily($"SEED-{runToken}");
    var year = DateTime.UtcNow.Year;

    // Build keys/values exactly as LatticeFactBackend would, then write them
    // in batches via SetManyAsync (one grain call per batch) instead of one
    // round-trip per key.
    //
    // Back-pressure (see docs/lattice/wal-saturation-signal.md and the
    // LatticeSaturatedException caller-contract): the writer-side WAL
    // admission gate refuses a batch with LatticeSaturatedException when the
    // tree's per-partition WAL is saturated for longer than the wait budget.
    // A well-behaved client MUST obey that signal - back off, retry the same
    // batch, and reduce sustained offered load - otherwise it outruns the
    // storage+replication drain rate and tips the cluster into a saturation
    // storm. We honour it here with bounded exponential backoff per batch,
    // plus a small steady-state pace between batches.
    var done = 0;
    var entries = new List<KeyValuePair<string, byte[]>>(options.BatchSize);
    for (var i = 1; i <= options.Count; i++)
    {
        var serial = PartSerialNumber.From(family, year, i);
        var hlc = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var factId = Guid.NewGuid();
        var fact = new ProcessStepCompleted
        {
            Serial = serial,
            FactId = factId,
            Hlc = hlc,
            Site = ProcessSite.OhioForge,
            Operator = OperatorId.Demo,
            Description = "Forge step completed (seeded)",
            Stage = ProcessStage.Forge,
        };

        var key = string.Create(CultureInfo.InvariantCulture,
            $"{serial.Value}/{hlc.WallClockTicks:D20}/{hlc.Counter:D10}/{factId:N}");
        entries.Add(new KeyValuePair<string, byte[]>(key, FactJsonCodec.Encode(fact)));

        if (entries.Count < options.BatchSize)
        {
            continue;
        }

        await WriteBatchAsync(tree, entries, options);
        done += entries.Count;
        entries.Clear();
        Console.Write($"\r  inserted {done}/{options.Count}...");
    }

    if (entries.Count > 0)
    {
        await WriteBatchAsync(tree, entries, options);
        done += entries.Count;
    }

    Console.WriteLine($"\r  inserted {done}/{options.Count}.   ");
    Console.WriteLine($"Done. Tree '{options.TreeId}' now holds {options.Count} additional parts " +
                      $"under family '{family.Value}'.");
}
finally
{
    await host.StopAsync();
}

// Writes one batch, obeying the lattice WAL back-pressure contract: on
// LatticeSaturatedException the same batch is retried after a bounded,
// jittered exponential backoff (the documented 1-10s recovery window) so the
// client reduces offered load until the tree drains, rather than amplifying
// the saturation. A small inter-batch pace keeps steady-state load below the
// pipeline's sustained drain rate.
static async Task WriteBatchAsync(
    ILattice tree, List<KeyValuePair<string, byte[]>> entries, SeedOptions options)
{
    var attempt = 0;
    while (true)
    {
        try
        {
            await tree.SetManyAsync(entries);
            if (options.PaceMs > 0)
            {
                await Task.Delay(options.PaceMs);
            }
            return;
        }
        catch (LatticeSaturatedException ex)
        {
            attempt++;
            var backoff = Math.Min(options.MaxBackoffMs, options.BaseBackoffMs * (1 << Math.Min(attempt - 1, 10)));
            var jittered = backoff / 2 + Random.Shared.Next(backoff / 2 + 1);
            Console.Write($"\r  back-pressure (tree '{ex.TreeId}'), backing off {jittered} ms (attempt {attempt})...   ");
            await Task.Delay(jittered);
        }
    }
}

internal sealed record SeedOptions(
    string Cluster,
    string ClusterId,
    string ServiceId,
    string TreeId,
    string TableConnectionString,
    int Count,
    int BatchSize,
    int PaceMs,
    int BaseBackoffMs,
    int MaxBackoffMs)
{
    // Azurite well-known dev account. The Table endpoint host differs per
    // cluster (azurite-us / azurite-eu), resolvable only on that cluster's
    // internal Docker network.
    private const string AzuriteAccount =
        "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    public static SeedOptions Parse(string[] args)
    {
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < args.Length - 1; i += 2)
        {
            map[args[i].TrimStart('-')] = args[i + 1];
        }

        var cluster = map.GetValueOrDefault("cluster", "us").ToLowerInvariant();
        var count = int.TryParse(map.GetValueOrDefault("count"), out var c) ? c : 500;
        // "concurrency" kept as an alias for the batch size for back-compat
        // with seed.ps1 / earlier invocations.
        var batchRaw = map.GetValueOrDefault("batch") ?? map.GetValueOrDefault("concurrency");
        var batchSize = int.TryParse(batchRaw, out var b) ? b : 250;
        var paceMs = int.TryParse(map.GetValueOrDefault("pace-ms"), out var p) ? p : 25;
        var baseBackoff = int.TryParse(map.GetValueOrDefault("base-backoff-ms"), out var bb) ? bb : 1000;
        var maxBackoff = int.TryParse(map.GetValueOrDefault("max-backoff-ms"), out var mb) ? mb : 10000;
        var treeId = map.GetValueOrDefault("tree", LatticeFactBackend.FactTreeId);

        // Default Azurite host follows the compose service naming; override
        // with --table-endpoint for a non-default deployment.
        var tableEndpoint = map.GetValueOrDefault(
            "table-endpoint", $"http://azurite-{cluster}:10002/devstoreaccount1");
        var conn = $"DefaultEndpointsProtocol=http;{AzuriteAccount};TableEndpoint={tableEndpoint};";

        return new SeedOptions(
            Cluster: cluster,
            ClusterId: map.GetValueOrDefault("cluster-id", $"msmfg-{cluster}"),
            ServiceId: map.GetValueOrDefault("service-id", "msmfg-service"),
            TreeId: treeId,
            TableConnectionString: conn,
            Count: count,
            BatchSize: Math.Max(1, batchSize),
            PaceMs: Math.Max(0, paceMs),
            BaseBackoffMs: Math.Max(1, baseBackoff),
            MaxBackoffMs: Math.Max(1, maxBackoff));
    }
}
