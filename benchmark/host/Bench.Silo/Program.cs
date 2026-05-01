using Azure.Data.Tables;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using OpenTelemetry.Metrics;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.Benchmark.Sink;
using VehicleFleetSimulator.Grains;
using VehicleFleetSimulator.Grains.Cities;
using VehicleFleetSimulator.Grains.Telemetry;

// We use WebApplication (Kestrel) so the OpenTelemetry Prometheus AspNetCore exporter can mount
// `/metrics` cleanly on Linux containers — the HttpListener variant fails on Linux for `0.0.0.0`
// prefix bindings. The HTTP surface only carries the scrape endpoint; Orleans clustering/gateway
// listen on the standard 11111/30000 ports independently.
var builder = WebApplication.CreateBuilder(args);

// ─── Reuse the simulator's grain configuration verbatim ────────────────────────
//
// The benchmark silo embeds the same VehicleGrain/FleetGrain/FanOut produced by the simulator's
// Grains assembly, so the producer pipeline matches the simulator one-to-one. The only added
// wiring is the swap-in telemetry sink and (optionally) the Lattice replication peer.

builder.Services.Configure<CityGraphOptions>(builder.Configuration.GetSection("Cities"));
builder.Services.AddSingleton<ICityGraphProvider>(sp =>
{
    var opts = sp.GetRequiredService<IOptions<CityGraphOptions>>().Value;
    return new StaticCityGraphProvider(opts.BuildGraph());
});

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<SimulationRuntimeState>();

var azuriteConnection = builder.Configuration["Persistence:ConnectionString"] ?? "UseDevelopmentStorage=true";
var clusterId = builder.Configuration["Orleans:ClusterId"] ?? "vfs-bench";
var serviceId = builder.Configuration["Orleans:ServiceId"] ?? "VehicleFleetSimulator";

// ─── Replication gRPC wiring (off unless explicitly enabled) ───────────────────
//
// The library default registered by `AddLatticeReplication` is `NoOpReplicationTransport`
// — the WAL appends, observers fire, but nothing is ever shipped to a peer. That's the
// correct default for libraries (no surprise network egress) but it's wrong for the
// replication-enabled benchmark scenarios (current-state-single-peer, replication-backpressure,
// receiver-crash, bidirectional-replication, replication-key-filter), which are supposed
// to measure ship/apply latency.
//
// What this block wires up:
//   • Sender side: `AddLatticeReplicationGrpcPushTransport` swaps the no-op
//     `IReplicationTransport` for the gRPC push transport when peers are configured.
//   • Receiver side: `AddLatticeReplicationGrpcServer` + `MapLatticeReplicationGrpcService`
//     register the receiver-side gRPC method so peer pushes land in `IReplicationApplier`.
//   • Tree opt-in: `LatticeReplicationOptions.ReplicatedTrees[treeId] = LwwRegister` so
//     the producer-side observer accepts mutations and records WAL entries (without this
//     the WAL is permanently empty on a replicated tree).
//
// Status note (do not delete without verifying first): the canonical outbound pump that
// drives WAL → `IReplicationTransport.SendAsync` is not yet shipped in
// `Orleans.Lattice.Replication`. The library exposes all the seams (`IChangeFeed`,
// `IReplicationTransport`, `ILatticeReplicationCursorRegistry`, `IReplicationBatchEncoder`)
// but no in-process consumer that wires them together — see the docs talking about an
// "outbound shipper" / "canonical shipper" in `docs/lattice.replication/transport.md` and
// the test-only `ChaosDeliveryPump` under `test/lattice.replication/Chaos`. Until the
// library ships that pump:
//   • `orleans_lattice_replication_wal_entries_appended_total` populates (producer side
//     records every committed mutation against a replicated tree).
//   • `orleans_lattice_replication_ship_duration_*` and
//     `orleans_lattice_replication_apply_*` histograms stay empty (no
//     `IReplicationTransport.SendAsync` is ever called from production code).
// The benchmark deliberately does NOT implement its own pump here — that would be a
// duplicate surface destined to be ripped out the moment the library lands the canonical
// loop. The wiring below is intentionally forward-compatible: when the library's pump
// ships, every replication histogram comes online without changing this file.
//
// Configuration knobs (read from env via the ASP.NET Core configuration binder):
//   • Replication:GrpcServerEnabled  — register the receiver service and map the gRPC
//                                      Push route on Kestrel. The receiver listens on
//                                      Replication:GrpcPort with HTTP/2.
//   • Replication:GrpcPort           — port the receiver binds (default 5001 inside the
//                                      container; the compose overlay maps host ports if
//                                      external access is needed).
//   • Replication:GrpcPeers:<id>     — peer endpoint map keyed by TargetClusterId. When
//                                      non-empty, replaces the no-op transport with the
//                                      gRPC push transport so the outbound shipper actually
//                                      delivers batches to the peers.
//
// Both halves are independent: a silo can be a sender-only (peers set, server off), a
// receiver-only (server on, no peers), or both (bidirectional-replication). The benchmark
// scenarios drive the matrix via env vars on the silo / silo-replica services in the
// docker-compose overlay.
var grpcServerEnabled = string.Equals(
    builder.Configuration["Replication:GrpcServerEnabled"],
    "true",
    StringComparison.OrdinalIgnoreCase);
var grpcPort = int.Parse(builder.Configuration["Replication:GrpcPort"] ?? "5001");
var grpcPeers = builder.Configuration.GetSection("Replication:GrpcPeers")
    .GetChildren()
    .Where(c => !string.IsNullOrWhiteSpace(c.Value))
    .ToDictionary(c => c.Key, c => new Uri(c.Value!));

// ─── Telemetry sink switch (Telemetry:Sink) ────────────────────────────────────
//
//   "fanout"  → simulator default; cross-grain dispatch to IFleetFanOutGrain.
//   "null"    → simulator-baseline producer baseline / observer-no-peer observer-off control.
//   "lattice" → current-state-no-replication onward; AddLatticeSink registers the bounded-channel drain loop.
//
// All three branches register exactly one ITelemetrySink so the consumer (VehicleGrain) hits
// a single sink — registering a second one would silently double-write and contaminate the
// measurement, per §2 of benchmark/benchmark-scenarios.md.
var telemetrySink = (builder.Configuration["Telemetry:Sink"] ?? "fanout").Trim().ToLowerInvariant();
var replicationEnabled = string.Equals(builder.Configuration["Replication:Enabled"], "true", StringComparison.OrdinalIgnoreCase);

switch (telemetrySink)
{
    case "null":
        builder.Services.AddSingleton<ITelemetrySink>(_ => NullTelemetrySink.Instance);
        break;
    case "lattice":
        builder.Services.AddLatticeSink(builder.Configuration.GetSection("LatticeSink"));
        break;
    case "fanout":
    default:
        builder.Services.AddSingleton<ITelemetrySink, FanOutTelemetrySink>();
        break;
}

// ─── Read-driver (optional) ────────────────────────────────────────────────────
//
// When ReadDriver:Enabled=true and the lattice sink is active, registers a hosted service
// that issues GetAsync calls against the same tree the sink writes into. Drives the
// read-heavy-* and read-write-mix-* scenarios. No-op when the lattice sink isn't active
// (no tree to read from) or when the master switch is off.
if (telemetrySink == "lattice")
{
    builder.Services.AddLatticeReadDriver(builder.Configuration.GetSection("ReadDriver"));
}

builder.Host.UseOrleans(silo =>
{
    silo.Configure<ClusterOptions>(opts =>
    {
        opts.ClusterId = clusterId;
        opts.ServiceId = serviceId;
    });

    silo.UseAzureStorageClustering(options =>
    {
        options.TableServiceClient = new TableServiceClient(azuriteConnection);
    });

    silo.AddMemoryGrainStorageAsDefault();

    silo.UseAzureTableReminderService(options =>
    {
        options.TableServiceClient = new TableServiceClient(azuriteConnection);
    });

    // ─── Lattice + replication (in-memory grain storage; benchmark scenarios stay ephemeral) ──
    //
    // Important: AddLattice/AddLatticeReplication run when EITHER the silo is producing
    // lattice telemetry (origin) OR is acting as a replication receiver (replica with
    // Telemetry:Sink=null but Replication:Enabled=true). Earlier this gating sat under
    // `telemetrySink == "lattice"` alone, which silently disabled the receiver — the
    // replica accepted incoming gRPC pushes but had no IReplicationApplier registered,
    // so every push deserialised the envelope and then dropped on the floor. Symptom:
    // results.json showed 0 replication metrics on every replication-overlay scenario.
    if (telemetrySink == "lattice" || replicationEnabled)
    {
        silo.AddLattice((s, name) => s.AddMemoryGrainStorage(name));
    }

    if (replicationEnabled)
    {
        silo.AddLatticeReplication(opts =>
        {
            opts.ClusterId = builder.Configuration["Replication:OriginClusterId"] ?? clusterId;

            // Opt the benchmark tree into replication. ReplicatedTrees is null by default
            // ("no trees replicate"); without this the producer-side observer rejects every
            // mutation at commit time and the WAL stays empty, so no `replication_*`
            // metric ever fires. The benchmark sink writes to a single tree (LatticeSink:TreeId,
            // default "vehicle-fleet"); LwwRegister is the right mode for current-state-by-vehicle-id
            // (each key holds the last reported telemetry, last-writer-wins is the natural merge).
            var treeId = builder.Configuration["LatticeSink:TreeId"] ?? "vehicle-fleet";
            opts.ReplicatedTrees = new Dictionary<string, ReplicationMode>
            {
                [treeId] = ReplicationMode.LwwRegister,
            };

            // replication-key-filter — per-key prefix filter. When Replication:KeyPrefixes is set the observer
            // evaluates the prefix list inline before recording the WAL append. Empty/missing
            // means "ship everything".
            var prefixes = builder.Configuration["Replication:KeyPrefixes"];
            if (!string.IsNullOrWhiteSpace(prefixes))
            {
                opts.KeyPrefixes = prefixes
                    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            }
        });

        // Receiver-side: register the gRPC Push handler so a peer that ships to this
        // cluster lands its batches in the IReplicationApplier. Idempotent across
        // multiple AddLatticeReplicationGrpcServer calls (uses TryAdd internally).
        if (grpcServerEnabled)
        {
            silo.ConfigureServices(services => services.AddLatticeReplicationGrpcServer());
        }

        // Sender-side: when peers are configured, swap the no-op transport for the
        // gRPC push transport so outbound batches actually hit the wire. The
        // dictionary is read once per peer on first dispatch (per the package
        // contract) — adding peers at runtime is not supported, but since the
        // benchmark stack is brought up once per scenario this is fine.
        if (grpcPeers.Count > 0)
        {
            silo.ConfigureServices(services => services.AddLatticeReplicationGrpcPushTransport(opts =>
            {
                foreach (var (target, endpoint) in grpcPeers)
                {
                    opts.PeerEndpoints[target] = endpoint;
                }
            }));
        }
    }
});

// ─── OpenTelemetry / Prometheus exporter ───────────────────────────────────────
//
// The AspNetCore Prometheus exporter mounts on Kestrel at /metrics. Telemetry:Prometheus:Port
// (default 9090) is bound below via Kestrel; the Dockerfile / docker-compose.yml expose it.
//
// Histogram buckets: the OpenTelemetry .NET SDK's default boundaries for `Histogram<double>`
// are `[0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000]` ms. That's
// far too coarse for the lattice/sink/read-driver latencies we measure here — at calibrated
// fleet sizes, leaf-commit, sink inline-publish, and read-driver durations all sit in the
// `[0, 5)` ms bucket, so every Prometheus `histogram_quantile(p, ...)` query for those
// histograms reports ~4.95 ms regardless of the actual distribution. That defeats the whole
// point of tracking p99 as a regression signal: a 2× shift inside `[0, 5)` is invisible.
//
// The view below applies a single set of finer boundaries to *every* `Histogram<double>` in
// the four meters we own. Sub-ms resolution where the action is, plus a long tail up to 10 s
// to keep the chaos/replication-lag tail visible. The change is purely additive from a
// dashboards perspective — every panel under `src/lattice.dashboards/Grafana/`,
// `benchmark/grafana/`, and `benchmark/history/grafana/` uses the canonical
// `histogram_quantile(p, sum by (le) (rate(name_bucket[5m])))` pattern with no hardcoded
// `le` literals, so finer boundaries simply produce more accurate quantiles without breaking
// any existing query. `Histogram<long>` instruments (e.g. `flush_batch_size`, which counts
// events per flush, not milliseconds) are excluded by the type guard.
double[] latencyMsBuckets = new[]
{
    0.1, 0.25, 0.5, 0.75,
    1.0, 1.5, 2.0, 3.0, 4.0, 5.0, 7.5,
    10.0, 15.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0
};

builder.Services
    .AddOpenTelemetry()
    .WithMetrics(b => b
        .AddRuntimeInstrumentation()
        .AddMeter("orleans.lattice")
        .AddMeter("orleans.lattice.replication")
        .AddMeter(LatticeSinkMetrics.MeterName)
        .AddMeter(LatticeReadDriverMetrics.MeterName)
        .AddView(instrument =>
        {
            // Apply finer latency buckets to every double-valued histogram emitted by the
            // four meters above. Type guard skips `Histogram<long>` (e.g. flush_batch_size).
            var meterName = instrument.Meter.Name;
            var isOurMeter =
                meterName == "orleans.lattice" ||
                meterName == "orleans.lattice.replication" ||
                meterName == LatticeSinkMetrics.MeterName ||
                meterName == LatticeReadDriverMetrics.MeterName;
            if (isOurMeter && instrument is System.Diagnostics.Metrics.Histogram<double>)
            {
                return new ExplicitBucketHistogramConfiguration { Boundaries = latencyMsBuckets };
            }
            return null;
        })
        .AddPrometheusExporter());

builder.Services.AddHealthChecks()
    .AddCheck("azurite-tables", new AzuriteTableHealthCheck(azuriteConnection), tags: ["ready"]);

var prometheusPort = int.Parse(builder.Configuration["Telemetry:Prometheus:Port"] ?? "9090");
builder.WebHost.ConfigureKestrel(opts =>
{
    // Default HTTP/1.1+HTTP/2 listener for the Prometheus scrape endpoint and /healthz.
    opts.ListenAnyIP(prometheusPort);

    // Dedicated HTTP/2-only listener for the replication gRPC receiver. We bind the
    // gRPC route on a separate port (rather than co-mounting on the prom port) because
    // (a) gRPC requires HTTP/2 prior knowledge for plaintext (h2c) and the OTel
    // Prometheus exporter expects HTTP/1.1 GETs, and (b) keeping them on different
    // ports lets us limit external surface (e.g. expose only 9090 if the receiver isn't
    // wanted). Only bind when the server is enabled — binding an unused HTTP/2 port
    // wastes a socket and complicates the docker port maps.
    if (grpcServerEnabled)
    {
        opts.ListenAnyIP(grpcPort, listenOptions =>
        {
            listenOptions.Protocols = HttpProtocols.Http2;
        });
    }
});

var app = builder.Build();
app.UseOpenTelemetryPrometheusScrapingEndpoint();
// /metrics is the AspNetCore exporter's default scrape endpoint; map a /healthz too so docker
// healthchecks can probe Kestrel cheaply.
app.MapHealthChecks("/healthz");

// Replication gRPC route — only when the server is enabled. The mapping is idempotent
// against repeat host startups within the same process (only relevant in tests; the
// benchmark binary always starts fresh).
if (grpcServerEnabled)
{
    app.MapLatticeReplicationGrpcService();
}

await app.RunAsync();

internal sealed class AzuriteTableHealthCheck(string connectionString) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            var client = new TableServiceClient(connectionString);
            await foreach (var _ in client.QueryAsync(maxPerPage: 1, cancellationToken: cancellationToken))
                break;
            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Azurite Table service unreachable", ex);
        }
    }
}
