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
using Orleans.Lattice.Storage.AzureTable;
using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.Benchmark.Sink;
using VehicleFleetSimulator.Grains;
using VehicleFleetSimulator.Grains.Cities;
using VehicleFleetSimulator.Grains.Telemetry;

// We use WebApplication (Kestrel) so the OpenTelemetry Prometheus AspNetCore exporter can mount
// `/metrics` cleanly on Linux containers - the HttpListener variant fails on Linux for `0.0.0.0`
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
// - the WAL appends, observers fire, but nothing is ever shipped to a peer. That's the
// correct default for libraries (no surprise network egress) but it's wrong for the
// replication-enabled benchmark scenarios (current-state-single-peer, replication-backpressure,
// receiver-crash, bidirectional-replication, replication-key-filter), which are supposed
// to measure ship/apply latency.
//
// What this block wires up:
//   • Sender + receiver: a single `AddLatticeReplicationGrpc` call registers the
//     live-push transport (outbound, client side), the snapshot transport (outbound,
//     client side), the live-push receiver service (inbound, server side), and the
//     snapshot sender service (inbound, server side). The composition is
//     registration-driven: empty `Peers` means "no outbound dial", an empty endpoint
//     map means "no receiver route binding required" (but the silo still binds it
//     for symmetry when `GrpcServerEnabled` is set).
//   • Receiver-side mapping: `MapLatticeReplicationGrpc` on the endpoint builder
//     maps both the live-push route and the snapshot routes so peer pushes land in
//     `IReplicationApplier` and snapshot pulls are served from the local store.
//   • Tree opt-in: `LatticeReplicationOptions.ReplicatedTrees[treeId] = LwwRegister` so
//     the producer-side observer accepts mutations and records WAL entries (without this
//     the WAL is permanently empty on a replicated tree).
//   • Driver opt-in: `LatticeReplicationOptions.ReplicationPeers` lists the peer cluster
//     ids the production driver activation service iterates to spin up one
//     `IReplicationShipperGrain` per (tree, peer). Without this the activation service
//     activates only the maintenance grain and zero shippers, so the WAL grows but
//     `IReplicationTransport.SendAsync` is never called and every ship/apply histogram
//     stays empty.
//
// The two "peers" knobs split by concern, on purpose:
//   • `LatticeReplicationOptions.ReplicationPeers` - transport-agnostic membership list
//     (the cluster ids); consumed by the production drivers in `Orleans.Lattice.Replication`.
//   • `LatticeReplicationGrpcOptions.Peers` - transport-specific cluster-id-to-URL map;
//     consumed by the unified gRPC binding in `Orleans.Lattice.Replication.Grpc` to
//     resolve a peer to a wire endpoint (the unified options projection populates both
//     the live-push and snapshot transports' internal per-transport endpoint maps from
//     this single dictionary). The benchmark binds both from the same env-driven
//     `Replication:GrpcPeers:<id>` configuration map so a single env var (e.g.
//     `BENCH_ORIGIN_PEER_ENDPOINT=http://silo-replica:5001`) wires both sides coherently.
//
// Configuration knobs (read from env via the ASP.NET Core configuration binder):
//   • Replication:GrpcServerEnabled  - register the receiver service and map the gRPC
//                                      Push route on Kestrel. The receiver listens on
//                                      Replication:GrpcPort with HTTP/2.
//   • Replication:GrpcPort           - port the receiver binds (default 5001 inside the
//                                      container; the compose overlay maps host ports if
//                                      external access is needed).
//   • Replication:GrpcPeers:<id>     - peer endpoint map keyed by TargetClusterId. When
//                                      non-empty, replaces the no-op transport with the
//                                      gRPC push transport so the outbound shipper actually
//                                      delivers batches to the peers. The keys also
//                                      populate `LatticeReplicationOptions.ReplicationPeers`
//                                      so the production drivers activate one shipper per
//                                      (tree, peer).
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
// a single sink - registering a second one would silently double-write and contaminate the
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

// ─── Write-driver (optional) ───────────────────────────────────────────────────
//
// When WriteDriver:Enabled=true, registers a hosted service that issues SetAsync calls
// against the configured tree. Used by bidirectional-replication scenarios so the replica
// silo produces its own outbound WAL traffic (the simulator API only writes to the origin
// cluster, so the replica's lattice sink is otherwise idle and the reverse-direction
// ship/apply histograms stay empty). Activates whenever Lattice itself is registered
// (telemetrySink=lattice OR replicationEnabled), gated at runtime by WriteDriver:Enabled.
if (telemetrySink == "lattice" || replicationEnabled)
{
    builder.Services.AddLatticeWriteDriver(builder.Configuration.GetSection("WriteDriver"));
}

// ─── Atomic-saga driver (optional) ─────────────────────────────────────────────
//
// When AtomicSagaDriver:Enabled=true, registers a hosted service that issues
// SetManyAtomicAsync sagas at a configured rate. Used by the atomic-write benchmark
// scenarios (single-cluster and bidirectional-replication variants) so we can measure
// saga throughput, fan-out latency, and (with replication) cross-cluster atomic
// visibility behaviour. Activates whenever Lattice itself is registered, gated at
// runtime by AtomicSagaDriver:Enabled.
if (telemetrySink == "lattice" || replicationEnabled)
{
    builder.Services.AddLatticeAtomicSagaDriver(builder.Configuration.GetSection("AtomicSagaDriver"));
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
    // `telemetrySink == "lattice"` alone, which silently disabled the receiver - the
    // replica accepted incoming gRPC pushes but had no IReplicationApplier registered,
    // so every push deserialised the envelope and then dropped on the floor. Symptom:
    // results.json showed 0 replication metrics on every replication-overlay scenario.
    if (telemetrySink == "lattice" || replicationEnabled)
    {
        silo.AddLattice((s, name) => s.AddMemoryGrainStorage(name));

        // ─── Lattice horizontal-scaling knobs (Phase A attribution sweep) ────
        //
        // The horizontal-scaling diagnostic plan sweeps two LatticeOptions
        // knobs across the local docker-compose scenarios: WalPartitions
        // (per-tree WAL shard count) and WalMaxPendingBatches (per-shard
        // in-flight flush ceiling). Without this ConfigureLattice block
        // the scenarios all run at the library defaults (1, 1) and the
        // sweep has nothing to observe.
        //
        // Both keys are optional and fall through to the LatticeOptions
        // defaults when unset. The env-var spelling matches what
        // benchmark-attribution.ps1 emits when it invokes benchmark.ps1
        // through docker-compose:
        //
        //   Lattice:WalPartitions=4
        //   Lattice:WalMaxPendingBatches=8
        //
        // Parsing is tolerant - a missing or unparseable value leaves
        // the default in place so a malformed override does not silently
        // disable the silo.
        var walPartitionsRaw = builder.Configuration["Lattice:WalPartitions"];
        var walMaxPendingBatchesRaw = builder.Configuration["Lattice:WalMaxPendingBatches"];
        if (!string.IsNullOrWhiteSpace(walPartitionsRaw)
            || !string.IsNullOrWhiteSpace(walMaxPendingBatchesRaw))
        {
            silo.ConfigureLattice(o =>
            {
                if (int.TryParse(walPartitionsRaw, out var p) && p >= 1)
                {
                    o.WalPartitions = p;
                }
                if (int.TryParse(walMaxPendingBatchesRaw, out var b) && b >= 1)
                {
                    o.WalMaxPendingBatches = b;
                }
            });
        }

        // ─── WAL storage provider switch (Lattice:Wal:Provider) ────────────────
        //
        //   "memory"      → default. The library falls through to InMemoryWalStorageProvider;
        //                   no AddWalStorage call is required. WAL state is process-scoped
        //                   and lost on silo restart - fine for non-durability-sensitive
        //                   benchmarks.
        //   "azuretable"  → AddAzureTableWalStorage points the WAL at the same Azurite
        //                   instance the silo already uses for clustering / reminders
        //                   (Persistence:ConnectionString). Every replication-relevant
        //                   commit becomes one Azure Tables transaction, so the durable-WAL
        //                   throughput / tail-latency penalty vs. the in-memory baseline
        //                   is what the *-azuretable scenarios measure.
        //
        // The provider is enabled regardless of replicationEnabled - current-state-no-replication-azuretable
        // exercises the WAL append path on a single silo with no downstream peer (the WAL
        // grain is engaged by every Set, the durability cost is the same).
        var walProvider = (builder.Configuration["Lattice:Wal:Provider"] ?? "memory")
            .Trim()
            .ToLowerInvariant();
        if (walProvider == "azuretable")
        {
            var walTableName = builder.Configuration["Lattice:Wal:TableName"] ?? "OrleansLatticeWal";
            // Lattice:Wal:PipelinePhaseTwo opts the Azure Table WAL provider into
            // pipelined phase-2 mode (F-070): AppendBatchAsync returns once the
            // *previous* batch's phase-2 manifest commit lands, while the current
            // batch's phase-2 continues asynchronously through the per-shard
            // PhaseTwoWorker. Phase 0+1 stay synchronous and durable on every call,
            // so durability and crash-recovery are unchanged - the toggle is a
            // pure throughput-vs-latency knob.
            var walPipelinePhaseTwo = string.Equals(
                builder.Configuration["Lattice:Wal:PipelinePhaseTwo"]?.Trim(),
                "true",
                StringComparison.OrdinalIgnoreCase);
            // Lattice:Wal:EliminateCandidateRowOnHotPath opts the Azure Table WAL
            // provider into D-mode: AppendBatchAsync skips the phase-0 candidate
            // row (C-row) upsert on the shard's manifest partition entirely.
            // Recovery falls back to a cross-partition scan above TAIL at
            // activation time. Used as the candidate cohort for the
            // perf/wal-elide-phase0-candidate-row hypothesis.
            var walEliminateCRow = string.Equals(
                builder.Configuration["Lattice:Wal:EliminateCandidateRowOnHotPath"]?.Trim(),
                "true",
                StringComparison.OrdinalIgnoreCase);
            silo.AddAzureTableWalStorage(o =>
            {
                o.ConnectionString = azuriteConnection;
                o.TableName = walTableName;
                o.PipelinePhaseTwoCommits = walPipelinePhaseTwo;
                o.EliminateCandidateRowOnHotPath = walEliminateCRow;
            });
        }
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
            opts.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
            {
                [treeId] = LatticeMergeMode.LwwRegister,
            };

            // replication-key-filter - per-key prefix filter. When Replication:KeyPrefixes is set the observer
            // evaluates the prefix list inline before recording the WAL append. Empty/missing
            // means "ship everything".
            var prefixes = builder.Configuration["Replication:KeyPrefixes"];
            if (!string.IsNullOrWhiteSpace(prefixes))
            {
                opts.KeyPrefixes = prefixes
                    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            }

            // Production driver membership: the same cluster ids that key the gRPC peer
            // endpoint map populate `ReplicationPeers` so `ReplicationDriverActivationService`
            // activates one `IReplicationShipperGrain` per (tree, peer) on host startup.
            // Empty `grpcPeers` (sender-off scenarios such as observer-no-peer and the
            // receiver side of single-direction overlays) leaves `ReplicationPeers` null,
            // which the activation service treats as "no shippers" - only the per-tree
            // maintenance grain is activated, which is the correct shape for a
            // receiver-only silo.
            if (grpcPeers.Count > 0)
            {
                opts.ReplicationPeers = grpcPeers.Keys.ToArray();
            }

            // Optional override of the per-(tree, peer) shipper grain's phase-timer period.
            // Used by the ship-cadence sweep to find the point where shipping stops being
            // bound by the timer cadence and becomes bound by network/apply throughput.
            // Unset = library default (100 ms).
            var shipPhaseTimerMs = builder.Configuration["Replication:ShipPhaseTimerMs"];
            if (!string.IsNullOrWhiteSpace(shipPhaseTimerMs)
                && int.TryParse(shipPhaseTimerMs, out var shipPhaseTimerMsValue)
                && shipPhaseTimerMsValue > 0)
            {
                opts.ShipPhaseTimerPeriod = TimeSpan.FromMilliseconds(shipPhaseTimerMsValue);
            }
        });

        // Replication gRPC binding (unified). A single AddLatticeReplicationGrpc call
        // wires both the sender (live-push client + snapshot client) and the receiver
        // (push server + snapshot server). The composition is registration-driven:
        //   - Receiver-only silo: grpcPeers is empty, the Peers map stays empty, no
        //     outbound dial is attempted but the server-side services are registered
        //     and ready to accept inbound pushes once MapLatticeReplicationGrpc runs.
        //   - Sender + receiver silo: grpcPeers is populated, the Peers map is
        //     projected onto the internal per-transport options so outbound batches
        //     hit the wire.
        // The call is registered whenever either side is needed - that is, when the
        // benchmark host has been told to expose the receiver, or has been told about
        // at least one peer to dial.
        if (grpcServerEnabled || grpcPeers.Count > 0)
        {
            silo.ConfigureServices(services => services.AddLatticeReplicationGrpc(opts =>
            {
                foreach (var (target, endpoint) in grpcPeers)
                {
                    opts.Peers[target] = endpoint;
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
// far too coarse for the lattice/sink/read-driver latencies we measure here - at calibrated
// fleet sizes, leaf-commit, sink inline-publish, and read-driver durations all sit in the
// `[0, 5)` ms bucket, so every Prometheus `histogram_quantile(p, ...)` query for those
// histograms reports ~4.95 ms regardless of the actual distribution. That defeats the whole
// point of tracking p99 as a regression signal: a 2× shift inside `[0, 5)` is invisible.
//
// The view below applies a single set of finer boundaries to *every* `Histogram<double>` in
// the four meters we own. Sub-ms resolution where the action is, plus a long tail up to 10 s
// to keep the chaos/replication-lag tail visible. The change is purely additive from a
// dashboards perspective - every panel under `src/lattice.dashboards/Grafana/`,
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

// `apply.lag` measures `now - entry.Timestamp.WallClockTicks` on the receiver - i.e. how
// long ago the entry was committed at the origin. Under healthy synchronous replication
// it sits in the sub-second range; under backpressure / receiver-crash / WAL-full chaos
// scenarios it can climb into the tens of seconds and beyond. The shared `latencyMsBuckets`
// set above tops out at 10s, which means every saturated apply lands in the `+Inf` bucket
// and `histogram_quantile(0.99, ...)` pins flat at 10000ms - the dashboard tile cannot
// distinguish "10s lag" from "5min lag", and the chaos signal is invisible.
//
// Apply.lag gets its own boundary set extending the long tail to 5min so saturated states
// remain measurable. The lower portion mirrors the shared set so the typical-case quantile
// resolution doesn't regress.
double[] applyLagMsBuckets = new[]
{
    0.1, 0.25, 0.5, 0.75,
    1.0, 1.5, 2.0, 3.0, 4.0, 5.0, 7.5,
    10.0, 15.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
    25000.0, 60000.0, 120000.0, 300000.0
};

builder.Services
    .AddOpenTelemetry()
    .WithMetrics(b => b
        .AddRuntimeInstrumentation()
        .AddMeter("orleans.lattice")
        .AddMeter("orleans.lattice.replication")
        .AddMeter(LatticeSinkMetrics.MeterName)
        .AddMeter(LatticeReadDriverMetrics.MeterName)
        .AddMeter(LatticeWriteDriverMetrics.MeterName)
        .AddMeter(LatticeAtomicSagaDriverMetrics.MeterName)
        .AddView(instrument =>
        {
            // Apply finer latency buckets to every double-valued histogram emitted by the
            // four meters above. Type guard skips `Histogram<long>` (e.g. flush_batch_size).
            var meterName = instrument.Meter.Name;
            var isOurMeter =
                meterName == "orleans.lattice" ||
                meterName == "orleans.lattice.replication" ||
                meterName == LatticeSinkMetrics.MeterName ||
                meterName == LatticeReadDriverMetrics.MeterName ||
                meterName == LatticeWriteDriverMetrics.MeterName ||
                meterName == LatticeAtomicSagaDriverMetrics.MeterName;
            if (!isOurMeter || instrument is not System.Diagnostics.Metrics.Histogram<double>)
            {
                return null;
            }

            // apply.lag gets the extended-tail boundary set; everything else gets the
            // standard latency boundaries. Match by canonical name to avoid coupling to
            // the exact Histogram<double> instance reference.
            if (instrument.Name == Orleans.Lattice.Replication.LatticeReplicationMetrics.ApplyLagName)
            {
                return new ExplicitBucketHistogramConfiguration { Boundaries = applyLagMsBuckets };
            }
            return new ExplicitBucketHistogramConfiguration { Boundaries = latencyMsBuckets };
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
    // wanted). Only bind when the server is enabled - binding an unused HTTP/2 port
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

// Replication gRPC route - only when the server is enabled. The mapping is idempotent
// against repeat host startups within the same process (only relevant in tests; the
// benchmark binary always starts fresh). MapLatticeReplicationGrpc maps both the
// live-push route and the snapshot routes on the endpoint builder in a single call.
if (grpcServerEnabled)
{
    app.MapLatticeReplicationGrpc();
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
