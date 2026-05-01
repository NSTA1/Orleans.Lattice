using Azure.Data.Tables;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using OpenTelemetry.Metrics;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Replication;
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

    // ─── Lattice (in-memory grain storage; benchmark scenarios stay ephemeral) ──
    if (telemetrySink == "lattice")
    {
        silo.AddLattice((s, name) => s.AddMemoryGrainStorage(name));

        if (replicationEnabled)
        {
            silo.AddLatticeReplication(opts =>
            {
                opts.ClusterId = builder.Configuration["Replication:OriginClusterId"] ?? clusterId;

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
        }
    }
});

// ─── OpenTelemetry / Prometheus exporter ───────────────────────────────────────
//
// The AspNetCore Prometheus exporter mounts on Kestrel at /metrics. Telemetry:Prometheus:Port
// (default 9090) is bound below via Kestrel; the Dockerfile / docker-compose.yml expose it.
builder.Services
    .AddOpenTelemetry()
    .WithMetrics(b => b
        .AddRuntimeInstrumentation()
        .AddMeter("orleans.lattice")
        .AddMeter("orleans.lattice.replication")
        .AddMeter(LatticeSinkMetrics.MeterName)
        .AddMeter(LatticeReadDriverMetrics.MeterName)
        .AddPrometheusExporter());

builder.Services.AddHealthChecks()
    .AddCheck("azurite-tables", new AzuriteTableHealthCheck(azuriteConnection), tags: ["ready"]);

var prometheusPort = int.Parse(builder.Configuration["Telemetry:Prometheus:Port"] ?? "9090");
builder.WebHost.ConfigureKestrel(opts =>
{
    opts.ListenAnyIP(prometheusPort);
});

var app = builder.Build();
app.UseOpenTelemetryPrometheusScrapingEndpoint();
// /metrics is the AspNetCore exporter's default scrape endpoint; map a /healthz too so docker
// healthchecks can probe Kestrel cheaply.
app.MapHealthChecks("/healthz");

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
