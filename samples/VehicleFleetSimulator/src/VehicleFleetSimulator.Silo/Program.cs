using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Orleans.Configuration;
using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.Grains;
using VehicleFleetSimulator.Grains.Cities;
using VehicleFleetSimulator.Grains.Telemetry;

var builder = Host.CreateApplicationBuilder(args);

// City graph: bind from "Cities" section and register a singleton provider.
builder.Services.Configure<CityGraphOptions>(builder.Configuration.GetSection("Cities"));
builder.Services.AddSingleton<ICityGraphProvider>(sp =>
{
    var opts = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<CityGraphOptions>>().Value;
    return new StaticCityGraphProvider(opts.BuildGraph());
});

// Determinism mode: ambient TimeProvider so grains and the simulator can be driven by tests.
builder.Services.AddSingleton(TimeProvider.System);

// Telemetry sink seam: VehicleGrain publishes through ITelemetrySink. The default
// FanOutTelemetrySink preserves the original direct-cross-grain dispatch to IFleetFanOutGrain;
// alternative sinks (NullTelemetrySink, future LatticeSink) can be swapped in for benchmark runs.
builder.Services.AddSingleton<ITelemetrySink, FanOutTelemetrySink>();

// Silo-scoped runtime state: shared in-process between SimulationConfigGrain (writer) and every
// VehicleGrain (reader) so slider changes propagate to thousands of grains in the time it takes
// a volatile field to flush — no polling, no streams, no per-tick grain calls.
builder.Services.AddSingleton<SimulationRuntimeState>();

var azuriteConnection = builder.Configuration["Persistence:ConnectionString"] ?? "UseDevelopmentStorage=true";
var clusterId = builder.Configuration["Orleans:ClusterId"] ?? "vfs-dev";
var serviceId = builder.Configuration["Orleans:ServiceId"] ?? "VehicleFleetSimulator";

builder.UseOrleans(silo =>
{
    silo.Configure<ClusterOptions>(opts =>
    {
        opts.ClusterId = clusterId;
        opts.ServiceId = serviceId;
    });

    // M7: Azure Storage clustering, grain storage, reminders, and PubSub store on Azurite.
    silo.UseAzureStorageClustering(options =>
    {
        options.TableServiceClient = new TableServiceClient(azuriteConnection);
    });

    silo.AddMemoryGrainStorageAsDefault();

    silo.UseAzureTableReminderService(options =>
    {
        options.TableServiceClient = new TableServiceClient(azuriteConnection);
    });

    // No streams: VehicleGrain dispatches directly to IFleetFanOutGrain via cross-grain calls.
    // This avoids the memory-stream pulling agent, queue cache, and 1 MB FixedSizeBuffer LOH
    // segments that previously dominated silo working set under load.
});

// M7: Health checks for Azurite Tables (still used for clustering + reminders).
builder.Services.AddHealthChecks()
    .AddCheck("azurite-tables", new AzuriteTableHealthCheck(azuriteConnection), tags: ["ready"]);

await builder.Build().RunAsync();

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
