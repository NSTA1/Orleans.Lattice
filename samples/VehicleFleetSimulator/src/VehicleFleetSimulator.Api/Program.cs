using System.Collections.Immutable;
using Azure.Data.Tables;
using Microsoft.AspNetCore.Mvc;
using Orleans.Configuration;
using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.Api.Services;
using VehicleFleetSimulator.Api.Streams;

var builder = WebApplication.CreateBuilder(args);

var clusterConnection = builder.Configuration["Persistence:ConnectionString"]
    ?? builder.Configuration["Orleans:ClusterConnectionString"]
    ?? "UseDevelopmentStorage=true";

builder.Host.UseOrleansClient(client =>
{
    client.UseAzureStorageClustering(options =>
    {
        options.TableServiceClient = new TableServiceClient(clusterConnection);
    });
    client.Configure<ClusterOptions>(opts =>
    {
        opts.ClusterId = builder.Configuration["Orleans:ClusterId"] ?? "vfs-dev";
        opts.ServiceId = builder.Configuration["Orleans:ServiceId"] ?? "VehicleFleetSimulator";
    });
});

builder.Services.AddProblemDetails();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new() { Title = "Vehicle Fleet Simulator API", Version = "v1" });
});

// Milestone 6: gRPC stream consumer surface.
builder.Services.AddSingleton<IFleetStreamHub, FleetStreamHub>();
builder.Services.AddHostedService<TelemetryFanOutService>();
builder.Services.AddSingleton<RecordingService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<RecordingService>());
builder.Services.AddSingleton<SimulationEventBroadcaster>();
builder.Services.AddGrpc(options =>
{
    options.Interceptors.Add<ApiKeyInterceptor>();
});
builder.Services.AddGrpcReflection();
builder.Services.AddSingleton<ApiKeyInterceptor>();
builder.Services.AddSingleton<ApiKeyEndpointFilter>();

// Browser clients (Blazor WASM UI) need CORS + gRPC-Web. The exposed headers are required so
// trailers and the dropped-count signal survive the gRPC-Web translation layer.
const string BrowserCorsPolicy = "BrowserClients";
builder.Services.AddCors(options =>
{
    options.AddPolicy(BrowserCorsPolicy, policy =>
    {
        policy
            .SetIsOriginAllowed(_ => true)
            .AllowAnyHeader()
            .AllowAnyMethod()
            .WithExposedHeaders("Grpc-Status", "Grpc-Message", "Grpc-Encoding", "Grpc-Accept-Encoding", "dropped-count");
    });
});

var app = builder.Build();

app.UseStatusCodePages();
app.UseExceptionHandler();
app.UseSwagger();
app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "Vehicle Fleet Simulator API v1"));

app.UseCors(BrowserCorsPolicy);
app.UseGrpcWeb(new GrpcWebOptions { DefaultEnabled = true });

app.MapGrpcService<FleetStreamService>().EnableGrpcWeb().RequireCors(BrowserCorsPolicy);
if (app.Environment.IsDevelopment())
{
    app.MapGrpcReflectionService();
}

app.MapGet("/", () => Results.Redirect("/swagger")).ExcludeFromDescription();

// Milestone 0: round-trip ping through the Orleans cluster.
app.MapGet("/api/ping/{key}", async (string key, IGrainFactory grains, string? message) =>
{
    var grain = grains.GetGrain<IPingGrain>(key);
    var reply = await grain.Ping(message ?? "hello");
    return Results.Ok(new { key, reply });
}).WithTags("Diagnostics");

static IFleetGrain Fleet(IGrainFactory grains) => grains.GetGrain<IFleetGrain>(IFleetGrain.Key);
static ICityGraphGrain CityGraph(IGrainFactory grains) => grains.GetGrain<ICityGraphGrain>(ICityGraphGrain.Key);
static ISimulationConfigGrain SimConfig(IGrainFactory grains) => grains.GetGrain<ISimulationConfigGrain>(ISimulationConfigGrain.Key);

static ProblemDetails BadRequest(string? detail) => new()
{
    Status = StatusCodes.Status400BadRequest,
    Title = "Invalid request",
    Detail = detail,
};

static ProblemDetails Conflict(string? detail) => new()
{
    Status = StatusCodes.Status409Conflict,
    Title = "Conflict",
    Detail = detail,
};

// All /api/* endpoints sit behind an API-key endpoint filter that mirrors the gRPC
// ApiKeyInterceptor (no-op when Auth:ApiKey is unconfigured, e.g. local dev). The /api/ping/{key}
// liveness probe above is intentionally left outside this group so health checks aren't gated on
// credentials.
var api = app.MapGroup("/api").AddEndpointFilter<ApiKeyEndpointFilter>();

// ─── Vehicles ────────────────────────────────────────────────────────────────

var vehicles = api.MapGroup("/vehicles").WithTags("Vehicles");

vehicles.MapPost("/", async (VehicleSpec spec, IGrainFactory grains) =>
{
    if (spec is null)
        return Results.Problem(BadRequest("Body is required."));

    if (spec.Route is { } r && !r.IsDefault && r.Length > 0)
    {
        if (r.Length < 2)
            return Results.Problem(BadRequest("Route must contain at least two cities."));
        var graph = await CityGraph(grains).GetGraph();
        if (!IsValidRoute(graph, r, out var routeError))
            return Results.Problem(BadRequest(routeError));
    }

    if (spec.Config is { } cfg && !TryValidateVehicleConfig(cfg, out var cfgError))
        return Results.Problem(BadRequest(cfgError));

    try
    {
        var id = await Fleet(grains).AddVehicle(spec);
        return Results.Created($"/api/vehicles/{id}", new { vehicleId = id });
    }
    catch (InvalidOperationException ex)
    {
        return Results.Problem(Conflict(ex.Message));
    }
    catch (ArgumentException ex)
    {
        return Results.Problem(BadRequest(ex.Message));
    }
});

vehicles.MapPost("/batch", async (VehicleSpec[] specs, IGrainFactory grains) =>
{
    if (specs is null || specs.Length == 0)
        return Results.Problem(BadRequest("Batch must contain at least one spec."));

    var graph = await CityGraph(grains).GetGraph();
    foreach (var spec in specs)
    {
        if (spec.Route is { } r && !r.IsDefault && r.Length > 0)
        {
            if (r.Length < 2)
                return Results.Problem(BadRequest("Invalid route in batch: must contain at least two cities."));
            if (!IsValidRoute(graph, r, out var routeError))
                return Results.Problem(BadRequest($"Invalid route in batch: {routeError}"));
        }
        if (spec.Config is { } cfg && !TryValidateVehicleConfig(cfg, out var cfgError))
            return Results.Problem(BadRequest($"Invalid config in batch: {cfgError}"));
    }

    try
    {
        var ids = await Fleet(grains).AddVehicleBatch(specs);
        return Results.Ok(new { count = ids.Count, vehicleIds = ids });
    }
    catch (InvalidOperationException ex)
    {
        return Results.Problem(Conflict(ex.Message));
    }
});

vehicles.MapGet("/", async (
    IGrainFactory grains,
    [FromQuery] VehicleStatus? status,
    [FromQuery] string? routeContains,
    [FromQuery] int? skip,
    [FromQuery] int? take) =>
{
    var ids = await Fleet(grains).ListVehicles();
    var snapshots = await Task.WhenAll(
        ids.Select(id => grains.GetGrain<IVehicleGrain>(id).GetSnapshot().AsTask()));

    IEnumerable<VehicleSnapshot> filtered = snapshots.OfType<VehicleSnapshot>();
    if (status is { } s)
        filtered = filtered.Where(v => v.Status == s);
    if (!string.IsNullOrWhiteSpace(routeContains))
        filtered = filtered.Where(v => v.Route.Contains(routeContains, StringComparer.OrdinalIgnoreCase));

    var list = filtered.ToArray();
    var total = list.Length;
    var skipN = Math.Max(0, skip ?? 0);
    var takeN = Math.Clamp(take ?? 100, 1, 1000);
    var page = list.Skip(skipN).Take(takeN).ToArray();

    return Results.Ok(new { total, skip = skipN, take = takeN, count = page.Length, vehicles = page });
});

vehicles.MapGet("/{id:guid}", async (Guid id, IGrainFactory grains) =>
{
    var snapshot = await grains.GetGrain<IVehicleGrain>(id).GetSnapshot();
    return snapshot is null ? Results.NotFound() : Results.Ok(snapshot);
});

vehicles.MapDelete("/{id:guid}", async (Guid id, IGrainFactory grains) =>
{
    var removed = await Fleet(grains).RemoveVehicle(id);
    return removed ? Results.NoContent() : Results.NotFound();
});

vehicles.MapDelete("/", async (IGrainFactory grains) =>
{
    // Snapshot the roster, clear it (cheap grain call), then fan out per-vehicle Stop() calls
    // from the API host with bounded concurrency. Blocking here lets the caller observe true
    // completion without bumping into Orleans' default per-grain-call response timeout.
    var ids = await Fleet(grains).ListVehicles();
    await Fleet(grains).RemoveAllVehicles();

    if (ids.Count == 0)
        return Results.Ok(new { removed = 0, stopped = 0 });

    using var throttle = new SemaphoreSlim(32);
    int stopped = 0;
    var tasks = ids.Select(async id =>
    {
        await throttle.WaitAsync();
        try
        {
            await grains.GetGrain<IVehicleGrain>(id).Clear();
            Interlocked.Increment(ref stopped);
        }
        catch
        {
            // Grain may already be deactivated or unreachable; the roster is already cleared.
        }
        finally
        {
            throttle.Release();
        }
    });
    await Task.WhenAll(tasks);

    return Results.Ok(new { removed = ids.Count, stopped });
});

vehicles.MapPost("/{id:guid}/route", async (Guid id, AssignRouteRequest body, IGrainFactory grains) =>
{
    if (body is null || body.Route.IsDefault || body.Route.Length < 2)
        return Results.Problem(BadRequest("Route must contain at least two cities."));

    var graph = await CityGraph(grains).GetGraph();
    if (!IsValidRoute(graph, body.Route, out var routeError))
        return Results.Problem(BadRequest(routeError));

    var snapshot = await grains.GetGrain<IVehicleGrain>(id).GetSnapshot();
    if (snapshot is null) return Results.NotFound();

    try
    {
        await grains.GetGrain<IVehicleGrain>(id).SetRoute(body.Route);
        return Results.NoContent();
    }
    catch (ArgumentException ex)
    {
        return Results.Problem(BadRequest(ex.Message));
    }
    catch (InvalidOperationException ex)
    {
        return Results.Problem(Conflict(ex.Message));
    }
});

vehicles.MapPost("/{id:guid}/start", async (Guid id, IGrainFactory grains) =>
{
    try
    {
        await grains.GetGrain<IVehicleGrain>(id).Start();
        return Results.NoContent();
    }
    catch (InvalidOperationException ex)
    {
        return Results.Problem(Conflict(ex.Message));
    }
});

vehicles.MapPost("/{id:guid}/stop", async (Guid id, IGrainFactory grains) =>
{
    await grains.GetGrain<IVehicleGrain>(id).Stop();
    return Results.NoContent();
});

// ─── Cities ──────────────────────────────────────────────────────────────────

var cities = api.MapGroup("/cities").WithTags("Cities");

cities.MapGet("/", async (IGrainFactory grains) =>
{
    var graph = await CityGraph(grains).GetGraph();
    return Results.Ok(new { cities = graph.Cities, edges = graph.Edges, positionOverrides = graph.PositionOverrides });
});

cities.MapPost("/{id}/position", async (string id, CityPosition body, IGrainFactory grains, SimulationEventBroadcaster bus) =>
{
    if (body is null) return Results.Problem(BadRequest("Body is required."));
    if (!double.IsFinite(body.X) || !double.IsFinite(body.Y))
        return Results.Problem(BadRequest("X and Y must be finite numbers."));
    var ok = await CityGraph(grains).SetCityPosition(id, body.X, body.Y);
    if (!ok) return Results.NotFound();
    bus.PublishCityMoved(id, body.X, body.Y);
    return Results.NoContent();
});

cities.MapDelete("/positions", async (IGrainFactory grains) =>
{
    await CityGraph(grains).ClearCityPositions();
    return Results.NoContent();
});

// ─── Fleet ───────────────────────────────────────────────────────────────────

var fleet = api.MapGroup("/fleet").WithTags("Fleet");

fleet.MapGet("/stats", async (IGrainFactory grains) =>
{
    var stats = await Fleet(grains).GetFleetStats();
    return Results.Ok(stats);
});

// ─── Bulk fleet operations (operate on every persisted vehicle) ──────────────

vehicles.MapPost("/start-all", async (IGrainFactory grains) =>
{
    var started = await Fleet(grains).StartAllVehicles();
    return Results.Ok(new { started });
});

vehicles.MapPost("/stop-all", async (IGrainFactory grains) =>
{
    var stopped = await Fleet(grains).StopAllVehicles();
    return Results.Ok(new { stopped });
});

vehicles.MapPost("/{id:guid}/fault", async (Guid id, FaultRequest body, IGrainFactory grains) =>
{
    if (body is null) return Results.Problem(BadRequest("Body is required."));
    if (!Enum.IsDefined(body.Fault)) return Results.Problem(BadRequest($"Unknown fault kind '{body.Fault}'."));
    var snapshot = await grains.GetGrain<IVehicleGrain>(id).GetSnapshot();
    if (snapshot is null) return Results.NotFound();
    var applied = await grains.GetGrain<IVehicleGrain>(id).InjectFault(body.Fault);
    return applied ? Results.Ok(new { fault = body.Fault.ToString() }) : Results.Problem(Conflict("Fault could not be applied."));
});

// ─── Simulation control (pause / resume) + config ────────────────────────────

var sim = api.MapGroup("/simulation").WithTags("Simulation");

sim.MapPost("/pause", async (IGrainFactory grains, SimulationEventBroadcaster bus) =>
{
    var cfg = await SimConfig(grains).UpdateConfig(new SimulationConfigPatch(IsPaused: true));
    bus.PublishConfigChanged(cfg);
    return Results.Ok(cfg);
});

sim.MapPost("/resume", async (IGrainFactory grains, SimulationEventBroadcaster bus) =>
{
    var cfg = await SimConfig(grains).UpdateConfig(new SimulationConfigPatch(IsPaused: false));
    bus.PublishConfigChanged(cfg);
    return Results.Ok(cfg);
});

// SSE feed of small "things changed" pings -- the UI uses this to live-refresh the Control
// flyout without polling. Intentionally kept as a single feed (config + city moves) because the
// payload is tiny and clients don't have to fan-out subscribe; if richer typed events are
// needed in the future, switch each line to a typed `event:` field instead of just `data:`.
api.MapGet("/events/stream", async (HttpContext http, SimulationEventBroadcaster bus, CancellationToken ct) =>
{
    http.Response.Headers.ContentType = "text/event-stream";
    http.Response.Headers.CacheControl = "no-cache";
    http.Response.Headers["X-Accel-Buffering"] = "no";
    await bus.WriteToAsync(http.Response, ct);
}).WithTags("Simulation").ExcludeFromDescription();

// ─── Scenario presets ────────────────────────────────────────────────────────

var scenarios = api.MapGroup("/scenarios").WithTags("Scenarios");

scenarios.MapGet("/", () => Results.Ok(new { scenarios = ScenarioCatalog.All }));

scenarios.MapPost("/{name}", async (string name, IGrainFactory grains) =>
{
    if (!ScenarioCatalog.TryGet(name, out var preset))
        return Results.NotFound();

    if (preset.StartCityId is { } start)
    {
        var graph = await CityGraph(grains).GetGraph();
        if (!graph.Cities.Any(c => string.Equals(c.Id, start, StringComparison.OrdinalIgnoreCase)))
            return Results.Problem(BadRequest($"Scenario '{name}' references unknown start city '{start}'."));
    }

    if (preset.ResetFleetFirst)
    {
        var existingIds = await Fleet(grains).ListVehicles();
        await Fleet(grains).RemoveAllVehicles();
        // Best-effort fan-out clear so grain timers stop promptly.
        using var throttle = new SemaphoreSlim(32);
        await Task.WhenAll(existingIds.Select(async id =>
        {
            await throttle.WaitAsync();
            try { await grains.GetGrain<IVehicleGrain>(id).Clear(); }
            catch { /* roster already cleared */ }
            finally { throttle.Release(); }
        }));
    }

    var specs = new VehicleSpec[preset.VehicleCount];
    for (int i = 0; i < specs.Length; i++)
        specs[i] = new VehicleSpec(VehicleId: null, StartCityId: preset.StartCityId);
    var ids = await Fleet(grains).AddVehicleBatch(specs);
    return Results.Ok(new { name = preset.Name, count = ids.Count, vehicleIds = ids });
});

// ─── Diagnostics: per-shard fan-out observer counts ──────────────────────────

api.MapGet("/diagnostics/fanout", async (IGrainFactory grains) =>
{
    var shards = new List<object>(StreamConstants.TelemetryAllShardCount);
    for (var i = 0; i < StreamConstants.TelemetryAllShardCount; i++)
    {
        var grain = grains.GetGrain<IFleetFanOutGrain>(IFleetFanOutGrain.ShardKey(i));
        var diag = await grain.GetDiagnostics();
        shards.Add(new { shard = i, observerCount = diag.ObserverCount, publishedCount = diag.PublishedCount });
    }
    var eventsGrain = grains.GetGrain<IFleetFanOutGrain>(IFleetFanOutGrain.EventsKey());
    var eventsDiag = await eventsGrain.GetDiagnostics();
    return Results.Ok(new
    {
        telemetryShards = shards,
        eventsActivation = new { observerCount = eventsDiag.ObserverCount, publishedCount = eventsDiag.PublishedCount },
    });
}).WithTags("Diagnostics");

// ─── Recording ───────────────────────────────────────────────────────────────

var recording = api.MapGroup("/recording").WithTags("Recording");

recording.MapPost("/start", (RecordingService rec, [FromQuery] int? capacity) =>
{
    var id = rec.Start(capacity ?? 100_000);
    return Results.Ok(new { id, capacity = capacity ?? 100_000 });
});

recording.MapPost("/{id:guid}/stop", (Guid id, RecordingService rec) =>
{
    var summary = rec.Stop(id);
    return summary is null ? Results.NotFound() : Results.Ok(summary);
});

recording.MapGet("/", (RecordingService rec) => Results.Ok(new { recordings = rec.List() }));

recording.MapGet("/{id:guid}", (Guid id, RecordingService rec) =>
{
    var dump = rec.Get(id);
    return dump is null ? Results.NotFound() : Results.Ok(dump);
});

recording.MapPost("/{id:guid}/replay", async (Guid id, RecordingService rec, IGrainFactory grains) =>
{
    var dump = rec.Get(id);
    if (dump is null) return Results.NotFound();
    // Replay is intentionally simple: spawn one fresh vehicle per distinct vehicle id observed in
    // the recording, starting from each vehicle's first telemetry city. The sim drives them with
    // newly-generated routes from there -- this is "re-create the same scene", not "re-emit the
    // same ticks". A true tick-replay would require a non-driving VehicleGrain mode, which the
    // roadmap calls out as a bigger lift; this lighter shape is sufficient for the demo flow.
    var distinct = dump.Telemetry
        .GroupBy(t => t.VehicleId)
        .Select(g => g.OrderBy(t => t.TimestampUtc).First())
        .ToArray();
    var specs = distinct.Select(t => new VehicleSpec(VehicleId: null, StartCityId: t.FromCityId)).ToArray();
    var ids = specs.Length == 0 ? Array.Empty<Guid>() : (await Fleet(grains).AddVehicleBatch(specs)).ToArray();
    return Results.Ok(new { source = id, replayed = ids.Length, vehicleIds = ids });
});

// ─── Simulation config ───────────────────────────────────────────────────────

var simCfg = api.MapGroup("/config/simulation").WithTags("Configuration");

simCfg.MapGet("/", async (IGrainFactory grains) =>
{
    var cfg = await SimConfig(grains).GetConfig();
    return Results.Ok(cfg);
});

simCfg.MapPut("/", async (SimulationConfigPatch patch, IGrainFactory grains, SimulationEventBroadcaster bus) =>
{
    if (patch is null) return Results.Problem(BadRequest("Body is required."));
    if (patch.TickInterval is { } t && t <= TimeSpan.Zero)
        return Results.Problem(BadRequest("TickInterval must be positive."));
    if (patch.DefaultVehicleConfig is { } cfg && !TryValidateVehicleConfig(cfg, out var cfgError))
        return Results.Problem(BadRequest(cfgError));
    if (patch.TimeScale is { } ts && (!double.IsFinite(ts) || ts <= 0 || ts > 10000))
        return Results.Problem(BadRequest("TimeScale must be a positive finite number ≤ 10000."));

    try
    {
        var updated = await SimConfig(grains).UpdateConfig(patch);
        bus.PublishConfigChanged(updated);
        return Results.Ok(updated);
    }
    catch (ArgumentException ex)
    {
        return Results.Problem(BadRequest(ex.Message));
    }
});

app.Run();

static bool IsValidRoute(CityGraphSnapshot graph, ImmutableArray<string> route, out string? error)
{
    var cityIds = graph.Cities.Select(c => c.Id).ToHashSet(StringComparer.Ordinal);
    var edgeSet = new HashSet<(string, string)>();
    foreach (var e in graph.Edges)
    {
        edgeSet.Add((e.FromCityId, e.ToCityId));
        edgeSet.Add((e.ToCityId, e.FromCityId));
    }

    for (int i = 0; i < route.Length; i++)
    {
        if (!cityIds.Contains(route[i]))
        {
            error = $"Unknown city '{route[i]}'.";
            return false;
        }
        if (i > 0)
        {
            if (route[i] == route[i - 1])
            {
                error = $"Consecutive duplicate city '{route[i]}'.";
                return false;
            }
            if (!edgeSet.Contains((route[i - 1], route[i])))
            {
                error = $"No road between '{route[i - 1]}' and '{route[i]}'.";
                return false;
            }
        }
    }
    error = null;
    return true;
}

static bool TryValidateVehicleConfig(VehicleConfig cfg, out string? error)
{
    if (cfg.MinSpeedKph < 0) { error = "MinSpeedKph must be non-negative."; return false; }
    if (cfg.MaxSpeedKph <= cfg.MinSpeedKph) { error = "MaxSpeedKph must be greater than MinSpeedKph."; return false; }
    if (cfg.FuelCapacityLitres <= 0) { error = "FuelCapacityLitres must be positive."; return false; }
    if (cfg.LitresPerKmAtOptimal <= 0) { error = "LitresPerKmAtOptimal must be positive."; return false; }
    if (cfg.RefuelDelay < TimeSpan.Zero) { error = "RefuelDelay must be non-negative."; return false; }
    if (cfg.SpeedSmoothingAlpha is < 0 or > 1) { error = "SpeedSmoothingAlpha must be in [0,1]."; return false; }
    if (cfg.SpeedResampleInterval <= TimeSpan.Zero) { error = "SpeedResampleInterval must be positive."; return false; }
    error = null;
    return true;
}

public sealed record AssignRouteRequest(ImmutableArray<string> Route);

public sealed record FaultRequest(VehicleFault Fault);

