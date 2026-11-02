using System.Net;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Orleans.Lattice.Explorer.EndToEnd.HostContract;
using Orleans.Lattice.Explorer.EndToEnd.Hosts.LineFeed;
using Orleans.Serialization;

// ---------------------------------------------------------------------------
// The line feed (issue #5359): the factory's upstream ingestion service, a
// process of its own that writes through its region's gateway as the service
// identity line-feed. The harness drives it over a loopback control port:
//
//   POST /control/start               begin writing telemetry
//   POST /control/attach-orders       also write orders
//   POST /control/pause, /resume
//   POST /control/wrong-price-burst?count=N
//   GET  /control/status
//
// Configured by environment variables only: LineFeed__Endpoint (the gateway),
// LineFeed__User, LineFeed__Password, LineFeed__IntervalMilliseconds,
// LineFeed__InvalidEvery, LATTICE_E2E_CA_CERT and LATTICE_E2E_CONTROL_PORT.
// ---------------------------------------------------------------------------

var builder = WebApplication.CreateBuilder(args);
var controlPort = builder.Configuration.GetValue<int?>(JourneyHostEnvironment.ControlPort)
    ?? throw new InvalidOperationException($"{JourneyHostEnvironment.ControlPort} is required.");

builder.WebHost.ConfigureKestrel(kestrel =>
    kestrel.Listen(IPAddress.Loopback, controlPort, listen => listen.Protocols = HttpProtocols.Http1));

builder.Services.AddSerializer();
builder.Services.AddSingleton<LineFeedService>();
builder.Services.AddHostedService(services => services.GetRequiredService<LineFeedService>());

var app = builder.Build();
var control = app.MapGroup("/control");
control.MapPost("/start", (LineFeedService feed) => { feed.Start(); return Results.Ok(feed.Status()); });
control.MapPost("/attach-orders", (LineFeedService feed) => { feed.AttachOrders(); return Results.Ok(feed.Status()); });
control.MapPost("/pause", (LineFeedService feed) => { feed.Pause(); return Results.Ok(feed.Status()); });
control.MapPost("/resume", (LineFeedService feed) => { feed.Resume(); return Results.Ok(feed.Status()); });
control.MapPost("/wrong-price-burst", (int count, LineFeedService feed) =>
{
    feed.InjectWrongPriceBurst(count);
    return Results.Ok(feed.Status());
});
control.MapGet("/status", (LineFeedService feed) => Results.Ok(feed.Status()));
app.MapGet("/health", () => Results.Ok("healthy"));

app.Run();
