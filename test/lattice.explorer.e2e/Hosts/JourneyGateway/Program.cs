using System.Net;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Orleans.Lattice.Explorer.EndToEnd.HostContract;
using Orleans.Lattice.Explorer.EndToEnd.Hosts.Gateway;
using Yarp.ReverseProxy;
using Yarp.ReverseProxy.Configuration;
using Yarp.ReverseProxy.Forwarder;
using Yarp.ReverseProxy.Health;

// ---------------------------------------------------------------------------
// The journey gateway (issue #5359).
//
// A region's stable gRPC endpoint, as an ingress or load balancer is in a real
// deployment: it terminates TLS with a certificate from the run-scoped CA,
// balances every call across the region's silos over TLS it verifies against
// that CA, and probes each silo's /health so a dead silo leaves rotation within
// one probe interval. The Explorer holds one gRPC address, so a silo can be
// lost and the Explorer keep serving only behind an endpoint like this one.
//
// Configured by environment variables only:
//   Gateway__Port                  the HTTPS port the region's clients dial
//   Gateway__Backends              ';'-separated https:// silo gRPC addresses
//   Gateway__HealthIntervalSeconds the active probe interval (default 2)
//   LATTICE_E2E_TLS_PFX(+_PASSWORD) the gateway's server certificate
//   LATTICE_E2E_CA_CERT            the CA the silo certificates chain to
//   LATTICE_E2E_CONTROL_PORT       the loopback port of the harness's control surface
//   Gateway__PlaintextPort         optional h2c loopback listener (Windows workstations only)
//   Gateway__Console__Backends     optional ';'-separated https:// Explorer replicas (#5362)
//   Gateway__Console__Path         the console's path on the gateway (default /console)
// ---------------------------------------------------------------------------

var builder = WebApplication.CreateBuilder(args);
var config = builder.Configuration;

var port = config.GetValue<int?>("Gateway:Port")
    ?? throw new InvalidOperationException("Gateway:Port is required.");
var backends = (config["Gateway:Backends"] ?? string.Empty)
    .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
if (backends.Length == 0)
{
    throw new InvalidOperationException("Gateway:Backends must name at least one silo.");
}

var interval = TimeSpan.FromSeconds(config.GetValue("Gateway:HealthIntervalSeconds", 2.0));
var certificate = JourneyHostEnvironment.LoadServerCertificate()
    ?? throw new InvalidOperationException($"The gateway terminates TLS: set {JourneyHostEnvironment.TlsPfxFile}.");
var authority = JourneyHostEnvironment.LoadCertificateAuthority()
    ?? throw new InvalidOperationException($"The gateway verifies its silos: set {JourneyHostEnvironment.CaCertificateFile}.");
var controlPort = config.GetValue<int?>(JourneyHostEnvironment.ControlPort);

// A plaintext (h2c) loopback listener, offered only where the harness cannot make
// the Explorer trust the run-scoped CA (a Windows workstation, which has no
// SSL_CERT_FILE equivalent short of the user's root store). CI never sets it.
var plaintextPort = config.GetValue<int?>("Gateway:PlaintextPort");
string[] servedHosts = plaintextPort is { } plain ? [$"*:{port}", $"*:{plain}"] : [$"*:{port}"];

builder.WebHost.ConfigureKestrel(kestrel =>
{
    kestrel.Listen(IPAddress.Loopback, port, listen =>
    {
        listen.Protocols = HttpProtocols.Http1AndHttp2;
        listen.UseHttps(certificate);
    });

    if (plaintextPort is { } plaintext)
    {
        kestrel.Listen(IPAddress.Loopback, plaintext, listen => listen.Protocols = HttpProtocols.Http2);
    }

    if (controlPort is { } control)
    {
        kestrel.Listen(IPAddress.Loopback, control, listen => listen.Protocols = HttpProtocols.Http1);
    }
});

var destinations = new Dictionary<string, DestinationConfig>(StringComparer.Ordinal);
for (var i = 0; i < backends.Length; i++)
{
    destinations[$"silo-{i}"] = new DestinationConfig { Address = backends[i] };
}

var cluster = new ClusterConfig
{
    ClusterId = "silos",
    LoadBalancingPolicy = "RoundRobin",
    Destinations = destinations,
    HttpRequest = new ForwarderRequestConfig
    {
        Version = HttpVersion.Version20,
        VersionPolicy = HttpVersionPolicy.RequestVersionExact,
    },
    HealthCheck = new HealthCheckConfig
    {
        Active = new ActiveHealthCheckConfig
        {
            Enabled = true,
            Interval = interval,
            Timeout = interval,
            Policy = HealthCheckConstants.ActivePolicy.ConsecutiveFailures,
            Path = "/health",
        },
        AvailableDestinationsPolicy = HealthCheckConstants.AvailableDestinations.HealthyOrPanic,
    },
    Metadata = new Dictionary<string, string>
    {
        // One failed probe takes a silo out of rotation.
        ["ConsecutiveFailuresHealthPolicy.Threshold"] = "1",
    },
};

var route = new RouteConfig
{
    RouteId = "grpc",
    ClusterId = "silos",
    Match = new RouteMatch { Path = "{**catch-all}", Hosts = servedHosts },
};

var routes = new List<RouteConfig> { route };
var clusters = new List<ClusterConfig> { cluster };

// The region's console (#5362), when its Explorer replicas sit behind the gateway as
// a scaled-out console sits behind its ingress: every request under the console's
// path goes to one replica, held there by an affinity cookie (a Blazor Server
// circuit lives on the replica that opened it), probed on /health like the silos.
// The browser's Host is kept, so a replica builds every address it returns on the
// gateway's, never on its own.
var consoleBackends = (config["Gateway:Console:Backends"] ?? string.Empty)
    .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
if (consoleBackends.Length > 0)
{
    var consolePath = "/" + (config["Gateway:Console:Path"] ?? "console").Trim('/');
    var replicas = new Dictionary<string, DestinationConfig>(StringComparer.Ordinal);
    for (var i = 0; i < consoleBackends.Length; i++)
    {
        replicas[$"explorer-{i}"] = new DestinationConfig { Address = consoleBackends[i] };
    }

    clusters.Add(new ClusterConfig
    {
        ClusterId = GatewayFaults.ConsoleCluster,
        LoadBalancingPolicy = "RoundRobin",
        Destinations = replicas,
        SessionAffinity = new SessionAffinityConfig
        {
            Enabled = true,
            Policy = "Cookie",
            FailurePolicy = "Redistribute",
            AffinityKeyName = ".Lattice.Gateway.Affinity",
            Cookie = new SessionAffinityCookieConfig { Path = consolePath, SecurePolicy = CookieSecurePolicy.Always },
        },
        HealthCheck = new HealthCheckConfig
        {
            Active = new ActiveHealthCheckConfig
            {
                Enabled = true,
                Interval = interval,
                Timeout = interval,
                Policy = HealthCheckConstants.ActivePolicy.ConsecutiveFailures,
                Path = "/health",
            },
            AvailableDestinationsPolicy = HealthCheckConstants.AvailableDestinations.HealthyOrPanic,
        },
        Metadata = new Dictionary<string, string> { ["ConsecutiveFailuresHealthPolicy.Threshold"] = "1" },
    });

    routes.Add(new RouteConfig
    {
        RouteId = "console",
        ClusterId = GatewayFaults.ConsoleCluster,
        Order = -1,
        Match = new RouteMatch { Path = consolePath + "/{**catch-all}", Hosts = servedHosts },
        Transforms = [new Dictionary<string, string> { ["RequestHeaderOriginalHost"] = "true" }],
    });
}

builder.Services.AddSingleton<GatewayFaults>();
builder.Services.AddReverseProxy()
    .LoadFromMemory(routes, clusters)
    .ConfigureHttpClient((_, handler) =>
        handler.SslOptions.RemoteCertificateValidationCallback = JourneyHostEnvironment.TrustOnly(authority));

var app = builder.Build();

if (controlPort is { } controlListener)
{
    app.MapGatewayControl(controlListener);
}

app.MapReverseProxy(pipeline =>
{
    pipeline.Use(async (context, next) =>
    {
        // The harness's dropped-call seam: the next proxied call whose path matches
        // is torn down mid-flight, as a lost connection would be.
        var faults = context.RequestServices.GetRequiredService<GatewayFaults>();
        if (context.Connection.LocalPort != controlPort && faults.TryConsume(context.Request.Path))
        {
            context.Abort();
            return;
        }

        // A drained destination leaves rotation for new requests, as an operator
        // draining a replica takes it out of the load balancer: a request held to it
        // by affinity is redistributed to another.
        var proxy = context.GetReverseProxyFeature();
        if (faults.HasDrained(proxy.Cluster.Config.ClusterId))
        {
            proxy.AvailableDestinations = proxy.AvailableDestinations
                .Where(destination => !faults.IsDrained(proxy.Cluster.Config.ClusterId, destination.DestinationId))
                .ToArray();
        }

        await next();
    });
    pipeline.UseSessionAffinity();
    pipeline.UseLoadBalancing();
});

app.Run();
