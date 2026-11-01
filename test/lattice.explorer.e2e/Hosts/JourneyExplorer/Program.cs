using System.Net;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Orleans.Lattice.Explorer.EndToEnd.HostContract;
using Orleans.Lattice.Explorer.Web;
using Orleans.Lattice.ReferenceArchitecture.Hosting;

// ---------------------------------------------------------------------------
// The journey Explorer head (issue #5359).
//
// reference-architecture/hosts/Explorer/Program.cs with Entra disabled, built
// from source and run as a real process. Two differences, both deployment
// choices:
//
//   * The built-in Basic sign-in is kept. The reference head replaces it with a
//     dev bypass when Entra is off; the journey estate's silo checks the
//     password, so the Explorer's own sign-in form is the path a persona takes.
//   * The head serves HTTPS itself (the reference head is behind an ingress that
//     terminates TLS), with a certificate from the run-scoped CA.
//
// It connects to its region's gateway, never to a silo: LATTICE_EXPLORER_ENDPOINT
// names the gateway, so a silo can be lost while the head keeps serving.
// ---------------------------------------------------------------------------

var builder = WebApplication.CreateBuilder(args);
var config = builder.Configuration;

builder.Logging.SuppressHealthProbeRequestLogs();
builder.WebHost.UseStaticWebAssets();

var port = config.GetValue<int?>("Explorer:Port")
    ?? throw new InvalidOperationException("Explorer:Port is required.");
var certificate = JourneyHostEnvironment.LoadServerCertificate();

builder.WebHost.ConfigureKestrel(kestrel =>
    kestrel.Listen(IPAddress.Loopback, port, listen =>
    {
        listen.Protocols = HttpProtocols.Http1AndHttp2;
        if (certificate is not null)
        {
            listen.UseHttps(certificate);
        }
    }));

var frontDoorId = config["LATTICE_FRONT_DOOR_ID"];
var configFilePath = config["Explorer:ConfigFilePath"]
    ?? Path.Combine(Path.GetTempPath(), "lattice-explorer", "config.json");
Directory.CreateDirectory(Path.GetDirectoryName(configFilePath)!);

// The key ring, when one is configured to outlive the process: a directory (one head's
// sessions survive its restart) or a blob (every replica of a region shares one ring).
// With neither, the ring is ephemeral and a restart ends every session.
var keysDirectory = config["Explorer:KeysDirectory"];
var keyRingConnection = config["Explorer:KeyRingBlobConnectionString"];

// The hosting variants the estate runs (#5362), each one of the web head's own
// options set through configuration: the mount (east's console is under /console),
// the Data Protection discriminator a scaled-out console's replicas share, the
// editable connection dialog (Fabrikam) and the environment credential seed (the
// Contoso kiosk head).
var replicaName = config["Explorer:ReplicaName"];

builder.Services.AddLatticeExplorerWeb(options =>
{
    options.ConfigFilePath = configFilePath;
    options.BasePath = config["Explorer:BasePath"] ?? "/";
    options.DataProtectionApplicationName = config["Explorer:ApplicationName"];
    options.AllowInteractiveEndpointConfiguration = config.GetValue("Explorer:AllowInteractiveEndpointConfiguration", false);
    options.AllowEnvironmentCredentialSeed = config.GetValue("Explorer:AllowEnvironmentCredentialSeed", false);
    if (config.GetValue<long?>("Explorer:MaxImportBytes") is { } maxImportBytes)
    {
        options.MaxImportBytes = maxImportBytes;
    }
    if (!string.IsNullOrWhiteSpace(keyRingConnection))
    {
        var container = config["Explorer:KeyRingBlobContainer"] ?? "explorer-keys";
        var blob = config["Explorer:KeyRingBlobName"] ?? "keys.xml";
        options.ConfigureDataProtection = dataProtection =>
            dataProtection.PersistKeysToAzureBlobStorage(keyRingConnection, container, blob);
    }
    else if (!string.IsNullOrWhiteSpace(keysDirectory))
    {
        options.ConfigureDataProtection = dataProtection =>
            dataProtection.PersistKeysToFileSystem(new DirectoryInfo(keysDirectory));
    }
    else
    {
        // JourneyExplorerKeyRing.None: a ring that dies with the process. The framework default
        // is not that on a CI runner - it persists keys to the user profile, so a restarted head
        // would still read the cookies it issued and C2.3's lost session would not be lost.
        options.ConfigureDataProtection = dataProtection => dataProtection.UseEphemeralDataProtectionProvider();
    }
});

var app = builder.Build();

// A harness diagnostic, not a product header: which replica of a scaled-out console
// served a response, so a journey can tell the gateway switched it.
if (!string.IsNullOrWhiteSpace(replicaName))
{
    app.Use((context, next) =>
    {
        context.Response.Headers["X-Lattice-E2E-Replica"] = replicaName;
        return next(context);
    });
}

app.UseFrontDoorOriginLock(frontDoorId);
app.UseAntiforgery();
app.MapLatticeExplorer();
app.MapMethods("/health", ["GET", "HEAD"], () => Results.Ok("healthy")).AllowAnonymous();

// A harness diagnostic, mapped only with the journey seams on: what the head holds after a
// forced collection, the reading the import's bounded-memory proof takes (#5475).
if (JourneyHostEnvironment.FaultsEnabled)
{
    app.MapJourneyMemoryProbe();
}

app.Run();
